package kapacitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	html "html/template"
	"os"
	"sync"
	text "text/template"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	alertservice "github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/bigpanda"
	"github.com/influxdata/kapacitor/services/discord"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/opsgenie2"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/servicenow"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/teams"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/services/zenoss"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/pkg/errors"
)

const (
	statsAlertsTriggered = "alerts_triggered"
	statsAlertsInhibited = "alerts_inhibited"
	statsOKsTriggered    = "oks_triggered"
	statsInfosTriggered  = "infos_triggered"
	statsWarnsTriggered  = "warns_triggered"
	statsCritsTriggered  = "crits_triggered"
	statsEventsDropped   = "events_dropped"
)

// The newest state change is weighted 'weightDiff' times more than oldest state change.
const weightDiff = 1.5

// Maximum weight applied to newest state change.
const maxWeight = 1.2

type AlertNode struct {
	node
	a           *pipeline.AlertNode
	topic       string
	anonTopic   string
	handlers    []alert.Handler
	levels      []stateful.Expression
	scopePools  []stateful.ScopePool
	idTmpl      *text.Template
	messageTmpl *text.Template
	detailsTmpl *html.Template

	alertsTriggered *expvar.Int
	alertsInhibited *expvar.Int
	oksTriggered    *expvar.Int
	infosTriggered  *expvar.Int
	warnsTriggered  *expvar.Int
	critsTriggered  *expvar.Int
	eventsDropped   *expvar.Int

	bufPool sync.Pool

	levelResets  []stateful.Expression
	lrScopePools []stateful.ScopePool
}

// Create a new  AlertNode which caches the most recent item and exposes it over the HTTP API.
func newAlertNode(et *ExecutingTask, n *pipeline.AlertNode, d NodeDiagnostic) (an *AlertNode, err error) {
	const oneMeg = 2 << 19
	ctx := []keyvalue.T{
		keyvalue.KV("task", et.Task.ID),
	}

	an = &AlertNode{
		node: node{Node: n, et: et, diag: d},
		a:    n,
	}
	an.node.runF = an.runAlert

	an.topic = n.Topic
	// Create anonymous topic name
	an.anonTopic = fmt.Sprintf("%s:%s:%s", et.tm.ID(), et.Task.ID, an.Name())

	// Create buffer pool for the templates
	an.bufPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}

	// Parse templates
	an.idTmpl, err = text.New("id").Parse(n.Id)
	if err != nil {
		return nil, err
	}

	an.messageTmpl, err = text.New("message").Parse(n.Message)
	if err != nil {
		return nil, err
	}

	an.detailsTmpl, err = html.New("details").Funcs(html.FuncMap{
		"jsonCompact": func(v interface{}) html.JS {
			tmpBuffer := an.bufPool.Get().(*bytes.Buffer)
			tmpBuffer2 := an.bufPool.Get().(*bytes.Buffer)

			defer func() {
				if tmpBuffer.Cap() < oneMeg { // only reuse the buffer if it is less than 500kb
					tmpBuffer.Reset()
					an.bufPool.Put(tmpBuffer)
				}
				if tmpBuffer2.Cap() < oneMeg { // only reuse the buffer if it is less than 500kb
					tmpBuffer2.Reset()
					an.bufPool.Put(tmpBuffer2)
				}
			}()

			_ = json.NewEncoder(tmpBuffer).Encode(v)
			_ = json.Compact(tmpBuffer2, tmpBuffer.Bytes())
			return html.JS(tmpBuffer2.String())
		},
		"json": func(v interface{}) html.JS {
			tmpBuffer := an.bufPool.Get().(*bytes.Buffer)

			defer func() {
				if tmpBuffer.Cap() < oneMeg { // only reuse the buffer if it is less than 500kb
					tmpBuffer.Reset()
					an.bufPool.Put(tmpBuffer)
				}
			}()

			_ = json.NewEncoder(tmpBuffer).Encode(v)
			return html.JS(tmpBuffer.String())
		},
		"safeHtml": func(v interface{}) html.HTML {
			return html.HTML(v.(string))
		},
	}).Parse(n.Details)
	if err != nil {
		return nil, err
	}

	for _, tcp := range n.TcpHandlers {
		c := alertservice.TCPHandlerConfig{
			Address: tcp.Address,
		}
		h := alertservice.NewTCPHandler(c, an.diag)
		an.handlers = append(an.handlers, h)
	}

	for _, email := range n.EmailHandlers {
		c := smtp.HandlerConfig{
			To:          email.ToList,
			ToTemplates: email.ToTemplatesList,
		}
		h := et.tm.SMTPService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.EmailHandlers) == 0 && (et.tm.SMTPService != nil && et.tm.SMTPService.Global()) {
		c := smtp.HandlerConfig{}
		h := et.tm.SMTPService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	// If email has been configured with state changes only set it.
	if et.tm.SMTPService != nil &&
		et.tm.SMTPService.Global() &&
		et.tm.SMTPService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, e := range n.ExecHandlers {
		c := alertservice.ExecHandlerConfig{
			Prog:      e.Command[0],
			Args:      e.Command[1:],
			Commander: et.tm.Commander,
		}
		h := alertservice.NewExecHandler(c, an.diag)
		an.handlers = append(an.handlers, h)
	}

	for _, log := range n.LogHandlers {
		c := alertservice.DefaultLogHandlerConfig()
		c.Path = log.FilePath
		if log.Mode != 0 {
			c.Mode = os.FileMode(log.Mode)
		}
		h, err := alertservice.NewLogHandler(c, an.diag)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create log alert handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, vo := range n.VictorOpsHandlers {
		c := victorops.HandlerConfig{
			RoutingKey: vo.RoutingKey,
		}
		h := et.tm.VictorOpsService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.VictorOpsHandlers) == 0 && (et.tm.VictorOpsService != nil && et.tm.VictorOpsService.Global()) {
		c := victorops.HandlerConfig{}
		h := et.tm.VictorOpsService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}

	for _, pd := range n.PagerDutyHandlers {
		c := pagerduty.HandlerConfig{
			ServiceKey: pd.ServiceKey,
		}
		h := et.tm.PagerDutyService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.PagerDutyHandlers) == 0 && (et.tm.PagerDutyService != nil && et.tm.PagerDutyService.Global()) {
		c := pagerduty.HandlerConfig{}
		h := et.tm.PagerDutyService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}

	for _, pd := range n.PagerDuty2Handlers {
		links := make([]pagerduty2.LinkTemplate, len(pd.Links))
		for i, l := range pd.Links {
			links[i] = pagerduty2.LinkTemplate{
				Href: l.Href,
				Text: l.Text,
			}
		}
		c := pagerduty2.HandlerConfig{
			RoutingKey: pd.RoutingKey,
			Links:      links,
		}
		h, err := et.tm.PagerDuty2Service.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create PagerDuty2 handler")
		}
		an.handlers = append(an.handlers, h)
	}
	if len(n.PagerDuty2Handlers) == 0 && (et.tm.PagerDuty2Service != nil && et.tm.PagerDuty2Service.Global()) {
		c := pagerduty2.HandlerConfig{}

		h, err := et.tm.PagerDuty2Service.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create PagerDuty2 handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, s := range n.SensuHandlers {
		c := sensu.HandlerConfig{
			Source:   s.Source,
			Handlers: s.HandlersList,
			Metadata: s.MetadataMap,
		}
		h, err := et.tm.SensuService.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create sensu alert handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, s := range n.SlackHandlers {
		c := slack.HandlerConfig{
			Workspace: s.Workspace,
			Channel:   s.Channel,
			Username:  s.Username,
			IconEmoji: s.IconEmoji,
		}
		h := et.tm.SlackService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.SlackHandlers) == 0 && (et.tm.SlackService != nil && et.tm.SlackService.Global()) {
		h := et.tm.SlackService.Handler(slack.HandlerConfig{}, ctx...)
		an.handlers = append(an.handlers, h)
	}
	// If slack has been configured with state changes only set it.
	if et.tm.SlackService != nil &&
		et.tm.SlackService.Global() &&
		et.tm.SlackService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, t := range n.TelegramHandlers {
		c := telegram.HandlerConfig{
			ChatId:                t.ChatId,
			ParseMode:             t.ParseMode,
			DisableWebPagePreview: t.IsDisableWebPagePreview,
			DisableNotification:   t.IsDisableNotification,
		}
		h := et.tm.TelegramService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}

	for _, s := range n.SNMPTrapHandlers {
		dataList := make([]snmptrap.Data, len(s.DataList))
		for i, d := range s.DataList {
			dataList[i] = snmptrap.Data{
				Oid:   d.Oid,
				Type:  d.Type,
				Value: d.Value,
			}
		}
		c := snmptrap.HandlerConfig{
			TrapOid:  s.TrapOid,
			DataList: dataList,
		}
		h, err := et.tm.SNMPTrapService.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create SNMP handler")
		}
		an.handlers = append(an.handlers, h)
	}

	if len(n.TelegramHandlers) == 0 && (et.tm.TelegramService != nil && et.tm.TelegramService.Global()) {
		c := telegram.HandlerConfig{}
		h := et.tm.TelegramService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	// If telegram has been configured with state changes only set it.
	if et.tm.TelegramService != nil &&
		et.tm.TelegramService.Global() &&
		et.tm.TelegramService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, hc := range n.HipChatHandlers {
		c := hipchat.HandlerConfig{
			Room:  hc.Room,
			Token: hc.Token,
		}
		h := et.tm.HipChatService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.HipChatHandlers) == 0 && (et.tm.HipChatService != nil && et.tm.HipChatService.Global()) {
		c := hipchat.HandlerConfig{}
		h := et.tm.HipChatService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	// If HipChat has been configured with state changes only set it.
	if et.tm.HipChatService != nil &&
		et.tm.HipChatService.Global() &&
		et.tm.HipChatService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, k := range n.KafkaHandlers {
		c := kafka.HandlerConfig{
			Cluster:              k.Cluster,
			Topic:                k.KafkaTopic,
			Template:             k.Template,
			DisablePartitionById: k.IsDisablePartitionById,
			PartitionAlgorithm:   k.PartitionHashAlgorithm,
		}
		h, err := et.tm.KafkaService.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create kafka handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, a := range n.AlertaHandlers {
		c := et.tm.AlertaService.DefaultHandlerConfig()
		if a.Token != "" {
			c.Token = a.Token
		}
		if a.Resource != "" {
			c.Resource = a.Resource
		}
		if a.Event != "" {
			c.Event = a.Event
		}
		if a.Environment != "" {
			c.Environment = a.Environment
		}
		if a.Group != "" {
			c.Group = a.Group
		}
		if a.Value != "" {
			c.Value = a.Value
		}
		if a.Origin != "" {
			c.Origin = a.Origin
		}
		if len(a.Service) != 0 {
			c.Service = a.Service
		}
		if len(a.Correlate) != 0 {
			c.Correlate = a.Correlate
		}
		if len(a.Attributes) != 0 {
			c.Attributes = a.Attributes
		}
		if a.Timeout != 0 {
			c.Timeout = a.Timeout
		}
		h, err := et.tm.AlertaService.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create Alerta handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, p := range n.PushoverHandlers {
		c := pushover.HandlerConfig{}
		if p.Device != "" {
			c.Device = p.Device
		}
		if p.Title != "" {
			c.Title = p.Title
		}
		if p.URL != "" {
			c.URL = p.URL
		}
		if p.URLTitle != "" {
			c.URLTitle = p.URLTitle
		}
		if p.Sound != "" {
			c.Sound = p.Sound
		}
		if p.UserKey != "" {
			c.UserKey = p.UserKey
		}
		h := et.tm.PushoverService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}

	for _, p := range n.HTTPPostHandlers {
		if p.Endpoint == "" && p.URL == "" {
			return nil, errors.New("Either URL or endpoint must be non-empty")
		}
		c := httppost.HandlerConfig{
			URL:             p.URL,
			Endpoint:        p.Endpoint,
			Headers:         p.Headers,
			CaptureResponse: p.CaptureResponseFlag,
			Timeout:         p.Timeout,
		}
		h, err := et.tm.HTTPPostService.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create HTTPPostService.Handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, og := range n.OpsGenieHandlers {
		c := opsgenie.HandlerConfig{
			TeamsList:      og.TeamsList,
			RecipientsList: og.RecipientsList,
		}
		h := et.tm.OpsGenieService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.OpsGenieHandlers) == 0 && (et.tm.OpsGenieService != nil && et.tm.OpsGenieService.Global()) {
		c := opsgenie.HandlerConfig{}
		h := et.tm.OpsGenieService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	for _, og := range n.OpsGenie2Handlers {
		c := opsgenie2.HandlerConfig{
			TeamsList:      og.TeamsList,
			RecipientsList: og.RecipientsList,
			RecoveryAction: og.RecoveryActionString,
		}
		h := et.tm.OpsGenie2Service.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.OpsGenie2Handlers) == 0 && (et.tm.OpsGenie2Service != nil && et.tm.OpsGenie2Service.Global()) {
		c := opsgenie2.HandlerConfig{}
		h := et.tm.OpsGenie2Service.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}

	for range n.TalkHandlers {
		h := et.tm.TalkService.Handler(ctx...)
		an.handlers = append(an.handlers, h)
	}

	for _, m := range n.MQTTHandlers {
		c := mqtt.HandlerConfig{
			BrokerName: m.BrokerName,
			Topic:      m.Topic,
			QoS:        mqtt.QoSLevel(m.Qos),
			Retained:   m.Retained,
		}
		h, err := et.tm.MQTTService.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create MQTT handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, s := range n.DiscordHandlers {
		c := discord.HandlerConfig{
			Workspace:  s.Workspace,
			Username:   s.Username,
			AvatarURL:  s.AvatarURL,
			EmbedTitle: s.EmbedTitle,
		}
		h, err := et.tm.DiscordService.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create Discord handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, s := range n.BigPandaHandlers {
		c := bigpanda.HandlerConfig{
			AppKey:            s.AppKey,
			Host:              s.Host,
			PrimaryProperty:   s.PrimaryProperty,
			SecondaryProperty: s.SecondaryProperty,
			Attributes:        s.Attributes,
		}
		h, err := et.tm.BigPandaService.Handler(c, ctx...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create BigPanda handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, t := range n.TeamsHandlers {
		c := teams.HandlerConfig{
			ChannelURL: t.ChannelURL,
		}
		h := et.tm.TeamsService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.TeamsHandlers) == 0 && (et.tm.TeamsService != nil && et.tm.TeamsService.Global()) {
		c := teams.HandlerConfig{}
		h := et.tm.TeamsService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	// If Teams has been configured with state changes only set it.
	if et.tm.TeamsService != nil &&
		et.tm.TeamsService.Global() &&
		et.tm.TeamsService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	if len(n.DiscordHandlers) == 0 && (et.tm.DiscordService != nil && et.tm.DiscordService.Global()) {
		h, err := et.tm.DiscordService.Handler(discord.HandlerConfig{}, ctx...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create Discord handler")
		}
		an.handlers = append(an.handlers, h)
	}
	// If discord has been configured with state changes only set it.
	if et.tm.DiscordService != nil &&
		et.tm.DiscordService.Global() &&
		et.tm.DiscordService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, s := range n.ServiceNowHandlers {
		c := servicenow.HandlerConfig{
			URL:            s.URL,
			Source:         s.Source,
			Node:           s.Node,
			Type:           s.Type,
			Resource:       s.Resource,
			MetricName:     s.MetricName,
			MessageKey:     s.MessageKey,
			AdditionalInfo: s.AdditionalInfoMap,
		}
		h := et.tm.ServiceNowService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.ServiceNowHandlers) == 0 && (et.tm.ServiceNowService != nil && et.tm.ServiceNowService.Global()) {
		h := et.tm.ServiceNowService.Handler(servicenow.HandlerConfig{}, ctx...)
		an.handlers = append(an.handlers, h)
	}
	// If servicenow has been configured with state changes only set it.
	if et.tm.ServiceNowService != nil &&
		et.tm.ServiceNowService.Global() &&
		et.tm.ServiceNowService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	if len(n.BigPandaHandlers) == 0 && (et.tm.BigPandaService != nil && et.tm.BigPandaService.Global()) {
		h, err := et.tm.BigPandaService.Handler(bigpanda.HandlerConfig{}, ctx...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create BigPanda handler")
		}
		an.handlers = append(an.handlers, h)
	}
	// If BigPanda has been configured with state changes only set it.
	if et.tm.BigPandaService != nil &&
		et.tm.BigPandaService.Global() &&
		et.tm.BigPandaService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, s := range n.ZenossHandlers {
		c := zenoss.HandlerConfig{
			Action:        s.Action,
			Method:        s.Method,
			Type:          s.Type,
			TID:           s.Tid,
			Summary:       s.Summary,
			Device:        s.Device,
			Component:     s.Component,
			EventClassKey: s.EventClassKey,
			EventClass:    s.EventClass,
			Message:       s.Message,
			Collector:     s.Collector,
			CustomFields:  s.CustomFieldsMap,
		}
		h := et.tm.ZenossService.Handler(c, ctx...)
		an.handlers = append(an.handlers, h)
	}
	if len(n.ZenossHandlers) == 0 && (et.tm.ZenossService != nil && et.tm.ZenossService.Global()) {
		h := et.tm.ZenossService.Handler(zenoss.HandlerConfig{}, ctx...)
		an.handlers = append(an.handlers, h)
	}
	// If zenoss has been configured with state changes only set it.
	if et.tm.ZenossService != nil &&
		et.tm.ZenossService.Global() &&
		et.tm.ZenossService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	// Parse level expressions
	an.levels = make([]stateful.Expression, alert.Critical+1)
	an.scopePools = make([]stateful.ScopePool, alert.Critical+1)

	an.levelResets = make([]stateful.Expression, alert.Critical+1)
	an.lrScopePools = make([]stateful.ScopePool, alert.Critical+1)

	if n.Info != nil {
		statefulExpression, expressionCompileError := stateful.NewExpression(n.Info.Expression)
		if expressionCompileError != nil {
			return nil, fmt.Errorf("Failed to compile stateful expression for info: %s", expressionCompileError)
		}

		an.levels[alert.Info] = statefulExpression
		an.scopePools[alert.Info] = stateful.NewScopePool(ast.FindReferenceVariables(n.Info.Expression))
		if n.InfoReset != nil {
			lstatefulExpression, lexpressionCompileError := stateful.NewExpression(n.InfoReset.Expression)
			if lexpressionCompileError != nil {
				return nil, fmt.Errorf("Failed to compile stateful expression for infoReset: %s", lexpressionCompileError)
			}
			an.levelResets[alert.Info] = lstatefulExpression
			an.lrScopePools[alert.Info] = stateful.NewScopePool(ast.FindReferenceVariables(n.InfoReset.Expression))
		}
	}

	if n.Warn != nil {
		statefulExpression, expressionCompileError := stateful.NewExpression(n.Warn.Expression)
		if expressionCompileError != nil {
			return nil, fmt.Errorf("Failed to compile stateful expression for warn: %s", expressionCompileError)
		}
		an.levels[alert.Warning] = statefulExpression
		an.scopePools[alert.Warning] = stateful.NewScopePool(ast.FindReferenceVariables(n.Warn.Expression))
		if n.WarnReset != nil {
			lstatefulExpression, lexpressionCompileError := stateful.NewExpression(n.WarnReset.Expression)
			if lexpressionCompileError != nil {
				return nil, fmt.Errorf("Failed to compile stateful expression for warnReset: %s", lexpressionCompileError)
			}
			an.levelResets[alert.Warning] = lstatefulExpression
			an.lrScopePools[alert.Warning] = stateful.NewScopePool(ast.FindReferenceVariables(n.WarnReset.Expression))
		}
	}

	if n.Crit != nil {
		statefulExpression, expressionCompileError := stateful.NewExpression(n.Crit.Expression)
		if expressionCompileError != nil {
			return nil, fmt.Errorf("Failed to compile stateful expression for crit: %s", expressionCompileError)
		}
		an.levels[alert.Critical] = statefulExpression
		an.scopePools[alert.Critical] = stateful.NewScopePool(ast.FindReferenceVariables(n.Crit.Expression))
		if n.CritReset != nil {
			lstatefulExpression, lexpressionCompileError := stateful.NewExpression(n.CritReset.Expression)
			if lexpressionCompileError != nil {
				return nil, fmt.Errorf("Failed to compile stateful expression for critReset: %s", lexpressionCompileError)
			}
			an.levelResets[alert.Critical] = lstatefulExpression
			an.lrScopePools[alert.Critical] = stateful.NewScopePool(ast.FindReferenceVariables(n.CritReset.Expression))
		}
	}

	// Setup states
	if n.History < 2 {
		n.History = 2
	}

	// Configure flapping
	if n.UseFlapping {
		if n.FlapLow > 1 || n.FlapHigh > 1 {
			return nil, errors.New("alert flap thresholds are percentages and should be between 0 and 1")
		}
	}

	return
}

func (n *AlertNode) runAlert([]byte) error {
	// Register delete hook
	if n.hasAnonTopic() {
		n.et.tm.registerDeleteHookForTask(n.et.Task.ID, deleteAlertHook(n.anonTopic))

		// Register Handlers on topic
		for _, h := range n.handlers {
			n.et.tm.AlertService.RegisterAnonHandler(n.anonTopic, h)
		}
		// Restore anonTopic
		n.et.tm.AlertService.RestoreTopic(n.anonTopic)
	}

	// Setup stats
	n.alertsTriggered = &expvar.Int{}
	n.statMap.Set(statsAlertsTriggered, n.alertsTriggered)

	n.alertsInhibited = &expvar.Int{}
	n.statMap.Set(statsAlertsInhibited, n.alertsInhibited)

	n.oksTriggered = &expvar.Int{}
	n.statMap.Set(statsOKsTriggered, n.oksTriggered)

	n.infosTriggered = &expvar.Int{}
	n.statMap.Set(statsInfosTriggered, n.infosTriggered)

	n.warnsTriggered = &expvar.Int{}
	n.statMap.Set(statsWarnsTriggered, n.warnsTriggered)

	n.critsTriggered = &expvar.Int{}
	n.statMap.Set(statsCritsTriggered, n.critsTriggered)

	n.eventsDropped = &expvar.Int{}
	n.statMap.Set(statsCritsTriggered, n.critsTriggered)

	// Setup consumer
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())

	if err := consumer.Consume(); err != nil {
		return err
	}

	// Close the anonymous topic.
	n.et.tm.AlertService.CloseTopic(n.anonTopic)

	// Deregister Handlers on topic
	for _, h := range n.handlers {
		n.et.tm.AlertService.DeregisterAnonHandler(n.anonTopic, h)
	}

	return nil
}

func (n *AlertNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	id, err := n.renderID(first.Name(), first.GroupID(), first.Tags())
	if err != nil {
		return nil, err
	}
	t := first.Time()

	state := n.restoreEventState(id, t, group.Tags)

	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(
			n.timer,
			state,
		),
	), nil
}

func (n *AlertNode) restoreEventState(id string, t time.Time, tags models.Tags) *alertState {
	state := n.newAlertState(tags)
	currentLevel, triggered := n.restoreEvent(id)
	if currentLevel != alert.OK {
		// Add initial event
		state.addEvent(t, currentLevel)
		// Record triggered time
		state.triggered(triggered)
	}
	return state
}

func (n *AlertNode) newAlertState(tags models.Tags) *alertState {
	inhibitors := make([]*alert.Inhibitor, len(n.a.Inhibitors))
	for i, in := range n.a.Inhibitors {
		tagset := make(models.Tags, len(in.EqualTags))
		for _, t := range in.EqualTags {
			tagset[t] = tags[t]
		}

		inhibitor := alert.NewInhibitor(in.Category, tagset)
		inhibitors[i] = inhibitor
		n.et.tm.AlertService.AddInhibitor(inhibitor)
	}
	return &alertState{
		history:    make([]alert.Level, n.a.History),
		n:          n,
		buffer:     new(edge.BatchBuffer),
		inhibitors: inhibitors,
	}
}

func (n *AlertNode) restoreEvent(id string) (alert.Level, time.Time) {
	var topicState, anonTopicState alert.EventState
	var anonFound, topicFound bool
	// Check for previous state on anonTopic
	if n.hasAnonTopic() {
		if state, ok, err := n.et.tm.AlertService.EventState(n.anonTopic, id); err != nil {
			n.diag.Error("failed to get event state for anonymous topic", err,
				keyvalue.KV("topic", n.anonTopic), keyvalue.KV("event", id))
		} else if ok {
			anonTopicState = state
			anonFound = true
		}
	}
	// Check for previous state on topic.
	if n.hasTopic() {
		if state, ok, err := n.et.tm.AlertService.EventState(n.topic, id); err != nil {
			n.diag.Error("failed to get event state for topic", err,
				keyvalue.KV("topic", n.anonTopic), keyvalue.KV("event", id))
		} else if ok {
			topicState = state
			topicFound = true
		}
	}
	if topicState.Level != anonTopicState.Level {
		if anonFound && topicFound {
			// Anon topic takes precedence
			if err := n.et.tm.AlertService.UpdateEvent(n.topic, anonTopicState); err != nil {
				n.diag.Error("failed to update topic event state", err, keyvalue.KV("topic", n.topic), keyvalue.KV("event", id))
			}
		} else if topicFound && n.hasAnonTopic() {
			// Update event state for topic
			if err := n.et.tm.AlertService.UpdateEvent(n.anonTopic, topicState); err != nil {
				n.diag.Error("failed to update topic event state", err, keyvalue.KV("topic", n.topic), keyvalue.KV("event", id))
			}
		} // else nothing was found, nothing to do
	}
	if anonFound {
		return anonTopicState.Level, anonTopicState.Time
	}
	return topicState.Level, topicState.Time
}

func deleteAlertHook(anonTopic string) deleteHook {
	return func(tm *TaskMaster) {
		tm.AlertService.DeleteTopic(anonTopic)
	}
}

func (n *AlertNode) hasAnonTopic() bool {
	return len(n.handlers) > 0
}
func (n *AlertNode) hasTopic() bool {
	return n.topic != ""
}

func (n *AlertNode) handleEvent(event alert.Event) {
	// Check if alert is inhibited
	if n.et.tm.AlertService.IsInhibited(event.Data.Category, event.Data.Tags) {
		n.alertsInhibited.Add(1)
		return
	}

	n.alertsTriggered.Add(1)
	switch event.State.Level {
	case alert.OK:
		n.oksTriggered.Add(1)
	case alert.Info:
		n.infosTriggered.Add(1)
	case alert.Warning:
		n.warnsTriggered.Add(1)
	case alert.Critical:
		n.critsTriggered.Add(1)
	}
	n.diag.AlertTriggered(event.State.Level, event.State.ID, event.State.Message, event.Data.Result.Series[0])

	// If we have anon handlers, emit event to the anonTopic
	if n.hasAnonTopic() {
		event.Topic = n.anonTopic
		err := n.et.tm.AlertService.Collect(event)
		if err != nil {
			n.eventsDropped.Add(1)
			n.diag.Error("encountered error collecting event", err)
		}
	}

	// If we have a user define topic, emit event to the topic.
	if n.hasTopic() {
		event.Topic = n.topic
		err := n.et.tm.AlertService.Collect(event)
		if err != nil {
			n.eventsDropped.Add(1)
			n.diag.Error("encountered error collecting event", err)
		}
	}
}

func (n *AlertNode) determineLevel(p edge.FieldsTagsTimeGetter, currentLevel alert.Level) alert.Level {
	if higherLevel, found := n.findFirstMatchLevel(alert.Critical, currentLevel-1, p); found {
		return higherLevel
	}
	if rse := n.levelResets[currentLevel]; rse != nil {
		if pass, err := EvalPredicate(rse, n.lrScopePools[currentLevel], p); err != nil {
			n.diag.Error("error evaluating reset expression for current level", err, keyvalue.KV("level", currentLevel.String()))
		} else if !pass {
			return currentLevel
		}
	}
	if newLevel, found := n.findFirstMatchLevel(currentLevel, alert.OK, p); found {
		return newLevel
	}
	return alert.OK
}

func (n *AlertNode) findFirstMatchLevel(start alert.Level, stop alert.Level, p edge.FieldsTagsTimeGetter) (alert.Level, bool) {
	if stop < alert.OK {
		stop = alert.OK
	}
	for l := start; l > stop; l-- {
		se := n.levels[l]
		if se == nil {
			continue
		}
		if pass, err := EvalPredicate(se, n.scopePools[l], p); err != nil {
			n.diag.Error("error evaluating expression for level", err, keyvalue.KV("level", alert.Level(l).String()))
			continue
		} else if pass {
			return alert.Level(l), true
		}
	}
	return alert.OK, false
}

func (n *AlertNode) event(
	id, name string,
	group models.GroupID,
	tags models.Tags,
	fields models.Fields,
	level alert.Level,
	t time.Time,
	d time.Duration,
	result models.Result,
) (alert.Event, error) {
	msg, details, err := n.renderMessageAndDetails(id, name, t, group, tags, fields, level, d)
	if err != nil {
		return alert.Event{}, err
	}
	event := alert.Event{
		Topic: n.anonTopic,
		State: alert.EventState{
			ID:       id,
			Message:  msg,
			Details:  details,
			Time:     t,
			Duration: d,
			Level:    level,
		},
		Data: alert.EventData{
			Name:        name,
			TaskName:    n.et.Task.ID,
			Category:    n.a.Category,
			Group:       string(group),
			Tags:        tags,
			Fields:      fields,
			Result:      result,
			Recoverable: !n.a.NoRecoveriesFlag,
		},
	}
	return event, nil
}

type alertState struct {
	n *AlertNode

	buffer *edge.BatchBuffer

	history []alert.Level
	idx     int

	flapping bool

	changed bool
	// Time when first alert was triggered
	firstTriggered time.Time
	// Time when last alert was triggered.
	// Note: Alerts are not triggered for every event.
	lastTriggered time.Time
	expired       bool

	inhibitors []*alert.Inhibitor
}

func (a *alertState) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	return nil, a.buffer.BeginBatch(begin)
}

func (a *alertState) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return nil, a.buffer.BatchPoint(bp)
}

func (a *alertState) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return a.BufferedBatch(a.buffer.BufferedBatchMessage(end))
}

func (a *alertState) BufferedBatch(b edge.BufferedBatchMessage) (edge.Message, error) {
	begin := b.Begin()
	id, err := a.n.renderID(begin.Name(), begin.GroupID(), begin.Tags())
	if err != nil {
		return nil, err
	}
	if len(b.Points()) == 0 {
		return nil, nil
	}
	// Keep track of lowest level for any point
	lowestLevel := alert.Critical
	// Keep track of highest level and point
	highestLevel := alert.OK
	var highestPoint edge.BatchPointMessage

	currentLevel := a.currentLevel()
	for _, bp := range b.Points() {
		l := a.n.determineLevel(bp, currentLevel)
		if l < lowestLevel {
			lowestLevel = l
		}
		if l > highestLevel || highestPoint == nil {
			highestLevel = l
			highestPoint = bp
		}
	}

	// Default the determined level to lowest.
	l := lowestLevel
	// Update determined level to highest if we don't care about all
	if !a.n.a.AllFlag {
		l = highestLevel
	}
	// Create alert Data
	t := highestPoint.Time()
	if a.n.a.AllFlag || l == alert.OK {
		t = begin.Time()
	}
	t = t.UTC()

	a.addEvent(t, l)

	// Trigger alert only if:
	//  l == OK and state.changed (aka recovery)
	//    OR
	//  l != OK and flapping/statechanges checkout
	if !(a.changed && l == alert.OK ||
		(l != alert.OK &&
			!((a.n.a.UseFlapping && a.flapping) ||
				(a.n.a.IsStateChangesOnly && !a.changed && !a.expired)))) {
		return nil, nil
	}

	a.triggered(t)

	// Suppress the recovery event.
	if a.n.a.NoRecoveriesFlag && l == alert.OK {
		return nil, nil
	}

	duration := a.duration()
	event, err := a.n.event(id, begin.Name(), begin.GroupID(), begin.Tags(), highestPoint.Fields(), l, t, duration, b.ToResult())
	if err != nil {
		return nil, err
	}

	a.n.handleEvent(event)

	// Update tags or fields with event state
	if a.n.a.LevelTag != "" ||
		a.n.a.LevelField != "" ||
		a.n.a.IdTag != "" ||
		a.n.a.IdField != "" ||
		a.n.a.DurationField != "" ||
		a.n.a.MessageField != "" {

		b = b.ShallowCopy()
		points := make([]edge.BatchPointMessage, len(b.Points()))
		for i, bp := range b.Points() {
			bp = bp.ShallowCopy()
			a.augmentTagsWithEventState(bp, event.State)
			a.augmentFieldsWithEventState(bp, event.State)
			points[i] = bp
		}
		b.SetPoints(points)

		newBegin := begin.ShallowCopy()
		a.augmentTagsWithEventState(newBegin, event.State)
		b.SetBegin(newBegin)
	}
	return b, nil
}

func (a *alertState) Point(p edge.PointMessage) (edge.Message, error) {
	id, err := a.n.renderID(p.Name(), p.GroupID(), p.Tags())
	if err != nil {
		return nil, err
	}
	l := a.n.determineLevel(p, a.currentLevel())

	a.addEvent(p.Time(), l)

	if (a.n.a.UseFlapping && a.flapping) || (a.n.a.IsStateChangesOnly && !a.changed && !a.expired) {
		return nil, nil
	}
	// send alert if we are not OK or we are OK and state changed (i.e recovery)
	if l != alert.OK || a.changed {
		a.triggered(p.Time())
		// Suppress the recovery event.
		if a.n.a.NoRecoveriesFlag && l == alert.OK {
			return nil, nil
		}
		// Create an alert event
		duration := a.duration()
		event, err := a.n.event(
			id,
			p.Name(),
			p.GroupID(),
			p.Tags(),
			p.Fields(),
			l,
			p.Time(),
			duration,
			p.ToResult(),
		)
		if err != nil {
			return nil, err
		}

		a.n.handleEvent(event)

		// Prepare an augmented point to return
		p = p.ShallowCopy()
		a.augmentTagsWithEventState(p, event.State)
		a.augmentFieldsWithEventState(p, event.State)
		return p, nil
	}
	return nil, nil
}

func (a *alertState) augmentTagsWithEventState(p edge.TagSetter, eventState alert.EventState) {
	if a.n.a.LevelTag != "" || a.n.a.IdTag != "" {
		tags := p.Tags().Copy()
		if a.n.a.LevelTag != "" {
			tags[a.n.a.LevelTag] = eventState.Level.String()
		}
		if a.n.a.IdTag != "" {
			tags[a.n.a.IdTag] = eventState.ID
		}
		p.SetTags(tags)
	}
}

func (a *alertState) augmentFieldsWithEventState(p edge.FieldSetter, eventState alert.EventState) {
	if a.n.a.LevelField != "" || a.n.a.IdField != "" || a.n.a.DurationField != "" || a.n.a.MessageField != "" {
		fields := p.Fields().Copy()
		if a.n.a.LevelField != "" {
			fields[a.n.a.LevelField] = eventState.Level.String()
		}
		if a.n.a.MessageField != "" {
			fields[a.n.a.MessageField] = eventState.Message
		}
		if a.n.a.IdField != "" {
			fields[a.n.a.IdField] = eventState.ID
		}
		if a.n.a.DurationField != "" {
			fields[a.n.a.DurationField] = int64(eventState.Duration)
		}
		p.SetFields(fields)
	}
}

func (a *alertState) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

func (a *alertState) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (a *alertState) Done() {
	for _, inhibitor := range a.inhibitors {
		a.n.et.tm.AlertService.RemoveInhibitor(inhibitor)
	}
}

// Return the duration of the current alert state.
func (a *alertState) duration() time.Duration {
	return a.lastTriggered.Sub(a.firstTriggered)
}

// Record that the alert was triggered at time t.
func (a *alertState) triggered(t time.Time) {
	a.lastTriggered = t
	// Check if we are being triggered for first time since an alert.OKAlert
	// If so reset firstTriggered time
	p := a.idx - 1
	if p == -1 {
		p = len(a.history) - 1
	}
	if a.history[p] == alert.OK {
		a.firstTriggered = t
	}

	// Update inhibitor state
	inhibited := a.history[a.idx] != alert.OK
	for _, in := range a.inhibitors {
		in.Set(inhibited)
	}
}

// Record an event in the alert history.
func (a *alertState) addEvent(t time.Time, level alert.Level) {
	// Check for changes
	a.changed = a.history[a.idx] != level

	// Add event to history
	a.idx = (a.idx + 1) % len(a.history)
	a.history[a.idx] = level

	a.updateFlapping()
	a.updateExpired(t)

}

// Return current level of this state
func (a *alertState) currentLevel() alert.Level {
	return a.history[a.idx]
}

// Compute the percentage change in the alert history.
func (a *alertState) percentChange() float64 {
	l := len(a.history)
	changes := 0.0
	weight := (maxWeight / weightDiff)
	step := (maxWeight - weight) / float64(l-1)
	for i := 0; i < l-1; i++ {
		// get current index
		c := (i + a.idx) % l
		// get previous index
		p := c - 1
		// check for wrap around
		if p < 0 {
			p = l - 1
		}
		if a.history[c] != a.history[p] {
			changes += weight
		}
		weight += step
	}

	p := changes / float64(l-1)
	return p
}

func (a *alertState) updateFlapping() {
	if !a.n.a.UseFlapping {
		return
	}
	p := a.percentChange()
	if a.flapping && p < a.n.a.FlapLow {
		a.flapping = false
	} else if !a.flapping && p > a.n.a.FlapHigh {
		a.flapping = true
	}
}

func (a *alertState) updateExpired(t time.Time) {
	a.expired = !a.changed && a.n.a.StateChangesOnlyDuration != 0 && t.Sub(a.lastTriggered) >= a.n.a.StateChangesOnlyDuration
}

type serverInfo struct {
	Hostname  string
	ClusterID string
	ServerID  string
}

// Type containing information available to ID template.
type idInfo struct {
	// Measurement name
	Name string

	// Task name
	TaskName string

	// Concatenation of all group-by tags of the form [key=value,]+.
	// If not groupBy is performed equal to literal 'nil'
	Group string

	// Map of tags
	Tags map[string]string

	ServerInfo serverInfo
}

type messageInfo struct {
	idInfo

	// The ID of the alert.
	ID string

	// Fields of alerting data point.
	Fields map[string]interface{}

	// Alert Level, one of: INFO, WARNING, CRITICAL.
	Level string

	// Time
	Time time.Time

	// Duration of the alert
	Duration time.Duration
}

type detailsInfo struct {
	messageInfo
	// The Message of the Alert
	Message string
}

func (n *AlertNode) serverInfo() serverInfo {
	return serverInfo{
		Hostname:  n.et.tm.ServerInfo.Hostname(),
		ClusterID: n.et.tm.ServerInfo.ClusterID().String(),
		ServerID:  n.et.tm.ServerInfo.ServerID().String(),
	}

}
func (n *AlertNode) renderID(name string, group models.GroupID, tags models.Tags) (string, error) {
	g := string(group)
	if group == models.NilGroup {
		g = "nil"
	}
	info := idInfo{
		Name:       name,
		TaskName:   n.et.Task.ID,
		Group:      g,
		Tags:       tags,
		ServerInfo: n.serverInfo(),
	}
	id := n.bufPool.Get().(*bytes.Buffer)
	defer func() {
		id.Reset()
		n.bufPool.Put(id)
	}()

	err := n.idTmpl.Execute(id, info)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (n *AlertNode) renderMessageAndDetails(id, name string, t time.Time, group models.GroupID, tags models.Tags, fields models.Fields, level alert.Level, d time.Duration) (string, string, error) {
	g := string(group)
	if group == models.NilGroup {
		g = "nil"
	}
	minfo := messageInfo{
		idInfo: idInfo{
			Name:       name,
			TaskName:   n.et.Task.ID,
			Group:      g,
			Tags:       tags,
			ServerInfo: n.serverInfo(),
		},
		ID:       id,
		Fields:   fields,
		Level:    level.String(),
		Time:     t,
		Duration: d,
	}

	// Grab a buffer for the message template and the details template
	tmpBuffer := n.bufPool.Get().(*bytes.Buffer)
	defer func() {
		tmpBuffer.Reset()
		n.bufPool.Put(tmpBuffer)
	}()
	tmpBuffer.Reset()

	err := n.messageTmpl.Execute(tmpBuffer, minfo)
	if err != nil {
		return "", "", err
	}

	msg := tmpBuffer.String()
	dinfo := detailsInfo{
		messageInfo: minfo,
		Message:     msg,
	}

	// Reuse the buffer, for the details template
	tmpBuffer.Reset()
	err = n.detailsTmpl.Execute(tmpBuffer, dinfo)
	if err != nil {
		return "", "", err
	}

	details := tmpBuffer.String()
	return msg, details, nil
}
