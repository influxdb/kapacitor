package integrations

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"io"
	"math/rand"
	"mime/quotedprintable"
	"net/http"
	"net/http/httptest"
	"net/mail"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/docker/docker/api/types/swarm"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/client"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/clock"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/command/commandtest"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	alertservice "github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alert/alerttest"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/alerta/alertatest"
	"github.com/influxdata/kapacitor/services/bigpanda"
	"github.com/influxdata/kapacitor/services/bigpanda/bigpandatest"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/discord"
	"github.com/influxdata/kapacitor/services/discord/discordtest"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/hipchat/hipchattest"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/httppost/httpposttest"
	k8s "github.com/influxdata/kapacitor/services/k8s/client"
	"github.com/influxdata/kapacitor/services/k8s/k8stest"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/kafka/kafkatest"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/opsgenie/opsgenietest"
	"github.com/influxdata/kapacitor/services/opsgenie2"
	"github.com/influxdata/kapacitor/services/opsgenie2/opsgenie2test"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty/pagerdutytest"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/pagerduty2/pagerduty2test"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/pushover/pushovertest"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/sensu/sensutest"
	"github.com/influxdata/kapacitor/services/servicenow"
	"github.com/influxdata/kapacitor/services/servicenow/servicenowtest"
	"github.com/influxdata/kapacitor/services/sideload"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/slack/slacktest"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/smtp/smtptest"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/snmptrap/snmptraptest"
	"github.com/influxdata/kapacitor/services/storage/storagetest"
	"github.com/influxdata/kapacitor/services/swarm/swarmtest"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/talk/talktest"
	"github.com/influxdata/kapacitor/services/teams"
	"github.com/influxdata/kapacitor/services/teams/teamstest"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/telegram/telegramtest"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/services/victorops/victoropstest"
	"github.com/influxdata/kapacitor/services/zenoss"
	"github.com/influxdata/kapacitor/services/zenoss/zenosstest"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/agent"
	udf_test "github.com/influxdata/kapacitor/udf/test"
	"github.com/influxdata/wlog"
	"github.com/k-sone/snmpgo"
	"github.com/zeebo/mwc"
)

var diagService *diagnostic.Service

func init() {
	testing.Init()
	flag.Parse()
	out := io.Discard
	if testing.Verbose() {
		out = os.Stderr
	}
	diagService = diagnostic.NewService(diagnostic.NewConfig(), out, out)
	if err := diagService.Open(); err != nil {
		panic(err)
	}
}

type testCtxStr string

var dbrps = []kapacitor.DBRP{
	{
		Database:        "dbname",
		RetentionPolicy: "rpname",
	},
}

func init() {
	wlog.SetLevel(wlog.OFF)
}

func TestStream_InfluxQLNodeMissingValue_Batch(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
	|window()
		.period(10s)
		.every(10s)
	|mean('is_missing_value')
	|httpOut('TestStream_InfluxQLNodeMissingValue')
`
	er := models.Result{}

	testStreamerWithOutput(t, "TestStream_InfluxQLNodeMissingValue", script, 15*time.Second, er, false, nil)
}

func TestStream_InfluxQLNodeMissingValue_Stream(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|mean('is_missing_value')
	|httpOut('TestStream_InfluxQLNodeMissingValue')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					1011.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_InfluxQLNodeMissingValue", script, 15*time.Second, er, false, nil)
}

func TestStream_ChangeDetect(t *testing.T) {

	var script = `stream
	|from().measurement('packets')
	|changeDetect('value')
    |window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_ChangeDetect')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						"bad",
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						"good",
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						"bad",
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						"good",
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						"bad",
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						"good",
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_ChangeDetect", script, 15*time.Second, er, false, nil)
}
func TestStream_ChangeDetect_Many(t *testing.T) {

	var script = `stream
	|from().measurement('packets')
	|changeDetect('a','b')
    |window()
		.period(6s)
		.every(6s)
	|httpOut('TestStream_ChangeDetect_Many')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "a", "b"},
				Values: [][]interface{}{
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						"bad",
						0.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						"good",
						0.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						"bad",
						1.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						"bad",
						0.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_ChangeDetect_Many", script, 15*time.Second, er, false, nil)
}

func TestStream_Derivative(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|httpOut('TestStream_Derivative')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Derivative", script, 15*time.Second, er, false, nil)
}

func TestStream_DerivativeAs(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
		.as('derivative')
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_Derivative')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "derivative", "value"},
				Values: [][]interface{}{
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						1.0,
						1001.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						1.0,
						1003.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1.0,
						1004.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						2.0,
						1006.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1.0,
						1007.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						0.0,
						1007.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						1.0,
						1008.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						1.0,
						1009.0,
					},
					[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						1.0,
						1010.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Derivative", script, 15*time.Second, er, false, nil)
}

func TestStream_DerivativeZeroElapsed(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_DerivativeZeroElapsed')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					9.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_DerivativeZeroElapsed", script, 15*time.Second, er, false, nil)
}

func TestStream_DerivativeUnit(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
		.unit(10s)
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|httpOut('TestStream_Derivative')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Derivative", script, 15*time.Second, er, false, nil)
}

func TestStream_DerivativeNN(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
		.nonNegative()
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|httpOut('TestStream_DerivativeNN')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_DerivativeNN", script, 15*time.Second, er, false, nil)
}

func TestStream_DerivativeN(t *testing.T) {

	var script = `
stream
	|from().measurement('packets')
	|derivative('value')
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|httpOut('TestStream_DerivativeNN')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					-99.7,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_DerivativeNN", script, 15*time.Second, er, false, nil)
}

func TestStream_HoltWinters(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('packets')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|holtWinters('value', 3, 0, 1s)
	|where(lambda: "host" == 'serverA')
	|httpOut('TestStream_HoltWinters')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    models.Tags{"host": "serverA"},
				Columns: []string{"time", "holtWinters"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						1009.324690106368,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						1009.7524349889708,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
						1010.105056042826,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_HoltWinters", script, 15*time.Second, er, false, nil)
}

func TestStream_HoltWintersWithFit(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('packets')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|holtWintersWithFit('value', 3, 0, 1s)
	|where(lambda: "host" == 'serverA')
	|httpOut('TestStream_HoltWinters')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    models.Tags{"host": "serverA"},
				Columns: []string{"time", "holtWinters"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1000.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						1000.7349380776699,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						1001.8935462884633,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						1003.1750039651934,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1004.4245269000132,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						1005.5685498251902,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1006.5782508658309,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						1007.4488388165385,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						1008.1877681696025,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						1008.8080773333872,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						1009.324690106368,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						1009.7524349889708,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
						1010.105056042826,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_HoltWinters", script, 15*time.Second, er, false, nil)
}

func TestStream_Elapsed(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('packets')
	|elapsed('value', 1s)
	|window()
		.period(10s)
		.every(10s)
	|max('elapsed')
	|httpOut('TestStream_Elapsed')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Elapsed", script, 15*time.Second, er, false, nil)
}

func TestStream_Difference(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('packets')
	|difference('value')
	|window()
		.period(10s)
		.every(10s)
	|max('difference')
	|httpOut('TestStream_Difference')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "max"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					5.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Difference", script, 15*time.Second, er, false, nil)
}

func TestStream_MovingAverage(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('packets')
	|movingAverage('value', 4)
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_MovingAverage')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "movingAverage"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						1001.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1005.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						1008.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1012.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						1015.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						1016.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						1017.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						1018.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						1019.5,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
						1020.5,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_MovingAverage", script, 16*time.Second, er, false, nil)
}

func TestStream_CumulativeSum(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('packets')
	|cumulativeSum('value')
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_CumulativeSum')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Tags:    nil,
				Columns: []string{"time", "cumulativeSum"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						6.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						10.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						15.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						21.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						28.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						36.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_CumulativeSum", script, 13*time.Second, er, false, nil)
}

func TestStream_WindowMissing(t *testing.T) {

	var script = `
var period = 3s
var every = 2s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
	|count('value')
	|httpOut('TestStream_WindowMissing')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					3.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_WindowMissing", script, 13*time.Second, er, false, nil)
}

func TestStream_WindowMissingAligned(t *testing.T) {

	var script = `
var period = 3s
var every = 2s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
		.align()
	|count('value')
	|httpOut('TestStream_WindowMissing')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					3.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_WindowMissing", script, 13*time.Second, er, false, nil)
}

func TestStream_Window(t *testing.T) {

	var script = `
var period = 10s
var every = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
	|httpOut('TestStream_Window')
`

	nums := []float64{
		97.1,
		92.6,
		95.6,
		93.1,
		92.6,
		95.8,
		92.7,
		96.0,
		93.4,
		95.3,
	}

	values := make([][]interface{}, len(nums))
	for i, num := range nums {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, i, 0, time.UTC),
			"serverA",
			"idle",
			num,
		}
	}

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "host", "type", "value"},
				Values:  values,
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window", script, 13*time.Second, er, false, nil)
}

func TestStream_Window_Count(t *testing.T) {

	var script = `
var period = 10
var every = 10
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.periodCount(period)
		.everyCount(every)
	|httpOut('TestStream_Window_Count')
`

	count := 10
	values := make([][]interface{}, count)
	for i := 0; i < count; i++ {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
			"serverA",
			"idle",
			float64(i + 1),
		}
	}

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "host", "type", "value"},
				Values:  values,
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_Count", script, 2*time.Second, er, false, nil)
}

func TestStream_Window_Count_Overlapping(t *testing.T) {

	var script = `
var period = 3
var every = 1
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.periodCount(period)
		.everyCount(every)
	|httpOut('TestStream_Window_Count')
`

	count := 3
	values := make([][]interface{}, count)
	for i := 0; i < count; i++ {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
			"serverA",
			"idle",
			float64(i + 10),
		}
	}

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "host", "type", "value"},
				Values:  values,
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_Count", script, 2*time.Second, er, false, nil)
}

func TestStream_Window_Count_Every_1(t *testing.T) {

	var script = `
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.groupBy('host')
	|window()
		.periodCount(3)
		.everyCount(1)
	|count('value')
	|window()
		.periodCount(20)
		.everyCount(1)
	|httpOut('TestStream_Window_Count')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						2.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						3.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_Count", script, 2*time.Second, er, false, nil)
}

func TestStream_Window_Count_FillPeriod(t *testing.T) {

	var script = `
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.groupBy('host')
	|window()
		.periodCount(4)
		.everyCount(1)
		.fillPeriod()
	|count('value')
	|window()
		.periodCount(20)
		.everyCount(1)
	|httpOut('TestStream_Window_Count')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						4.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_Count", script, 2*time.Second, er, false, nil)
}

func TestStream_Window_Every_0(t *testing.T) {

	var script = `
var period = 10s
// Emit the window on every point
var every = 0s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.groupBy('host')
	|window()
		.period(period)
		.every(every)
	|count('value')
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_Window')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						6.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						7.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						8.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						9.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						10.0,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						3.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						4.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						6.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						7.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						8.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						9.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						10.0,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						2.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window", script, 13*time.Second, er, false, nil)
}

func TestStream_Window_Every_0_FillPeriod(t *testing.T) {

	var script = `
var period = 5s
// Emit the window on every point
var every = 0s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.groupBy('host')
	|window()
		.period(period)
		.every(every)
		.fillPeriod()
	|count('value')
	|window()
		.period(10s)
		.every(0s)
	|httpOut('TestStream_Window')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						5.0,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						5.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						5.0,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
						1.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
						1.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window", script, 13*time.Second, er, false, nil)
}

func TestStream_Window_Overlapping(t *testing.T) {

	var script = `
var period = 14s
var every = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
	|httpOut('TestStream_Window_FillPeriod')
`

	nums := []float64{
		93.1,
		97.1,
		92.6,
		95.6,
		93.1,
		92.6,
		95.8,
		92.7,
		96.0,
		93.4,
	}

	values := make([][]interface{}, len(nums))
	for i, num := range nums {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, i, 0, time.UTC),
			"serverA",
			"idle",
			num,
		}
	}

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "host", "type", "value"},
				Values:  values,
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_FillPeriod", script, 16*time.Second, er, false, nil)
}

func TestStream_Window_FillPeriod(t *testing.T) {

	var script = `
var period = 14s
var every = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
		.fillPeriod()
	|httpOut('TestStream_Window_FillPeriod')
`

	nums := []float64{
		93.1,
		97.1,
		92.6,
		95.6,
		93.1,
		92.6,
		95.8,
		92.7,
		96.0,
		93.4,
		95.3,
		96.4,
		95.1,
		91.1,
	}

	values := make([][]interface{}, len(nums))
	for i, num := range nums {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, i, 0, time.UTC),
			"serverA",
			"idle",
			num,
		}
	}

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "host", "type", "value"},
				Values:  values,
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_FillPeriod", script, 16*time.Second, er, false, nil)
}
func TestStream_Window_FillPeriod_Aligned(t *testing.T) {

	var script = `
var period = 14s
var every = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(period)
		.every(every)
		.fillPeriod()
		.align()
	|httpOut('TestStream_Window_FillPeriod_Aligned')
`

	nums := []float64{
		95.8,
		92.7,
		96.0,
		93.4,
		95.3,
		96.4,
		95.1,
		91.1,
		95.7,
		96.2,
		96.6,
		91.2,
		98.2,
		96.1,
	}

	values := make([][]interface{}, len(nums))
	for i, num := range nums {
		values[i] = []interface{}{
			time.Date(1971, 1, 1, 0, 0, i+6, 0, time.UTC),
			"serverA",
			"idle",
			num,
		}
	}

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "host", "type", "value"},
				Values:  values,
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Window_FillPeriod_Aligned", script, 21*time.Second, er, false, nil)
}

func TestStream_Barrier_Idle_No_Data(t *testing.T) {
	// Set up clock to current time - 22 seconds since we're going to send 21 data points
	clock := clock.New(time.Now().UTC())
	requestCount := int32(0)

	testValues := make([][]interface{}, 1)
	for i := 0; i < 1; i++ {
		testValues[i] = []interface{}{
			clock.Zero().Add(time.Duration(i) * time.Second),
			rand.Float64(),
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
	}))
	defer ts.Close()

	var script = `
var period = 14s
var every = 2s
var idle = 2s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
	|barrier().idle(idle)
	|window()
		.period(period)
		.every(every)
	|httpPost('` + ts.URL + `')
`

	dataChannel := make(chan edge.PointMessage)
	tm, _, cleanupTest := testStreamerWithInputChannel(t, "TestStream_Barrier_Idle_No_Data", script, dataChannel, clock, nil, nil, false)
	defer checkDeferredErrors(t, tm.Close)()
	defer func() {
		cleanupTest()

		if rc := atomic.LoadInt32(&requestCount); rc != 2 {
			t.Errorf("got %v exp %v", rc, 2)
		}
	}()

	// groupedconsumers do not run any logic until at least one message is seen
	data := make([]edge.PointMessage, len(testValues))
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}

	time.Sleep(5 * time.Second)
	close(dataChannel)
}

func TestStream_Barrier_Idle(t *testing.T) {
	// Set up clock to current time - 22 seconds since we're going to send 21 data points
	clock := clock.New(time.Now().UTC().Add(-22 * time.Second))
	clock.Set(time.Now().UTC())
	testValues := make([][]interface{}, 21)
	for i := 0; i < 21; i++ {
		testValues[i] = []interface{}{
			clock.Zero().Add(time.Duration(i) * time.Second),
			rand.Float64(),
		}
	}
	requestCount := int32(0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var er models.Result
		switch rc {
		case 1:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[0:10],
					},
				},
			}
		case 2:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[6:20],
					},
				},
			}
		case 3:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[16:21],
					},
				},
			}
		}
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var period = 14s
var every = 10s
var idle = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
	|barrier().idle(idle)
	|window()
		.period(period)
		.every(every)
	|httpPost('` + ts.URL + `')
`

	dataChannel := make(chan edge.PointMessage)
	tm, _, cleanupTest := testStreamerWithInputChannel(t, "TestStream_Barrier_Idle", script, dataChannel, clock, nil, nil, false)
	defer checkDeferredErrors(t, tm.Close)()
	defer func() {
		cleanupTest()

		// Force emit should force the last window to emit
		if rc := atomic.LoadInt32(&requestCount); rc != 3 {
			t.Errorf("got %v exp %v", rc, 3)
		}
	}()

	data := make([]edge.PointMessage, len(testValues))
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}
	time.Sleep(11 * time.Second)
	close(dataChannel)
}

func TestStream_Barrier_Idle_No_Idle(t *testing.T) {
	// Set up clock to current time - 22 seconds since we're going to send 21 data points
	clock := clock.New(time.Now().UTC().Add(-22 * time.Second))
	clock.Set(time.Now().UTC())
	testValues := make([][]interface{}, 21)
	for i := 0; i < 21; i++ {
		testValues[i] = []interface{}{
			clock.Zero().Add(time.Duration(i) * time.Second),
			rand.Float64(),
		}
	}
	requestCount := int32(0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var er models.Result
		switch rc {
		case 1:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[0:10],
					},
				},
			}
		case 2:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[6:20],
					},
				},
			}
		case 3:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[16:21],
					},
				},
			}
		}
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var period = 14s
var every = 10s
var idle = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
	|barrier().idle(idle)
	|window()
		.period(period)
		.every(every)
	|httpPost('` + ts.URL + `')
`

	dataChannel := make(chan edge.PointMessage)
	tm, _, cleanupTest := testStreamerWithInputChannel(t, "TestStream_Barrier_Idle_No_Idle", script, dataChannel, clock, nil, nil, false)
	defer checkDeferredErrors(t, tm.Close)()
	defer func() {
		cleanupTest()

		// Force emit should force the last window to emit
		if rc := atomic.LoadInt32(&requestCount); rc != 2 {
			t.Errorf("got %v exp %v", rc, 2)
		}
	}()

	data := make([]edge.PointMessage, len(testValues))
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}
	close(dataChannel)
}

func TestStream_Barrier_Idle_Replay_After_Idle(t *testing.T) {
	// Set up clock to current time - 22 seconds since we're going to send 21 data points
	clock := clock.New(time.Now().UTC().Add(-22 * time.Second))
	clock.Set(time.Now().UTC())
	testValues := make([][]interface{}, 21)
	for i := 0; i < 21; i++ {
		testValues[i] = []interface{}{
			clock.Zero().Add(time.Duration(i) * time.Second),
			rand.Float64(),
		}
	}
	requestCount := int32(0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var er models.Result
		switch rc {
		case 1:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[0:10],
					},
				},
			}
		case 2:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[6:20],
					},
				},
			}
		case 3:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[16:21],
					},
				},
			}
		}
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var period = 14s
var every = 10s
var idle = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
	|barrier().idle(idle)
	|window()
		.period(period)
		.every(every)
	|httpPost('` + ts.URL + `')
`

	dataChannel := make(chan edge.PointMessage)
	tm, _, cleanupTest := testStreamerWithInputChannel(t, "TestStream_Barrier_Idle", script, dataChannel, clock, nil, nil, false)
	defer checkDeferredErrors(t, tm.Close)()
	defer func() {
		cleanupTest()

		// Force emit should force the last window to emit
		if rc := atomic.LoadInt32(&requestCount); rc != 3 {
			t.Errorf("got %v exp %v", rc, 3)
		}
	}()

	data := make([]edge.PointMessage, len(testValues))
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}
	time.Sleep(11 * time.Second)
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}
	close(dataChannel)
}

func TestStream_Barrier_Period_No_Data(t *testing.T) {
	// Set up clock to current time - 22 seconds since we're going to send 21 data points
	clock := clock.New(time.Now().UTC())
	requestCount := int32(0)

	testValues := make([][]interface{}, 1)
	for i := 0; i < 1; i++ {
		testValues[i] = []interface{}{
			clock.Zero().Add(time.Duration(i) * time.Second),
			rand.Float64(),
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
	}))
	defer ts.Close()

	// NOTE: using a slightly higher than 2s idle because timer was going off within a few ms of window period 50% of
	// the time
	var script = `
var period = 14s
var every = 2s
var idle = 2100ms
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
	|barrier().period(idle)
	|window()
		.period(period)
		.every(every)
	|httpPost('` + ts.URL + `')
`

	dataChannel := make(chan edge.PointMessage)
	tm, _, cleanupTest := testStreamerWithInputChannel(t, "TestStream_Barrier_Period_No_Data", script, dataChannel, clock, nil, nil, false)
	defer checkDeferredErrors(t, tm.Close)()
	defer func() {
		cleanupTest()

		// barrier should emit at least 4 times
		if rc := atomic.LoadInt32(&requestCount); rc != 2 {
			t.Errorf("got %v exp %v", rc, 2)
		}
	}()

	// groupedconsumers do not run any logic until at least one message is seen
	data := make([]edge.PointMessage, len(testValues))
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}

	time.Sleep(5 * time.Second)
	close(dataChannel)
}

func TestStream_Barrier_Period(t *testing.T) {
	// Set up clock to current time - 22 seconds since we're going to send 21 data points
	clock := clock.New(time.Now().UTC().Add(-22 * time.Second))
	clock.Set(time.Now().UTC())
	testValues := make([][]interface{}, 21)
	for i := 0; i < 21; i++ {
		testValues[i] = []interface{}{
			clock.Zero().Add(time.Duration(i) * time.Second),
			rand.Float64(),
		}
	}
	requestCount := int32(0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var er models.Result
		switch rc {
		case 1:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[0:10],
					},
				},
			}
		case 2:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[6:20],
					},
				},
			}
		case 3:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[16:21],
					},
				},
			}
		}
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var period = 14s
var every = 10s
var idle = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
	|barrier().period(idle)
	|window()
		.period(period)
		.every(every)
	|httpPost('` + ts.URL + `')
`

	dataChannel := make(chan edge.PointMessage)
	tm, _, cleanupTest := testStreamerWithInputChannel(t, "TestStream_Barrier_Period", script, dataChannel, clock, nil, nil, false)
	defer checkDeferredErrors(t, tm.Close)()
	defer func() {
		cleanupTest()

		// Force emit should force the last window to emit
		if rc := atomic.LoadInt32(&requestCount); rc != 3 {
			t.Errorf("got %v exp %v", rc, 3)
		}
	}()

	data := make([]edge.PointMessage, len(testValues))
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}
	time.Sleep(11 * time.Second)
	close(dataChannel)
}

func TestStream_Barrier_Period_No_Idle(t *testing.T) {
	// Set up clock to current time - 22 seconds since we're going to send 21 data points
	clock := clock.New(time.Now().UTC().Add(-22 * time.Second))
	clock.Set(time.Now().UTC())
	testValues := make([][]interface{}, 21)
	for i := 0; i < 21; i++ {
		testValues[i] = []interface{}{
			clock.Zero().Add(time.Duration(i) * time.Second),
			rand.Float64(),
		}
	}
	requestCount := int32(0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var er models.Result
		switch rc {
		case 1:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[0:10],
					},
				},
			}
		case 2:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[6:20],
					},
				},
			}
		case 3:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[16:21],
					},
				},
			}
		}
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var period = 14s
var every = 10s
var idle = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
	|barrier().period(idle)
	|window()
		.period(period)
		.every(every)
	|httpPost('` + ts.URL + `')
`

	dataChannel := make(chan edge.PointMessage)
	tm, _, cleanupTest := testStreamerWithInputChannel(t, "TestStream_Barrier_Period_No_Idle", script, dataChannel, clock, nil, nil, false)
	defer checkDeferredErrors(t, tm.Close)()
	defer func() {
		cleanupTest()

		// Force emit should force the last window to emit
		if rc := atomic.LoadInt32(&requestCount); rc != 2 {
			t.Errorf("got %v exp %v", rc, 2)
		}
	}()

	data := make([]edge.PointMessage, len(testValues))
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}
	close(dataChannel)
}

func TestStream_Barrier_Period_Replay_After_Idle(t *testing.T) {
	// Set up clock to current time - 22 seconds since we're going to send 21 data points
	clock := clock.New(time.Now().UTC().Add(-22 * time.Second))
	clock.Set(time.Now().UTC())
	testValues := make([][]interface{}, 21)
	for i := 0; i < 21; i++ {
		testValues[i] = []interface{}{
			clock.Zero().Add(time.Duration(i) * time.Second),
			rand.Float64(),
		}
	}
	requestCount := int32(0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var er models.Result
		switch rc {
		case 1:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[0:10],
					},
				},
			}
		case 2:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[6:20],
					},
				},
			}
		case 3:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Columns: []string{"time", "value"},
						Values:  testValues[16:21],
					},
				},
			}
		}
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var period = 14s
var every = 10s
var idle = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('cpu')
	|barrier().period(idle)
	|window()
		.period(period)
		.every(every)
	|httpPost('` + ts.URL + `')
`

	dataChannel := make(chan edge.PointMessage)
	tm, _, cleanupTest := testStreamerWithInputChannel(t, "TestStream_Barrier_Period", script, dataChannel, clock, nil, nil, false)
	defer checkDeferredErrors(t, tm.Close)()
	defer func() {
		cleanupTest()

		// Force emit should force the last window to emit
		if rc := atomic.LoadInt32(&requestCount); rc != 3 {
			t.Errorf("got %v exp %v", rc, 3)
		}
	}()

	data := make([]edge.PointMessage, len(testValues))
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}
	time.Sleep(11 * time.Second)
	for i, testValue := range testValues {
		data[i] = edge.NewPointMessage(
			"cpu",
			"dbname",
			"rpname",
			models.Dimensions{},
			models.Fields{"value": testValue[1]},
			models.Tags{},
			testValue[0].(time.Time),
		)
	}
	for _, point := range data {
		dataChannel <- point
	}
	close(dataChannel)
}

func TestStream_Aggregate_Changing_Type(t *testing.T) {

	var script = `
var period = 10s
var every = 10s
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('m')
	|window()
		.period(period)
		.every(every)
	|where(lambda: "c")
	|count('value')
	|httpOut('TestStream_Aggregate_Changing_Type')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "m",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 20, 0, time.UTC),
						1.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Aggregate_Changing_Type", script, 25*time.Second, er, false, nil)
}

func TestStream_Shift(t *testing.T) {

	var script = `
var period  = 5s

var data  = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')

var past = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')
	|shift(period)

var current = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: "current.count" - "past.count")
		.keep()
		.as('diff')
	|httpOut('TestStream_Shift')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "current.count", "diff", "past.count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					1.0,
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, false, nil)
}

func TestStream_ShiftBatch(t *testing.T) {

	var script = `
var period  = 5s

var data  = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')

var past = data
	|window()
		.period(period)
		.every(period)
		.align()
	|shift(period)
	|count('value')

var current = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: "current.count" - "past.count")
		.keep()
		.as('diff')
	|httpOut('TestStream_Shift')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "current.count", "diff", "past.count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					1.0,
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, false, nil)
}

func TestStream_ShiftNegative(t *testing.T) {

	var script = `
var period  = 5s

var data  = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')

var past = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')

var current = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')
	|shift(-period)

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: "current.count" - "past.count")
		.keep()
		.as('diff')
	|httpOut('TestStream_Shift')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "current.count", "diff", "past.count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
					1.0,
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, false, nil)
}

func TestStream_ShiftBatchNegative(t *testing.T) {

	var script = `
var period  = 5s

var data  = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')

var past = data
	|window()
		.period(period)
		.every(period)
		.align()
	|count('value')

var current = data
	|window()
		.period(period)
		.every(period)
		.align()
	|shift(-period)
	|count('value')

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: "current.count" - "past.count")
		.keep()
		.as('diff')
	|httpOut('TestStream_Shift')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "current.count", "diff", "past.count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
					1.0,
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Shift", script, 15*time.Second, er, false, nil)
}

func TestStream_SimpleMR(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_SimpleMR')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
}

func TestStream_Eval_AllTypes(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: "str" + 'suffix', lambda: !"bool", lambda: "int" + 14, lambda: "float" * 2.0)
		.as( 'str', 'bool', 'int', 'float')
	|httpOut('TestStream_EvalAllTypes')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    nil,
				Columns: []string{"time", "bool", "float", "int", "str"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					true,
					84.0,
					19.0,
					"bobsuffix",
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_EvalAllTypes", script, 2*time.Second, er, false, nil)
}

func TestStream_EvalDivisionByZero(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('data')
	|eval(lambda: 10/"n")
		.as('n')
	|httpOut('TestStream_EvalDivisionByZero')
`

	testStreamerNoOutput(t, "TestStream_EvalDivisionByZero", script, 2*time.Second, nil)

}

func TestStream_Eval_KeepAll(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: "value0" + "value1", lambda: "value0" - "value1")
		.as( 'pos', 'neg')
		.keep()
	|httpOut('TestStream_Eval_Keep')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    nil,
				Columns: []string{"time", "neg", "pos", "value0", "value1"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					-1.0,
					1.0,
					0.0,
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Keep", script, 2*time.Second, er, false, nil)
}

func TestStream_Eval_KeepSome(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: "value0" + "value1", lambda: "value0" - "value1")
		.as( 'pos', 'neg')
		.keep('value0', 'pos', 'neg', 'other')
	|httpOut('TestStream_Eval_KeepSome')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    nil,
				Columns: []string{"time", "neg", "other", "pos", "value0"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					-1.0,
					5.0,
					1.0,
					0.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_KeepSome", script, 2*time.Second, er, false, nil)
}

func TestStream_Eval_KeepSomeWithHidden(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: "value0" + "value1", lambda: "pos" - "value1")
		.as( 'pos', 'zero')
		.keep('value0', 'zero')
	|httpOut('TestStream_Eval_Keep')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    nil,
				Columns: []string{"time", "value0", "zero"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					0.0,
					0.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Keep", script, 2*time.Second, er, false, nil)
}

func TestStream_Eval_Tags(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: string("value"))
		.as('value')
		.tags('value')
	|groupBy('value')
	|httpOut('TestStream_Eval_Tags')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"value": "0"},
				Columns: []string{"time"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				}},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"value": "1"},
				Columns: []string{"time"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Tags", script, 2*time.Second, er, true, nil)
}

func TestStream_Eval_Tags_Keep(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: string("value"))
		.as('value')
		.tags('value')
		.keep()
	|groupBy('value')
	|httpOut('TestStream_Eval_Tags')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"value": "0"},
				Columns: []string{"time", "another", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2.0,
					"0",
				}},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"value": "1"},
				Columns: []string{"time", "another", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2.0,
					"1",
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Tags", script, 2*time.Second, er, true, nil)
}

func TestStream_Eval_Tags_KeepSome(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
	|eval(lambda: string("value"))
		.as('value_tag')
		.tags('value_tag')
		.keep('value', 'another')
	|groupBy('value_tag')
	|httpOut('TestStream_Eval_Tags')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"value_tag": "0"},
				Columns: []string{"time", "another", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2.0,
					0.0,
				}},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"value_tag": "1"},
				Columns: []string{"time", "another", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2.0,
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Tags", script, 2*time.Second, er, true, nil)
}

func TestStream_EvalGroups(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
		.groupBy('group')
	|eval(lambda: count())
		.as('count')
	|httpOut('TestStream_EvalGroups')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"group": "A"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
					},
				},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"group": "B"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_EvalGroups", script, 3*time.Second, er, false, nil)
}

func TestStream_Eval_Time(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('types')
		.groupBy('group')
	|eval(lambda: hour("time"))
		.as('hour')
	|httpOut('TestStream_Eval_Time')
`
	hour := float64(time.Date(1971, 1, 1, 1, 0, 0, 0, time.UTC).Local().Hour())
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "types",
				Tags:    map[string]string{"group": "A"},
				Columns: []string{"time", "hour"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 1, 0, 0, 0, time.UTC),
						hour,
					},
				},
			},
			{
				Name:    "types",
				Tags:    map[string]string{"group": "B"},
				Columns: []string{"time", "hour"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 1, 0, 0, 0, time.UTC),
						hour,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Time", script, 2*time.Hour, er, false, nil)
}

func TestStream_Eval_Missing(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('missing')
	|eval(lambda: "or_not_to_be")
		.as('that_is_the_question')
	|httpOut('TestStream_Eval_Missing')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "missing",
				Tags:    map[string]string{"t": "t1"},
				Columns: []string{"time", "that_is_the_question"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						float64(42),
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Missing", script, 2*time.Hour, er, false, nil)
}

func TestStream_Eval_Missing_isPresent(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('missing')
	|where(lambda: isPresent("or_not_to_be"))
	|eval(lambda: !isPresent("or_not_to_be"))
		.as('that_is_the_question')
	|httpOut('TestStream_Eval_Missing')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "missing",
				Tags:    map[string]string{"t": "t1"},
				Columns: []string{"time", "that_is_the_question"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						false,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Eval_Missing", script, 2*time.Hour, er, false, nil)
}

func TestStream_Default(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
	|default()
		.field('value', 1.0)
		.tag('host', 'serverA')
	|where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|sum('value')
	|httpOut('TestStream_Default')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					57.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Default", script, 15*time.Second, er, false, nil)
}

func TestStream_DefaultEmptyTags(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
	|default()
		.tag('host', '')
	|default()
		.tag('host', 'serverA')
	|default()
		.tag('host', 'serverB')
	|httpOut('TestStream_DefaultEmptyTags')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"cpu": "cpu-total", "host": "serverA"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					9.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_DefaultEmptyTags", script, 15*time.Second, er, false, nil)
}

func TestStream_Delete(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
	|delete()
		.field('anothervalue')
		.tag('type')
	|groupBy(*)
	|httpOut('TestStream_Delete')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					9.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					6.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					3.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Delete", script, 15*time.Second, er, true, nil)
}

func TestStream_Delete_GroupBy(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
		.groupBy('host', 'type')
	|delete()
		.field('anothervalue')
		.tag('type')
	|window()
		.period(2s)
		.every(2s)
	|sum('value')
		.as('value')
	|httpOut('TestStream_Delete_GroupBy')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
					18.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
					12.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
					6.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Delete_GroupBy", script, 15*time.Second, er, true, nil)
}

func TestStream_AllMeasurements(t *testing.T) {

	var script = `
stream
	|from()
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_AllMeasurements')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					23.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_AllMeasurements", script, 15*time.Second, er, false, nil)
}

func TestStream_HttpPost(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var er models.Result
		switch rc {
		case 1:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							97.1,
						}},
					},
				},
			}
		case 2:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
							92.6,
						}},
					},
				},
			}
		case 3:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
							95.6,
						}},
					},
				},
			}
		case 4:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
							93.1,
						}},
					},
				},
			}
		case 5:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							92.6,
						}},
					},
				},
			}
		case 6:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
							95.8,
						}},
					},
				},
			}
		}
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|httpPost('` + ts.URL + `')
	|httpOut('TestStream_HttpPost')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					95.8,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_HttpPost", script, 13*time.Second, er, false, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 6 {
		t.Errorf("got %v exp %v", rc, 6)
	}
}

func TestStream_HttpPost_URL_Template(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		er := struct {
			models.Result
			url string
		}{}
		switch rc {
		case 1:
			er.Result = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle", "cpu": "a"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							97.1,
						}},
					},
				},
			}
			er.url = `/cpu/?host=serverA&cpu=a`
		case 2:
			er.Result = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle", "cpu": "b"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
							92.6,
						}},
					},
				},
			}
			er.url = `/cpu/?host=serverA&cpu=b`
		case 3:
			er.Result = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle", "cpu": "b"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
							95.6,
						}},
					},
				},
			}
			er.url = `/cpu/?host=serverA&cpu=b`
		case 4:
			er.Result = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle", "cpu": "c"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
							93.1,
						}},
					},
				},
			}
			er.url = `/cpu/?host=serverA&cpu=c`
		case 5:
			er.Result = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle", "cpu": "c"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							92.6,
						}},
					},
				},
			}
			er.url = `/cpu/?host=serverA&cpu=c`
		case 6:
			er.Result = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle", "cpu": "a"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
							95.8,
						}},
					},
				},
			}
			er.url = `/cpu/?host=serverA&cpu=a`
		}
		if eq, msg := compareResults(er.Result, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
		if er.url != r.URL.String() {
			t.Errorf("enexpected url for data, expected: %s  got: %s", er.url, r.URL.String())
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|httpPost('` + ts.URL + `/{{ .Name }}/?host={{ index .Tags "host"}}&cpu={{ index .Tags "cpu" }}')
	|httpOut('TestStream_HttpPost_URL_Template')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle", "cpu": "a"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					95.8,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_HttpPost_URL_Template", script, 13*time.Second, er, false, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 6 {
		t.Errorf("got %v exp %v", rc, 6)
	}
}

func TestStream_HttpPostEndpoint(t *testing.T) {
	headers := map[string]string{"my": "header"}
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range headers {
			nv := r.Header.Get(k)
			if nv != v {
				t.Fatalf("got '%s:%s', exp '%s:%s'", k, nv, k, v)
			}
		}
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var er models.Result
		switch rc {
		case 1:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							97.1,
						}},
					},
				},
			}
		case 2:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
							92.6,
						}},
					},
				},
			}
		case 3:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
							95.6,
						}},
					},
				},
			}
		case 4:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
							93.1,
						}},
					},
				},
			}
		case 5:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							92.6,
						}},
					},
				},
			}
		case 6:
			er = models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
							95.8,
						}},
					},
				},
			}
		}
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|httpPost()
	  .endpoint('test')
	  .header('my', 'header')
	|httpOut('TestStream_HttpPost')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					95.8,
				}},
			},
		},
	}

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := httppost.Config{}
		c.URLTemplate = ts.URL
		c.Endpoint = "test"
		sl, _ := httppost.NewService(httppost.Configs{c}, diagService.NewHTTPPostHandler())
		tm.HTTPPostService = sl
	}

	testStreamerWithOutput(t, "TestStream_HttpPost", script, 13*time.Second, er, false, tmInit)

	if rc := atomic.LoadInt32(&requestCount); rc != 6 {
		t.Errorf("got %v exp %v", rc, 6)
	}
}
func TestStream_HttpPostEndpoint_CustomBody(t *testing.T) {
	headers := map[string]string{"my": "header"}
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range headers {
			nv := r.Header.Get(k)
			if nv != v {
				t.Fatalf("got '%s:%s', exp '%s:%s'", k, nv, k, v)
			}
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		got := string(data)
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		var exp string
		switch rc {
		case 1:
			exp = "cpu host=serverA type=idle 1971-01-01 00:00:00 +0000 UTC 97.1"
		case 2:
			exp = "cpu host=serverA type=idle 1971-01-01 00:00:01 +0000 UTC 92.6"
		case 3:
			exp = "cpu host=serverA type=idle 1971-01-01 00:00:02 +0000 UTC 95.6"
		case 4:
			exp = "cpu host=serverA type=idle 1971-01-01 00:00:03 +0000 UTC 93.1"
		case 5:
			exp = "cpu host=serverA type=idle 1971-01-01 00:00:04 +0000 UTC 92.6"
		case 6:
			exp = "cpu host=serverA type=idle 1971-01-01 00:00:05 +0000 UTC 95.8"
		}
		if exp != got {
			t.Errorf("unexpected alert data for request: %d\n%s\n%s\n", rc, exp, got)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|httpPost()
	  .endpoint('test')
	  .header('my', 'header')
	|httpOut('TestStream_HttpPost')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					95.8,
				}},
			},
		},
	}

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := httppost.Config{}
		c.URLTemplate = ts.URL
		c.Endpoint = "test"
		c.RowTemplate = `{{.Name}} host={{index .Tags "host"}} type={{index .Tags "type"}}{{range .Values}} {{index . "time"}} {{index . "value"}}{{end}}`
		sl, _ := httppost.NewService(httppost.Configs{c}, diagService.NewHTTPPostHandler())
		tm.HTTPPostService = sl
	}

	testStreamerWithOutput(t, "TestStream_HttpPost", script, 13*time.Second, er, false, tmInit)

	if rc := atomic.LoadInt32(&requestCount); rc != 6 {
		t.Errorf("got %v exp %v", rc, 6)
	}
}

func TestStream_HttpPostEndpoint_StatusCodes(t *testing.T) {
	headers := map[string]string{"my": "header"}
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range headers {
			nv := r.Header.Get(k)
			if nv != v {
				t.Fatalf("got '%s:%s', exp '%s:%s'", k, nv, k, v)
			}
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)

		switch rc {
		case 1:
			w.WriteHeader(http.StatusOK)
		case 2:
			w.WriteHeader(http.StatusCreated)
		case 3:
			w.WriteHeader(http.StatusNotFound)
		case 4:
			w.WriteHeader(http.StatusForbidden)
		case 5:
			w.WriteHeader(http.StatusInternalServerError)
		case 6:
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|httpPost()
	  .endpoint('test')
	  .header('my', 'header')
	  .codeField('code')
	|window()
		.every(5s)
		.period(5s)
	|httpOut('TestStream_HttpPost')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "code", "type", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						200.0,
						"idle",
						97.1,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						201.0,
						"idle",
						92.6,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						404.0,
						"idle",
						95.6,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						403.0,
						"idle",
						93.1,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						500.0,
						"idle",
						92.6,
					},
				},
			},
		},
	}

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := httppost.Config{}
		c.URLTemplate = ts.URL
		c.Endpoint = "test"
		sl, _ := httppost.NewService(httppost.Configs{c}, diagService.NewHTTPPostHandler())
		tm.HTTPPostService = sl
	}

	testStreamerWithOutput(t, "TestStream_HttpPost", script, 13*time.Second, er, false, tmInit)

	if rc := atomic.LoadInt32(&requestCount); rc != 6 {
		t.Errorf("got %v exp %v", rc, 6)
	}
}

func TestStream_HttpOutPassThrough(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('unused')
	|httpOut('TestStream_SimpleMR')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
}

func TestStream_BatchGroupBy(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
	|window()
		.period(5s)
		.every(5s)
	|groupBy('host')
	|count('value')
	|httpOut('TestStream_BatchGroupBy')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_BatchGroupBy", script, 15*time.Second, er, true, nil)
}

func TestStream_BatchGroupByAll(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
	|window()
		.period(5s)
		.every(5s)
	|groupBy(*)
	|count('value')
	|httpOut('TestStream_BatchGroupBy')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB", "type": "idle"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					5.0,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverC", "type": "idle"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_BatchGroupBy", script, 15*time.Second, er, true, nil)
}

func TestStream_BatchGroupByAllExclude(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
	|window()
		.period(5s)
		.every(5s)
	|groupBy(*)
		.exclude('host')
	|count('value')
	|httpOut('TestStream_BatchGroupBy')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"type": "idle"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
					11.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_BatchGroupBy", script, 15*time.Second, er, true, nil)
}

func TestStream_GroupByAllExclude(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('mock')
	|groupBy(*)
		.exclude('s')
	|window()
		.period(2s)
		.every(2s)
	|count('value')
	|httpOut('TestStream_GroupByExclude')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "mock",
				Tags:    map[string]string{"t": "A"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
					4.0,
				}},
			},
			{
				Name:    "mock",
				Tags:    map[string]string{"t": "B"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
					4.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_GroupByExclude", script, 5*time.Second, er, true, nil)
}

func TestStream_SimpleWhere(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.where(lambda: "host" != 'serverB')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|where(lambda: "count" > 0)
	|where(lambda: "count" < 12)
	|httpOut('TestStream_SimpleMR')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
}

func TestStream_Where_NoSideEffect(t *testing.T) {

	var script = `
var data = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.where(lambda: "host" != 'serverB')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|where(lambda: "count" > 0)

// Unused where clause should not side-effect
data
	|where(lambda: FALSE)

data
	|httpOut('TestStream_SimpleMR')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
}

func TestStream_VarWhereString(t *testing.T) {

	var script = `
var serverStr = 'serverA'
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == serverStr )
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_SimpleMR')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
}

func TestStream_VarWhereRegex(t *testing.T) {

	var script = `
var serverPattern = /^serverA$/
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" =~ serverPattern )
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_SimpleMR')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_SimpleMR", script, 15*time.Second, er, false, nil)
}

func TestStream_GroupBy(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
	|sum('value')
	|httpOut('TestStream_GroupBy')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					47.0,
				}},
			},
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					45.0,
				}},
			},
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					32.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_GroupBy", script, 13*time.Second, er, false, nil)
}

func TestStream_GroupByWhere(t *testing.T) {

	var script = `
var serverA = stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')

var byCpu = serverA
	|groupBy('host', 'cpu')

var total = serverA
	|where(lambda: "cpu" == 'cpu-total')

byCpu
	|join(total)
		.on('host')
		.as('cpu', 'total')
	|eval(lambda: "cpu.value" / "total.value")
		.as('cpu_percent')
	|window()
		.period(10s)
		.every(10s)
	|mean('cpu_percent')
	|httpOut('TestStream_GroupByWhere')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"cpu": "cpu0", "host": "serverA"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.7823116704593873,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"cpu": "cpu1", "host": "serverA"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.7676074281820646,
				}},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"cpu": "cpu-total", "host": "serverA"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					1.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_GroupByWhere", script, 13*time.Second, er, true, nil)
}

func TestStream_GroupByMeasurement(t *testing.T) {

	var script = `
stream
	|from()
		.groupBy('service')
		.groupByMeasurement()
	|window()
		.period(10s)
		.every(10s)
	|sum('value')
	|httpOut('TestStream_GroupByMeasurement')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					47.0,
				}},
			},
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					45.0,
				}},
			},
			{
				Name:    "errors",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					32.0,
				}},
			},
			{
				Name:    "disk",
				Tags:    map[string]string{"service": "sda"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					810.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_GroupByMeasurement", script, 13*time.Second, er, true, nil)
}

func TestStream_Flatten(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|flatten()
		.on('service', 'host')
		.tolerance(1s)
    |httpOut('TestStream_Flatten')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A"},
				Columns: []string{"time", "auth.server01.value", "auth.server02.value", "cart.server01.value", "cart.server02.value", "log.server01.value", "log.server02.value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					700.0,
					702.0,
					800.0,
					802.0,
					600.0,
					602.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B"},
				Columns: []string{"time", "auth.server01.value", "auth.server02.value", "cart.server01.value", "cart.server02.value", "log.server01.value", "log.server02.value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					750.0,
					752.0,
					850.0,
					852.0,
					650.0,
					652.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Flatten", script, 13*time.Second, er, true, nil)
}

func TestStream_FlattenDropOriginalFieldName(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|flatten()
		.on('service', 'host')
		.tolerance(1s)
		.dropOriginalFieldName()
		|httpOut('TestStream_Flatten')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A"},
				Columns: []string{"time", "auth.server01", "auth.server02", "cart.server01", "cart.server02", "log.server01", "log.server02"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					700.0,
					702.0,
					800.0,
					802.0,
					600.0,
					602.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B"},
				Columns: []string{"time", "auth.server01", "auth.server02", "cart.server01", "cart.server02", "log.server01", "log.server02"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					750.0,
					752.0,
					850.0,
					852.0,
					650.0,
					652.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Flatten", script, 13*time.Second, er, true, nil)
}

func TestStream_Combine_All(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|combine(lambda: TRUE, lambda: TRUE)
		.as('first', 'second')
		.tolerance(1s)
		.delimiter('.')
	|groupBy('first.service', 'second.service', 'dc')
	|eval(lambda: "first.value" / "second.value")
		.as('ratio')
    |httpOut('TestStream_Combine')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "second.service": "log", "first.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 6.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "second.service": "cart", "first.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 8.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "second.service": "cart", "first.service": "log"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					6.0 / 8.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "second.service": "log", "first.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.5 / 6.5,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "second.service": "cart", "first.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.5 / 8.5,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "second.service": "cart", "first.service": "log"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					6.5 / 8.5,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Combine", script, 13*time.Second, er, true, nil)
}

func TestStream_Combine_Filtered(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|combine(lambda: "service" == 'auth', lambda: TRUE)
		.as('auth', 'other')
		.tolerance(1s)
		.delimiter('.')
	|groupBy('other.service','dc')
	|eval(lambda: "auth.value" / "other.value")
		.as('ratio')
    |httpOut('TestStream_Combine')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "other.service": "log", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 6.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "other.service": "cart", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 8.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "other.service": "log", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.5 / 6.5,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "other.service": "cart", "auth.service": "auth"},
				Columns: []string{"time", "ratio"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.5 / 8.5,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Combine", script, 13*time.Second, er, true, nil)
}

func TestStream_Combine_All_Triples(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('request_latency')
		.groupBy('dc')
	|combine(lambda: TRUE, lambda: TRUE, lambda: TRUE)
		.as('first', 'second', 'third')
		.tolerance(1s)
		.delimiter('.')
	|groupBy('first.service', 'second.service', 'third.service', 'dc')
	|eval(lambda: "first.value" + "second.value" + "third.value")
		.as('sum')
    |httpOut('TestStream_Combine')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "A", "first.service": "auth", "second.service": "log", "third.service": "cart"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2100.0,
				}},
			},
			{
				Name:    "request_latency",
				Tags:    map[string]string{"dc": "B", "first.service": "auth", "second.service": "log", "third.service": "cart"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					2250.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Combine", script, 13*time.Second, er, true, nil)
}

func TestStream_Join(t *testing.T) {

	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.streamName('error_view')
	|eval(lambda: "errors.sum" / "views.sum")
		.as('error_percent')
		.keep()
	|httpOut('TestStream_Join')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "error_percent", "errors.sum", "views.sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					47.0,
					4700.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "error_percent", "errors.sum", "views.sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					45.0,
					4500.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "error_percent", "errors.sum", "views.sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					32.0,
					3200.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Join", script, 13*time.Second, er, true, nil)
}

func TestStream_Delete_Join(t *testing.T) {
	var script = `
var errorCounts = stream
	|from()
		.measurement('cpu')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
		.align()

	|sum('value')
	|barrier()
		.idle(1s)
		.delete(TRUE)

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')
	|barrier()
		.idle(1s)
		.delete(TRUE)

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.streamName('error_view')
		.tolerance(2s)
		.deleteAll(TRUE)
	|eval(lambda: "errors.sum" / "views.sum")
		.as('error_percent')
		.keep()
	|httpOut('TestStream_Delete_Join')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "error_percent", "errors.sum", "views.sum"},
				Values: [][]interface{}{
					{time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC), 1.0, 18.0, 18.0},
				},
			},
		},
	}
	testStreamerWithOutput(t, "TestStream_Delete_Join", script, 30*time.Second, er, true, nil)
}

func TestStream_Join_Delimiter(t *testing.T) {

	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.delimiter('#')
		.streamName('error_view')
	|eval(lambda: "errors#sum" / "views#sum")
		.as('error_percent')
		.keep()
	|httpOut('TestStream_Join')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "error_percent", "errors#sum", "views#sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					47.0,
					4700.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "error_percent", "errors#sum", "views#sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					45.0,
					4500.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "error_percent", "errors#sum", "views#sum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					32.0,
					3200.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Join", script, 13*time.Second, er, true, nil)
}
func TestStream_Join_DelimiterEmpty(t *testing.T) {

	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.delimiter('')
		.streamName('error_view')
	|eval(lambda: "errorssum" / "viewssum")
		.as('error_percent')
		.keep()
	|httpOut('TestStream_Join')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "error_percent", "errorssum", "viewssum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					47.0,
					4700.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "error_percent", "errorssum", "viewssum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					45.0,
					4500.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "error_percent", "errorssum", "viewssum"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					0.01,
					32.0,
					3200.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Join", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinTolerance(t *testing.T) {

	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.tolerance(2s)
		.streamName('error_view')
	|window()
		.period(10s)
		.every(10s)
	|httpOut('TestStream_JoinTolerance')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "errors.value", "views.value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						7.0,
						700.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						9.0,
						900.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						3.0,
						300.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						11.0,
						1100.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						12.0,
						1200.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						6.0,
						600.0,
					},
				},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "errors.value", "views.value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						9.0,
						900.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						5.0,
						500.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						9.0,
						900.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						2.0,
						200.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						7.0,
						700.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						10.0,
						1000.0,
					},
				},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "errors.value", "views.value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						2.0,
						200.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						9.0,
						900.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						7.0,
						700.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						5.0,
						500.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						4.0,
						400.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						6.0,
						600.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
						4.0,
						400.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinTolerance", script, 13*time.Second, er, true, nil)
}

func TestStream_Join_Fill_Null(t *testing.T) {
	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.fill('null')
		.streamName('error_view')
	|default()
		.field('errors.value', 0.0)
		.field('views.value', 0.0)
	|eval(lambda:  "errors.value" + "views.value")
		.as('error_percent')
	|window()
		.period(10s)
		.every(10s)
	|count('error_percent')
	|httpOut('TestStream_Join_Fill')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					8.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Join_Fill", script, 13*time.Second, er, true, nil)
}

func TestStream_Join_Fill_Num(t *testing.T) {
	var script = `
var errorCounts = stream
	|from()
		.measurement('errors')
		.groupBy('service')

var viewCounts = stream
	|from()
		.measurement('views')
		.groupBy('service')

errorCounts
	|join(viewCounts)
		.as('errors', 'views')
		.fill(0.0)
		.streamName('error_view')
	|eval(lambda:  "errors.value" + "views.value")
		.as('error_percent')
	|window()
		.period(10s)
		.every(10s)
	|count('error_percent')
	|httpOut('TestStream_Join_Fill')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "cartA"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "login"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
				}},
			},
			{
				Name:    "error_view",
				Tags:    map[string]string{"service": "front"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					8.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Join_Fill", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinN(t *testing.T) {

	var script = `
var cpu = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == 'total')
var mem = stream
	|from()
		.measurement('memory')
		.where(lambda: "type" == 'free')
var disk = stream
	|from()
		.measurement('disk')
		.where(lambda: "device" == 'sda')

cpu
	|join(mem, disk)
		.as('cpu', 'mem', 'disk')
		.streamName('magic')
		.fill(0.0)
	|window()
		.period(10s)
		.every(10s)
	|count('cpu.value')
	|httpOut('TestStream_JoinN')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "magic",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinN", script, 15*time.Second, er, false, nil)
}

func TestStream_JoinOn(t *testing.T) {
	var script = `
var errorsByServiceDC = stream
	|from()
		.measurement('errors')
		.groupBy('dc', 'service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

var errorsByServiceGlobal = stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

errorsByServiceGlobal
	|join(errorsByServiceDC)
		.as('service', 'dc')
		.on('service')
		.streamName('dc_error_percent')
	|eval(lambda: "dc.sum" / "service.sum")
		.keep()
		.as('value')
	|httpOut('TestStream_JoinOn')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "cartA"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					15.0,
					47.0,
					15.0 / 47.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "cartA"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					32.0,
					47.0,
					32.0 / 47.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "login"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					15.0,
					45.0,
					15.0 / 45.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "login"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					23.0,
					45.0,
					23.0 / 45.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "C", "service": "login"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
					45.0,
					7.0 / 45.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "front"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					19.0,
					32.0,
					19.0 / 32.0,
				}},
			},
			{
				Name:    "dc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "front"},
				Columns: []string{"time", "dc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					13.0,
					32.0,
					13.0 / 32.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinOnGap(t *testing.T) {
	var script = `
var errorsByServiceDCRack = stream
	|from()
		.measurement('errors')
		.groupBy('dc', 'service', 'rack')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

var errorsByServiceGlobal = stream
	|from()
		.measurement('errors')
		.groupBy('service')
	|window()
		.period(10s)
		.every(10s)
		.align()
	|sum('value')

errorsByServiceGlobal
	|join(errorsByServiceDCRack)
		.as('service', 'loc')
		.on('service')
		.streamName('loc_error_percent')
	|eval(lambda: "loc.sum" / "service.sum")
		.keep()
		.as('value')
	|httpOut('TestStream_JoinOn')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "cartA", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					47.0,
					10.0 / 47.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "cartA", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					47.0,
					5.0 / 47.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "cartA", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					14.0,
					47.0,
					14.0 / 47.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "cartA", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					18.0,
					47.0,
					18.0 / 47.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "login", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					45.0,
					5.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "login", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					45.0,
					10.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "login", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					3.0,
					45.0,
					3.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "login", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					20.0,
					45.0,
					20.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "C", "service": "login", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					7.0,
					45.0,
					7.0 / 45.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "front", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					9.0,
					32.0,
					9.0 / 32.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "A", "service": "front", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					32.0,
					10.0 / 32.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "front", "rack": "0"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					4.0,
					32.0,
					4.0 / 32.0,
				}},
			},
			{
				Name:    "loc_error_percent",
				Tags:    map[string]string{"dc": "B", "service": "front", "rack": "1"},
				Columns: []string{"time", "loc.sum", "service.sum", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					9.0,
					32.0,
					9.0 / 32.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinOn_AcrossMeasurement(t *testing.T) {
	var script = `
var building = stream
    |from()
        .measurement('building_power')
        .groupBy('building')

var floor = stream
    |from()
        .measurement('floor_power')
        .groupBy('building', 'floor')

building
    |join(floor)
        .as('building', 'floor')
        .on('building')
        .streamName('power_floor_percentage')
	|log()
		.prefix('JOINED')
    |eval(lambda: "floor.value" / "building.value")
        .as('value')
    |httpOut('TestStream_JoinOn_AcrossMeasurement')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "shack", "floor": "1"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					7.0 / 30.0,
				}},
			},
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "shack", "floor": "2"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					11.0 / 30.0,
				}},
			},
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "shack", "floor": "3"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					12.0 / 30.0,
				}},
			},
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "hut", "floor": "1"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					19.0 / 40.0,
				}},
			},
			{
				Name:    "power_floor_percentage",
				Tags:    map[string]string{"building": "hut", "floor": "2"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
					21.0 / 40.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn_AcrossMeasurement", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinOn_Fill_Num(t *testing.T) {
	var script = `
var maintlock = stream
    |from()
        .measurement('maintlock')
        .groupBy('host')

stream
    |from()
        .measurement('disk')
        .groupBy('host', 'path')
    |join(maintlock)
        .as('disk', 'maintlock')
        .on('host')
        .fill(0.0)
        .tolerance(1s)
        .streamName('disk')
    |default()
        .field('maintlock.count', 0)
    |window()
        .period(4s)
        .every(4s)
    |httpOut('TestStream_JoinOn_Fill')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "disk",
				Tags:    map[string]string{"host": "A", "path": "/"},
				Columns: []string{"time", "disk.used_percent", "maintlock.count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						50.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						60.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						70.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						80.0,
						1.0,
					},
				},
			},
			{
				Name:    "disk",
				Tags:    map[string]string{"host": "A", "path": "/tmp"},
				Columns: []string{"time", "disk.used_percent", "maintlock.count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						40.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						30.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						20.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						10.0,
						1.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn_Fill", script, 13*time.Second, er, true, nil)
}

func TestStream_JoinOn_Fill_Null(t *testing.T) {
	var script = `
var maintlock = stream
    |from()
        .measurement('maintlock')
        .groupBy('host')

stream
    |from()
        .measurement('disk')
        .groupBy('host', 'path')
    |join(maintlock)
        .as('disk', 'maintlock')
        .on('host')
        .fill('null')
        .tolerance(1s)
        .streamName('disk')
    |default()
        .field('maintlock.count', 0)
        .field('disk.used_percent', 0)
    |window()
        .period(4s)
        .every(4s)
    |httpOut('TestStream_JoinOn_Fill')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "disk",
				Tags:    map[string]string{"host": "A", "path": "/"},
				Columns: []string{"time", "disk.used_percent", "maintlock.count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						50.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						60.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						70.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						80.0,
						1.0,
					},
				},
			},
			{
				Name:    "disk",
				Tags:    map[string]string{"host": "A", "path": "/tmp"},
				Columns: []string{"time", "disk.used_percent", "maintlock.count"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						40.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						30.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						20.0,
						0.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						10.0,
						1.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_JoinOn_Fill", script, 13*time.Second, er, true, nil)
}

func TestStream_Union(t *testing.T) {

	var script = `
var cpuT = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == 'total')
var cpu0 = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == '0')
var cpu1 = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == '1')

cpuT
	|union(cpu0, cpu1)
		.rename('cpu_all')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|httpOut('TestStream_Union')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu_all",
				Tags:    nil,
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					20.0,
				}},
			},
		},
	}
	testStreamerWithOutput(t, "TestStream_Union", script, 15*time.Second, er, false, nil)
}

func TestStream_Union_Stepped(t *testing.T) {
	var script = `
var cpuT = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == 'total')
var cpu0 = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == '0')
var cpu1 = stream
	|from()
		.measurement('cpu')
		.where(lambda: "cpu" == '1')

cpuT
	|union(cpu0, cpu1)
		.rename('cpu_all')
	|groupBy('cpu')
	|httpOut('TestStream_Union_Stepped')
`

	steps := []step{
		{
			t: time.Second,
		},
		{
			t: 3 * time.Second,
			er: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "0"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
							98.0,
						}},
					},
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "1"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
							92.0,
						}},
					},
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "total"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
							92.0,
						}},
					},
				},
			},
		},
		{
			t: 6 * time.Second,
			er: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "0"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
							92.0,
						}},
					},
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "1"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
							92.0,
						}},
					},
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "total"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							93.0,
						}},
					},
				},
			},
		},
		{
			t: 15 * time.Second,
			er: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "0"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
							96.0,
						}},
					},
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "1"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
							93.0,
						}},
					},
					{
						Name:    "cpu_all",
						Tags:    map[string]string{"cpu": "total"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
							96.0,
						}},
					},
				},
			},
		},
	}
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu_all",
				Tags:    map[string]string{"cpu": "0"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
					95.0,
				}},
			},
			{
				Name:    "cpu_all",
				Tags:    map[string]string{"cpu": "1"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
					95.0,
				}},
			},
			{
				Name:    "cpu_all",
				Tags:    map[string]string{"cpu": "total"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					96.0,
				}},
			},
		},
	}

	testStreamerWithSteppedOutput(t, "TestStream_Union_Stepped", script, steps, er, true, nil)
}

func TestStream_InfluxQL_Float(t *testing.T) {

	type testCase struct {
		Method        string
		Args          string
		ER            models.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|{{ .Method }}({{ .Args }})
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	|httpOut('TestStream_InfluxQL_Float')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "sum",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "sum"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							940.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "count",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "count"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							10.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "distinct",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								98.0,
							},
							{
								endTime,
								91.0,
							},
							{
								endTime,
								95.0,
							},
							{
								endTime,
								93.0,
							},
							{
								endTime,
								92.0,
							},
							{
								endTime,
								96.0,
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "mean",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "mean"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							94.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "median",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "median"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							94.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "mode",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "mode"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "min",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "min"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
							91.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "min",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "min"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							91.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "max",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "max"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "max",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "max"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "spread",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "spread"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							7.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "stddev",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "stddev"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							2.160246899469287,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "first",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "first",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "last",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "last",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "percentile",
			Args:   "'value', 50.0",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "percentile"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							93.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "top",
			UsePointTimes: true,
			Args:          "2, 'value'",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "top", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								98.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								96.0,
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "top",
			Args:   "2, 'value'",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "top", "type"},
						Values: [][]interface{}{
							{
								endTime,
								98.0,
								"idle",
							},
							{
								endTime,
								96.0,
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method:        "bottom",
			UsePointTimes: true,
			Args:          "3, 'value'",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "bottom", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								91.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								92.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								92.0,
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "bottom",
			Args:   "3, 'value'",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "bottom", "type"},
						Values: [][]interface{}{
							{
								endTime,
								91.0,
								"idle",
							},
							{
								endTime,
								92.0,
								"idle",
							},
							{
								endTime,
								92.0,
								"idle",
							},
						},
					},
				},
			},
		},
	}

	tmpl, err := template.New("script").Parse(scriptTmpl)
	if err != nil {
		t.Fatal(err)
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%d", tc.Method, i), func(t *testing.T) {
			t.Log("Method:", tc.Method)
			var script bytes.Buffer
			if tc.Args == "" {
				tc.Args = "'value'"
			}
			tmpl.Execute(&script, tc)
			testStreamerWithOutput(
				t,
				"TestStream_InfluxQL_Float",
				script.String(),
				13*time.Second,
				tc.ER,
				false,
				nil,
			)
		})
	}
}

func TestStream_InfluxQL_Integer(t *testing.T) {
	type testCase struct {
		Method        string
		Args          string
		ER            models.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|{{ .Method }}({{ .Args }})
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	|httpOut('TestStream_InfluxQL_Integer')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "sum",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "sum"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							940.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "count",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "count"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							10.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "distinct",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								98.0,
							},
							{
								endTime,
								91.0,
							},
							{
								endTime,
								95.0,
							},
							{
								endTime,
								93.0,
							},
							{
								endTime,
								92.0,
							},
							{
								endTime,
								96.0,
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "mean",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "mean"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							94.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "median",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "median"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							94.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "mode",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "mode"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "min",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "min"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
							91.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "min",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "min"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							91.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "max",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "max"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "max",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "max"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "spread",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "spread"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							7.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "stddev",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "stddev"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							2.160246899469287,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "first",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "first",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							98.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "last",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "last",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							95.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "percentile",
			Args:   "'value', 50.0",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "percentile"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							93.0,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "top",
			UsePointTimes: true,
			Args:          "2, 'value'",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "top", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								98.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								96.0,
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "top",
			Args:   "2, 'value'",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "top", "type"},
						Values: [][]interface{}{
							{
								endTime,
								98.0,
								"idle",
							},
							{
								endTime,
								96.0,
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method:        "bottom",
			UsePointTimes: true,
			Args:          "3, 'value'",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "bottom", "type"},
						Values: [][]interface{}{
							{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								91.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								92.0,
								"idle",
							},
							{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								92.0,
								"idle",
							},
						},
					},
				},
			},
		},
		testCase{
			Method: "bottom",
			Args:   "3, 'value'",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "bottom", "type"},
						Values: [][]interface{}{
							{
								endTime,
								91.0,
								"idle",
							},
							{
								endTime,
								92.0,
								"idle",
							},
							{
								endTime,
								92.0,
								"idle",
							},
						},
					},
				},
			},
		},
	}

	tmpl, err := template.New("script").Parse(scriptTmpl)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range testCases {
		t.Log("Method:", tc.Method)
		var script bytes.Buffer
		if tc.Args == "" {
			tc.Args = "'value'"
		}
		tmpl.Execute(&script, tc)
		testStreamerWithOutput(
			t,
			"TestStream_InfluxQL_Integer",
			script.String(),
			13*time.Second,
			tc.ER,
			false,
			nil,
		)
	}
}
func TestStream_InfluxQL_String(t *testing.T) {
	type testCase struct {
		Method        string
		Args          string
		ER            models.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|{{ .Method }}({{ .Args }})
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	|httpOut('TestStream_InfluxQL_String')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "count",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "count"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							10.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "distinct",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								"98",
							},
							{
								endTime,
								"91",
							},
							{
								endTime,
								"95",
							},
							{
								endTime,
								"93",
							},
							{
								endTime,
								"92",
							},
							{
								endTime,
								"96",
							},
						},
					},
				},
			},
		},
		testCase{
			Method:        "first",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							"98",
						}},
					},
				},
			},
		},
		testCase{
			Method: "first",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							"98",
						}},
					},
				},
			},
		},
		testCase{
			Method:        "last",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
							"95",
						}},
					},
				},
			},
		},
		testCase{
			Method: "last",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							"95",
						}},
					},
				},
			},
		},
	}

	tmpl, err := template.New("script").Parse(scriptTmpl)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range testCases {
		t.Log("Method:", tc.Method)
		var script bytes.Buffer
		if tc.Args == "" {
			tc.Args = "'value'"
		}
		tmpl.Execute(&script, tc)
		testStreamerWithOutput(
			t,
			"TestStream_InfluxQL_String",
			script.String(),
			13*time.Second,
			tc.ER,
			false,
			nil,
		)
	}
}

func TestStream_InfluxQL_Boolean(t *testing.T) {
	type testCase struct {
		Method        string
		Args          string
		ER            models.Result
		UsePointTimes bool
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|{{ .Method }}({{ .Args }})
		{{ if .UsePointTimes }}.usePointTimes(){{ end }}
	|httpOut('TestStream_InfluxQL_Boolean')
`
	endTime := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
	testCases := []testCase{
		testCase{
			Method: "count",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "count"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							10.0,
						}},
					},
				},
			},
		},
		testCase{
			Method: "distinct",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    models.Tags{"host": "serverA"},
						Columns: []string{"time", "distinct"},
						Values: [][]interface{}{
							{
								endTime,
								false,
							},
							{
								endTime,
								true,
							},
						},
					},
				},
			},
		},
		testCase{
			Method:        "first",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
							false,
						}},
					},
				},
			},
		},
		testCase{
			Method: "first",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "first"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							false,
						}},
					},
				},
			},
		},
		testCase{
			Method:        "last",
			UsePointTimes: true,
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
							true,
						}},
					},
				},
			},
		},
		testCase{
			Method: "last",
			ER: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "last"},
						Values: [][]interface{}{[]interface{}{
							endTime,
							true,
						}},
					},
				},
			},
		},
	}

	tmpl, err := template.New("script").Parse(scriptTmpl)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range testCases {
		t.Log("Method:", tc.Method)
		var script bytes.Buffer
		if tc.Args == "" {
			tc.Args = "'value'"
		}
		tmpl.Execute(&script, tc)
		testStreamerWithOutput(
			t,
			"TestStream_InfluxQL_Boolean",
			script.String(),
			13*time.Second,
			tc.ER,
			false,
			nil,
		)
	}
}

func TestStream_CustomFunctions(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	@customFunc()
		.opt1('count')
		.opt2(FALSE, 1, 1.0, '1.0', 1s)
	|httpOut('TestStream_CustomFunctions')
`

	udfService := UDFService{}
	udfService.ListFunc = func() []string {
		return []string{"customFunc"}
	}
	udfService.InfoFunc = func(name string) (info udf.Info, ok bool) {
		if name != "customFunc" {
			return
		}
		info.Wants = agent.EdgeType_STREAM
		info.Provides = agent.EdgeType_STREAM
		info.Options = map[string]*agent.OptionInfo{
			"opt1": {
				ValueTypes: []agent.ValueType{agent.ValueType_STRING},
			},
			"opt2": {
				ValueTypes: []agent.ValueType{
					agent.ValueType_BOOL,
					agent.ValueType_INT,
					agent.ValueType_DOUBLE,
					agent.ValueType_STRING,
					agent.ValueType_DURATION,
				},
			},
		}
		return
	}
	uio := udf_test.NewIO()
	udfService.CreateFunc = func(name, taskID, nodeID string, d udf.Diagnostic, abortCallback func()) (udf.Interface, error) {
		if name != "customFunc" {
			return nil, fmt.Errorf("unknown function %s", name)
		}
		return udf_test.New(taskID, nodeID, uio, d), nil
	}

	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.UDFService = udfService
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		req := <-uio.Requests
		i, ok := req.Message.(*agent.Request_Init)
		if !ok {
			t.Error("expected init message")
		}
		init := i.Init
		if got, exp := init.TaskID, "TestStream_CustomFunctions"; got != exp {
			t.Errorf("unexpected task ID got %q exp %q", got, exp)
		}
		if got, exp := init.NodeID, "customFunc4"; got != exp {
			t.Errorf("unexpected task ID got %q exp %q", got, exp)
		}

		if got, exp := len(init.Options), 2; got != exp {
			t.Errorf("unexpected number of options in init request, got %d exp %d", got, exp)
		}
		for i, opt := range init.Options {
			exp := &agent.Option{}
			switch i {
			case 0:
				exp.Name = "opt1"
				exp.Values = []*agent.OptionValue{
					{
						Type:  agent.ValueType_STRING,
						Value: &agent.OptionValue_StringValue{StringValue: "count"},
					},
				}
			case 1:
				exp.Name = "opt2"
				exp.Values = []*agent.OptionValue{
					{
						Type:  agent.ValueType_BOOL,
						Value: &agent.OptionValue_BoolValue{BoolValue: false},
					},
					{
						Type:  agent.ValueType_INT,
						Value: &agent.OptionValue_IntValue{IntValue: 1},
					},
					{
						Type:  agent.ValueType_DOUBLE,
						Value: &agent.OptionValue_DoubleValue{DoubleValue: 1.0},
					},
					{
						Type:  agent.ValueType_STRING,
						Value: &agent.OptionValue_StringValue{StringValue: "1.0"},
					},
					{
						Type:  agent.ValueType_DURATION,
						Value: &agent.OptionValue_DurationValue{DurationValue: int64(time.Second)},
					},
				}
			}
			if !reflect.DeepEqual(exp, opt) {
				t.Errorf("unexpected init option %d\ngot %v\nexp %v", i, opt, exp)
			}
		}

		resp := &agent.Response{
			Message: &agent.Response_Init{
				Init: &agent.InitResponse{
					Success: true,
				},
			},
		}
		uio.Responses <- resp

		// read all requests and wait till the chan is closed
		for req := range uio.Requests {
			p, ok := req.Message.(*agent.Request_Point)
			if ok {
				pt := p.Point
				resp := &agent.Response{
					Message: &agent.Response_Point{
						Point: &agent.Point{
							Name:         pt.Name,
							Time:         pt.Time,
							Group:        pt.Group,
							Tags:         pt.Tags,
							FieldsDouble: map[string]float64{"customField": 42.0},
						},
					},
				}
				uio.Responses <- resp
			}
		}

		close(uio.Responses)

		if err := <-uio.ErrC; err != nil {
			t.Error(err)
		}
	}()

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"time", "customField"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					42.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_CustomFunctions", script, 15*time.Second, er, false, tmInit)
	<-done
}

func TestStream_Alert(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		expAd := alert.Data{
			ID:          "kapacitor/cpu/serverA",
			Message:     "kapacitor/cpu/serverA is CRITICAL",
			Details:     "details",
			Time:        time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
			Level:       alert.Critical,
			Recoverable: true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA"},
						Columns: []string{"time", "count"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
							10.0,
						}},
					},
				},
			},
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var infoThreshold = 6.0
var warnThreshold = 7.0
var critThreshold = 8.0

stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.idField('id')
		.idTag('id')
		.levelField('level')
		.messageField('msg')
		.levelTag('level')
		.info(lambda: "count" > infoThreshold)
		.warn(lambda: "count" > warnThreshold)
		.crit(lambda: "count" > critThreshold)
		.post('` + ts.URL + `')
	|httpOut('TestStream_Alert')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "level": "CRITICAL", "id": "kapacitor/cpu/serverA"},
				Columns: []string{"time", "count", "id", "level", "msg"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					10.0,
					"kapacitor/cpu/serverA",
					"CRITICAL",
					"kapacitor/cpu/serverA is CRITICAL",
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert", script, 13*time.Second, er, false, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 1 {
		t.Errorf("got %v exp %v", rc, 1)
	}
}

func TestStream_Alert_NoRecoveries(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		var expAd alert.Data
		switch rc {
		case 1:
			expAd = alert.Data{
				ID:          "kapacitor/cpu/serverA",
				Message:     "kapacitor/cpu/serverA is WARNING",
				Time:        time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Duration:    0,
				Level:       alert.Warning,
				Recoverable: false,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								7.0,
							}},
						},
					},
				},
			}
		case 2:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Time:          time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
				Duration:      0,
				Level:         alert.Info,
				PreviousLevel: alert.Warning,
				Recoverable:   false,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								6.0,
							}},
						},
					},
				},
			}
		case 3:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Time:          time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
				Duration:      time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Info,
				Recoverable:   false,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
								7.0,
							}},
						},
					},
				},
			}
		case 4:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Time:          time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration:      2 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Warning,
				Recoverable:   false,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								7.0,
							}},
						},
					},
				},
			}
		case 5:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is CRITICAL",
				Time:          time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
				Duration:      3 * time.Second,
				Level:         alert.Critical,
				PreviousLevel: alert.Warning,
				Recoverable:   false,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								8.0,
							}},
						},
					},
				},
			}
		case 6:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Time:          time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
				Duration:      0,
				Level:         alert.Info,
				PreviousLevel: alert.Critical,
				Recoverable:   false,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								6.0,
							}},
						},
					},
				},
			}
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var infoThreshold = 6.0
var warnThreshold = 7.0
var critThreshold = 8.0

stream
	|from()
		.measurement('cpu')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('')
		.info(lambda: "value" >= infoThreshold)
		.warn(lambda: "value" >= warnThreshold)
		.crit(lambda: "value" >= critThreshold)
		.noRecoveries()
		.post('` + ts.URL + `')
	|httpOut('TestStream_Alert_NoRecoveries')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
					6.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert_NoRecoveries", script, 13*time.Second, er, false, nil)

	if rc, exp := atomic.LoadInt32(&requestCount), int32(6); rc != exp {
		t.Errorf("unexpected requestCount: got %v exp %v", rc, exp)
	}
}

func TestStream_Alert_WithReset_0(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		var expAd alert.Data
		switch rc {
		case 1:
			expAd = alert.Data{
				ID:          "kapacitor/cpu/serverA",
				Message:     "kapacitor/cpu/serverA is INFO",
				Details:     "details",
				Time:        time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Level:       alert.Info,
				Recoverable: true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 2:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
				Duration:      time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								40.0,
							}},
						},
					},
				},
			}
		case 3:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
				Duration:      2 * time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								30.0,
							}},
						},
					},
				},
			}
		case 4:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is OK",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
				Duration:      3 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
								9.0,
							}},
						},
					},
				},
			}
		case 5:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration:      0 * time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.OK,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 6:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
				Duration:      1 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								61.0,
							}},
						},
					},
				},
			}
		case 7:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
				Duration:      2 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								30.0,
							}},
						},
					},
				},
			}
		case 8:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is OK",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
				Duration:      3 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								19.0,
							}},
						},
					},
				},
			}
		case 9:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
				Duration:      0 * time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.OK,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 10:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
				Duration:      1 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
								61.0,
							}},
						},
					},
				},
			}
		case 11:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is CRITICAL",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Duration:      2 * time.Second,
				Level:         alert.Critical,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
								81.0,
							}},
						},
					},
				},
			}
		case 12:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is OK",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
				Duration:      3 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Critical,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
								29.0,
							}},
						},
					},
				},
			}
		}

		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var infoThreshold = 40.0
var warnThreshold = 60.0
var critThreshold = 80.0

var infoResetThreshold = 10.0
var warnResetThreshold = 20.0
var critResetThreshold = 30.0

stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.idField('id')
		.idTag('id')
		.levelField('level')
		.levelTag('level')
		.info(lambda: "value" > infoThreshold)
		.infoReset(lambda: "value" < infoResetThreshold)
		.warn(lambda: "value" > warnThreshold)
		.warnReset(lambda: "value" < warnResetThreshold)
		.crit(lambda: "value" > critThreshold)
		.critReset(lambda: "value" < critResetThreshold)
		.post('` + ts.URL + `')
	|httpOut('TestStream_Alert_WithReset_0')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "level": "OK", "id": "kapacitor/cpu/serverA", "type": "usage"},
				Columns: []string{"time", "id", "level", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
					"kapacitor/cpu/serverA",
					"OK",
					29.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert_WithReset_0", script, 13*time.Second, er, false, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 12 {
		t.Errorf("got %v exp %v", rc, 1)
	}
}

func TestStream_Alert_WithReset_1(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		var expAd alert.Data
		switch rc {
		case 1:
			expAd = alert.Data{
				ID:          "kapacitor/cpu/serverA",
				Message:     "kapacitor/cpu/serverA is INFO",
				Details:     "details",
				Time:        time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Level:       alert.Info,
				Recoverable: true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 2:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
				Duration:      time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
								40.0,
							}},
						},
					},
				},
			}
		case 3:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
				Duration:      2 * time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								30.0,
							}},
						},
					},
				},
			}
		case 4:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is OK",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
				Duration:      3 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
								29.0,
							}},
						},
					},
				},
			}
		case 5:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration:      0 * time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.OK,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 6:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
				Duration:      1 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								61.0,
							}},
						},
					},
				},
			}
		case 7:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
				Duration:      2 * time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
								49.0,
							}},
						},
					},
				},
			}
		case 8:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is OK",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
				Duration:      3 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								29.0,
							}},
						},
					},
				},
			}
		case 9:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
				Duration:      0 * time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.OK,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								45.0,
							}},
						},
					},
				},
			}
		case 10:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
				Duration:      1 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 9, 0, time.UTC),
								61.0,
							}},
						},
					},
				},
			}
		case 11:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is CRITICAL",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Duration:      2 * time.Second,
				Level:         alert.Critical,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
								81.0,
							}},
						},
					},
				},
			}
		case 12:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
				Duration:      3 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Critical,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 11, 0, time.UTC),
								69.0,
							}},
						},
					},
				},
			}
		case 13:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
				Duration:      4 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 12, 0, time.UTC),
								50.0,
							}},
						},
					},
				},
			}
		case 14:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is INFO",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 13, 0, time.UTC),
				Duration:      5 * time.Second,
				Level:         alert.Info,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 13, 0, time.UTC),
								41.0,
							}},
						},
					},
				},
			}
		case 15:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is OK",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 14, 0, time.UTC),
				Duration:      6 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "usage"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 14, 0, time.UTC),
								25.0,
							}},
						},
					},
				},
			}
		}

		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var infoThreshold = 40.0
var warnThreshold = 60.0
var critThreshold = 80.0

var infoResetThreshold = 30.0
var warnResetThreshold = 50.0
var critResetThreshold = 70.0

stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.idField('id')
		.idTag('id')
		.levelField('level')
		.levelTag('level')
		.info(lambda: "value" > infoThreshold)
		.infoReset(lambda: "value" < infoResetThreshold)
		.warn(lambda: "value" > warnThreshold)
		.warnReset(lambda: "value" < warnResetThreshold)
		.crit(lambda: "value" > critThreshold)
		.critReset(lambda: "value" < critResetThreshold)
		.post('` + ts.URL + `')
	|httpOut('TestStream_Alert_WithReset_1')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "level": "OK", "id": "kapacitor/cpu/serverA", "type": "usage"},
				Columns: []string{"time", "id", "level", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 14, 0, time.UTC),
					"kapacitor/cpu/serverA",
					"OK",
					25.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Alert_WithReset_1", script, 15*time.Second, er, false, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 15 {
		t.Errorf("got %v exp %v", rc, 1)
	}
}

func TestStream_AlertDuration(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		var expAd alert.Data
		rc := atomic.LoadInt32(&requestCount)
		switch rc {
		case 1:
			expAd = alert.Data{
				ID:          "kapacitor/cpu/serverA",
				Message:     "kapacitor/cpu/serverA is CRITICAL",
				Details:     "details",
				Time:        time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
				Duration:    0,
				Level:       alert.Critical,
				Recoverable: true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
								9.0,
							}},
						},
					},
				},
			}
		case 2:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
				Duration:      2 * time.Second,
				Level:         alert.Warning,
				PreviousLevel: alert.Critical,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
								8.0,
							}},
						},
					},
				},
			}
		case 3:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is OK",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
				Duration:      4 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
								6.0,
							}},
						},
					},
				},
			}
		case 4:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is WARNING",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
				Duration:      0,
				Level:         alert.Warning,
				PreviousLevel: alert.OK,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 5, 0, time.UTC),
								8.0,
							}},
						},
					},
				},
			}
		case 5:
			expAd = alert.Data{
				ID:            "kapacitor/cpu/serverA",
				Message:       "kapacitor/cpu/serverA is OK",
				Details:       "details",
				Time:          time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
				Duration:      3 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Warning,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								3.0,
							}},
						},
					},
				},
			}
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
var warnThreshold = 7.0
var critThreshold = 8.0

stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.durationField('duration')
		.warn(lambda: "value" > warnThreshold)
		.crit(lambda: "value" > critThreshold)
		.stateChangesOnly()
		.post('` + ts.URL + `')
	|httpOut('TestStream_AlertDuration')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "duration", "value"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
					float64(3 * time.Second),
					3.0,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_AlertDuration", script, 13*time.Second, er, false, nil)

	if exp, rc := 5, int(atomic.LoadInt32(&requestCount)); rc != exp {
		t.Errorf("got %v exp %v", rc, exp)
	}
}

func TestStream_AlertSensu(t *testing.T) {
	ts, err := sensutest.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.sensu()
			.metadata('k1', 'v1')
			.metadata('k2', 5)
`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := sensu.NewConfig()
		c.Enabled = true
		c.Addr = ts.Addr
		c.Source = "Kapacitor"
		sl := sensu.NewService(c, diagService.NewSensuHandler())
		tm.SensuService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		sensutest.Request{
			Source: "Kapacitor",
			Output: "kapacitor.cpu.serverA is CRITICAL",
			Name:   "kapacitor.cpu.serverA",
			Status: 2,
			Metadata: map[string]interface{}{
				"k1": "v1",
				"k2": float64(5),
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}

}

func TestStream_AlertSlack(t *testing.T) {
	ts := slacktest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.slack()
		.workspace('company_private')
		.channel('#alerts')
		.slack()
		.channel('@jim')
`

	tmInit := func(tm *kapacitor.TaskMaster) {
		c1 := slack.NewConfig()
		c1.Default = true
		c1.Enabled = true
		c1.URL = ts.URL + "/test/slack/url"
		c1.Channel = "#channel"
		c2 := slack.NewConfig()
		c2.Workspace = "company_private"
		c2.Enabled = true
		c2.URL = ts.URL + "/test/slack/url2"
		c2.Token = "my_secret_token"
		c2.Channel = "#channel"
		d := diagService.NewSlackHandler().WithContext(keyvalue.KV("test", "slack"))
		sl, err := slack.NewService([]slack.Config{c1, c2}, d)
		if err != nil {
			t.Error(err)
		}
		tm.SlackService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		slacktest.Request{
			URL:        "/test/slack/url",
			AuthHeader: "",
			PostData: slacktest.PostData{
				Channel:  "@jim",
				Username: "kapacitor",
				Text:     "",
				Attachments: []slacktest.Attachment{
					{
						Fallback:  "kapacitor/cpu/serverA is CRITICAL",
						Color:     "danger",
						Text:      "kapacitor/cpu/serverA is CRITICAL",
						Mrkdwn_in: []string{"text"},
					},
				},
			},
		},
		slacktest.Request{
			URL:        "/test/slack/url2",
			AuthHeader: "Bearer my_secret_token",
			PostData: slacktest.PostData{
				Channel:  "#alerts",
				Username: "kapacitor",
				Text:     "",
				Attachments: []slacktest.Attachment{
					{
						Fallback:  "kapacitor/cpu/serverA is CRITICAL",
						Color:     "danger",
						Text:      "kapacitor/cpu/serverA is CRITICAL",
						Mrkdwn_in: []string{"text"},
					},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertKafka(t *testing.T) {
	ts, err := kafkatest.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.kafka()
		.cluster('default')
		.kafkaTopic('testTopic')
		.template('{{.Message}}')
`

	tmInit := func(tm *kapacitor.TaskMaster) {
		configs := kafka.Configs{{
			Enabled:   true,
			ID:        "default",
			Brokers:   []string{ts.Addr.String()},
			BatchSize: 1,
		}}
		d := diagService.NewKafkaHandler().WithContext(keyvalue.KV("test", "kafka"))
		tm.KafkaService = kafka.NewService(configs, d)
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		kafkatest.Message{
			Topic:     "testTopic",
			Partition: 2,
			Offset:    0,
			Key:       "kapacitor/cpu/serverA",
			Message:   "kapacitor/cpu/serverA is CRITICAL",
			Time:      time.Now().UTC(),
		},
	}

	// Wait for kakfa messages to be written
	time.Sleep(time.Second)

	ts.Close()
	msgs, err := ts.Messages()
	if err != nil {
		t.Fatal(err)
	}
	got := make([]interface{}, len(msgs))
	for i, m := range msgs {
		got[i] = m
	}

	cmpopts := []cmp.Option{
		cmp.Comparer(func(a, b time.Time) bool {
			diff := a.Sub(b)
			if diff < 0 {
				diff = -diff
			}
			// It is ok as long as the timestamp is within
			// 5 seconds of the current time. If we are that close,
			// then it likely means the timestamp was correctly
			// written.
			return diff < 5*time.Second
		}),
	}
	cmpF := func(got, exp interface{}) bool {
		return cmp.Equal(exp, got, cmpopts...)
	}
	if err := compareListIgnoreOrder(got, exp, cmpF); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertKafka_Partitioning(t *testing.T) {
	ts, err := kafkatest.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
	|alert()
		.id('{{ index .Tags "host" }}')
		.crit(lambda: TRUE)
		.kafka()
		.cluster('default')
		.kafkaTopic('testTopic')
		.template('{{.Message}}')
`

	tmInit := func(tm *kapacitor.TaskMaster) {
		configs := kafka.Configs{{
			Enabled:   true,
			ID:        "default",
			Brokers:   []string{ts.Addr.String()},
			BatchSize: 1,
		}}
		d := diagService.NewKafkaHandler().WithContext(keyvalue.KV("test", "kafka"))
		tm.KafkaService = kafka.NewService(configs, d)
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	// Wait for kakfa messages to be written
	time.Sleep(time.Second)

	ts.Close()
	msgs, err := ts.Messages()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]map[int32]struct{})
	for _, m := range msgs {
		// Record the partitions used per message key.
		partitions, ok := got[m.Key]
		if !ok {
			got[m.Key] = make(map[int32]struct{})
			partitions = got[m.Key]
		}
		partitions[m.Partition] = struct{}{}
	}

	for msg, partitions := range got {
		if len(partitions) > 1 {
			t.Fatalf("messages for %q were sent to %d partitions, expected only 1 partition", msg, len(partitions))
		}
	}
}

func TestStream_AlertTelegram(t *testing.T) {
	ts := telegramtest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.telegram()
			.chatId('12345678')
				.disableNotification()
				.parseMode('HTML')
		.telegram()
			.chatId('87654321')
`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := telegram.NewConfig()
		c.Enabled = true
		c.URL = ts.URL + "/bot"
		c.Token = "TOKEN:AUTH"
		c.ChatId = "123456789"
		c.DisableWebPagePreview = true
		c.DisableNotification = false
		tl := telegram.NewService(c, diagService.NewTelegramHandler())
		tm.TelegramService = tl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		telegramtest.Request{
			URL: "/botTOKEN:AUTH/sendMessage",
			PostData: telegramtest.PostData{
				ChatId:                "12345678",
				Text:                  "kapacitor/cpu/serverA is CRITICAL",
				ParseMode:             "HTML",
				DisableWebPagePreview: true,
				DisableNotification:   true,
			},
		},
		telegramtest.Request{
			URL: "/botTOKEN:AUTH/sendMessage",
			PostData: telegramtest.PostData{
				ChatId:                "87654321",
				Text:                  "kapacitor/cpu/serverA is CRITICAL",
				ParseMode:             "",
				DisableWebPagePreview: true,
				DisableNotification:   false,
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertTCP(t *testing.T) {
	ts, err := alerttest.NewTCPServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.details('')
		.tcp('` + ts.Addr + `')
`
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, nil)

	exp := []interface{}{
		alert.Data{
			ID:          "kapacitor.cpu.serverA",
			Message:     "kapacitor.cpu.serverA is CRITICAL",
			Time:        time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
			Level:       alert.Critical,
			Recoverable: true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA"},
						Columns: []string{"time", "count"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
							10.0,
						}},
					},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Data() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertHipChat(t *testing.T) {
	ts := hipchattest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.hipChat()
			.room('1234567')
			.token('testtoken1234567')
		.hipChat()
			.room('Test Room')
			.token('testtokenTestRoom')
`
	tmInit := func(tm *kapacitor.TaskMaster) {

		c := hipchat.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.Room = "1231234"
		c.Token = "testtoken1231234"
		sl := hipchat.NewService(c, diagService.NewHipChatHandler())
		tm.HipChatService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		hipchattest.Request{
			URL: "/1234567/notification?auth_token=testtoken1234567",
			PostData: hipchattest.PostData{
				From:    "kapacitor",
				Message: "kapacitor/cpu/serverA is CRITICAL",
				Color:   "red",
				Notify:  true,
			},
		},
		hipchattest.Request{
			URL: "/Test%20Room/notification?auth_token=testtokenTestRoom",
			PostData: hipchattest.PostData{
				From:    "kapacitor",
				Message: "kapacitor/cpu/serverA is CRITICAL",
				Color:   "red",
				Notify:  true,
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertAlerta(t *testing.T) {
	ts := alertatest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('{{ index .Tags "host" }}')
		.message('kapacitor/{{ .Name }}/{{ index .Tags "host" }} is {{ .Level }} @{{.Time}}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.alerta()
			.token('testtoken1234567')
			.environment('production')
			.timeout(1h)
		.alerta()
			.token('anothertesttoken')
			.resource('resource: {{ index .Tags "host" }}')
			.event('event: {{ .TaskName }}')
			.environment('{{ index .Tags "host" }}')
			.origin('override')
			.group('{{ .ID }}')
			.value('{{ index .Fields "count" }}')
			.services('serviceA', 'serviceB', '{{ .Name }}')
			.correlated('{{ .Name }}')
			.attribute('attributeA', '{{ .Name }}')
			.attribute('attributeB', TRUE)
			.attribute('attributeC', 9001.0)

`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := alerta.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.Origin = "Kapacitor"
		sl := alerta.NewService(c, diagService.NewAlertaHandler())
		tm.AlertaService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		alertatest.Request{
			URL:           "/alert",
			Authorization: "Bearer testtoken1234567",
			PostData: alertatest.PostData{
				Resource:    "cpu",
				Event:       "serverA",
				Group:       "host=serverA",
				Environment: "production",
				Text:        "kapacitor/cpu/serverA is CRITICAL @1971-01-01 00:00:10 +0000 UTC",
				Origin:      "Kapacitor",
				Service:     []string{"cpu"},
				Correlate:   []string{"cpu"},
				Attributes:  nil,
				Timeout:     3600,
			},
		},
		alertatest.Request{
			URL:           "/alert",
			Authorization: "Bearer anothertesttoken",
			PostData: alertatest.PostData{
				Resource:    "resource: serverA",
				Event:       "event: TestStream_Alert",
				Group:       "serverA",
				Environment: "serverA",
				Text:        "kapacitor/cpu/serverA is CRITICAL @1971-01-01 00:00:10 +0000 UTC",
				Origin:      "override",
				Service:     []string{"serviceA", "serviceB", "cpu"},
				Correlate:   []string{"cpu"},
				Value:       "10",
				Attributes:  map[string]interface{}{"attributeA": "cpu", "attributeB": true, "attributeC": 9001.0},
				Timeout:     86400,
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertDiscord(t *testing.T) {
	ts := discordtest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.discord()
		.workspace('company_private')
		.discord()
		.username('testy')
`

	tmInit := func(tm *kapacitor.TaskMaster) {
		c1 := discord.NewConfig()
		c1.Default = true
		c1.Enabled = true
		c1.URL = ts.URL + "/test/discord/url"
		c2 := discord.NewConfig()
		c2.Workspace = "company_private"
		c2.Username = "comp testy"
		c2.Enabled = true
		c2.URL = ts.URL + "/test/discord/url2"
		d := diagService.NewDiscordHandler().WithContext(keyvalue.KV("test", "discord"))
		sl, err := discord.NewService([]discord.Config{c1, c2}, d)
		if err != nil {
			t.Error(err)
		}
		tm.DiscordService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		discordtest.Request{
			URL: "/test/discord/url",
			PostData: discordtest.PostData{
				Username:  "testy",
				AvatarURL: "",
				Embeds: []discordtest.Embed{
					{
						Color:       0xF95F53,
						Description: "kapacitor/cpu/serverA is CRITICAL",
						Title:       "",
						Timestamp:   "",
					},
				},
			},
		},
		discordtest.Request{
			URL: "/test/discord/url2",
			PostData: discordtest.PostData{
				Username:  "comp testy",
				AvatarURL: "",
				Embeds: []discordtest.Embed{
					{
						Color:       0xF95F53,
						Description: "kapacitor/cpu/serverA is CRITICAL",
						Title:       "",
						Timestamp:   "",
					},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertBigPanda(t *testing.T) {
	ts := bigpandatest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.message('kapacitor/{{ .Name }}/{{ index .Tags "host" }} is {{ .Level }} @{{.Time}}')
		.details('https://example.org/link')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.bigPanda()
			.appKey('111111')
			.host('{{.Tags.host}}')
			.primaryProperty('host')
		.bigPanda()
			.appKey('222222')
			.host('serverA-1')
			.secondaryProperty('application')
		.bigPanda()
			.attribute('x_host', '{{.Tags.host}}')
			.attribute('x_duration', '{{.Duration}}')
			.attribute('x_detail', '{{.Details}}')
			.attribute('x_value', '{{.Fields.count}}')
`
	tmInit := func(tm *kapacitor.TaskMaster) {

		c := bigpanda.NewConfig()
		c.Enabled = true
		c.AppKey = "012345"
		c.Token = "testtoken1231234"
		c.URL = ts.URL + "/test/bigpanda/url"
		c.AutoAttributes = ""

		d := diagService.NewBigPandaHandler().WithContext(keyvalue.KV("test", "111"))
		sl, err := bigpanda.NewService(c, d)
		if err != nil {
			t.Error(err)
		}

		tm.BigPandaService = sl
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		bigpandatest.Request{
			URL: "/test/bigpanda/url",
			PostData: bigpandatest.PostData{
				Check:           "kapacitor/cpu/serverA",
				Description:     "kapacitor/cpu/serverA is CRITICAL @1971-01-01 00:00:10 +0000 UTC",
				AppKey:          "111111",
				Status:          "critical",
				Host:            "serverA",
				Timestamp:       31536010,
				Task:            "TestStream_Alert:cpu",
				Details:         "https://example.org/link",
				PrimaryProperty: "host",
			},
		},
		bigpandatest.Request{
			URL: "/test/bigpanda/url",
			PostData: bigpandatest.PostData{
				Check:             "kapacitor/cpu/serverA",
				Description:       "kapacitor/cpu/serverA is CRITICAL @1971-01-01 00:00:10 +0000 UTC",
				AppKey:            "222222",
				Status:            "critical",
				Host:              "serverA-1",
				Timestamp:         31536010,
				Task:              "TestStream_Alert:cpu",
				Details:           "https://example.org/link",
				SecondaryProperty: "application",
			},
		},
		bigpandatest.Request{
			URL: "/test/bigpanda/url",
			PostData: bigpandatest.PostData{
				Check:       "kapacitor/cpu/serverA",
				Description: "kapacitor/cpu/serverA is CRITICAL @1971-01-01 00:00:10 +0000 UTC",
				AppKey:      "012345",
				Status:      "critical",
				Host:        "",
				Timestamp:   31536010,
				Task:        "TestStream_Alert:cpu",
				Details:     "https://example.org/link",
				Attributes: map[string]string{
					"x_detail":   "https://example.org/link",
					"x_duration": "0s",
					"x_host":     "serverA",
					"x_value":    "10",
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertPushover(t *testing.T) {
	ts := pushovertest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('{{ index .Tags "host" }}')
		.message('kapacitor/{{ .Name }}/{{ index .Tags "host" }} is {{ .Level }} @{{.Time}}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.pushover()
			.sound('siren')
			.device('mydev')
			.title('mytitle')
			.URL('http://example.com')
			.URLTitle('myurltitle')
		.pushover()
			.title('othertitle')
			.device('otherdev')
`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := pushover.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.UserKey = "user"
		c.Token = "KzGDORePKggMaC0QOYAMyEEuzJnyUi"
		sl := pushover.NewService(c, diagService.NewPushoverHandler())
		tm.PushoverService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		pushovertest.Request{
			PostData: pushovertest.PostData{
				Token:    "KzGDORePKggMaC0QOYAMyEEuzJnyUi",
				UserKey:  "user",
				Message:  "kapacitor/cpu/serverA is CRITICAL @1971-01-01 00:00:10 +0000 UTC",
				Device:   "mydev",
				Sound:    "siren",
				Title:    "mytitle",
				URL:      "http://example.com",
				URLTitle: "myurltitle",
				Priority: 1,
			},
		},
		pushovertest.Request{
			PostData: pushovertest.PostData{
				Token:    "KzGDORePKggMaC0QOYAMyEEuzJnyUi",
				UserKey:  "user",
				Message:  "kapacitor/cpu/serverA is CRITICAL @1971-01-01 00:00:10 +0000 UTC",
				Device:   "otherdev",
				Title:    "othertitle",
				Priority: 1,
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertOpsGenie(t *testing.T) {
	ts := opsgenietest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.opsGenie()
			.teams('test_team', 'another_team')
			.recipients('test_recipient', 'another_recipient')
		.opsGenie()
			.teams('test_team2' )
			.recipients('test_recipient2', 'another_recipient')
`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := opsgenie.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.APIKey = "api_key"
		og := opsgenie.NewService(c, diagService.NewOpsGenieHandler())
		tm.OpsGenieService = og
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		opsgenietest.Request{
			URL: "/",
			PostData: opsgenietest.PostData{
				ApiKey:  "api_key",
				Message: "kapacitor/cpu/serverA is CRITICAL",
				Entity:  "kapacitor/cpu/serverA",
				Alias:   "kapacitor/cpu/serverA",
				Note:    "",
				Details: map[string]interface{}{
					"Level":           "CRITICAL",
					"Monitoring Tool": "Kapacitor",
				},
				Description: `{"series":[{"name":"cpu","tags":{"host":"serverA"},"columns":["time","count"],"values":[["1971-01-01T00:00:10Z",10]]}]}`,
				Teams:       []string{"test_team", "another_team"},
				Recipients:  []string{"test_recipient", "another_recipient"},
			},
		},
		opsgenietest.Request{
			URL: "/",
			PostData: opsgenietest.PostData{
				ApiKey:  "api_key",
				Message: "kapacitor/cpu/serverA is CRITICAL",
				Entity:  "kapacitor/cpu/serverA",
				Alias:   "kapacitor/cpu/serverA",
				Note:    "",
				Details: map[string]interface{}{
					"Level":           "CRITICAL",
					"Monitoring Tool": "Kapacitor",
				},
				Description: `{"series":[{"name":"cpu","tags":{"host":"serverA"},"columns":["time","count"],"values":[["1971-01-01T00:00:10Z",10]]}]}`,
				Teams:       []string{"test_team2"},
				Recipients:  []string{"test_recipient2", "another_recipient"},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertOpsGenie2(t *testing.T) {
	ts := opsgenie2test.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.opsGenie2()
			.teams('test_team', 'another_team')
			.recipients('test_recipient', 'another_recipient')
		.opsGenie2()
			.teams('test_team2' )
			.recipients('test_recipient2', 'another_recipient')
`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := opsgenie2.NewConfig()
		c.Enabled = true
		c.Details = false
		c.URL = ts.URL
		c.APIKey = "api_key"
		og := opsgenie2.NewService(c, diagService.NewOpsGenie2Handler())
		tm.OpsGenie2Service = og
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		opsgenie2test.Request{
			URL:           "/",
			Authorization: "GenieKey api_key",
			PostData: opsgenie2test.PostData{
				Message:  "kapacitor/cpu/serverA is CRITICAL",
				Entity:   "kapacitor/cpu/serverA",
				Alias:    "a2FwYWNpdG9yL2NwdS9zZXJ2ZXJB",
				Note:     "",
				Priority: "P1",
				Details: map[string]string{
					"Level":               "CRITICAL",
					"Monitoring Tool":     "Kapacitor",
					"Kapacitor Task Name": "cpu",
					"host":                "serverA",
				},
				Description: `{"series":[{"name":"cpu","tags":{"host":"serverA"},"columns":["time","count"],"values":[["1971-01-01T00:00:10Z",10]]}]}`,
				Responders: []map[string]string{
					{"name": "test_team", "type": "team"},
					{"name": "another_team", "type": "team"},
					{"username": "test_recipient", "type": "user"},
					{"username": "another_recipient", "type": "user"},
				},
			},
		},
		opsgenie2test.Request{
			URL:           "/",
			Authorization: "GenieKey api_key",
			PostData: opsgenie2test.PostData{
				Message:  "kapacitor/cpu/serverA is CRITICAL",
				Entity:   "kapacitor/cpu/serverA",
				Alias:    "a2FwYWNpdG9yL2NwdS9zZXJ2ZXJB",
				Note:     "",
				Priority: "P1",
				Details: map[string]string{
					"Level":               "CRITICAL",
					"Monitoring Tool":     "Kapacitor",
					"Kapacitor Task Name": "cpu",
					"host":                "serverA",
				},
				Description: `{"series":[{"name":"cpu","tags":{"host":"serverA"},"columns":["time","count"],"values":[["1971-01-01T00:00:10Z",10]]}]}`,
				Responders: []map[string]string{
					{"name": "test_team2", "type": "team"},
					{"username": "test_recipient2", "type": "user"},
					{"username": "another_recipient", "type": "user"},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertOpsGenie2_Recovery(t *testing.T) {
	ts := opsgenie2test.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.crit(lambda: "v" > 1.0)
		.opsGenie2()
			.teams('test_team')
`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := opsgenie2.NewConfig()
		c.Enabled = true
		c.Details = false
		c.URL = ts.URL
		c.RecoveryAction = "notes"
		c.APIKey = "api_key"
		og := opsgenie2.NewService(c, diagService.NewOpsGenie2Handler())
		tm.OpsGenie2Service = og
	}
	testStreamerNoOutput(t, "TestStream_AlertRecovery", script, 4*time.Second, tmInit)

	exp := []interface{}{
		opsgenie2test.Request{
			URL:           "/",
			Authorization: "GenieKey api_key",
			PostData: opsgenie2test.PostData{
				Message:  "kapacitor/cpu/serverA is CRITICAL",
				Entity:   "kapacitor/cpu/serverA",
				Alias:    "a2FwYWNpdG9yL2NwdS9zZXJ2ZXJB",
				Note:     "",
				Priority: "P1",
				Details: map[string]string{
					"Level":               "CRITICAL",
					"Monitoring Tool":     "Kapacitor",
					"Kapacitor Task Name": "cpu",
					"host":                "serverA",
					"type":                "idle",
				},
				Description: `{"series":[{"name":"cpu","tags":{"host":"serverA","type":"idle"},"columns":["time","v"],"values":[["1971-01-01T00:00:00Z",2]]}]}`,
				Responders: []map[string]string{
					{"name": "test_team", "type": "team"},
				},
			},
		},
		opsgenie2test.Request{
			URL:           "/a2FwYWNpdG9yL2NwdS9zZXJ2ZXJB/notes?identifierType=alias",
			Authorization: "GenieKey api_key",
			PostData: opsgenie2test.PostData{
				Note: "kapacitor/cpu/serverA is OK",
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if !cmp.Equal(got, exp) {
		t.Errorf("unexpected OpsGenie2 requests -got/+want%s", cmp.Diff(got, exp))
	}
}

func TestStream_AlertPagerDuty(t *testing.T) {
	ts := pagerdutytest.NewServer()
	defer ts.Close()

	defaultDetailsTmpl := `{"Name":"cpu","TaskName":"TestStream_Alert","Group":"host=serverA","Tags":{"host":"serverA"},"ServerInfo":{"Hostname":"%v","ClusterID":"%v","ServerID":"%v"},"ID":"kapacitor/cpu/serverA","Fields":{"count":10},"Level":"CRITICAL","Time":"1971-01-01T00:00:10Z","Duration":0,"Message":"CRITICAL alert for kapacitor/cpu/serverA"}
`
	var defaultDetails string

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.message('{{ .Level }} alert for {{ .ID }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.pagerDuty()
		.pagerDuty()
			.serviceKey('test_override_key')
`

	var kapacitorURL string
	tmInit := func(tm *kapacitor.TaskMaster) {
		si := tm.ServerInfo
		defaultDetails = fmt.Sprintf(defaultDetailsTmpl,
			si.Hostname(),
			si.ClusterID(),
			si.ServerID(),
		)
		c := pagerduty.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.ServiceKey = "service_key"
		pd := pagerduty.NewService(c, diagService.NewPagerDutyHandler())
		pd.HTTPDService = tm.HTTPDService
		tm.PagerDutyService = pd

		kapacitorURL = tm.HTTPDService.URL()
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		pagerdutytest.Request{
			URL: "/",
			PostData: pagerdutytest.PostData{
				ServiceKey:  "service_key",
				EventType:   "trigger",
				Description: "CRITICAL alert for kapacitor/cpu/serverA",
				Client:      "kapacitor",
				ClientURL:   kapacitorURL,
				Details:     html.EscapeString(defaultDetails),
			},
		},
		pagerdutytest.Request{
			URL: "/",
			PostData: pagerdutytest.PostData{
				ServiceKey:  "test_override_key",
				EventType:   "trigger",
				Description: "CRITICAL alert for kapacitor/cpu/serverA",
				Client:      "kapacitor",
				ClientURL:   kapacitorURL,
				Details:     html.EscapeString(defaultDetails),
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertPagerDuty2(t *testing.T) {
	ts := pagerduty2test.NewServer()
	defer ts.Close()

	detailsTmpl := map[string]interface{}{
		"result": map[string]interface{}{
			"series": []interface{}{
				map[string]interface{}{
					"name": "cpu",
					"tags": map[string]interface{}{
						"host": "serverA",
					},
					"columns": []interface{}{"time", "count"},
					"values": []interface{}{
						[]interface{}{"1971-01-01T00:00:10Z", float64(10)},
					},
				},
			},
		},
	}

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.message('{{ .Level }} alert for {{ .ID }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.pagerDuty2()
		.pagerDuty2()
		    .routingKey('test_override_key')
			.link('http://example.com')
			.link('http://example.com/{{.TaskName}}','task')
	`

	var kapacitorURL string
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := pagerduty2.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.RoutingKey = "routing_key"
		pd := pagerduty2.NewService(c, diagService.NewPagerDuty2Handler())
		pd.HTTPDService = tm.HTTPDService
		tm.PagerDuty2Service = pd

		kapacitorURL = tm.HTTPDService.URL()
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		pagerduty2test.Request{
			URL: "/",
			PostData: pagerduty2test.PostData{
				Client:      "kapacitor",
				ClientURL:   kapacitorURL,
				EventAction: "trigger",
				DedupKey:    "kapacitor/cpu/serverA",
				Payload: &pagerduty2test.PDCEF{
					Summary:       "CRITICAL alert for kapacitor/cpu/serverA",
					Source:        "serverA",
					Severity:      "critical",
					Class:         "TestStream_Alert",
					CustomDetails: detailsTmpl,
					Timestamp:     "1971-01-01T00:00:10.000000000Z",
				},
				RoutingKey: "routing_key",
			},
		},
		pagerduty2test.Request{
			URL: "/",
			PostData: pagerduty2test.PostData{
				Client:      "kapacitor",
				ClientURL:   kapacitorURL,
				EventAction: "trigger",
				DedupKey:    "kapacitor/cpu/serverA",
				Payload: &pagerduty2test.PDCEF{
					Summary:       "CRITICAL alert for kapacitor/cpu/serverA",
					Source:        "serverA",
					Severity:      "critical",
					Class:         "TestStream_Alert",
					CustomDetails: detailsTmpl,
					Timestamp:     "1971-01-01T00:00:10.000000000Z",
				},
				RoutingKey: "test_override_key",
				Links: []pagerduty2test.Link{
					{Href: "http://example.com", Text: "http://example.com"},
					{Href: "http://example.com/TestStream_Alert", Text: "task"},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertPagerDuty2_ServiceKey(t *testing.T) {
	ts := pagerduty2test.NewServer()
	defer ts.Close()

	detailsTmpl := map[string]interface{}{
		"result": map[string]interface{}{
			"series": []interface{}{
				map[string]interface{}{
					"name": "cpu",
					"tags": map[string]interface{}{
						"host": "serverA",
					},
					"columns": []interface{}{"time", "count"},
					"values": []interface{}{
						[]interface{}{"1971-01-01T00:00:10Z", float64(10)},
					},
				},
			},
		},
	}

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.message('{{ .Level }} alert for {{ .ID }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.pagerDuty2()
		.pagerDuty2()
		    .serviceKey('test_override_key')
	`

	var kapacitorURL string
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := pagerduty2.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.RoutingKey = "routing_key"
		pd := pagerduty2.NewService(c, diagService.NewPagerDuty2Handler())
		pd.HTTPDService = tm.HTTPDService
		tm.PagerDuty2Service = pd

		kapacitorURL = tm.HTTPDService.URL()
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		pagerduty2test.Request{
			URL: "/",
			PostData: pagerduty2test.PostData{
				Client:      "kapacitor",
				ClientURL:   kapacitorURL,
				EventAction: "trigger",
				DedupKey:    "kapacitor/cpu/serverA",
				Payload: &pagerduty2test.PDCEF{
					Summary:       "CRITICAL alert for kapacitor/cpu/serverA",
					Source:        "serverA",
					Severity:      "critical",
					Class:         "TestStream_Alert",
					CustomDetails: detailsTmpl,
					Timestamp:     "1971-01-01T00:00:10.000000000Z",
				},
				RoutingKey: "routing_key",
			},
		},
		pagerduty2test.Request{
			URL: "/",
			PostData: pagerduty2test.PostData{
				Client:      "kapacitor",
				ClientURL:   kapacitorURL,
				EventAction: "trigger",
				DedupKey:    "kapacitor/cpu/serverA",
				Payload: &pagerduty2test.PDCEF{
					Summary:       "CRITICAL alert for kapacitor/cpu/serverA",
					Source:        "serverA",
					Severity:      "critical",
					Class:         "TestStream_Alert",
					CustomDetails: detailsTmpl,
					Timestamp:     "1971-01-01T00:00:10.000000000Z",
				},
				RoutingKey: "test_override_key",
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertHTTPPost(t *testing.T) {
	ts := httpposttest.NewAlertServer(nil, false)
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.details('')
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, nil)

	exp := []interface{}{
		httpposttest.AlertRequest{
			MatchingHeaders: true,
			Data: alert.Data{
				ID:          "kapacitor.cpu.serverA",
				Message:     "kapacitor.cpu.serverA is CRITICAL",
				Time:        time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Level:       alert.Critical,
				Recoverable: true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "count"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
								10.0,
							}},
						},
					},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Data() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertHTTPPostEndpoint(t *testing.T) {
	headers := map[string]string{"Authorization": "works"}
	ts := httpposttest.NewAlertServer(headers, false)
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.details('')
		.post()
		 .endpoint('test')
`
	tmInit := func(tm *kapacitor.TaskMaster) {
		c := httppost.Config{}
		c.URLTemplate = ts.URL
		c.Endpoint = "test"
		c.Headers = headers
		sl, _ := httppost.NewService(httppost.Configs{c}, diagService.NewHTTPPostHandler())
		tm.HTTPPostService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		httpposttest.AlertRequest{
			MatchingHeaders: true,
			Data: alert.Data{
				ID:          "kapacitor.cpu.serverA",
				Message:     "kapacitor.cpu.serverA is CRITICAL",
				Time:        time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Level:       alert.Critical,
				Recoverable: true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA"},
							Columns: []string{"time", "count"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
								10.0,
							}},
						},
					},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Data() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertVictorOps(t *testing.T) {
	ts := victoropstest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.victorOps()
			.routingKey('test_key')
		.victorOps()
			.routingKey('test_key2')
`

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := victorops.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.APIKey = "api_key"
		c.RoutingKey = "routing_key"
		d := diagService.NewVictorOpsHandler().WithContext(keyvalue.KV("test", "vo"))
		vo := victorops.NewService(c, d)
		tm.VictorOpsService = vo
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		victoropstest.Request{
			URL: "/api_key/test_key",
			PostData: victoropstest.PostData{
				MessageType:    "CRITICAL",
				EntityID:       "kapacitor/cpu/serverA",
				StateMessage:   "kapacitor/cpu/serverA is CRITICAL",
				Timestamp:      31536010,
				MonitoringTool: "kapacitor",
				Data:           `{"series":[{"name":"cpu","tags":{"host":"serverA"},"columns":["time","count"],"values":[["1971-01-01T00:00:10Z",10]]}]}`,
			},
		},
		victoropstest.Request{
			URL: "/api_key/test_key2",
			PostData: victoropstest.PostData{
				MessageType:    "CRITICAL",
				EntityID:       "kapacitor/cpu/serverA",
				StateMessage:   "kapacitor/cpu/serverA is CRITICAL",
				Timestamp:      31536010,
				MonitoringTool: "kapacitor",
				Data:           `{"series":[{"name":"cpu","tags":{"host":"serverA"},"columns":["time","count"],"values":[["1971-01-01T00:00:10Z",10]]}]}`,
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertVictorOps_JSON_Data(t *testing.T) {
	ts := victoropstest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.victorOps()
`

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := victorops.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.APIKey = "api_key"
		c.RoutingKey = "routing_key"
		c.JSONData = true
		d := diagService.NewVictorOpsHandler().WithContext(keyvalue.KV("test", "vo"))
		vo := victorops.NewService(c, d)
		tm.VictorOpsService = vo
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		victoropstest.Request{
			URL: "/api_key/routing_key",
			PostData: victoropstest.PostData{
				MessageType:    "CRITICAL",
				EntityID:       "kapacitor/cpu/serverA",
				StateMessage:   "kapacitor/cpu/serverA is CRITICAL",
				Timestamp:      31536010,
				MonitoringTool: "kapacitor",
				Data: map[string]interface{}{
					"series": []interface{}{
						map[string]interface{}{
							"name": "cpu",
							"tags": map[string]interface{}{
								"host": "serverA",
							},
							"columns": []interface{}{"time", "count"},
							"values": []interface{}{
								[]interface{}{"1971-01-01T00:00:10Z", 10.0},
							},
						},
					},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertTalk(t *testing.T) {
	ts := talktest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.talk()
`

	tmInit := func(tm *kapacitor.TaskMaster) {
		c := talk.NewConfig()
		c.Enabled = true
		c.URL = ts.URL
		c.AuthorName = "Kapacitor"
		sl := talk.NewService(c, diagService.NewTalkHandler())
		tm.TalkService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		talktest.Request{
			URL: "/",
			PostData: talktest.PostData{
				AuthorName: "Kapacitor",
				Text:       "kapacitor/cpu/serverA is CRITICAL",
				Title:      "kapacitor/cpu/serverA",
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertTeams(t *testing.T) {
	ts := teamstest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.teams()
		.teams()
			.channelURL('%s')
`
	// To test with live webhook, replace "ts.URL" in line below with your
	// webhook URL.  The test will fail, but ONE message will post to Teams.
	script = fmt.Sprintf(script, ts.URL)

	tmInit := func(tm *kapacitor.TaskMaster) {

		c := teams.NewConfig()
		c.Enabled = true
		c.ChannelURL = ts.URL
		sl := teams.NewService(c, diagService.NewTeamsHandler())
		tm.TeamsService = sl
	}
	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		teamstest.Request{
			URL: "/",
			Card: teams.Card{
				CardType:   "MessageCard",
				Context:    "http://schema.org/extensions",
				Title:      "CRITICAL: [kapacitor/cpu/serverA]",
				Text:       "kapacitor/cpu/serverA is CRITICAL",
				Summary:    "CRITICAL: [kapacitor/cpu/serverA] - kapacitor/cpu/serverA is CRITICAL...",
				ThemeColor: "CC4A31",
			},
		},
		teamstest.Request{
			URL: "/",
			Card: teams.Card{
				CardType:   "MessageCard",
				Context:    "http://schema.org/extensions",
				Title:      "CRITICAL: [kapacitor/cpu/serverA]",
				Text:       "kapacitor/cpu/serverA is CRITICAL",
				Summary:    "CRITICAL: [kapacitor/cpu/serverA] - kapacitor/cpu/serverA is CRITICAL...",
				ThemeColor: "CC4A31",
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertServiceNow(t *testing.T) {
	ts := servicenowtest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host', 'type')
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "mean" > 15.0)
		.warn(lambda: "mean" > 50.0)
		.crit(lambda: "mean" > 90.0)
		.serviceNow()
		.serviceNow()
			.node('{{ index .Tags "host" }}')
			.type('CPU')
			.resource('CPU-Total')
			.metricName('{{ index .Tags "type" }}')
			.messageKey('Alert: {{ .ID }}')
			.additionalInfo('link', 'http://push/alert?id={{ .ID }}')
			.additionalInfo('ticks', 666)
`
	tmInit := func(tm *kapacitor.TaskMaster) {

		c := servicenow.NewConfig()
		c.Enabled = true
		c.URL = ts.URL + "/api/global/em/jsonv2"
		c.Source = "Kapacitor"
		sl := servicenow.NewService(c, diagService.NewServiceNowHandler())
		tm.ServiceNowService = sl
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		servicenowtest.Request{
			URL: "/api/global/em/jsonv2",
			Alerts: servicenow.Events{
				Records: []servicenow.Event{
					{
						Source:         "Kapacitor",
						Node:           "serverA",
						Type:           "CPU",       // literal since there is no tag for this in the testdata
						Resource:       "CPU-Total", // literal since there is no tag for this in the testdata
						MetricName:     "idle",
						MessageKey:     "Alert: kapacitor/cpu/serverA",
						Severity:       "1",
						Description:    "kapacitor/cpu/serverA is CRITICAL",
						AdditionalInfo: "{\"link\":\"http://push/alert?id=kapacitor/cpu/serverA\",\"ticks\":\"666\"}",
					},
				},
			},
		},
		servicenowtest.Request{
			URL: "/api/global/em/jsonv2",
			Alerts: servicenow.Events{
				Records: []servicenow.Event{
					{
						Source:      "Kapacitor",
						MessageKey:  "kapacitor/cpu/serverA",
						Severity:    "1",
						Description: "kapacitor/cpu/serverA is CRITICAL",
					},
				},
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertLog(t *testing.T) {
	tmpDir := t.TempDir()
	normalPath := filepath.Join(tmpDir, "normal.log")
	modePath := filepath.Join(tmpDir, "mode.log")

	normal := alerttest.NewLog(normalPath)
	mode := alerttest.NewLog(modePath)

	var script = fmt.Sprintf(`
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
		.details('')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.log('%s')
		.log('%s')
			.mode(0644)
`, normalPath, modePath)

	expAD := []alert.Data{{
		ID:          "kapacitor.cpu.serverA",
		Message:     "kapacitor.cpu.serverA is CRITICAL",
		Time:        time.Date(1971, 01, 01, 0, 0, 10, 0, time.UTC),
		Level:       alert.Critical,
		Recoverable: true,
		Data: models.Result{
			Series: models.Rows{
				{
					Name:    "cpu",
					Tags:    map[string]string{"host": "serverA"},
					Columns: []string{"time", "count"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						10.0,
					}},
				},
			},
		},
	}}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, nil)

	testLog := func(name string, expData []alert.Data, expMode os.FileMode, l *alerttest.Log) error {
		m, err := l.Mode()
		if err != nil {
			return err
		}
		if got, exp := m, expMode; exp != got {
			return fmt.Errorf("%s unexpected file mode: got %v exp %v", name, got, exp)
		}
		data, err := l.Data()
		if err != nil {
			return err
		}
		if got, exp := data, expData; !reflect.DeepEqual(got, exp) {
			return fmt.Errorf("%s unexpected alert data written to log:\ngot\n%+v\nexp\n%+v\n", name, got, exp)
		}
		return nil
	}

	if err := testLog("normal", expAD, 0600, normal); err != nil {
		t.Error(err)
	}
	if err := testLog("mode", expAD, 0644, mode); err != nil {
		t.Error(err)
	}

}

func TestStream_AlertExec(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
		.details('')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.exec('/bin/my-script', 'arg1', 'arg2')
		.exec('/bin/my-other-script')
`

	expAD := alert.Data{
		ID:          "kapacitor.cpu.serverA",
		Message:     "kapacitor.cpu.serverA is CRITICAL",
		Time:        time.Date(1971, 01, 01, 0, 0, 10, 0, time.UTC),
		Level:       alert.Critical,
		Recoverable: true,
		Data: models.Result{
			Series: models.Rows{
				{
					Name:    "cpu",
					Tags:    map[string]string{"host": "serverA"},
					Columns: []string{"time", "count"},
					Values: [][]interface{}{[]interface{}{
						time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
						10.0,
					}},
				},
			},
		},
	}
	expStdin, err := json.Marshal(expAD)
	if err != nil {
		t.Fatal(err)
	}
	// Append trailing new line
	expStdin = append(expStdin, '\n')

	te := alerttest.NewExec()
	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.Commander = te.Commander
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	expCmds := []interface{}{
		&commandtest.Command{
			Spec: command.Spec{
				Prog: "/bin/my-script",
				Args: []string{"arg1", "arg2"},
			},
			Started:   true,
			Waited:    true,
			Killed:    false,
			StdinData: expStdin,
		},
		&commandtest.Command{
			Spec: command.Spec{
				Prog: "/bin/my-other-script",
				Args: []string{},
			},
			Started:   true,
			Waited:    true,
			Killed:    false,
			StdinData: expStdin,
		},
	}

	cmds := te.Commands()
	cmdsI := make([]interface{}, len(cmds))
	for i := range cmds {
		cmdsI[i] = cmds[i]
	}
	if err := compareListIgnoreOrder(cmdsI, expCmds, func(got, exp interface{}) bool {
		g := got.(*commandtest.Command)
		e := exp.(*commandtest.Command)
		return e.Compare(g) == nil
	}); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertEmail(t *testing.T) {
	var cases = []struct {
		name     string
		script   string
		expected []*smtptest.Message
	}{
		{
			name: "emails directly",
			script: `stream
			|from()
				.measurement('cpu')
				.where(lambda: "host" == 'serverA')
				.groupBy('host')
			|window()
				.period(10s)
				.every(10s)
			|count('value')
			|alert()
				.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
				.details('''
<b>{{.Message}}</b>

Value: {{ index .Fields "count" }}
<a href="http://graphs.example.com/host/{{index .Tags "host"}}">Details</a>
''')
						.info(lambda: "count" > 6.0)
						.warn(lambda: "count" > 7.0)
						.crit(lambda: "count" > 8.0)
						.email('user1@example.com', 'user2@example.com')
						.email()
							.to('user1@example.com', 'user2@example.com')
				`,
			expected: []*smtptest.Message{
				{
					Header: mail.Header{
						"Mime-Version":              []string{"1.0"},
						"Content-Type":              []string{"text/html; charset=UTF-8"},
						"Content-Transfer-Encoding": []string{"quoted-printable"},
						"To":                        []string{"user1@example.com, user2@example.com"},
						"From":                      []string{"test@example.com"},
						"Subject":                   []string{"kapacitor.cpu.serverA is CRITICAL"},
					},
					Body: `
<b>kapacitor.cpu.serverA is CRITICAL</b>

Value: 10
<a href=3D"http://graphs.example.com/host/serverA">Details</a>
`,
				},
				{
					Header: mail.Header{
						"Mime-Version":              []string{"1.0"},
						"Content-Type":              []string{"text/html; charset=UTF-8"},
						"Content-Transfer-Encoding": []string{"quoted-printable"},
						"To":                        []string{"user1@example.com, user2@example.com"},
						"From":                      []string{"test@example.com"},
						"Subject":                   []string{"kapacitor.cpu.serverA is CRITICAL"},
					},
					Body: `
<b>kapacitor.cpu.serverA is CRITICAL</b>

Value: 10
<a href=3D"http://graphs.example.com/host/serverA">Details</a>
`,
				}},
		},
		{
			name: "emails directly and in fields",
			script: `stream
					|from()
						.measurement('cpu')
						.where(lambda: "host" == 'serverA')
						.groupBy('host')
					|window()
						.period(10s)
						.every(10s)
					|count('value')
					|default()
						.field('extraemail','bob@example.com')
						.tag('tagemail','bob2@example.com')
					|alert()
						.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
						.details('''
<b>{{.Message}}</b>

Value: {{ index .Fields "count" }}
<a href="http://graphs.example.com/host/{{index .Tags "host"}}">Details</a>
''')
						.info(lambda: "count" > 6.0)
						.warn(lambda: "count" > 7.0)
						.crit(lambda: "count" > 8.0)
						.email()
							.to('user1@example.com', 'user2@example.com')
							.toTemplates('{{ index .Fields "extraemail" }}')
				`,
			expected: []*smtptest.Message{
				{
					Header: mail.Header{
						"Mime-Version":              []string{"1.0"},
						"Content-Type":              []string{"text/html; charset=UTF-8"},
						"Content-Transfer-Encoding": []string{"quoted-printable"},
						"To":                        []string{"user1@example.com, user2@example.com, bob@example.com"},
						"From":                      []string{"test@example.com"},
						"Subject":                   []string{"kapacitor.cpu.serverA is CRITICAL"},
					},
					Body: `
<b>kapacitor.cpu.serverA is CRITICAL</b>

Value: 10
<a href=3D"http://graphs.example.com/host/serverA">Details</a>
`,
				}},
		},
		{
			name: "emails directly and in tag",
			script: `stream
							|from()
								.measurement('cpu')
								.where(lambda: "host" == 'serverA')
								.groupBy('host')
							|window()
								.period(10s)
								.every(10s)
							|count('value')
							|default()
								.field('extraemail','bob@example.com')
								.tag('tagemail','bob2@example.com')
							|alert()
								.id('kapacitor.{{ .Name }}.{{ index .Tags "host" }}')
								.details('''
<b>{{.Message}}</b>

Value: {{ index .Fields "count" }}
<a href="http://graphs.example.com/host/{{index .Tags "host"}}">Details</a>
''')
								.info(lambda: "count" > 6.0)
								.warn(lambda: "count" > 7.0)
								.crit(lambda: "count" > 8.0)
								.email()
									.to('user1@example.com', 'user2@example.com')
									.toTemplates('{{ index .Tags "tagemail" }}')
						`,
			expected: []*smtptest.Message{
				{
					Header: mail.Header{
						"Mime-Version":              []string{"1.0"},
						"Content-Type":              []string{"text/html; charset=UTF-8"},
						"Content-Transfer-Encoding": []string{"quoted-printable"},
						"To":                        []string{"user1@example.com, user2@example.com, bob2@example.com"},
						"From":                      []string{"test@example.com"},
						"Subject":                   []string{"kapacitor.cpu.serverA is CRITICAL"},
					},
					Body: `
<b>kapacitor.cpu.serverA is CRITICAL</b>

Value: 10
<a href=3D"http://graphs.example.com/host/serverA">Details</a>
`,
				},
			},
		},
	}
	for i := range cases {
		t.Run(cases[i].name, func(t *testing.T) {
			script := cases[i].script
			expMail := cases[i].expected
			smtpServer, err := smtptest.NewServer()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				err := smtpServer.Close()
				if err != nil {
					t.Fatalf("error in closing smtpService %v", err)
				}
			}()

			sc := smtp.Config{
				Enabled: true,
				Host:    smtpServer.Host,
				Port:    smtpServer.Port,
				From:    "test@example.com",
			}
			smtpService := smtp.NewService(sc, diagService.NewSMTPHandler())
			if err := smtpService.Open(); err != nil {
				t.Fatal(err)
			}

			// make sure its closed
			defer func() {
				err := smtpService.Close()
				if err != nil {
					t.Fatalf("error in closing smtpService %v", err)
				}
			}()

			tmInit := func(tm *kapacitor.TaskMaster) {
				tm.SMTPService = smtpService
			}

			testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

			// Close both client and server to ensure all message are processed
			if err := smtpService.Close(); err != nil {
				t.Fatalf("error in closing smtpService %v", err)
			}
			if err := smtpServer.Close(); err != nil {
				t.Fatalf("error in closing smtpServer %v", err)
			}

			errors := smtpServer.Errors()
			if got, exp := len(errors), 0; got != exp {
				t.Errorf("unexpected smtp server errors: %v", errors)
			}

			msgs := smtpServer.SentMessages()
			if got, exp := len(msgs), len(expMail); got != exp {
				t.Errorf("unexpected number of messages sent: got %d exp %d", got, exp)
			}
			for i, exp := range expMail {
				got := msgs[i]
				if err := exp.Compare(got); err != nil {
					t.Errorf("%d %s", i, err)
				}
			}
		})
	}
}

func TestStream_AlertEmail_HtmlEscape(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|eval(lambda: 'HTML <i arg="1">field</i>')
		.keep()
		.as('htmlFieldTest')
	|eval(lambda: 'HTML <i arg="1">tag</i>')
		.keep()
		.as('htmlTagTest')
		.tags('htmlTagTest')
	|alert()
		.id('')
		.details('
Field, escaped: {{ index .Fields "htmlFieldTest" }}
Field, safeHtml: {{ index .Fields "htmlFieldTest" | safeHtml }}
Tag, escaped: {{ index .Tags "htmlTagTest" }}
Tag, safeHtml: {{ index .Tags "htmlTagTest" | safeHtml }}
')
		.crit(lambda: "count" > 8.0)
		.email('user1@example.com')
`
	expField := "HTML <i arg=\"1\">field</i>"
	expTag := "HTML <i arg=\"1\">tag</i>"
	expDetails := fmt.Sprintf(
		"\nField, escaped: %v\nField, safeHtml: %v\nTag, escaped: %v\nTag, safeHtml: %v\n",
		html.EscapeString(expField),
		expField,
		html.EscapeString(expTag),
		expTag)
	var quotedPrintableDetails bytes.Buffer
	quotedPrintableWriter := quotedprintable.NewWriter(&quotedPrintableDetails)
	_, _ = quotedPrintableWriter.Write([]byte(expDetails))
	_ = quotedPrintableWriter.Close()

	expMail := []*smtptest.Message{
		{
			Header: mail.Header{
				"Mime-Version":              []string{"1.0"},
				"Content-Type":              []string{"text/html; charset=UTF-8"},
				"Content-Transfer-Encoding": []string{"quoted-printable"},
			},
			Body: strings.Replace(quotedPrintableDetails.String(), "\r", "", -1),
		},
	}

	smtpServer, err := smtptest.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer smtpServer.Close()
	sc := smtp.Config{
		Enabled: true,
		Host:    smtpServer.Host,
		Port:    smtpServer.Port,
		From:    "test@example.com",
	}
	smtpService := smtp.NewService(sc, diagService.NewSMTPHandler())
	if err := smtpService.Open(); err != nil {
		t.Fatal(err)
	}
	defer smtpService.Close()

	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.SMTPService = smtpService
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	// Close both client and server to ensure all message are processed
	smtpService.Close()
	smtpServer.Close()

	errors := smtpServer.Errors()
	if got, exp := len(errors), 0; got != exp {
		t.Errorf("unexpected smtp server errors: %v", errors)
	}

	msgs := smtpServer.SentMessages()
	if got, exp := len(msgs), len(expMail); got != exp {
		t.Errorf("unexpected number of messages sent: got %d exp %d", got, exp)
	}
	for i, exp := range expMail {
		got := msgs[i]
		if err := exp.Compare(got); err != nil {
			t.Errorf("%d %s", i, err)
		}
	}
}

func TestStream_AlertSNMPTrap(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "count" > 6.0)
		.warn(lambda: "count" > 7.0)
		.crit(lambda: "count" > 8.0)
		.details('{{ "here" }}/{{ .Name }}')
		.snmpTrap('1.1.1')
			.data('1.1.1.2', 'c', '1')
			.data('1.1.1.2', 's', 'SNMP ALERT')
			.data('1.1.1.2', 's', '{{.Message}}')
		.snmpTrap('1.1.2')
			.data('1.1.2.3', 'i', '10')
			.data('1.1.2.3', 'n', '')
			.data('1.1.2.3', 't', '20000')
			.data('1.1.2.3', 's', '{{ .Details }}')
`

	expTraps := []interface{}{
		snmptraptest.Trap{
			Pdu: snmptraptest.Pdu{
				Type:        snmpgo.SNMPTrapV2,
				ErrorStatus: snmpgo.NoError,
				VarBinds: snmptraptest.VarBinds{
					{
						Oid:   "1.3.6.1.2.1.1.3.0",
						Value: "1000",
						Type:  "TimeTicks",
					},
					{
						Oid:   "1.3.6.1.6.3.1.1.4.1.0",
						Value: "1.1.1",
						Type:  "Oid",
					},
					{
						Oid:   "1.1.1.2",
						Value: "1",
						Type:  "Counter64",
					},
					{
						Oid:   "1.1.1.2",
						Value: "SNMP ALERT",
						Type:  "OctetString",
					},
					{
						Oid:   "1.1.1.2",
						Value: "kapacitor/cpu/serverA is CRITICAL",
						Type:  "OctetString",
					},
				},
			},
		},
		snmptraptest.Trap{
			Pdu: snmptraptest.Pdu{
				Type:        snmpgo.SNMPTrapV2,
				ErrorStatus: snmpgo.NoError,
				VarBinds: snmptraptest.VarBinds{
					{
						Oid:   "1.3.6.1.2.1.1.3.0",
						Value: "1000",
						Type:  "TimeTicks",
					},
					{
						Oid:   "1.3.6.1.6.3.1.1.4.1.0",
						Value: "1.1.2",
						Type:  "Oid",
					},
					{
						Oid:   "1.1.2.3",
						Value: "10",
						Type:  "Integer",
					},
					{
						Oid:   "1.1.2.3",
						Value: "",
						Type:  "Null",
					},
					{
						Oid:   "1.1.2.3",
						Value: "20000",
						Type:  "TimeTicks",
					},
					{
						Oid:   "1.1.2.3",
						Value: "here/cpu",
						Type:  "OctetString",
					},
				},
			},
		},
	}

	snmpServer, err := snmptraptest.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer snmpServer.Close()

	c := snmptrap.NewConfig()
	c.Enabled = true
	c.Addr = snmpServer.Addr
	c.Community = snmpServer.Community
	c.Retries = 2
	st := snmptrap.NewService(c, diagService.NewSNMPTrapHandler())
	if err := st.Open(); err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.SNMPTrapService = st
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	// TODO make snmpServer Close gauruntee that all traps have been processed
	time.Sleep(10 * time.Millisecond)
	snmpServer.Close()

	traps := snmpServer.Traps()
	got := make([]interface{}, len(traps))
	for i, t := range traps {
		got[i] = t
	}
	if err := compareListIgnoreOrder(got, expTraps, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertSigma(t *testing.T) {
	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		var expAd alert.Data
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		if rc := atomic.LoadInt32(&requestCount); rc == 1 {
			expAd = alert.Data{
				ID:          "cpu:nil",
				Message:     "cpu:nil is INFO",
				Details:     "cpu:nil is INFO",
				Time:        time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
				Level:       alert.Info,
				Recoverable: true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "sigma", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
								2.469916402324427,
								16.0,
							}},
						},
					},
				},
			}
		} else {
			expAd = alert.Data{
				ID:            "cpu:nil",
				Message:       "cpu:nil is OK",
				Details:       "cpu:nil is OK",
				Time:          time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
				Duration:      time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Info,
				Recoverable:   true,
				Data: models.Result{
					Series: models.Rows{
						{
							Name:    "cpu",
							Tags:    map[string]string{"host": "serverA", "type": "idle"},
							Columns: []string{"time", "sigma", "value"},
							Values: [][]interface{}{[]interface{}{
								time.Date(1971, 1, 1, 0, 0, 8, 0, time.UTC),
								0.3053477916297622,
								93.4,
							}},
						},
					},
				},
			}
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|eval(lambda: sigma("value"))
		.as('sigma')
		.keep()
	|alert()
		.details('{{ .Message }}')
		.info(lambda: "sigma" > 2.0)
		.warn(lambda: "sigma" > 3.0)
		.crit(lambda: "sigma" > 3.5)
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertSigma", script, 13*time.Second, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 2 {
		t.Errorf("got %v exp %v", rc, 2)
	}
}

func TestStream_AlertComplexWhere(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
		expAd := alert.Data{
			ID:          "cpu:nil",
			Message:     "cpu:nil is CRITICAL",
			Details:     "",
			Time:        time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
			Level:       alert.Critical,
			Recoverable: true,
			Data: models.Result{
				Series: models.Rows{
					{
						Name:    "cpu",
						Tags:    map[string]string{"host": "serverA", "type": "idle"},
						Columns: []string{"time", "value"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 7, 0, time.UTC),
							16.0,
						}},
					},
				},
			},
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Error(msg)
		}
	}))
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA' AND sigma("value") > 2)
	|alert()
		.details('')
		.crit(lambda: TRUE)
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertComplexWhere", script, 13*time.Second, nil)

	if rc := atomic.LoadInt32(&requestCount); rc != 1 {
		t.Errorf("got %v exp %v", rc, 1)
	}
}

func TestStream_AlertStateChangesOnly(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
	}))
	defer ts.Close()
	var script = `
stream
	|from()
		.measurement('cpu')
	|alert()
		.crit(lambda: "value" < 93)
		.stateChangesOnly()
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertStateChangesOnly", script, 13*time.Second, nil)

	// Only 4 points below 93 so 8 state changes.
	if rc := atomic.LoadInt32(&requestCount); rc != 8 {
		t.Errorf("got %v exp %v", rc, 5)
	}
}
func TestStream_AlertStateChangesOnlyExpired(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ad := alert.Data{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&ad)
		if err != nil {
			t.Fatal(err)
		}
		//We don't care about the data for this test
		ad.Data = models.Result{}
		var expAd alert.Data
		atomic.AddInt32(&requestCount, 1)
		rc := atomic.LoadInt32(&requestCount)
		if rc == 1 {
			expAd = alert.Data{
				ID:            "cpu:nil",
				Message:       "cpu:nil is CRITICAL",
				Time:          time.Date(1971, 1, 1, 0, 0, int(rc)*2-1, 0, time.UTC),
				Duration:      time.Duration(rc-1) * 2 * time.Second,
				Level:         alert.Critical,
				PreviousLevel: alert.OK,
				Recoverable:   true,
			}
		} else if rc < 6 {
			expAd = alert.Data{
				ID:            "cpu:nil",
				Message:       "cpu:nil is CRITICAL",
				Time:          time.Date(1971, 1, 1, 0, 0, int(rc)*2-1, 0, time.UTC),
				Duration:      time.Duration(rc-1) * 2 * time.Second,
				Level:         alert.Critical,
				PreviousLevel: alert.Critical,
				Recoverable:   true,
			}
		} else {
			expAd = alert.Data{
				ID:            "cpu:nil",
				Message:       "cpu:nil is OK",
				Time:          time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
				Duration:      9 * time.Second,
				Level:         alert.OK,
				PreviousLevel: alert.Critical,
				Recoverable:   true,
			}
		}
		if eq, msg := compareAlertData(expAd, ad); !eq {
			t.Errorf("unexpected alert data for request: %d %s", rc, msg)
		}
	}))
	defer ts.Close()
	var script = `
stream
	|from()
		.measurement('cpu')
	|alert()
		.crit(lambda: "value" < 97)
		.details('')
		.stateChangesOnly(2s)
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertStateChangesOnlyExpired", script, 13*time.Second, nil)

	// 9 points below 97 once per second so 5 alerts + 1 recovery
	if exp, rc := 6, int(atomic.LoadInt32(&requestCount)); rc != exp {
		t.Errorf("got %v exp %v", rc, exp)
	}
}

func TestStream_AlertFlapping(t *testing.T) {

	requestCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
	}))
	defer ts.Close()
	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|alert()
		.info(lambda: "value" < 95)
		.warn(lambda: "value" < 94)
		.crit(lambda: "value" < 93)
		.flapping(0.25, 0.50)
		.history(21)
		.post('` + ts.URL + `')
`

	testStreamerNoOutput(t, "TestStream_AlertFlapping", script, 13*time.Second, nil)

	// Flapping detection should drop the last alerts.
	if rc := atomic.LoadInt32(&requestCount); rc != 9 {
		t.Errorf("got %v exp %v", rc, 9)
	}
}

func TestStream_AlertZenoss(t *testing.T) {
	ts := zenosstest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host', 'type')
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "mean" > 15.0)
		.warn(lambda: "mean" > 50.0)
		.crit(lambda: "mean" > 90.0)
		.zenoss()
			.device('#DEVICE001')
			.component('CPU')
			.eventClass('/App')
`

	tmInit := func(tm *kapacitor.TaskMaster) {

		c := zenoss.NewConfig()
		c.Enabled = true
		c.URL = ts.URL + "/zport/dmd/evconsole_router"
		c.Collector = ""
		svc := zenoss.NewService(c, diagService.NewZenossHandler())
		tm.ZenossService = svc
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		zenosstest.Request{
			URL: "/zport/dmd/evconsole_router",
			Event: zenoss.Event{
				Action: "EventsRouter",
				Method: "add_event",
				Data: []map[string]interface{}{
					{
						"summary":    "kapacitor/cpu/serverA is CRITICAL",
						"device":     "#DEVICE001",
						"component":  "CPU",
						"severity":   "Critical",
						"evclasskey": "",
						"evclass":    "/App",
					},
				},
				Type: "rpc",
				TID:  1,
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_AlertZenoss_Custom(t *testing.T) {
	ts := zenosstest.NewServer()
	defer ts.Close()

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
		.groupBy('host', 'type')
	|window()
		.period(10s)
		.every(10s)
	|mean('value')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.info(lambda: "mean" > 15.0)
		.warn(lambda: "mean" > 50.0)
		.crit(lambda: "mean" > 90.0)
		.details('{{.}}')
		.zenoss()
			.action('ScriptsRouter')
			.method('kapa_handler')
			.eventClass('/App')
			.message('This is message for alert {{ .ID }}')
			.collector('{{ index .Tags "host" }}')
			.customField('data', '{"id":"{{.ID}}","message":"{{.Message}}","time":"{{.Time}}","duration":"{{ .Duration}}","level":"{{.Level}}","recoverable":{{.Recoverable}}}')
			.customField('ticks', 33)
`
	tmInit := func(tm *kapacitor.TaskMaster) {

		c := zenoss.NewConfig()
		c.Enabled = true
		c.URL = ts.URL + "/zport/dmd/evconsole_router"
		c.SeverityMap = zenoss.SeverityMap{OK: 0, Info: 2, Warning: 3, Critical: 5}
		svc := zenoss.NewService(c, diagService.NewZenossHandler())
		tm.ZenossService = svc
	}

	testStreamerNoOutput(t, "TestStream_Alert", script, 13*time.Second, tmInit)

	exp := []interface{}{
		zenosstest.Request{
			URL: "/zport/dmd/evconsole_router",
			Event: zenoss.Event{
				Action: "ScriptsRouter",
				Method: "kapa_handler",
				Data: []map[string]interface{}{
					{
						"summary":    "kapacitor/cpu/serverA is CRITICAL",
						"device":     "",
						"component":  "",
						"severity":   float64(5),
						"evclasskey": "",
						"evclass":    "/App",
						"collector":  "serverA",
						"message":    "This is message for alert kapacitor/cpu/serverA",
						"data": map[string]interface{}{
							"id":          "kapacitor/cpu/serverA",
							"level":       "CRITICAL",
							"message":     "kapacitor/cpu/serverA is CRITICAL",
							"time":        "1971-01-01 00:00:10 +0000 UTC",
							"duration":    "0s",
							"recoverable": true,
						},
						"ticks": float64(33),
					},
				},
				Type: "rpc",
				TID:  1,
			},
		},
	}

	ts.Close()
	var got []interface{}
	for _, g := range ts.Requests() {
		got = append(got, g)
	}

	if err := compareListIgnoreOrder(got, exp, nil); err != nil {
		t.Error(err)
	}
}

func TestStream_LambdaNow(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('account')
	|where(lambda: "expiration" < unixNano(now()))
	|groupBy('owner')
	|httpOut('TestStream_LambdaNow')
`

	expectedOutput := models.Result{
		Series: models.Rows{
			{
				Name:    "account",
				Tags:    map[string]string{"owner": "ownerA"},
				Columns: []string{"time", "expiration"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						float64(3.15533e+17), // 1980-01-01 00:00:00 (Unix ns timestamp)

						// we expect "expiration" to be float64 and not int64 (even with input data consisting of ints)
						// because httpOut uses JSON as serialization format for the results,
						// resulting in the integers becoming floats
					},
				},
			},

			// the point with expiration = 4102440000000000000 should not be in the results
			// as it represents a date past now (2100-01-01 00:00:00)

			{
				Name:    "account",
				Tags:    map[string]string{"owner": "ownerC"},
				Columns: []string{"time", "expiration"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						float64(6.56419e+17), // 1990-10-20 10:42:42
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_LambdaNow", script, time.Second, expectedOutput, false, nil)
}

func TestStream_EvalNow(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('account')
	|eval(lambda: year(now()))
		.as('currentYear')
	|httpOut('TestStream_EvalNow')
`

	expectedOutput := models.Result{
		Series: models.Rows{
			{
				Name:    "account",
				Tags:    map[string]string{"owner": "ownerA"},
				Columns: []string{"time", "currentYear"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						float64(time.Now().Year()),
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_EvalNow", script, time.Second, expectedOutput, false, nil)
}

func TestStream_EvalRand(t *testing.T) {
	var script = `
stream
	|from()
		.measurement('data')
	|eval(lambda: rand(0), lambda: rand(20), lambda: rand(1), lambda: rand())
		.as('rand0', 'rand20','rand1','rand')
	|httpOut('TestStream_EvalRand')
`
	// make the rng deterministic for the test
	tempRand := stateful.NewRand
	stateful.NewRand = func() *stateful.Rand {
		r := mwc.T{}
		r.Seed(time.Unix(0, 0).UTC().Unix())
		return (*stateful.Rand)(&r)
	}
	defer func() { stateful.NewRand = tempRand }()
	expectedOutput := models.Result{
		Series: models.Rows{
			{
				Name:    "data",
				Tags:    map[string]string{"owner": "ownerA"},
				Columns: []string{"time", "rand", "rand0", "rand1", "rand20"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						float64(3131420824542496000), // this is due to a limitation of JSON, which httpOut uses
						float64(0),
						float64(0),
						float64(3),
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_EvalRand", script, time.Second, expectedOutput, false, nil)
}

func TestStream_Autoscale(t *testing.T) {
	testCases := map[string]struct {
		script           string
		result           models.Result
		minMaxResult     models.Result
		setup            func(*kapacitor.TaskMaster) context.Context
		updatesByService func(context.Context) map[string][]int
	}{
		"k8sAutoscale": {
			script: `|k8sAutoscale().resourceNameTag('deployment')`,
			result: models.Result{
				Series: models.Rows{
					{
						Name: "scale",
						Tags: map[string]string{
							"deployment": "serviceA",
							"namespace":  "default",
							"kind":       "deployments",
							"resource":   "serviceA",
						},
						Columns: []string{"time", "new", "old"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							2.0,
							1000.0,
						}},
					},
					{
						Name: "scale",
						Tags: map[string]string{
							"deployment": "serviceB",
							"namespace":  "default",
							"kind":       "deployments",
							"resource":   "serviceB",
						},
						Columns: []string{"time", "new", "old"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							20.0,
							1000.0,
						}},
					},
				},
			},
			minMaxResult: models.Result{
				Series: models.Rows{
					{
						Name: "scale",
						Tags: map[string]string{
							"deployment": "serviceA",
							"namespace":  "default",
							"kind":       "deployments",
							"resource":   "serviceA",
						},
						Columns: []string{"time", "new", "old"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							3.0,
							500.0,
						}},
					},
					{
						Name: "scale",
						Tags: map[string]string{
							"deployment": "serviceB",
							"namespace":  "default",
							"kind":       "deployments",
							"resource":   "serviceB",
						},
						Columns: []string{"time", "new", "old"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							20.0,
							500.0,
						}},
					},
				},
			},
			setup: func(tm *kapacitor.TaskMaster) context.Context {
				scaleUpdates := make(chan k8s.Scale, 100)
				ctx := context.WithValue(context.Background(), testCtxStr("updates"), scaleUpdates)
				k8sAutoscale := k8stest.Client{}
				k8sAutoscale.ScalesGetFunc = func(kind, name string) (*k8s.Scale, error) {
					var replicas int32
					switch name {
					case "serviceA":
						replicas = 1
					case "serviceB":
						replicas = 10
					}
					return &k8s.Scale{
						ObjectMeta: k8s.ObjectMeta{
							Name: name,
						},
						Spec: k8s.ScaleSpec{
							Replicas: replicas,
						},
					}, nil
				}
				k8sAutoscale.ScalesUpdateFunc = func(kind string, scale *k8s.Scale) error {
					scaleUpdates <- *scale
					return nil
				}
				tm.K8sService = k8sAutoscale
				return ctx
			},
			updatesByService: func(ctx context.Context) map[string][]int {
				scaleUpdates := ctx.Value(testCtxStr("updates")).(chan k8s.Scale)
				close(scaleUpdates)
				updatesByService := make(map[string][]int)
				for scale := range scaleUpdates {
					updatesByService[scale.Name] = append(updatesByService[scale.Name], int(scale.Spec.Replicas))
				}
				return updatesByService
			},
		},
		"swarmAutoscale": {
			script: `|swarmAutoscale().serviceNameTag('deployment')`,
			result: models.Result{
				Series: models.Rows{
					{
						Name: "scale",
						Tags: map[string]string{
							"deployment": "serviceA",
						},
						Columns: []string{"time", "new", "old"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							2.0,
							1000.0,
						}},
					},
					{
						Name: "scale",
						Tags: map[string]string{
							"deployment": "serviceB",
						},
						Columns: []string{"time", "new", "old"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							20.0,
							1000.0,
						}},
					},
				},
			},
			minMaxResult: models.Result{
				Series: models.Rows{
					{
						Name: "scale",
						Tags: map[string]string{
							"deployment": "serviceA",
						},
						Columns: []string{"time", "new", "old"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							3.0,
							500.0,
						}},
					},
					{
						Name: "scale",
						Tags: map[string]string{
							"deployment": "serviceB",
						},
						Columns: []string{"time", "new", "old"},
						Values: [][]interface{}{[]interface{}{
							time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
							20.0,
							500.0,
						}},
					},
				},
			},
			setup: func(tm *kapacitor.TaskMaster) context.Context {
				serviceUpdates := make(chan swarm.Service, 100)
				ctx := context.WithValue(context.Background(), testCtxStr("updates"), serviceUpdates)
				swarmAutoscale := swarmtest.Client{}
				swarmAutoscale.ServiceFunc = func(name string) (*swarm.Service, error) {
					var replicas uint64
					switch name {
					case "serviceA":
						replicas = 1
					case "serviceB":
						replicas = 10
					}
					return &swarm.Service{
						ID: name,
						Spec: swarm.ServiceSpec{
							Mode: swarm.ServiceMode{
								Replicated: &swarm.ReplicatedService{
									Replicas: &replicas,
								},
							},
						},
					}, nil
				}
				swarmAutoscale.UpdateServiceFunc = func(service *swarm.Service) error {
					serviceUpdates <- *service
					return nil
				}
				tm.SwarmService = swarmAutoscale
				return ctx
			},
			updatesByService: func(ctx context.Context) map[string][]int {
				updates := ctx.Value(testCtxStr("updates")).(chan swarm.Service)
				close(updates)
				updatesByService := make(map[string][]int)
				for service := range updates {
					updatesByService[service.ID] = append(updatesByService[service.ID], int(*service.Spec.Mode.Replicated.Replicas))
				}
				return updatesByService
			},
		},
	}
	expUpdatesByService := map[string][]int{
		"serviceA": []int{2, 1, 1000, 2},
		"serviceB": []int{20, 1, 1000, 20},
	}
	expMinMaxUpdatesByService := map[string][]int{
		"serviceA": []int{3, 500, 3},
		"serviceB": []int{20, 3, 500, 20},
	}

	var scriptTmpl = `
stream
	|from()
		.measurement('scale')
		.groupBy('deployment')
	%s
		.replicas(lambda: int("replicas"))
	|httpOut('TestStream_Autoscale')
`

	var scriptMinMaxTmpl = `
stream
	|from()
		.measurement('scale')
		.groupBy('deployment')
	%s
		.replicas(lambda: int("replicas"))
		.min(3)
		.max(500)
	|httpOut('TestStream_Autoscale')
`

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			var ctx context.Context
			tmInit := func(tm *kapacitor.TaskMaster) {
				ctx = tc.setup(tm)
			}

			testStreamerWithOutput(t, "TestStream_Autoscale", fmt.Sprintf(scriptTmpl, tc.script), 13*time.Second, tc.result, false, tmInit)

			updatesByService := tc.updatesByService(ctx)

			if !reflect.DeepEqual(updatesByService, expUpdatesByService) {
				t.Errorf("unexpected updates\ngot\n%v\nexp\n%v\n", updatesByService, expUpdatesByService)
			}
		})
		t.Run("min-max-"+name, func(t *testing.T) {
			var ctx context.Context
			tmInit := func(tm *kapacitor.TaskMaster) {
				ctx = tc.setup(tm)
			}

			testStreamerWithOutput(t, "TestStream_Autoscale", fmt.Sprintf(scriptMinMaxTmpl, tc.script), 13*time.Second, tc.minMaxResult, false, tmInit)

			updatesByService := tc.updatesByService(ctx)

			if !reflect.DeepEqual(updatesByService, expMinMaxUpdatesByService) {
				t.Errorf("unexpected updates\ngot\n%v\nexp\n%v\n", updatesByService, expMinMaxUpdatesByService)
			}
		})
	}
}

func TestStream_KapacitorLoopback_PreventLoop(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|kapacitorLoopback()
		.database('dbname')
		.retentionPolicy('rpname')
`

	// Create a new execution env
	tm, _, err := createTaskMaster(t, "testStreamer", false)
	if err != nil {
		t.Fatal(err)
	}
	tm.Open()

	// Create the task
	task, err := tm.NewTask("KapacitorLoopbackWithLoop", script, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Start the task
	_, err = tm.StartTask(task)
	if err == nil {
		t.Error("expected error about starting a task with a loop")
	}
}

func TestStream_KapacitorLoopback(t *testing.T) {
	var scriptLoop = `
stream
	|from()
		.measurement('cpu')
	|kapacitorLoopback()
		.database('new-dbname')
		.retentionPolicy('new-rpname')
`
	var scriptCount = `
stream
	|from()
		.measurement('cpu')
	|window()
		.every(10s)
		.period(10s)
	|count('value')
	|httpOut('TestStream_KapacitorLoopback')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					4.0,
				}},
			},
		},
	}
	var newDBRPs = []kapacitor.DBRP{
		{
			Database:        "new-dbname",
			RetentionPolicy: "new-rpname",
		},
	}
	// Create a new execution env
	tm, _, err := createTaskMaster(t, "testStreamer", false)
	if err != nil {
		t.Fatal(err)
	}
	tm.Open()
	defer checkDeferredErrors(t, tm.Close)()

	// Create the loopback task
	taskLoop, err := tm.NewTask("KapacitorLoopback-Loop", scriptLoop, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Create the count task
	taskCount, err := tm.NewTask("KapacitorLoopback-Count", scriptCount, kapacitor.StreamTask, newDBRPs, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Load test data
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	name := "TestStream_KapacitorLoopback"
	data, err := os.Open(path.Join(dir, "testdata", name+".srpl"))
	if err != nil {
		t.Fatal(err)
	}

	// Start the tasks
	etLoop, err := tm.StartTask(taskLoop)
	if err != nil {
		t.Fatal(err)
	}
	etCount, err := tm.StartTask(taskCount)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	stream, err := tm.Stream(name)
	if err != nil {
		t.Fatal(err)
	}
	// Use 1971 so that we don't get true negatives on Epoch 0 collisions
	clock := clock.New(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))

	replayErr := kapacitor.ReplayStreamFromIO(clock, data, stream, false, "s")

	// Advance time
	// Move time forward
	clock.Set(clock.Zero().Add(20 * time.Second))
	// Wait till the replay has finished
	if err := <-replayErr; err != nil {
		t.Fatal(err)
	}
	// Give the loopback data a chance to process, since we can't track it with the clock
	time.Sleep(10 * time.Millisecond)
	// Drain the task master and wait for the tasks to finish
	tm.Drain()
	etLoop.StopStats()
	etCount.StopStats()
	if err := etLoop.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := etCount.Wait(); err != nil {
		t.Fatal(err)
	}

	// Get the result
	output, err := etCount.GetOutput(name)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := models.Result{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		t.Fatal(err)
	}
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}

func TestBatch_KapacitorLoopback(t *testing.T) {
	var scriptLoop = `
stream
	|from()
		.measurement('cpu')
	|window()
		.every(5s)
		.period(5s)
	|kapacitorLoopback()
		.database('new-dbname')
		.retentionPolicy('new-rpname')
`
	var scriptCount = `
stream
	|from()
		.measurement('cpu')
	|window()
		.every(10s)
		.period(10s)
	|count('value')
	|httpOut('TestStream_KapacitorLoopback')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Columns: []string{"time", "count"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					4.0,
				}},
			},
		},
	}
	var newDBRPs = []kapacitor.DBRP{
		{
			Database:        "new-dbname",
			RetentionPolicy: "new-rpname",
		},
	}
	// Create a new execution env
	tm, _, err := createTaskMaster(t, "testStreamer", false)
	if err != nil {
		t.Fatal(err)
	}
	tm.Open()
	defer checkDeferredErrors(t, tm.Close)()

	// Create the loopback task
	taskLoop, err := tm.NewTask("KapacitorLoopback-Loop", scriptLoop, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Create the count task
	taskCount, err := tm.NewTask("KapacitorLoopback-Count", scriptCount, kapacitor.StreamTask, newDBRPs, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Load test data
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	name := "TestStream_KapacitorLoopback"
	data, err := os.Open(path.Join(dir, "testdata", name+".srpl"))
	if err != nil {
		t.Fatal(err)
	}

	// Start the tasks
	etLoop, err := tm.StartTask(taskLoop)
	if err != nil {
		t.Fatal(err)
	}
	etCount, err := tm.StartTask(taskCount)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	stream, err := tm.Stream(name)
	if err != nil {
		t.Fatal(err)
	}
	// Use 1971 so that we don't get true negatives on Epoch 0 collisions
	clock := clock.New(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))

	replayErr := kapacitor.ReplayStreamFromIO(clock, data, stream, false, "s")

	// Advance time
	// Move time forward
	clock.Set(clock.Zero().Add(20 * time.Second))
	// Wait till the replay has finished
	if err := <-replayErr; err != nil {
		t.Fatal(err)
	}
	// Give the loopback data a chance to process, since we can't track it with the clock
	time.Sleep(10 * time.Millisecond)
	// Drain the task master and wait for the tasks to finish
	tm.Drain()
	etLoop.StopStats()
	etCount.StopStats()
	if err := etLoop.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := etCount.Wait(); err != nil {
		t.Fatal(err)
	}

	// Get the result
	output, err := etCount.GetOutput(name)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := models.Result{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		t.Fatal(err)
	}
	if eq, msg := compareResults(er, result); !eq {
		t.Error(msg)
	}
}

func TestStream_Sideload(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var script = fmt.Sprintf(`
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('m')
		.groupBy('t0', 't1', 't2')
	|sideload()
		.source('file://%s/testdata/sideload')
		.order('t0/{{.t0}}.yml', 't1/{{.t1}}.yml', 't2/{{.t2}}.yml')
		.field('f1', 0)
		.field('f2', 0.0)
		.tag('t3', 'one')
	|log()
	|httpOut('TestStream_Sideload')
`, wd)

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "a", "t1": "m", "t2": "x", "t3": "one"},
				Columns: []string{"time", "f1", "f2", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.0,
						0.0,
						1.0,
					},
				},
			},
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "b", "t1": "n", "t2": "y", "t3": "why"},
				Columns: []string{"time", "f1", "f2", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						2.0,
						3.5,
						1.0,
					},
				},
			},
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "c", "t1": "o", "t2": "y", "t3": "why"},
				Columns: []string{"time", "f1", "f2", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						12.0,
						13.5,
						1.0,
					},
				},
			},
		},
	}
	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.SideloadService = sideload.NewService(diagService.NewSideloadHandler())
	}

	testStreamerWithOutput(t, "TestStream_Sideload", script, 1*time.Second, er, true, tmInit)
}

func TestStream_Sideload_JSON(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var script = fmt.Sprintf(`
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('m')
		.groupBy('t0', 't1', 't2')
	|sideload()
		.source('file://%s/testdata/sideload')
		.order('t0/{{.t0}}.json', 't1/{{.t1}}.json', 't2/{{.t2}}.yml')
		.field('f1', 0)
		.field('f2', 0.0)
		.tag('t3', 'one')
	|log()
	|httpOut('TestStream_Sideload')
`, wd)

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "a", "t1": "m", "t2": "x", "t3": "one"},
				Columns: []string{"time", "f1", "f2", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.0,
						0.0,
						1.0,
					},
				},
			},
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "b", "t1": "n", "t2": "y", "t3": "why"},
				Columns: []string{"time", "f1", "f2", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						2.0,
						3.5,
						1.0,
					},
				},
			},
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "c", "t1": "o", "t2": "y", "t3": "why"},
				Columns: []string{"time", "f1", "f2", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						12.0,
						13.5,
						1.0,
					},
				},
			},
		},
	}
	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.SideloadService = sideload.NewService(diagService.NewSideloadHandler())
	}

	testStreamerWithOutput(t, "TestStream_Sideload", script, 1*time.Second, er, true, tmInit)
}

func TestStream_Sideload_Multiple(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var script = fmt.Sprintf(`
stream
	|from()
		.database('dbname')
		.retentionPolicy('rpname')
		.measurement('m')
		.groupBy('t0', 't1', 't2')
	|sideload()
		.source('file://%[1]s/testdata/sideload')
		.order('t0/{{.t0}}.yml', 't1/{{.t1}}.yml', 't2/{{.t2}}.yml')
		.field('f1', 0)
		.field('f2', 0.0)
		.tag('t3', 'one')
	|sideload()
		.source('file://%[1]s/testdata/sideload')
		.order('t0/{{.t0}}.yml', 't1/{{.t1}}.yml', 't2/{{.t2}}.yml')
		.field('other', -1.0)
	|log()
	|httpOut('TestStream_Sideload')
`, wd)

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "a", "t1": "m", "t2": "x", "t3": "one"},
				Columns: []string{"time", "f1", "f2", "other", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.0,
						0.0,
						-1.0,
						1.0,
					},
				},
			},
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "b", "t1": "n", "t2": "y", "t3": "why"},
				Columns: []string{"time", "f1", "f2", "other", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						2.0,
						3.5,
						56.0,
						1.0,
					},
				},
			},
			{
				Name:    "m",
				Tags:    map[string]string{"t0": "c", "t1": "o", "t2": "y", "t3": "why"},
				Columns: []string{"time", "f1", "f2", "other", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						12.0,
						13.5,
						56.0,
						1.0,
					},
				},
			},
		},
	}
	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.SideloadService = sideload.NewService(diagService.NewSideloadHandler())
	}

	testStreamerWithOutput(t, "TestStream_Sideload", script, 1*time.Second, er, true, tmInit)
}

func TestStream_InfluxDBOut(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|count('value')
	|influxDBOut()
		.database('db')
		.retentionPolicy('rp')
		.measurement('m')
		.precision('s')
		.tag('key', 'value')
		.flushInterval(1ms)
`
	done := make(chan error, 1)
	var points []imodels.Point
	var database string
	var rp string
	var precision string

	influxdb := NewMockInfluxDBService(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		//Respond
		var data client.Response
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
		//Get request data
		database = r.URL.Query().Get("db")
		rp = r.URL.Query().Get("rp")
		precision = r.URL.Query().Get("precision")

		b, err := io.ReadAll(r.Body)
		if err != nil {
			done <- err
			return
		}
		points, err = imodels.ParsePointsWithPrecision(b, time.Unix(0, 0), precision)
		done <- err
	}))

	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.InfluxDBService = influxdb
	}
	testStreamerNoOutput(t, "TestStream_InfluxDBOut", script, 15*time.Second, tmInit)

	if database != "db" {
		t.Errorf("got %v exp %v", database, "db")
	}
	if rp != "rp" {
		t.Errorf("got %v exp %v", rp, "rp")
	}
	if precision != "s" {
		t.Errorf("got %v exp %v", precision, "s")
	}
	if len(points) != 1 {
		t.Errorf("got %v exp %v", len(points), 1)
	} else {
		p := points[0]
		if string(p.Name()) != "m" {
			t.Errorf("got %v exp %v", p.Name(), "m")
		}
		if fields, err := p.Fields(); fields["count"] != int64(10) {
			t.Errorf("got %v exp %v", fields["count"], 10.0)
		} else if err != nil {
			t.Errorf("expected no error on fields but got %v", err)
		}
		if len(p.Tags()) != 1 {
			t.Errorf("got %v exp %v", len(p.Tags()), 1)
		}
		if got, exp := p.Tags().GetString("key"), "value"; got != exp {
			t.Errorf("got %s exp %s", got, exp)
		}
		tm := time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC)
		if !tm.Equal(p.Time()) {
			t.Errorf("times are not equal exp %s got %s", tm, p.Time())
		}
	}
}
func TestStream_InfluxDBOut_CreateDatabase(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'nonexistant')
	|influxDBOut()
		.create()
		.database('db')
`

	done := make(chan error, 1)
	var createQuery string

	influxdb := NewMockInfluxDBService(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		createQuery = r.URL.Query().Get("q")
		done <- nil
	}))

	name := "TestStream_InfluxDBOut"

	// Create a new execution env
	tm, _, err := createTaskMaster(t, "testStreamer", false)
	if err != nil {
		t.Fatal(err)
	}
	tm.InfluxDBService = influxdb
	tm.Open()

	//Create the task
	task, err := tm.NewTask(name, script, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(et.Task.Dot()))
	defer checkDeferredErrors(t, tm.Close)()

	// Wait till we received a request
	if e := <-done; e != nil {
		t.Error(e)
	}

	expCreateQuery := `CREATE DATABASE db`
	if createQuery != expCreateQuery {
		t.Errorf("unexpected create database query: got %q exp: %q", createQuery, expCreateQuery)
	}
}
func TestStream_InfluxDBOut_CreateDatabaseAndRP(t *testing.T) {

	var script = `
stream
	|from()
		.measurement('cpu')
		.where(lambda: "host" == 'nonexistant')
	|influxDBOut()
		.create()
		.database('db')
		.retentionPolicy('rp')
`

	done := make(chan error, 1)
	var createQuery string

	influxdb := NewMockInfluxDBService(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		createQuery = r.URL.Query().Get("q")
		done <- nil
	}))

	name := "TestStream_InfluxDBOut"

	// Create a new execution env
	tm, _, err := createTaskMaster(t, "testStreamer", false)
	if err != nil {
		t.Fatal(err)
	}
	tm.InfluxDBService = influxdb
	tm.Open()

	//Create the task
	task, err := tm.NewTask(name, script, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(et.Task.Dot()))
	defer checkDeferredErrors(t, tm.Close)()

	// Wait till we received a request
	if e := <-done; e != nil {
		t.Error(e)
	}

	expCreateQuery := `CREATE DATABASE db WITH NAME rp`
	if createQuery != expCreateQuery {
		t.Errorf("unexpected create database query: got %q exp: %q", createQuery, expCreateQuery)
	}
}

func TestStream_Selectors(t *testing.T) {

	var script = `
stream
	|from().measurement('cpu')
	.where(lambda: "host" == 'serverA')
	|window()
		.period(10s)
		.every(10s)
	|last('value')
	|httpOut('TestStream_Selectors')
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA", "type": "idle"},
				Columns: []string{"time", "another", "last"},
				Values: [][]interface{}{[]interface{}{
					time.Date(1971, 1, 1, 0, 0, 10, 0, time.UTC),
					5.0,
					95.3,
				}},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Selectors", script, 15*time.Second, er, false, nil)
}

func TestStream_TopSelector(t *testing.T) {
	var script = `
stream
    |from()
		.measurement('scores')
		// Get the most recent score for each player
		.groupBy('game', 'player')
    |window()
        .period(2s)
        .every(2s)
        .align()
    |last('value')
    // Calculate the top 5 scores per game
    |groupBy('game')
    |top(5, 'last', 'player')
    |httpOut('TestStream_TopSelector')
`

	tw := time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC)
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "scores",
				Tags:    map[string]string{"game": "g0"},
				Columns: []string{"time", "player", "top"},
				Values: [][]interface{}{
					{tw, "p7", 978.0},
					{tw, "p10", 957.0},
					{tw, "p9", 878.0},
					{tw, "p5", 877.0},
					{tw, "p15", 791.0},
				},
			},
			{
				Name:    "scores",
				Tags:    map[string]string{"game": "g1"},
				Columns: []string{"time", "player", "top"},
				Values: [][]interface{}{
					{tw, "p19", 926.0},
					{tw, "p12", 887.0},
					{tw, "p0", 879.0},
					{tw, "p15", 872.0},
					{tw, "p16", 863.0},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_TopSelector", script, 10*time.Second, er, false, nil)
}

func TestStream_Sample_Count(t *testing.T) {
	var script = `
stream
    |from()
		.measurement('packets')
    |sample(2)
	|window()
		.every(4s)
		.period(4s)
		.align()
	|httpOut('TestStream_Sample')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 4, 0, time.UTC),
						1004.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1006.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Sample", script, 12*time.Second, er, false, nil)
}

func TestStream_Sample_Time(t *testing.T) {
	var script = `
stream
    |from()
		.measurement('packets')
    |sample(3s)
	|window()
		.every(4s)
		.period(4s)
		.align()
	|httpOut('TestStream_Sample')
`

	er := models.Result{
		Series: models.Rows{
			{
				Name:    "packets",
				Columns: []string{"time", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 6, 0, time.UTC),
						1006.0,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_Sample", script, 12*time.Second, er, false, nil)
}

func TestStream_DerivativeCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |derivative('usage_user')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"derivative2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_WhereCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |where(lambda: "host" == 'localhost') // replace with localhost
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"where2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_SampleCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |sample(2)
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"sample2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_WindowCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |window()
      .period(1s)
      .every(1s)
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"window2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_InfluxQLCardinalityStream(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |max('usage_user')
      .as('max')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"max2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_InfluxQLCardinalityBatch(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |window()
      .period(1s)
      .every(1s)
    |max('usage_user')
      .as('max')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"window2": map[string]interface{}{
			"emitted":             int64(81),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
		"max3": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(81),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_EvalCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |eval(lambda: sigma("usage_user"))
      .as('sigma')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"eval2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_FlattenCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |flatten()
     .on('host','cpu')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"flatten2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_GroupByCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
    |window()
     .period(1s)
     .every(1s)
    |groupBy('cpu')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"window2": map[string]interface{}{
			"emitted":             int64(9),
			"working_cardinality": int64(1),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
		"groupby3": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(9),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_AlertCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |alert()
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"alert2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
			"warns_triggered":     int64(0),
			"crits_triggered":     int64(0),
			"alerts_triggered":    int64(0),
			"alerts_inhibited":    int64(0),
			"oks_triggered":       int64(0),
			"infos_triggered":     int64(0),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_HTTPOutCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |httpOut('usage_user')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"http_out2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_K8sAutoscaleCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |k8sAutoscale()
     .resourceName('a')
     .replicas(lambda: int(0))
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"k8s_autoscale2": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(9),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(90),
			"increase_events":     int64(1),
			"decrease_events":     int64(0),
			"cooldown_drops":      int64(0),
		},
	}

	scaleUpdates := make(chan k8s.Scale, 100)
	k8sAutoscale := k8stest.Client{}
	k8sAutoscale.ScalesGetFunc = func(kind, name string) (*k8s.Scale, error) {
		var replicas int32
		switch name {
		case "serviceA":
			replicas = 1
		case "serviceB":
			replicas = 10
		}
		return &k8s.Scale{
			ObjectMeta: k8s.ObjectMeta{
				Name: name,
			},
			Spec: k8s.ScaleSpec{
				Replicas: replicas,
			},
		}, nil
	}
	k8sAutoscale.ScalesUpdateFunc = func(kind string, scale *k8s.Scale) error {
		scaleUpdates <- *scale
		return nil
	}
	tmInit := func(tm *kapacitor.TaskMaster) {
		tm.K8sService = k8sAutoscale
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, tmInit)
	close(scaleUpdates)
}

func TestStream_JoinCardinality(t *testing.T) {

	var script = `
var s1 = stream
    |from()
        .measurement('cpu')
        .groupBy('host')

var s2 = stream
    |from()
        .measurement('cpu')
        .groupBy('cpu')

s2|join(s1)
   .as('s1','s2')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(180),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from2": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"join4": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(10),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(180),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_CombineCardinality(t *testing.T) {

	var script = `
var s1 = stream
    |from()
        .measurement('cpu')
        .groupBy('cpu','host')
    |combine(lambda: TRUE, lambda: TRUE)
        .as('total','true')
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"combine2": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(9),
			"collected":           int64(90),
			"emitted":             int64(0),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func TestStream_MixedCardinality(t *testing.T) {

	var script = `
stream
    |from()
        .measurement('cpu')
        .groupBy('host','cpu')
    |where(lambda: "host" == 'localhost')
    |eval(lambda: sigma("usage_user"))
      .as('sigma')
    |where(lambda: "cpu" == 'cpu-total' OR "cpu" == 'cpu0' OR "cpu" == 'cpu1')
    |derivative('sigma')
    |alert()
`

	// Expected Stats
	es := map[string]map[string]interface{}{
		"stream0": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"from1": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(0),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"where2": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(9),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"eval3": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(9),
			"collected":           int64(90),
			"emitted":             int64(90),
		},
		"where4": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(9),
			"collected":           int64(90),
			"emitted":             int64(30),
		},
		"derivative5": map[string]interface{}{
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"working_cardinality": int64(3),
			"collected":           int64(30),
			"emitted":             int64(27),
		},
		"alert6": map[string]interface{}{
			"emitted":             int64(0),
			"working_cardinality": int64(3),
			"avg_exec_time_ns":    int64(0),
			"errors":              int64(0),
			"collected":           int64(27),
			"warns_triggered":     int64(0),
			"crits_triggered":     int64(0),
			"alerts_triggered":    int64(0),
			"alerts_inhibited":    int64(0),
			"oks_triggered":       int64(0),
			"infos_triggered":     int64(0),
		},
	}

	testStreamerCardinality(t, "TestStream_Cardinality", script, es, nil)
}

func testStreamerCardinality(
	t *testing.T,
	name, script string,
	expectedStats map[string]map[string]interface{},
	tmInit func(tm *kapacitor.TaskMaster),
) {
	clock, et, replayErr, tm := testStreamer(t, name, script, tmInit)
	defer checkDeferredErrors(t, tm.Close)()

	err := fastForwardTask(clock, et, replayErr, tm, 20*time.Second)
	if err != nil {
		t.Fatalf("Encountered error: %v", err)
	}
	stats, err := et.ExecutionStats()
	if err != nil {
		t.Fatalf("Encountered error: %v", err)
	}
	if !reflect.DeepEqual(expectedStats, stats.NodeStats) {
		t.Errorf("got:\n%+v\n\nexp:\n%+v\n", stats.NodeStats, expectedStats)
	}
}

func TestStream_StateDuration(t *testing.T) {
	var script = `
var data = stream
	|from().measurement('cpu')
	|groupBy('host')
data
	|stateDuration(lambda: "value" > 95)
		.unit(1ms)
		.as('my_duration')
	|window().period(4s).every(4s)
	|httpOut('TestStream_StateTracking')
data
	|stateDuration(lambda: "value" > 95) // discard
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "my_duration", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						0.0,
						97.1,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						1000.0,
						96.6,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						-1.0,
						83.6,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						0.0,
						99.1,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "my_duration", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						-1.0,
						47.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						0.0,
						95.1,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						2000.0,
						96.1,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_StateTracking", script, 4*time.Second, er, false, nil)
}

func TestStream_StateCount(t *testing.T) {
	var script = `
var data = stream
	|from().measurement('cpu')
	|groupBy('host')
data
	|stateCount(lambda: "value" > 95)
		.as('my_count')
	|window().period(4s).every(4s)
	|httpOut('TestStream_StateTracking')
data
	|stateCount(lambda: "value" > 95) // discard
`
	er := models.Result{
		Series: models.Rows{
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverA"},
				Columns: []string{"time", "my_count", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						1.0,
						97.1,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						2.0,
						96.6,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 2, 0, time.UTC),
						-1.0,
						83.6,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						1.0,
						99.1,
					},
				},
			},
			{
				Name:    "cpu",
				Tags:    map[string]string{"host": "serverB"},
				Columns: []string{"time", "my_count", "value"},
				Values: [][]interface{}{
					{
						time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
						-1.0,
						47.0,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC),
						1.0,
						95.1,
					},
					{
						time.Date(1971, 1, 1, 0, 0, 3, 0, time.UTC),
						2.0,
						96.1,
					},
				},
			},
		},
	}

	testStreamerWithOutput(t, "TestStream_StateTracking", script, 4*time.Second, er, false, nil)
}

func TestStream_AlertReset(t *testing.T) {
	requestCount := int32(0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result := models.Result{}
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddInt32(&requestCount, 1)
	}))
	defer ts.Close()

	var script = `
var critThreshold = 80.0

var critResetThreshold = 70.0

stream
	|from()
		.measurement('cpu')
		.groupBy('host')
	|alert()
		.id('kapacitor/{{ .Name }}/{{ index .Tags "host" }}')
		.details('details')
		.idField('id')
		.idTag('id')
		.levelField('level')
		.levelTag('level')
		.crit(lambda: "value" > critThreshold)
		.critReset(lambda: "value" < critResetThreshold)
		.topic('cpu')
		|httpPost('` + ts.URL + `')
`
	const count = 5
	const alertName = "TestStream_Alert"

	// Create a clock with a Zero point twice the number of
	clck := clock.New(time.Now().UTC().Add(time.Duration(count*-2) * time.Second))
	clck.Set(time.Now().UTC())

	dataChannel, fillFunc := makeAlertResetTestChannel(clck, count, 85.0, 100.0)
	go fillFunc()
	tm, store, cleanup := testStreamerWithInputChannel(t, alertName, script, dataChannel, clck, nil, nil, true)
	defer checkDeferredErrors(t, tm.Close)()
	cleanup()
	const alertID = "kapacitor/cpu/serverA"
	keys1, alertExists1, err := store.BucketEntries("cpu", alertID)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys1) != 2 {
		t.Fatalf("wrong number of keys, expected 2, got %d: %s", len(keys1), strings.Join(keys1, ", "))
	} else if !alertExists1 {
		t.Fatalf("missing alert history for %q", alertID)
	}

	dataChannel, fillFunc = makeAlertResetTestChannel(clck, 1, 23.0, 36.0)
	go fillFunc()
	_, _, cleanup = testStreamerWithInputChannel(t, alertName, script, dataChannel, clck, tm, nil, true)
	cleanup()

	keys2, alertExists2, err := store.BucketEntries("cpu", alertID)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys2) != 1 {
		t.Fatalf("wrong number of keys, expected 1, got %d: %s", len(keys2), strings.Join(keys2, ", "))
	} else if alertExists2 {
		t.Fatalf("alert history for %q not deleted", alertID)
	}
	if rc := atomic.LoadInt32(&requestCount); rc != count+1 {
		t.Errorf("got %v exp %v", rc, count+1)
	}
}

func makeAlertResetTestChannel(c clock.Clock, n int, low float64, high float64) (<-chan edge.PointMessage, func()) {
	dataChannel := make(chan edge.PointMessage)

	f := func() {
		for i := 0; i < n; i++ {
			var host string
			if i%2 == 0 {
				host = "serverA"
			} else {
				host = "serverB"
			}
			dataChannel <- edge.NewPointMessage(
				"cpu",
				"dbname",
				"rpname",
				models.Dimensions{},
				models.Fields{"value": rand.Float64()*(high-low) + low},
				models.Tags{"host": host, "type": "idle"},
				c.Zero().Add(time.Duration(i)*time.Second),
			)
		}
		time.Sleep(5 * time.Second)
		close(dataChannel)
	}
	return dataChannel, f
}

// Helper test function for streamer
func testStreamer(
	t *testing.T,
	name string,
	script string,
	tmInit func(tm *kapacitor.TaskMaster),
) (
	clock.Setter,
	*kapacitor.ExecutingTask,
	<-chan error,
	*kapacitor.TaskMaster,
) {
	t.Helper()
	if testing.Verbose() {
		wlog.SetLevel(wlog.DEBUG)
	} else {
		wlog.SetLevel(wlog.OFF)
	}

	// Create a new execution env
	tm, _, err := createTaskMaster(t, "testStreamer", false)
	if err != nil {
		t.Fatal(err)
	}
	if tmInit != nil {
		tmInit(tm)
	}
	if err = tm.Open(); err != nil {
		t.Fatal(err)
	}
	//Create the task
	task, err := tm.NewTask(name, script, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Load test data
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.Open(path.Join(dir, "testdata", name+".srpl"))
	if err != nil {
		t.Fatal(err)
	}
	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	// Replay test data to executor
	stream, err := tm.Stream(name)
	if err != nil {
		t.Fatal(err)
	}
	// Use 1971 so that we don't get true negatives on Epoch 0 collisions
	c := clock.New(time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC))

	replayErr := kapacitor.ReplayStreamFromIO(c, data, stream, false, "s")

	t.Log(string(et.Task.Dot()))
	return c, et, replayErr, tm
}

func fastForwardTask(
	clock clock.Setter,
	et *kapacitor.ExecutingTask,
	replayErr <-chan error,
	tm *kapacitor.TaskMaster,
	duration time.Duration,
) error {
	// Move time forward
	clock.Set(clock.Zero().Add(duration))
	// Wait till the replay has finished
	if err := <-replayErr; err != nil {
		return err
	}
	tm.Drain()
	et.StopStats()
	// Wait till the task is finished
	if err := et.Wait(); err != nil {
		return err
	}
	return nil
}

func testStreamerWithInputChannel(
	t *testing.T,
	name,
	script string,
	points <-chan edge.PointMessage,
	clck clock.Clock,
	tm *kapacitor.TaskMaster,
	tmInit func(tm *kapacitor.TaskMaster),
	persistTopic bool,
) (
	taskMaster *kapacitor.TaskMaster,
	store *storagetest.TestStore,
	cleanup func(),
) {
	if testing.Verbose() {
		wlog.SetLevel(wlog.DEBUG)
	} else {
		wlog.SetLevel(wlog.OFF)
	}

	var err error

	if tm == nil {
		// Create a new execution env
		tm, store, err = createTaskMaster(t, "testStreamer", persistTopic)
		if err != nil {
			t.Fatal(err)
		}
		if tmInit != nil {
			tmInit(tm)
		}
		if err = tm.Open(); err != nil {
			t.Fatal(err)
		}
	}
	//Create the task
	task, err := tm.NewTask(name, script, kapacitor.StreamTask, dbrps, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	//Start the task
	et, err := tm.StartTask(task)
	if err != nil {
		t.Fatal(err)
	}

	// Replay data from channel to exectutor
	stream, err := tm.Stream(name)
	if err != nil {
		t.Fatal(err)
	}
	replayErr := kapacitor.ReplayStreamFromChan(clck, points, stream, false)

	// Return cleanup function to caller to execute after data has all been sent
	cleanup = func() {
		// Wait till the replay has finished
		if err := <-replayErr; err != nil {
			t.Error(err)
		}
		tm.Drain()
		et.StopStats()
		// Wait till the task is finished
		if err := et.Wait(); err != nil {
			t.Error(err)
		}
		t.Log(string(et.Task.Dot()))
	}
	return tm, store, cleanup
}

func testStreamerNoOutput(
	t *testing.T,
	name,
	script string,
	duration time.Duration,
	tmInit func(tm *kapacitor.TaskMaster),
) {
	t.Helper()
	clock, et, replayErr, tm := testStreamer(t, name, script, tmInit)
	defer checkDeferredErrors(t, tm.Close)()
	err := fastForwardTask(clock, et, replayErr, tm, duration)
	if err != nil {
		t.Error(err)
	}
}

func testStreamerWithOutput(
	t *testing.T,
	name,
	script string,
	duration time.Duration,
	er models.Result,
	ignoreOrder bool,
	tmInit func(tm *kapacitor.TaskMaster),
) {
	t.Helper()
	clock, et, replayErr, tm := testStreamer(t, name, script, tmInit)
	defer checkDeferredErrors(t, tm.Close)()

	err := fastForwardTask(clock, et, replayErr, tm, duration)
	if err != nil {
		t.Error(err)
	}
	// Get the result
	output, err := et.GetOutput(name)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	// Assert we got the expected result
	result := models.Result{}
	if resp.Header.Get("Content-Encoding") == "gzip" {
		resp.Body, err = gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		t.Fatal(err)
	}
	if ignoreOrder {
		if eq, msg := compareResultsIgnoreSeriesOrder(er, result); !eq {
			t.Error(msg)
		}
	} else {
		if eq, msg := compareResults(er, result); !eq {
			t.Error(msg)
		}
	}
}

type step struct {
	t  time.Duration
	er models.Result
}

func testStreamerWithSteppedOutput(
	t *testing.T,
	name,
	script string,
	steps []step,
	er models.Result,
	ignoreOrder bool,
	tmInit func(tm *kapacitor.TaskMaster),
) {
	t.Skip("Test is not deterministic, need a mechanisim to safely step task execution.")
	clock, et, replayErr, tm := testStreamer(t, name, script, tmInit)
	defer checkDeferredErrors(t, tm.Close)()

	for s, step := range steps {
		// Move time forward
		clock.Set(clock.Zero().Add(step.t))
		// TODO: make this deterministic via a barrier or some such.
		time.Sleep(100 * time.Millisecond)

		// Get the result
		output, err := et.GetOutput(name)
		if err != nil {
			t.Fatal(err)
		}

		// TODO: this creates a race condition with the executing task
		//Read at github.com/influxdata/kapacitor.(*HTTPOutNode).Endpoint()
		//Previous write at github.com/influxdata/kapacitor.(*HTTPOutNode).runOut()
		resp, err := http.Get(output.Endpoint())
		if err != nil {
			t.Fatal(err)
		}

		// Assert we got the expected result
		result := models.Result{}
		err = json.NewDecoder(resp.Body).Decode(&result)
		if err != nil {
			t.Fatal(err)
		}
		if ignoreOrder {
			if eq, msg := compareResultsIgnoreSeriesOrder(step.er, result); !eq {
				t.Errorf("step %d: %s", s, msg)
			}
		} else {
			if eq, msg := compareResults(step.er, result); !eq {
				t.Errorf("step %d: %s", s, msg)
			}
		}
	}
	// Wait till the replay has finished
	if err := <-replayErr; err != nil {
		if err != nil {
			t.Error(err)
		}
	}
	tm.Drain()
	et.StopStats()
	// Wait till the task is finished
	if err := et.Wait(); err != nil {
		if err != nil {
			t.Error(err)
		}
	}

	// Get the last result
	output, err := et.GetOutput(name)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(output.Endpoint())
	if err != nil {
		t.Fatal(err)
	}

	// Assert we got the expected result
	result := models.Result{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		t.Fatal(err)
	}
	if ignoreOrder {
		if eq, msg := compareResultsIgnoreSeriesOrder(er, result); !eq {
			t.Errorf("final %s", msg)
		}
	} else {
		if eq, msg := compareResults(er, result); !eq {
			t.Errorf("final %s", msg)
		}
	}
}

func compareListIgnoreOrder(got, exp []interface{}, cmpF func(got, exp interface{}) bool) error {
	if len(got) != len(exp) {
		return fmt.Errorf("unequal lists ignoring order:\ngot\n%s\nexp\n%s\n", spew.Sdump(got), spew.Sdump(exp))
	}

	if cmpF == nil {
		cmpF = func(got, exp interface{}) bool {
			return reflect.DeepEqual(got, exp)
		}
	}

	for _, e := range exp {
		found := false
		for _, g := range got {
			if cmpF(g, e) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unequal lists ignoring order:\ngot\n%s\nexp\n%s\n", spew.Sdump(got), spew.Sdump(exp))
		}
	}
	return nil
}

func createTaskMaster(t storagetest.CleanedTest, name string, persistTopics bool) (*kapacitor.TaskMaster, *storagetest.TestStore, error) {
	d := diagService.NewKapacitorHandler()
	tm := kapacitor.NewTaskMaster(name, newServerInfo(), d)
	httpdService := newHTTPDService()
	tm.HTTPDService = httpdService
	tm.TaskStore = taskStore{}
	tm.DeadmanService = deadman{}
	tm.HTTPPostService, _ = httppost.NewService(nil, diagService.NewHTTPPostHandler())
	as := alertservice.NewService(diagService.NewAlertServiceHandler(), nil, 0)
	as.PersistTopics = persistTopics
	store := storagetest.New(t, diagService.NewStorageHandler())
	tm.TestCloser = store
	as.StorageService = store
	as.HTTPDService = httpdService
	if err := as.Open(); err != nil {
		return nil, nil, err
	}
	tm.AlertService = as
	return tm, store, nil
}
