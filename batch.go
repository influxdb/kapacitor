package kapacitor

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/gorhill/cronexpr"
	"github.com/influxdata/influxql"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/pkg/errors"
)

const (
	statsBatchesQueried = "batches_queried"
	statsPointsQueried  = "points_queried"
)

type BatchNode struct {
	node
	s   *pipeline.BatchNode
	idx int
}

func newBatchNode(et *ExecutingTask, n *pipeline.BatchNode, d NodeDiagnostic) (*BatchNode, error) {
	sn := &BatchNode{
		node: node{Node: n, et: et, diag: d},
		s:    n,
	}
	return sn, nil
}

func (n *BatchNode) linkChild(c Node) error {

	// add child
	if n.Provides() != c.Wants() {
		return fmt.Errorf("cannot add child mismatched edges: %s -> %s", n.Provides(), c.Wants())
	}
	n.children = append(n.children, c)

	// add parent
	c.addParent(n)

	return nil
}

func (n *BatchNode) addParentEdge(in edge.StatsEdge) {
	// Pass edges down to children
	n.children[n.idx].addParentEdge(in)
	n.idx++
}

func (n *BatchNode) start([]byte) {
}

func (n *BatchNode) Wait() error {
	return nil
}

// Return list of databases and retention policies
// the batcher will query.
func (n *BatchNode) DBRPs() ([]DBRP, error) {
	var dbrps []DBRP
	for _, b := range n.children {
		switch b := b.(type) {
		case *QueryNode:
			if b != nil {
				d, err := b.DBRPs()
				if err != nil {
					return nil, err
				}
				dbrps = append(dbrps, d...)
			}
		case *FluxQueryNode:
			// flux queries don't really have DBRPs
		default:
			panic("BatchNode shouldn't be followed by anything except QueryNode or QueryFluxNode")
		}
	}
	return dbrps, nil
}

func (n *BatchNode) Count() int {
	return len(n.children)
}

func (n *BatchNode) Start() {
	for _, b := range n.children {
		switch b := b.(type) {
		case *QueryNode:
			b.Start()
		case *FluxQueryNode:
			b.Start()
		default:
			panic("BatchNode shouldn't be followed by anything except QueryNode or QueryFluxNode")
		}
	}
}

func (n *BatchNode) Abort() {
	for _, b := range n.children {
		switch b := b.(type) {
		case *QueryNode:
			b.Abort()
		case *FluxQueryNode:
			b.Abort()
		default:
			panic("BatchNode shouldn't be followed by anything except QueryNode or QueryFluxNode")
		}
	}
}

type BatchQueries struct {
	Queries            []*Query
	FluxQueries        []*QueryFlux
	Cluster            string
	GroupByMeasurement bool
}

func (n *BatchNode) Queries(start, stop time.Time) ([]BatchQueries, error) {
	queries := make([]BatchQueries, len(n.children))
	for i, qn := range n.children {
		switch qn := qn.(type) {
		case *QueryNode:
			qs, err := qn.Queries(start, stop)
			if err != nil {
				return nil, err
			}
			queries[i] = BatchQueries{
				Queries:            qs,
				Cluster:            qn.Cluster(),
				GroupByMeasurement: qn.GroupByMeasurement(),
			}

		case *FluxQueryNode:
			qs, err := qn.Queries(start, stop)
			if err != nil {
				return nil, err
			}
			queries[i] = BatchQueries{
				FluxQueries: qs,
				Cluster:     qn.Cluster(),
			}

		default:
			panic("BatchNode shouldn't be followed by anything except QueryNode or QueryFluxNode")
		}

	}
	return queries, nil
}

// Do not add the source batch node to the dot output
// since its not really an edge.
func (n *BatchNode) edot(*bytes.Buffer, bool) {}

func (n *BatchNode) collectedCount() (count int64) {
	for _, child := range n.children {
		count += child.collectedCount()
	}
	return
}

type QueryNode struct {
	node
	b        *pipeline.QueryNode
	query    *Query
	ticker   ticker
	queryMu  sync.Mutex
	queryErr chan error
	closing  chan struct{}
	aborting chan struct{}

	batchesQueried *expvar.Int
	pointsQueried  *expvar.Int
	byName         bool
}

func newQueryNode(et *ExecutingTask, n *pipeline.QueryNode, d NodeDiagnostic) (*QueryNode, error) {
	bn := &QueryNode{
		node:     node{Node: n, et: et, diag: d},
		b:        n,
		closing:  make(chan struct{}),
		aborting: make(chan struct{}),
		byName:   n.GroupByMeasurementFlag,
	}
	bn.node.runF = bn.runBatch
	bn.node.stopF = bn.stopBatch

	// Create query
	q, err := NewQuery(n.QueryStr)
	if err != nil {
		return nil, err
	}
	bn.query = q
	// Add in dimensions
	err = bn.query.Dimensions(n.Dimensions)
	if err != nil {
		return nil, err
	}
	// Set offset alignment
	if n.AlignGroupFlag {
		bn.query.AlignGroup()
	}
	// Set fill
	switch fill := n.Fill.(type) {
	case string:
		switch fill {
		case "null":
			bn.query.Fill(influxql.NullFill, nil)
		case "none":
			bn.query.Fill(influxql.NoFill, nil)
		case "previous":
			bn.query.Fill(influxql.PreviousFill, nil)
		case "linear":
			bn.query.Fill(influxql.LinearFill, nil)
		default:
			return nil, fmt.Errorf("unexpected fill option %s", fill)
		}
	case int64, float64:
		bn.query.Fill(influxql.NumberFill, fill)
	}

	// Determine schedule
	if n.Every != 0 && n.Cron != "" {
		return nil, errors.New("must not set both 'every' and 'cron' properties")
	}
	switch {
	case n.Every > 0:
		bn.ticker = newTimeTicker(n.Every, n.AlignFlag)
	case n.Cron != "":
		var err error
		bn.ticker, err = newCronTicker(n.Cron)
		if err != nil {
			return nil, err
		}
	case n.Every < 0:
		return nil, errors.New("'every' duration must must non-negative")
	default:
		return nil, errors.New("must define one of 'every' or 'cron'")
	}

	return bn, nil
}

func (n *QueryNode) GroupByMeasurement() bool {
	return n.byName
}

// Return list of databases and retention policies
// the batcher will query.
func (n *QueryNode) DBRPs() ([]DBRP, error) {
	return n.query.DBRPs()
}

func (n *QueryNode) Start() {
	n.queryMu.Lock()
	defer n.queryMu.Unlock()
	n.queryErr = make(chan error, 1)
	go func() {
		n.queryErr <- n.doQuery(n.ins[0])
	}()
}

func (n *QueryNode) Abort() {
	close(n.aborting)
}

func (n *QueryNode) Cluster() string {
	return n.b.Cluster
}

func (n *QueryNode) Queries(start, stop time.Time) ([]*Query, error) {
	now := time.Now()
	if stop.IsZero() {
		stop = now
	}
	// Crons are sensitive to timezones.
	// Make sure we are using local time.
	current := start.Local()
	queries := make([]*Query, 0)
	for {
		current = n.ticker.Next(current)
		if current.IsZero() || current.After(stop) {
			break
		}
		qstop := current.Add(-1 * n.b.Offset)
		if qstop.After(now) {
			break
		}

		q, err := n.query.Clone()
		if err != nil {
			return nil, err
		}
		q.SetStartTime(qstop.Add(-1 * n.b.Period))
		q.SetStopTime(qstop)
		queries = append(queries, q)
	}
	return queries, nil
}

// Query InfluxDB and collect batches on batch collector.
func (n *QueryNode) doQuery(in edge.Edge) error {
	defer in.Close()
	n.batchesQueried = &expvar.Int{}
	n.pointsQueried = &expvar.Int{}

	n.statMap.Set(statsBatchesQueried, n.batchesQueried)
	n.statMap.Set(statsPointsQueried, n.pointsQueried)

	if n.et.tm.InfluxDBService == nil {
		return errors.New("InfluxDB not configured, cannot query InfluxDB for batch query")
	}

	con, err := n.et.tm.InfluxDBService.NewNamedClient(n.b.Cluster)
	if err != nil {
		return errors.Wrap(err, "failed to get InfluxDB client")
	}
	tickC := n.ticker.Start()
	for {
		select {
		case <-n.closing:
			return nil
		case <-n.aborting:
			return errors.New("batch doQuery aborted")
		case now := <-tickC:
			n.timer.Start()
			// Update times for query
			stop := now.Add(-1 * n.b.Offset)
			n.query.SetStartTime(stop.Add(-1 * n.b.Period))
			n.query.SetStopTime(stop)

			qStr := n.query.String()
			n.diag.StartingBatchQuery(qStr)

			// Execute query
			q := influxdb.Query{
				Command: qStr,
			}
			resp, err := con.Query(q)
			if err != nil {
				n.diag.Error("error executing query", err)
				n.timer.Stop()
				break
			}

			// Collect batches
			for _, res := range resp.Results {
				batches, err := edge.ResultToBufferedBatches(res, n.byName)
				if err != nil {
					n.diag.Error("failed to understand query result", err)
					continue
				}
				for _, bch := range batches {
					// Set stop time based off query bounds
					if bch.Begin().Time().IsZero() || n.query.IsGroupedByTime() {
						bch.Begin().SetTime(stop)
					}

					n.batchesQueried.Add(1)
					n.pointsQueried.Add(int64(len(bch.Points())))

					n.timer.Pause()
					if err := in.Collect(bch); err != nil {
						return err
					}
					n.timer.Resume()
				}
			}
			n.timer.Stop()
		}
	}
}

func (n *QueryNode) runBatch([]byte) error {
	errC := make(chan error, 1)
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				errC <- fmt.Errorf("%v", err)
			}
		}()
		for bt, ok := n.ins[0].Emit(); ok; bt, ok = n.ins[0].Emit() {
			for _, child := range n.outs {
				err := child.Collect(bt)
				if err != nil {
					errC <- err
					return
				}
			}
		}
		errC <- nil
	}()
	var queryErr error
	n.queryMu.Lock()
	if n.queryErr != nil {
		n.queryMu.Unlock()
		select {
		case queryErr = <-n.queryErr:
		case <-n.aborting:
			queryErr = errors.New("batch queryErr aborted")
		}
	} else {
		n.queryMu.Unlock()
	}

	var err error
	select {
	case err = <-errC:
	case <-n.aborting:
		err = errors.New("batch run aborted")
	}
	if queryErr != nil {
		return queryErr
	}
	return err
}

func (n *QueryNode) stopBatch() {
	if n.ticker != nil {
		n.ticker.Stop()
	}
	close(n.closing)
}

type ticker interface {
	Start() <-chan time.Time
	Stop()
	// Return the next time the ticker will tick after now.
	Next(now time.Time) time.Time
}

type timeTicker struct {
	every     time.Duration
	align     bool
	alignChan chan time.Time
	stopping  chan struct{}
	ticker    *time.Ticker
	mu        sync.Mutex
	wg        sync.WaitGroup
}

func newTimeTicker(every time.Duration, align bool) *timeTicker {
	t := &timeTicker{
		align: align,
		every: every,
	}
	if align {
		t.alignChan = make(chan time.Time)
		t.stopping = make(chan struct{})
	}
	return t
}

func (t *timeTicker) Start() <-chan time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.alignChan != nil {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			// Sleep until we are roughly aligned
			now := time.Now()
			next := now.Truncate(t.every).Add(t.every)
			after := time.NewTicker(next.Sub(now))
			select {
			case <-after.C:
				after.Stop()
			case <-t.stopping:
				after.Stop()
				return
			}
			t.ticker = time.NewTicker(t.every)
			// Send first event since we waited for it explicitly
			t.alignChan <- next
			for {
				select {
				case <-t.stopping:
					return
				case now := <-t.ticker.C:
					now = now.Round(t.every)
					t.alignChan <- now
				}
			}
		}()
		return t.alignChan
	} else {
		t.ticker = time.NewTicker(t.every)
		return t.ticker.C
	}
}

func (t *timeTicker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ticker != nil {
		t.ticker.Stop()
	}
	if t.alignChan != nil {
		close(t.stopping)
	}
	t.wg.Wait()
}

func (t *timeTicker) Next(now time.Time) time.Time {
	next := now.Add(t.every)
	if t.align {
		next = next.Round(t.every)
	}
	return next
}

type cronTicker struct {
	expr    *cronexpr.Expression
	ticker  chan time.Time
	closing chan struct{}
	wg      sync.WaitGroup
}

func newCronTicker(cronExpr string) (*cronTicker, error) {
	expr, err := cronexpr.Parse(cronExpr)
	if err != nil {
		return nil, err
	}
	return &cronTicker{
		expr:    expr,
		ticker:  make(chan time.Time),
		closing: make(chan struct{}),
	}, nil
}

func (c *cronTicker) Start() <-chan time.Time {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			now := time.Now()
			next := c.expr.Next(now)
			diff := next.Sub(now)
			select {
			case <-time.After(diff):
				c.ticker <- next
			case <-c.closing:
				return
			}
		}
	}()
	return c.ticker
}

func (c *cronTicker) Stop() {
	close(c.closing)
	c.wg.Wait()
}

func (c *cronTicker) Next(now time.Time) time.Time {
	return c.expr.Next(now)
}

// FluxQueryNode is a node for making flux queries
type FluxQueryNode struct {
	node
	b        *pipeline.QueryFluxNode
	query    *QueryFlux
	ticker   ticker
	queryMu  sync.Mutex
	queryErr chan error
	closing  chan struct{}
	aborting chan struct{}

	batchesQueried *expvar.Int
	pointsQueried  *expvar.Int
	byName         bool
}

func newQueryFluxNode(et *ExecutingTask, n *pipeline.QueryFluxNode, d NodeDiagnostic) (*FluxQueryNode, error) {
	bn := &FluxQueryNode{
		node:     node{Node: n, et: et, diag: d},
		b:        n,
		closing:  make(chan struct{}),
		aborting: make(chan struct{}),
	}
	bn.node.runF = bn.runBatch
	bn.node.stopF = bn.stopBatch

	// Create query
	q, err := NewQueryFlux(n.QueryStr, n.Org, n.OrgID)
	if err != nil {
		return nil, err
	}
	bn.query = q
	// Determine schedule
	if n.Every != 0 && n.Cron != "" {
		return nil, errors.New("must not set both 'every' and 'cron' properties")
	}
	switch {
	case n.Every > 0:
		bn.ticker = newTimeTicker(n.Every, n.AlignFlag)
	case n.Cron != "":
		var err error
		bn.ticker, err = newCronTicker(n.Cron)
		if err != nil {
			return nil, err
		}
	case n.Every < 0:
		return nil, errors.New("'every' duration must must non-negative")
	default:
		return nil, errors.New("must define one of 'every' or 'cron'")
	}

	return bn, nil
}

func (n *FluxQueryNode) Start() {
	n.queryMu.Lock()
	defer n.queryMu.Unlock()
	n.queryErr = make(chan error, 1)
	go func() {
		n.queryErr <- n.doQuery(n.ins[0])
	}()
}

func (n *FluxQueryNode) Abort() {
	close(n.aborting)
}

func (n *FluxQueryNode) Cluster() string {
	return n.b.Cluster
}

func (n *FluxQueryNode) Queries(start, stop time.Time) ([]*QueryFlux, error) {
	now := time.Now()
	if stop.IsZero() {
		stop = now
	}
	// Crons are sensitive to timezones.
	// Make sure we are using local time.
	current := start.Local()
	queries := make([]*QueryFlux, 0)
	for {
		current = n.ticker.Next(current)
		if current.IsZero() || current.After(stop) {
			break
		}
		qstop := current.Add(-1 * n.b.Offset)
		if qstop.After(now) {
			break
		}

		q, err := n.query.Clone()
		if err != nil {
			return nil, err
		}
		q.Now = now
		queries = append(queries, q)
	}
	return queries, nil
}

// Query InfluxDB and collect batches on batch collector.
func (n *FluxQueryNode) doQuery(in edge.Edge) (err error) {
	defer in.Close()
	n.batchesQueried = &expvar.Int{}
	n.pointsQueried = &expvar.Int{}

	n.statMap.Set(statsBatchesQueried, n.batchesQueried)
	n.statMap.Set(statsPointsQueried, n.pointsQueried)

	if n.et.tm.InfluxDBService == nil {
		return errors.New("InfluxDB not configured, cannot query InfluxDB for batch query")
	}

	con, err := n.et.tm.InfluxDBService.NewNamedClient(n.b.Cluster)
	if err != nil {
		return errors.Wrap(err, "failed to get InfluxDB client")
	}
	tickC := n.ticker.Start()
	for {
		select {
		case <-n.closing:
			return nil
		case <-n.aborting:
			return errors.New("batch doQuery aborted")
		case now := <-tickC:
			n.timer.Start()
			// Update times for query
			n.query.Now = now.Add(-1 * n.b.Offset) //SetStartTime(stop.Add(-1 * n.b.Period))
			n.diag.StartingBatchQuery(n.query.stmt)

			// Execute query
			resp, err := con.QueryFluxResponse(influxdb.FluxQuery{
				Query: n.query.stmt,
				Org:   n.query.org,
				OrgID: n.query.orgID,
				Now:   n.query.Now,
			})
			if err != nil {
				n.diag.Error("error executing query", err)
				n.timer.Stop()
				break
			}
			//Collect batches
			for _, res := range resp.Results {
				batches, err := edge.ResultToBufferedBatches(res, n.byName)
				if err != nil {
					n.diag.Error("failed to understand query result", err)
					continue
				}
				for _, bch := range batches {
					// Set stop time based off query bounds
					if bch.Begin().Time().IsZero() {
						bch.Begin().SetTime(now)
					}
					n.batchesQueried.Add(1)
					n.pointsQueried.Add(int64(len(bch.Points())))

					n.timer.Pause()
					if err := in.Collect(bch); err != nil {
						return err
					}
					n.timer.Resume()
				}
			}
			n.timer.Stop()
		}
	}
}

func (n *FluxQueryNode) runBatch([]byte) error {
	errC := make(chan error, 1)
	go func() {
		defer func() {
			err := recover()
			if err != nil {
				errC <- fmt.Errorf("%v", err)
			}
		}()
		for bt, ok := n.ins[0].Emit(); ok; bt, ok = n.ins[0].Emit() {
			for _, child := range n.outs {
				err := child.Collect(bt)
				if err != nil {
					errC <- err
					return
				}
			}
		}
		errC <- nil
	}()
	var queryErr error
	n.queryMu.Lock()
	if n.queryErr != nil {
		n.queryMu.Unlock()
		select {
		case queryErr = <-n.queryErr:
		case <-n.aborting:
			queryErr = errors.New("batch queryErr aborted")
		}
	} else {
		n.queryMu.Unlock()
	}

	var err error
	select {
	case err = <-errC:
	case <-n.aborting:
		err = errors.New("batch run aborted")
	}
	if queryErr != nil {
		return queryErr
	}
	return err
}

func (n *FluxQueryNode) stopBatch() {
	if n.ticker != nil {
		n.ticker.Stop()
	}
	close(n.closing)
}
