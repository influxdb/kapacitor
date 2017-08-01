package kapacitor

import (
	"log"
	"time"
	"strings"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type DerivativeNode struct {
	node
	d *pipeline.DerivativeNode
}

// Create a new derivative node.
func newDerivativeNode(et *ExecutingTask, n *pipeline.DerivativeNode, l *log.Logger) (*DerivativeNode, error) {
	dn := &DerivativeNode{
		node: node{Node: n, et: et, logger: l},
		d:    n,
	}
	// Create stateful expressions
	dn.node.runF = dn.runDerivative
	return dn, nil
}

func (n *DerivativeNode) runDerivative([]byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *DerivativeNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup()),
	), nil
}

func (n *DerivativeNode) newGroup() *derivativeGroup {
	return &derivativeGroup{
		n: n,
	}
}

type derivativeGroup struct {
	n        *DerivativeNode
	previous edge.FieldsTagsTimeGetter
}

func (g *derivativeGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	if s := begin.SizeHint(); s > 0 {
		begin = begin.ShallowCopy()
		begin.SetSizeHint(s - 1)
	}
	g.previous = nil
	return begin, nil
}

func (g *derivativeGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	np := bp.ShallowCopy()
	emit := g.doDerivative(bp, np)
	if emit {
		return np, nil
	}
	return nil, nil
}

func (g *derivativeGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *derivativeGroup) Point(p edge.PointMessage) (edge.Message, error) {
	np := p.ShallowCopy()
	emit := g.doDerivative(p, np)
	if emit {
		return np, nil
	}
	return nil, nil
}

// doDerivative computes the derivative with respect to g.previous and p.
// The resulting derivative value will be set on n.
func (g *derivativeGroup) doDerivative(p edge.FieldsTagsTimeGetter, n edge.FieldsTagsTimeSetter) bool {
	var prevFields, currFields models.Fields
	var prevTime, currTime time.Time
	if g.previous != nil {
		prevFields = g.previous.Fields()
		prevTime = g.previous.Time()
	}
	currFields = p.Fields()
	currTime = p.Time()
	value, store, emit := g.n.derivative(
		prevFields, currFields,
		prevTime, currTime,
	)
	if store {
		g.previous = p
	}
	if !emit {
		return false
	}

	fields := n.Fields().Copy()
	fields[g.n.d.As] = value
	n.SetFields(fields)
	return true
}

func (g *derivativeGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *derivativeGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}

// derivative calculates the derivative between prev and cur.
// Return is the resulting derivative, whether the current point should be
// stored as previous, and whether the point result should be emitted.
func (n *DerivativeNode) derivative(prev, curr models.Fields, prevTime, currTime time.Time) (float64, bool, bool) {
	elapsed := float64(currTime.Sub(prevTime))
	if elapsed == 0 {
		n.incrementErrorCount()
		n.logger.Printf("E! cannot perform derivative elapsed time was 0")
		return 0, true, false
	}
	f1 := curr[n.d.Field]
	f0 := prev[n.d.Field]
	diff, ok := diffFunc(f0,f1)
	if !ok {
		// The only time this will fail to parse is if there is no previous.
		// Because we only return `store=true` if current parses successfully, we will
		// never get a previous which doesn't parse.
		return 0, true, false
	}
	// Drop negative values for non-negative derivatives
	if n.d.NonNegativeFlag && diff < 0 {
		return 0, true, false
	}

	value := float64(diff) / (elapsed / float64(n.d.Unit))
	return value, true, true
}

func numToFloat(num interface{}) (float64, bool) {
	switch n := num.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func diffFunc(f0,f1 interface{})(float64, bool){
	n0 , ok := numToFloat(f0)
	n1 , ok1 := numToFloat(f1)
	if ok&&ok1{
		return n1-n0,true
	}
	s0 , ok := f0.(string)
	s1 , ok1 := f1.(string)
	if ok&&ok1{
		return float64(strings.Compare(s1,s0)),true
	}
	return 0 , false
}

