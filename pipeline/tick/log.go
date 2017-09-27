package tick

import (
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// Log converts the Log pipeline node into the TICKScript AST
type Log struct {
	Function
}

// Build creates a Log ast.Node
func (n *Log) Build(l *pipeline.LogNode) (ast.Node, error) {
	n.Pipe("log").
		Dot("level", l.Level).
		Dot("prefix", l.Prefix)

	return n.prev, n.err
}