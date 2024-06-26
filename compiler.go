package eval

import (
	"fmt"
	"math"
	"sort"
)

type CompileOption string

const (
	Optimize        CompileOption = "optimize" // switch for all optimizations
	Reordering      CompileOption = "reordering"
	FastEvaluation  CompileOption = "fast_evaluation"
	ReduceNesting   CompileOption = "reduce_nesting"
	ConstantFolding CompileOption = "constant_folding"

	Debug                  CompileOption = "debug"
	ReportEvent            CompileOption = "report_event"
	InfixNotation          CompileOption = "infix_notation"
	AllowUndefinedVariable CompileOption = "allow_undefined_variable"
)

type optimizer func(config *Config, root *astNode)

var (
	optimizations = []CompileOption{ConstantFolding, ReduceNesting, FastEvaluation, Reordering}
	optimizerMap  = map[CompileOption]optimizer{
		ConstantFolding: optimizeConstantFolding,
		ReduceNesting:   optimizeReduceNesting,
		FastEvaluation:  optimizeFastEvaluation,
		Reordering:      optimizeReordering,
	}
)

func CopyConfig(origin *Config) *Config {
	conf := NewConfig()
	if origin == nil {
		return conf
	}
	copyConfig(conf, origin)
	return conf
}

func copyConfig(dst, src *Config) {
	for k, v := range src.ConstantMap {
		dst.ConstantMap[k] = v
	}
	for k, v := range src.VariableKeyMap {
		dst.VariableKeyMap[k] = v
	}
	for k, v := range src.OperatorMap {
		dst.OperatorMap[k] = v
	}
	for k, v := range src.CompileOptions {
		dst.CompileOptions[k] = v
	}
	for k, v := range src.CostsMap {
		dst.CostsMap[k] = v
	}
	for _, op := range src.StatelessOperators {
		dst.StatelessOperators = append(dst.StatelessOperators, op)
	}
}

type Option func(conf *Config)

var (
	EnableUndefinedVariable Option = func(c *Config) {
		c.CompileOptions[AllowUndefinedVariable] = true
	}
	EnableDebug Option = func(c *Config) {
		c.CompileOptions[Debug] = true
	}
	EnableReportEvent Option = func(c *Config) {
		c.CompileOptions[ReportEvent] = true
	}
	Optimizations = func(enable bool, opts ...CompileOption) Option {
		return func(c *Config) {
			if len(opts) == 0 || (len(opts) == 1 && opts[0] == Optimize) {
				opts = optimizations
			}

			for _, opt := range opts {
				if optimizerMap[opt] != nil {
					c.CompileOptions[opt] = enable
				}
			}
		}
	}
	EnableInfixNotation Option = func(c *Config) {
		c.CompileOptions[InfixNotation] = true
	}

	// RegVarAndOp registers variables and operators to config
	RegVarAndOp = func(vals map[string]interface{}) Option {
		return func(c *Config) {
			for k, v := range vals {
				switch a := v.(type) {
				case Operator:
					c.OperatorMap[k] = a
				case func(*Ctx, []Value) (Value, error):
					c.OperatorMap[k] = a
				default:
					GetOrRegisterKey(c, k)
				}
			}
		}
	}

	// ExtendConf extends source config
	ExtendConf = func(src *Config) Option {
		return func(c *Config) {
			if src != nil {
				copyConfig(c, src)
			}
		}
	}
)

func NewConfig(opts ...Option) *Config {
	conf := &Config{
		ConstantMap:        make(map[string]Value),
		OperatorMap:        make(map[string]Operator),
		VariableKeyMap:     make(map[string]VariableKey),
		CompileOptions:     make(map[CompileOption]bool),
		CostsMap:           make(map[string]float64),
		StatelessOperators: []string{},
	}
	for _, opt := range opts {
		opt(conf)
	}
	return conf
}

type Config struct {
	ConstantMap    map[string]Value
	OperatorMap    map[string]Operator
	VariableKeyMap map[string]VariableKey

	// cost of performance
	CostsMap map[string]float64

	// compile options
	CompileOptions map[CompileOption]bool

	// StatelessOperators will be used in optimizeConstantFolding,
	// so please make sure when adding new operators into StatelessOperators
	StatelessOperators []string
}

func (cc *Config) getCosts(nodeType uint8, nodeName string) float64 {
	const (
		defaultCost  float64 = 5
		variableCost float64 = 7
		operatorCost float64 = 10

		variableNode = "variable"
		operatorNode = "operator"
	)

	if v, exist := cc.CostsMap[nodeName]; exist {
		return v
	}

	switch nodeType {
	case variable:
		if v, exist := cc.CostsMap[variableNode]; exist {
			return v
		}
		return variableCost
	case operator, fastOperator:
		if v, exist := cc.CostsMap[operatorNode]; exist {
			return v
		}
		return operatorCost
	default:
		return defaultCost
	}
}

func Compile(originConf *Config, exprStr string) (*Expr, error) {
	ast, conf, err := newParser(originConf, exprStr).parse()
	if err != nil {
		return nil, err
	}

	optimize(conf, ast)

	res := check(ast)
	if res.err != nil {
		return nil, res.err
	}

	expr := buildExpr(conf, ast, res.size)

	return expr, nil
}

func optimize(cc *Config, root *astNode) {
	for _, opt := range optimizations {
		enabled, exist := cc.CompileOptions[opt]
		if enabled || !exist {
			optimizerMap[opt](cc, root)
		}
	}
}

func optimizeReduceNesting(cc *Config, root *astNode) {
	for _, child := range root.children {
		optimizeReduceNesting(cc, child)
	}

	n := root.node
	if !isBoolOpNode(n) {
		return
	}

	var children []*astNode
	rootOpType := isAndOpNode(n)
	for _, child := range root.children {
		cn := child.node
		if typ := cn.getNodeType(); typ == constant || typ == variable {
			children = append(children, child)
			continue
		}
		if !isBoolOpNode(cn) {
			return
		}
		if isAndOpNode(cn) == rootOpType {
			children = append(children, child.children...)
			continue
		}
		return
	}

	root.children = children
}

func isBoolOpNode(n *node) bool {
	return isAndOpNode(n) || isOrOpNode(n)
}

func isAndOpNode(n *node) bool {
	nodeType := n.getNodeType()
	if nodeType != operator && nodeType != fastOperator {
		return false
	}
	v := n.value.(string)
	return v == "and" || v == "&" || v == "&&"
}

func isOrOpNode(n *node) bool {
	nodeType := n.getNodeType()
	if nodeType != operator && nodeType != fastOperator {
		return false
	}
	v := n.value.(string)
	return v == "or" || v == "|" || v == "||"
}

func parentNode(e *Expr, idx int16) (*node, int16) {
	pIdx := e.parentIdx[idx]
	if pIdx == -1 {
		return nil, -1
	}
	return e.nodes[pIdx], pIdx
}

func optimizeReordering(cc *Config, root *astNode) {
	for _, child := range root.children {
		optimizeReordering(cc, child)
	}

	calculateNodeCosts(cc, root)

	if !isBoolOpNode(root.node) {
		return
	}

	// reordering child nodes based on node cost
	sort.SliceStable(root.children, func(i, j int) bool {
		return root.children[i].cost < root.children[j].cost
	})
}

func calculateNodeCosts(conf *Config, root *astNode) {
	children := root.children
	const (
		loops       float64 = 1
		inlinedCall float64 = 1
		funcCall    float64 = 5
	)

	var (
		baseCost      float64
		operationCost float64
		childrenCost  float64
	)

	n := root.node
	nodeType := n.flag & nodeTypeMask

	// base cost
	switch nodeType {
	case constant:
		baseCost = inlinedCall
	case variable:
		baseCost = funcCall
	case fastOperator:
		baseCost = funcCall
	case operator:
		// The operator needs to add all its children to the stack frame
		// So it will result in more loops
		baseCost = loops*float64(len(children)+1) + funcCall
	case cond:
		baseCost = loops * 4
	default:
		baseCost = 10
	}

	// operation cost
	if nodeType == variable ||
		nodeType == operator ||
		nodeType == fastOperator {
		operationCost = conf.getCosts(nodeType, n.value.(string))
	}

	if nodeType == cond && n.value == keywordIf {
		childrenCost = children[0].cost + math.Max(children[1].cost, children[2].cost)
	} else {
		for _, child := range children {
			childrenCost += child.cost
		}
	}

	root.cost = baseCost + operationCost + childrenCost
}

func optimizeConstantFolding(cc *Config, root *astNode) {
	for _, child := range root.children {
		optimizeConstantFolding(cc, child)
	}

	n := root.node
	stateless, fn := isStatelessOp(cc, n)
	if !stateless {
		return
	}

	if isBoolOpNode(n) {
		for _, child := range root.children {
			if child.node.getNodeType() != constant {
				continue
			}

			b, ok := child.node.value.(bool)
			if !ok {
				return
			}

			if (b && isOrOpNode(n)) || (!b && isAndOpNode(n)) {
				root.node = &node{
					flag:  constant,
					value: b,
				}
				root.children = nil
				return
			}
		}
	}

	params := make([]Value, len(root.children))
	for i, child := range root.children {
		if child.node.getNodeType() != constant {
			return
		}
		params[i] = child.node.value
	}

	res, err := fn(nil, params)
	if err != nil {
		return
	}
	root.children = nil
	root.node = &node{
		flag:  constant,
		value: res,
	}
	return
}

func isStatelessOp(c *Config, n *node) (bool, Operator) {
	if typ := n.getNodeType(); typ != operator && typ != fastOperator {
		return false, nil
	}

	op, ok := n.value.(string)
	if !ok {
		return false, nil
	}

	// builtinOperators stateless functions
	for _, so := range builtinStatelessOperations {
		if so == op {
			return true, builtinOperators[op]
		}
	}

	for _, so := range c.StatelessOperators {
		if so == op {
			if fn := c.OperatorMap[op]; fn != nil {
				return true, fn
			}
			break
		}
	}

	return false, nil
}

func optimizeFastEvaluation(cc *Config, root *astNode) {
	for _, child := range root.children {
		optimizeFastEvaluation(cc, child)
	}
	n := root.node
	if (n.flag&nodeTypeMask) != operator || len(root.children) != 2 {
		return
	}

	for _, child := range root.children {
		typ := child.node.getNodeType()
		if typ == constant || typ == variable {
			continue
		}
		return
	}

	otherPartMask := nodeTypeMask ^ uint8(0xFF)

	root.node.flag = fastOperator | (root.node.flag & otherPartMask)
}

type checkRes struct {
	size int
	err  error
}

func check(root *astNode) checkRes {
	if len(root.children) > math.MaxInt8 {
		return checkRes{
			err: fmt.Errorf("operators cannot exceed a maximum of 127 parameters, got: [%d]", len(root.children)),
		}
	}

	size := 0

	for _, child := range root.children {
		res := check(child)
		if res.err != nil {
			return res
		}
		size = size + res.size
	}

	size = size + 1

	if size > math.MaxInt16 {
		return checkRes{
			err: fmt.Errorf("expression cannot exceed a maximum of 32767 nodes, got: [%d]", size),
		}
	}

	return checkRes{
		size: size,
	}
}

func buildExpr(cc *Config, ast *astNode, size int) *Expr {
	e := &Expr{
		nodes:     make([]*node, 0, size),
		parentIdx: make([]int16, 0, size),
	}

	calAndSetNodes(e, ast)
	calAndSetParentIndex(e, ast)
	calAndSetStackSize(e)
	calAndSetShortCircuit(e)
	calAndSetShortCircuitForRCO(e)

	if cc.CompileOptions[ReportEvent] || cc.CompileOptions[Debug] {
		calAndSetEventNode(e)
	}

	return e
}

func calAndSetNodes(e *Expr, root *astNode) {
	root.parentIdx = -1
	n := root.node
	switch n.getNodeType() {
	case constant, variable:
		e.nodes = append(e.nodes, n)
		root.idx = len(e.nodes) - 1
	case operator:
		for _, child := range root.children {
			calAndSetNodes(e, child)
		}
		e.nodes = append(e.nodes, n)
		root.idx = len(e.nodes) - 1
	case fastOperator:
		e.nodes = append(e.nodes, n)
		root.idx = len(e.nodes) - 1
		for _, child := range root.children {
			calAndSetNodes(e, child)
		}
	case cond:
		if n.value == keywordIf {
			var (
				condNode    = root.children[0]
				trueBranch  = root.children[1]
				falseBranch = root.children[2]
				endIfNode   = root.children[3]
			)

			calAndSetNodes(e, condNode) // condition node

			e.nodes = append(e.nodes, n) // check condition node result
			root.idx = len(e.nodes) - 1

			calAndSetNodes(e, trueBranch) // true branch
			calAndSetNodes(e, endIfNode)  // jump to the end of if logic
			n.scIdx = int16(len(e.nodes) - 1)

			calAndSetNodes(e, falseBranch) // false branch
			endIfNode.node.scIdx = int16(len(e.nodes) - 1)
		} else {
			e.nodes = append(e.nodes, n)
			root.idx = len(e.nodes) - 1
		}
	}

	n.childCnt = int8(len(root.children))
	for _, child := range root.children {
		child.parentIdx = root.idx
	}
}

func calAndSetParentIndex(e *Expr, root *astNode) {
	size := int16(len(e.nodes))
	f := make([]int16, size)

	queue := make([]*astNode, 0, size)
	queue = append(queue, root)

	for len(queue) != 0 {
		root, queue = queue[0], queue[1:]
		f[root.idx] = int16(root.parentIdx)
		queue = append(queue, root.children...)
	}
	e.parentIdx = f
	//copy(e.parentIdx, f)
}

func calAndSetStackSize(e *Expr) {
	var (
		size = int16(len(e.nodes))
		f    = make([]int16, size) // stack size
	)

	var isEndIfNode = func(e *Expr, idx int16) bool {
		n := e.nodes[idx]
		return n.getNodeType() == cond && n.value == "fi"
	}

	f[0] = 1
	for i := int16(1); i < size; i++ {
		p, pIdx := parentNode(e, i)
		if pIdx != -1 && p.getNodeType() == fastOperator {
			f[i] = f[i-1]
			continue
		}

		prev := i - 1

		// if its previous node is `fi`. it's the false node,
		// so it should calculate stack size based `if` node
		if isEndIfNode(e, prev) {
			_, prev = parentNode(e, prev)
		}

		n := e.nodes[i]
		switch n.getNodeType() {
		case constant, variable, fastOperator:
			f[i] = f[prev] + 1
		case operator:
			f[i] = f[prev] - int16(n.childCnt) + 1
		case cond:
			if n.value == keywordIf {
				f[i] = f[prev] - 1
			} else {
				f[i] = f[prev]
			}
		}
	}

	maxStackSize := f[0]
	for i, n := range e.nodes {
		maxStackSize = maxInt16(maxStackSize, f[i])
		n.osTop = f[i] - 1
	}
	e.maxStackSize = maxStackSize
}

func calAndSetShortCircuit(e *Expr) {
	var (
		size = int16(len(e.nodes))
		f    = make([]int16, size)
	)
	var (
		isLastChild = func(e *Expr, idx int16) bool {
			nodeType := e.nodes[idx].getNodeType()
			_, pIdx := parentNode(e, idx)
			if nodeType == fastOperator {
				return pIdx == idx+3
			} else {
				return pIdx == idx+1
			}
		}
	)

	for i := size - 1; i >= 0; i-- {
		n := e.nodes[i]
		p, pIdx := parentNode(e, i)
		if pIdx == -1 {
			f[i] = i
			continue
		}

		var flag uint8
		switch {
		case isAndOpNode(p):
			flag |= scIfFalse
		case isOrOpNode(p):
			flag |= scIfTrue
		default:
			f[i] = i
			continue
		}
		if isLastChild(e, i) {
			flag |= scIfTrue
			flag |= scIfFalse
		}

		// when its parent node is a bool operator (and/or)
		// it can definitely short-circuit to its parent node
		n.flag |= flag
		f[i] = pIdx

		// if its parent node can short-circuit its type,
		// it can directly short-circuit to the target node of its parent
		for p.flag&flag == flag {
			f[i] = f[pIdx]

			// parent's sc flag exactly equals current node
			if p.flag&scMask == flag {
				break
			}

			p, pIdx = parentNode(e, pIdx)
		}
	}

	for i := int16(0); i < size; i++ {
		n := e.nodes[i]
		p, pIdx := parentNode(e, i)
		// check is if it's true or false branch
		if pIdx != -1 && p.getNodeType() == cond && i > pIdx {
			if f[pIdx] != pIdx {
				n.flag |= p.flag & scMask
				f[i] = f[pIdx]
			}
		}

		if n.getNodeType() == cond {
			continue
		}

		if f[i] == size-1 {
			n.scIdx = -1
		} else {
			n.scIdx = f[i]
		}
	}
}

func calAndSetShortCircuitForRCO(e *Expr) {
	for i, n := range e.nodes {
		p, _ := parentNode(e, int16(i))
		switch {
		case p == nil:
			continue
		case isAndOpNode(p):
			n.flag |= andOp
		case isOrOpNode(p):
			n.flag |= orOp
		}
	}
}

type NodeType uint8

const (
	ConstantNode     = NodeType(constant)
	VariableNode     = NodeType(variable)
	OperatorNode     = NodeType(operator)
	FastOperatorNode = NodeType(fastOperator)
	CondNode         = NodeType(cond)
	EventNode        = NodeType(event)
)

func (t NodeType) String() string {
	switch t {
	case ConstantNode:
		return "constant"
	case VariableNode:
		return "variable"
	case OperatorNode:
		return "operator"
	case FastOperatorNode:
		return "fast_operator"
	case CondNode:
		return "cond"
	case EventNode:
		return "event"
	}
	return "unknown"
}

type OpEventData struct {
	IsFastOp bool
	OpName   string
	Params   []Value
	Res      Value
	Err      error
}

type LoopEventData struct {
	CurtIdx   int16
	NodeType  NodeType
	NodeValue Value
}

func calAndSetEventNode(e *Expr) {
	var wrapOpEvent = func(n *node) Operator {
		var (
			op       = n.operator
			name     = n.value.(string)
			isFastOp = n.getNodeType() == fastOperator
		)
		return func(ctx *Ctx, params []Value) (res Value, err error) {
			res, err = op(ctx, params)
			e.EventChan <- Event{
				EventType: OpExecEvent,
				Data: OpEventData{
					IsFastOp: isFastOp,
					OpName:   name,
					Params:   params,
					Res:      res,
					Err:      err,
				},
			}
			return
		}
	}

	var (
		nodes          = e.nodes
		size           = int16(len(nodes))
		res            = make([]*node, 0, size*2)
		parents        = make([]int16, 0, size*2)
		eventNodeIdxes = make([]int16, size)
		realIdxes      = make([]int16, size)
	)

	for i := int16(0); i < size; i++ {
		realNode := nodes[i]
		debugNode := &node{
			flag:     event,
			childCnt: realNode.childCnt,
			osTop:    realNode.osTop,
			scIdx:    realNode.scIdx,
			varKey:   realNode.varKey,
			value: LoopEventData{
				CurtIdx:   int16(len(res) + 1), // the real node index
				NodeType:  NodeType(realNode.flag & nodeTypeMask),
				NodeValue: realNode.value,
			},
		}
		res = append(res, debugNode)
		eventNodeIdxes[i] = int16(len(res) - 1)
		res = append(res, realNode)
		realIdxes[i] = int16(len(res) - 1)

		parents = append(parents, e.parentIdx[i], e.parentIdx[i])

		switch realNode.flag & nodeTypeMask {
		case operator:
			realNode.operator = wrapOpEvent(realNode)
		case fastOperator:
			realNode.operator = wrapOpEvent(realNode)
			// append child nodes of fast operator
			res = append(res, nodes[i+1], nodes[i+2])
			parents = append(parents, e.parentIdx[i+1], e.parentIdx[i+2])
			realIdxes[i+1] = int16(len(res) - 2)
			realIdxes[i+2] = int16(len(res) - 1)
			i += 2
		}
	}

	for i, n := range res {
		if n.scIdx != -1 {
			n.scIdx = realIdxes[n.scIdx]
		}

		p := parents[i]
		if p == -1 {
			continue
		}

		if n.getNodeType() == event {
			parents[i] = eventNodeIdxes[p]
		} else {
			parents[i] = realIdxes[p]
		}
	}

	e.nodes = res
	e.parentIdx = parents
}
