package eval

import (
	"fmt"
	"math"
	"sort"
)

type Option string

const (
	Optimize        Option = "optimize" // switch all optimizations
	Reordering      Option = "reordering"
	FastEvaluation  Option = "fast_evaluation"
	ReduceNesting   Option = "reduce_nesting"
	ConstantFolding Option = "constant_folding"

	Debug                 Option = "debug"
	ReportEvent           Option = "report_event"
	InfixNotation         Option = "infix_notation"
	AllowUnknownSelectors Option = "allow_unknown_selectors"
)

type optimizer func(config *CompileConfig, root *astNode)

var (
	optimizations = []Option{ConstantFolding, ReduceNesting, FastEvaluation, Reordering}
	optimizerMap  = map[Option]optimizer{
		ConstantFolding: optimizeConstantFolding,
		ReduceNesting:   optimizeReduceNesting,
		FastEvaluation:  optimizeFastEvaluation,
		Reordering:      optimizeReordering,
	}
)

func CopyCompileConfig(origin *CompileConfig) *CompileConfig {
	conf := NewCompileConfig()
	if origin == nil {
		return conf
	}
	for k, v := range origin.ConstantMap {
		conf.ConstantMap[k] = v
	}
	for k, v := range origin.SelectorMap {
		conf.SelectorMap[k] = v
	}
	for k, v := range origin.OperatorMap {
		conf.OperatorMap[k] = v
	}
	for k, v := range origin.CompileOptions {
		conf.CompileOptions[k] = v
	}
	for k, v := range origin.CostsMap {
		conf.CostsMap[k] = v
	}
	for _, op := range origin.StatelessOperators {
		conf.StatelessOperators = append(conf.StatelessOperators, op)
	}
	return conf
}

type CompileOption func(conf *CompileConfig)

var (
	EnableStringSelectors CompileOption = func(c *CompileConfig) {
		c.CompileOptions[AllowUnknownSelectors] = true
	}
	EnableDebug CompileOption = func(c *CompileConfig) {
		c.CompileOptions[Debug] = true
	}
	EnableReportEvent CompileOption = func(c *CompileConfig) {
		c.CompileOptions[ReportEvent] = true
	}
	Optimizations = func(enable bool, opts ...Option) CompileOption {
		return func(c *CompileConfig) {
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
	EnableInfixNotation CompileOption = func(c *CompileConfig) {
		c.CompileOptions[InfixNotation] = true
	}

	RegisterVals = func(vals map[string]interface{}) CompileOption {
		return func(c *CompileConfig) {
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
)

func NewCompileConfig(opts ...CompileOption) *CompileConfig {
	conf := &CompileConfig{
		ConstantMap:        make(map[string]Value),
		SelectorMap:        make(map[string]SelectorKey),
		OperatorMap:        make(map[string]Operator),
		CompileOptions:     make(map[Option]bool),
		CostsMap:           make(map[string]float64),
		StatelessOperators: []string{},
	}
	for _, opt := range opts {
		opt(conf)
	}
	return conf
}

type CompileConfig struct {
	ConstantMap map[string]Value
	SelectorMap map[string]SelectorKey
	OperatorMap map[string]Operator

	// cost of performance
	CostsMap map[string]float64

	// compile options
	CompileOptions map[Option]bool

	StatelessOperators []string
}

func (cc *CompileConfig) getCosts(nodeType uint8, nodeName string) float64 {
	const (
		defaultCost  float64 = 5
		selectorCost float64 = 7
		operatorCost float64 = 10

		selectorNode = "selector"
		operatorNode = "operator"
	)

	if v, exist := cc.CostsMap[nodeName]; exist {
		return v
	}

	switch nodeType {
	case selector:
		if v, exist := cc.CostsMap[selectorNode]; exist {
			return v
		}
		return selectorCost
	case operator, fastOperator:
		if v, exist := cc.CostsMap[operatorNode]; exist {
			return v
		}
		return operatorCost
	default:
		return defaultCost
	}
}

func Compile(originConf *CompileConfig, exprStr string) (*Expr, error) {
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

func optimize(cc *CompileConfig, root *astNode) {
	for _, opt := range optimizations {
		enabled, exist := cc.CompileOptions[opt]
		if enabled || !exist {
			optimizerMap[opt](cc, root)
		}
	}
}

func optimizeReduceNesting(cc *CompileConfig, root *astNode) {
	for _, child := range root.children {
		optimizeReduceNesting(cc, child)
	}

	n := root.node
	// todo expand to operators with associative property
	if !isBoolOpNode(n) {
		return
	}

	var children []*astNode
	rootOpType := isAndOpNode(n)
	for _, child := range root.children {
		cn := child.node
		if typ := cn.getNodeType(); typ == constant || typ == selector {
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

func optimizeReordering(cc *CompileConfig, root *astNode) {

	var helper func(curt, parent *astNode)

	helper = func(curt, parent *astNode) {
		for _, child := range curt.children {
			helper(child, curt)
		}

		calculateNodeCosts(cc, curt, parent)

		if !isBoolOpNode(curt.node) {
			return
		}

		// reordering child nodes based on node cost
		sort.SliceStable(curt.children, func(i, j int) bool {
			return curt.children[i].cost < curt.children[j].cost
		})
	}

	helper(root, nil)
}

func calculateNodeCosts(conf *CompileConfig, curt, parent *astNode) {
	n := curt.node
	children := curt.children
	nodeType := n.flag & nodeTypeMask

	if (nodeType == fastOperator || nodeType == selector) &&
		parent != nil && isBoolOpNode(parent.node) {
		identifier := getCostIdentifier(parent, curt, children)
		if score, exist := conf.CostsMap[identifier]; exist {
			curt.cost = score
			return
		}
	}

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

	// base cost
	switch nodeType {
	case constant:
		baseCost = inlinedCall
	case selector:
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
	switch nodeType {
	case operator, fastOperator:
		var found bool
		if parent != nil && isBoolOpNode(parent.node) {
			identifier := getCostIdentifier(parent, curt, nil)
			operationCost, found = conf.CostsMap[identifier]
		}
		if !found {
			operationCost = conf.getCosts(nodeType, n.value.(string))
		}
	case selector:
		operationCost = conf.getCosts(nodeType, n.value.(string))
	}

	if nodeType == cond && n.value == keywordIf {
		childrenCost = children[0].cost + math.Max(children[1].cost, children[2].cost)
	} else {
		for _, child := range children {
			childrenCost += child.cost
		}
	}

	curt.cost = baseCost + operationCost + childrenCost
}

func getCostIdentifier(parent *astNode, curt *astNode, children []*astNode) string {
	var tree Tree
	var appendChildren = func(parentIdx int, children ...*astNode) {
		if parentIdx != -1 {
			tree[parentIdx].ChildCnt = len(children)
			tree[parentIdx].ChildIdx = len(tree)
		}

		for _, c := range children {
			n := c.node
			tree = append(tree,
				TreeNode{
					NodeType: NodeType(n.getNodeType()),
					SelKey:   n.selKey,
					Value:    n.value,
					Operator: n.operator,

					Idx:       len(tree),
					ChildCnt:  0,
					ChildIdx:  -1,
					ParentIdx: parentIdx,
				},
			)
		}
	}

	appendChildren(-1, parent)
	appendChildren(0, curt)
	if len(children) != 0 {
		appendChildren(1, children...)
	}

	return tree.DumpCode(false)
}

func optimizeConstantFolding(cc *CompileConfig, root *astNode) {
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

func isStatelessOp(c *CompileConfig, n *node) (bool, Operator) {
	if typ := n.getNodeType(); typ != operator && typ != fastOperator {
		return false, nil
	}

	op, ok := n.value.(string)
	if !ok {
		return false, nil
	}

	// builtinOperators are all stateless functions
	fn, exist := builtinOperators[op]
	if exist {
		return true, fn
	}

	for _, so := range c.StatelessOperators {
		if so == op {
			if fn = c.OperatorMap[op]; fn != nil {
				return true, fn
			}
			break
		}
	}

	return false, fn
}

func optimizeFastEvaluation(cc *CompileConfig, root *astNode) {
	for _, child := range root.children {
		optimizeFastEvaluation(cc, child)
	}
	n := root.node
	if (n.flag&nodeTypeMask) != operator || len(root.children) != 2 {
		return
	}

	for _, child := range root.children {
		typ := child.node.getNodeType()
		if typ == constant || typ == selector {
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

func buildExpr(cc *CompileConfig, ast *astNode, size int) *Expr {
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
	case constant, selector:
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
		case constant, selector, fastOperator:
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
	SelectorNode     = NodeType(selector)
	OperatorNode     = NodeType(operator)
	FastOperatorNode = NodeType(fastOperator)
	CondNode         = NodeType(cond)
	EventNode        = NodeType(event)
)

func (t NodeType) String() string {
	switch t {
	case ConstantNode:
		return "constant"
	case SelectorNode:
		return "selector"
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
			selKey:   realNode.selKey,
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
