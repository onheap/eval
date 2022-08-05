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
	ConstantFolding Option = "constant_folding"

	Debug                 Option = "debug"
	AllowUnknownSelectors Option = "allow_unknown_selectors"
)

var AllOptimizations = []Option{Reordering, FastEvaluation, ConstantFolding}

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
	Optimizations = func(enable bool, opts ...Option) CompileOption {
		return func(c *CompileConfig) {
			if len(opts) == 0 || opts[0] == Optimize {
				opts = AllOptimizations
			}
			for _, opt := range opts {
				c.CompileOptions[opt] = enable
			}
		}
	}

	RegisterSelKeys = func(vals map[string]interface{}) CompileOption {
		return func(c *CompileConfig) {
			for s := range vals {
				GetOrRegisterKey(c, s)
			}
		}
	}
)

func NewCompileConfig(opts ...CompileOption) *CompileConfig {
	conf := &CompileConfig{
		ConstantMap:    make(map[string]Value),
		SelectorMap:    make(map[string]SelectorKey),
		OperatorMap:    make(map[string]Operator),
		CompileOptions: make(map[Option]bool),
		CostsMap:       make(map[string]int),
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
	CostsMap map[string]int

	// compile options
	CompileOptions map[Option]bool
}

func (cc *CompileConfig) getCosts(nodeType uint8, nodeName string) int {
	const (
		defaultCost  = 5
		selectorCost = 7
		operatorCost = 10
	)

	fallback := defaultCost
	var prefix string
	switch nodeType {
	case selector:
		prefix = "selector"
		fallback = selectorCost
	case operator, fastOperator:
		prefix = "operator"
		fallback = operatorCost
	}

	keys := []string{
		fmt.Sprintf("%s.%s", prefix, nodeName), // operator.abs
		nodeName,                               // abs
		fmt.Sprintf("%ss", prefix),             // operators
	}

	for _, key := range keys {
		if v, exist := cc.CostsMap[key]; exist {
			return v
		}
	}
	return fallback
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
	if enabled, exist := cc.CompileOptions[ConstantFolding]; enabled || !exist {
		_ = optimizeConstantFolding(cc, root)
	}

	if enabled, exist := cc.CompileOptions[FastEvaluation]; enabled || !exist {
		optimizeFastEvaluation(cc, root)
	}

	if enabled, exist := cc.CompileOptions[Reordering]; enabled || !exist {
		calculateNodeCosts(cc, root)
		optimizeReordering(root)
	}
}

func isBoolOpNode(n *node) bool {
	nodeType := n.getNodeType()
	if nodeType != operator && nodeType != fastOperator {
		return false
	}

	switch n.value.(string) {
	case "and", "&":
		return true
	case "or", "|":
		return true
	default:
		return false
	}
}

func isAndOpNode(n *node) bool {
	if !isBoolOpNode(n) {
		return false
	}
	v := n.value.(string)
	return v == "and" || v == "&"
}

func isOrOpNode(n *node) bool {
	if !isBoolOpNode(n) {
		return false
	}
	v := n.value.(string)
	return v == "or" || v == "|"
}

func calculateNodeCosts(conf *CompileConfig, root *astNode) {
	children := root.children
	for _, child := range children {
		calculateNodeCosts(conf, child)
	}

	const (
		loops       int64 = 1
		inlinedCall int64 = 1
		funcCall    int64 = 5
	)

	var (
		baseCost      int64
		operationCost int64
		childrenCost  int64
	)

	n := root.node
	nodeType := n.flag & nodeTypeMask

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
		baseCost = loops*(int64(len(children))+1) + funcCall
	case cond:
		baseCost = loops * 4
	case end:
		baseCost = 0
	default:
		baseCost = 10
	}

	// operation cost
	if nodeType == selector ||
		nodeType == operator ||
		nodeType == fastOperator {
		operationCost = int64(conf.getCosts(nodeType, n.value.(string)))
	}

	if nodeType == cond {
		childrenCost = int64(children[0].cost) + int64(max(children[1].cost, children[2].cost))
	} else {
		for _, child := range children {
			childrenCost += int64(child.cost)
			if childrenCost >= math.MaxInt {
				childrenCost = math.MaxInt
				break
			}
		}
	}

	cost := baseCost + operationCost + childrenCost
	if cost >= math.MaxInt {
		cost = math.MaxInt
	}

	root.cost = int(cost)
}

func optimizeReordering(root *astNode) {
	for _, child := range root.children {
		optimizeReordering(child)
	}

	if !isBoolOpNode(root.node) {
		return
	}

	// reordering child nodes based on node cost
	sort.SliceStable(root.children, func(i, j int) bool {
		return root.children[i].cost < root.children[j].cost
	})
}

func optimizeConstantFolding(cc *CompileConfig, root *astNode) error {
	for _, child := range root.children {
		err := optimizeConstantFolding(cc, child)
		if err != nil {
			return err
		}
	}

	n := root.node
	stateless, fn := isStatelessOp(cc, n)
	if !stateless {
		return nil
	}

	if isBoolOpNode(n) {
		for _, child := range root.children {
			if child.node.getNodeType() != constant {
				continue
			}

			b, ok := child.node.value.(bool)
			if !ok {
				return ParamTypeError(n.value.(string), "bool", child.node.value)
			}

			if (b && isOrOpNode(n)) || (!b && isAndOpNode(n)) {
				root.node = &node{
					flag:  constant,
					value: b,
				}
				root.children = nil
				return nil
			}
		}
	}

	params := make([]Value, len(root.children))
	for i, child := range root.children {
		if child.node.getNodeType() != constant {
			return nil
		}
		params[i] = child.node.value
	}

	res, err := fn(nil, params)
	if err != nil {
		return err
	}
	root.children = nil
	root.node = &node{
		flag:  constant,
		value: res,
	}
	return nil
}

func isStatelessOp(c *CompileConfig, n *node) (bool, Operator) {
	if typ := n.getNodeType(); typ != operator && typ != fastOperator {
		return false, nil
	}

	s, ok := n.value.(string)
	if !ok {
		return false, nil
	}

	// by default, we only do constant folding on builtin operators
	if _, exist := c.OperatorMap[s]; exist {
		return false, nil
	}

	fn, exist := builtinOperators[s] // should be stateless function
	if !exist {
		return false, nil
	}
	return true, fn
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
		nodes:     make([]*node, size),
		scIdx:     make([]int16, size),
		osSize:    make([]int16, size),
		parentIdx: make([]int16, size),
	}

	calAndSetNodes(e, ast)
	calAndSetParentIndex(e)
	calAndSetStackSize(e)
	calAndSetShortCircuit(e)
	calAndSetRpnNode(e)
	if cc.CompileOptions[Debug] {
		calAndSetDebugInfo(e)
	}

	return e
}

func calAndSetRpnNode(e *Expr) {
	var (
		nodes    = e.nodes
		size     = len(nodes)
		res      = make([]*rpnNode, 0, size)
		posToIdx = make(map[int16]int16, size)
		idxToPos = make(map[int16]int16, size)

		appendNode func(i int16)

		appendNodeAtIdx = func(i int16) (pos int16) {
			n := nodes[i]
			res = append(res, &rpnNode{
				flag:     n.flag,
				value:    n.value,
				selKey:   n.selKey,
				child:    n.childCnt,
				operator: n.operator,
				osTop:    e.osSize[i] - 1,
			})
			return int16(len(res) - 1)
		}

		condCheck Operator = func(_ *Ctx, params []Value) (Value, error) {
			if b, ok := params[0].(bool); ok {
				return !b, nil // sc when cond node returns false
			}

			return nil, fmt.Errorf("condition node returns a non bool result: [%v]", params[0])
		}

		alwaysTrue Operator = func(_ *Ctx, _ []Value) (Value, error) {
			return true, nil
		}
	)

	appendNode = func(i int16) {
		n := nodes[i]
		var pos int16
		switch n.getNodeType() {
		case constant, selector:
			pos = appendNodeAtIdx(i)
		case operator:
			cCnt := int16(n.childCnt)
			cIdx := n.childIdx
			for j := cIdx; j < cIdx+cCnt; j++ {
				appendNode(j)
			}
			pos = appendNodeAtIdx(i)
		case fastOperator:
			pos = appendNodeAtIdx(i)

			cCnt := int16(n.childCnt)
			cIdx := n.childIdx
			for j := cIdx; j < cIdx+cCnt; j++ {
				appendNode(j)
			}
		case cond:
			var (
				condNode  = n.childIdx
				trueNode  = n.childIdx + 1
				falseNode = n.childIdx + 2
				endNode   = n.childIdx + 3
			)

			// append condition node
			appendNode(condNode)

			// append first jump node
			// if the condition check returns false
			// it will jump to the false branch
			pos1 := appendNodeAtIdx(i)

			// append true branch node
			appendNode(trueNode)

			// append second jump node
			// it will jump to the end of if logic
			pos2 := appendNodeAtIdx(endNode)

			// update condition check node, jump to the false branch
			res[pos1].flag = cond
			res[pos1].operator = condCheck
			res[pos1].scPos = int16(len(res) - 1)

			// append false branch node
			appendNode(falseNode)

			// update end node, jump to the end of if logic
			res[pos2].flag = cond
			res[pos2].operator = alwaysTrue
			res[pos2].scPos = int16(len(res) - 1)

			pos = pos1
		}

		posToIdx[pos] = i
		idxToPos[i] = pos
	}

	appendNode(0)

	for pos, n := range res {
		typ := n.flag & nodeTypeMask
		if typ == cond {
			continue
		}
		idx := posToIdx[int16(pos)]
		pIdx := e.nodes[idx].scIdx
		if pIdx == -1 {
			n.scPos = -1
		} else {
			n.scPos = idxToPos[pIdx]
		}
	}

	e.rpnNodes = res
}

func calAndSetNodes(e *Expr, root *astNode) {
	size := len(e.nodes)
	nodes := make([]*node, 0, size)
	queue := make([]*astNode, 0, size)
	queue = append(queue, root)

	idx := 0
	for idx < len(queue) {
		curt := queue[idx]
		childIdx := len(queue)
		childCnt := len(curt.children)

		n := curt.node
		n.childCnt = int8(childCnt)
		switch n.getNodeType() {
		case constant, selector:
			n.childIdx = -1
		default:
			n.childIdx = int16(childIdx)
		}
		nodes = append(nodes, n)

		for _, child := range curt.children {
			queue = append(queue, child)
		}
		idx++
	}

	copy(e.nodes, nodes)
}

func calAndSetParentIndex(e *Expr) {
	size := int16(len(e.nodes))
	f := make([]int16, size)
	f[0] = -1

	for i := int16(0); i < size; i++ {
		n := e.nodes[i]
		cCnt := int(n.childCnt)
		if cCnt == 0 {
			continue
		}
		cIdx := int(n.childIdx)
		for j := cIdx; j < cIdx+cCnt; j++ {
			f[j] = i
		}
	}

	//e.parentIdx = f
	copy(e.parentIdx, f)
}

func calAndSetStackSize(e *Expr) {
	var isLeaf = func(e *Expr, idx int16) bool {
		n := e.nodes[idx]
		return n.childCnt == 0 || n.flag&nodeTypeMask == fastOperator
	}

	var isCondNode = func(e *Expr, idx int16) bool {
		return e.nodes[idx].flag&nodeTypeMask == cond
	}

	size := int16(len(e.nodes))
	f := make([]int16, size) // for operator stack
	for i := int16(1); i < size; i++ {
		pIdx := e.parentIdx[i]

		if isLeaf(e, pIdx) {
			f[i] = f[pIdx]
			continue
		}

		if isCondNode(e, pIdx) {
			if isLeaf(e, i) {
				f[i] = f[pIdx] + 1
			} else {
				f[i] = f[pIdx]
			}
			continue
		}

		// left sibling count
		siblingCount := i - e.nodes[pIdx].childIdx
		if isLeaf(e, i) {
			// f[i] = f[pIdx] + left sibling count + 1
			f[i] = f[pIdx] + siblingCount + 1
		} else {
			// f[i] = f[pIdx] + left sibling count
			f[i] = f[pIdx] + siblingCount
		}
	}

	var res int16 = 1
	for i := int16(0); i < size; i++ {
		res = maxInt16(res, f[i])
	}

	e.maxStackSize = res

	copy(e.osSize, f)
}

func calAndSetShortCircuit(e *Expr) {
	var isLastChild = func(idx int16) bool {
		parentIdx := e.parentIdx[idx]
		if parentIdx == -1 {
			return false
		}

		cnt := int16(e.nodes[parentIdx].childCnt)
		childIdx := e.nodes[parentIdx].childIdx
		if childIdx+cnt-1 == idx {
			return true
		}
		return false
	}

	var isFirstChild = func(idx int16) bool {
		parentIdx := e.parentIdx[idx]
		if parentIdx == -1 {
			return false
		}
		return e.nodes[parentIdx].childIdx == idx
	}

	var parentNode = func(idx int16) (*node, int16) {
		pIdx := e.parentIdx[idx]
		if pIdx == -1 {
			return nil, -1
		}
		return e.nodes[pIdx], pIdx
	}

	size := int16(len(e.nodes))

	f := make([]int16, size)
	for i := int16(1); i < size; i++ {
		n := e.nodes[i]
		p, pIdx := parentNode(i)

		if p.getNodeType() == cond {
			if isFirstChild(i) || isLastChild(i) {
				f[i] = i
				continue
			}

			if f[pIdx] == pIdx {
				continue
			}

			n.flag |= p.flag & scMask
			f[i] = f[pIdx]
			continue
		}

		if !isBoolOpNode(p) {
			f[i] = i
			continue
		}
		var flag uint8
		switch {
		case isLastChild(i):
			flag |= scIfTrue
			flag |= scIfFalse
		case isAndOpNode(p):
			flag |= scIfFalse
		case isOrOpNode(p):
			flag |= scIfTrue
		}
		// when its parent node is a bool operator (and/or)
		// it can definitely short-circuit to its parent node
		n.flag |= flag
		f[i] = pIdx

		// if its parent node can short-circuit its type,
		// it can directly short-circuit to the target node of its parent
		for p.flag&flag == flag {
			f[i] = f[pIdx]
			p, pIdx = parentNode(pIdx)
		}
	}

	//e.scIdx = f
	copy(e.scIdx, f)

	for i := int16(0); i < size; i++ {
		e.nodes[i].scIdx = f[i]
	}
}

func calAndSetDebugInfo(e *Expr) {
	var wrapDebugInfo = func(name Value, op Operator) Operator {
		return func(ctx *Ctx, params []Value) (res Value, err error) {
			res, err = op(ctx, params)
			fmt.Printf("%13s: op: %v, params: %v, res: %v, err: %v\n", "Exec Operator", name, params, res, err)
			return
		}
	}

	nodes := e.rpnNodes
	size := int16(len(nodes))
	res := make([]*rpnNode, 0, size*2)

	debugPos := make([]int16, size)

	for i := int16(0); i < size; i++ {
		realNode := nodes[i]
		debugNode := &rpnNode{
			flag:   debug,
			child:  realNode.child,
			osTop:  realNode.osTop,
			scPos:  realNode.scPos,
			selKey: realNode.selKey,
			value:  realNode.value,
		}
		res = append(res, debugNode, realNode)
		debugPos[i] = int16(len(res) - 1)

		switch realNode.flag & nodeTypeMask {
		case operator:
			realNode.operator = wrapDebugInfo(realNode.value, realNode.operator)
		case fastOperator:
			realNode.operator = wrapDebugInfo(realNode.value, realNode.operator)
			// append child nodes of fast operator
			res = append(res, nodes[i+1], nodes[i+2])
			i += 2
		}
	}

	for _, n := range res {
		n.scPos = debugPos[n.scPos]
	}

	e.rpnNodes = res
}
