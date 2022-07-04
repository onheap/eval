package eval

import (
	"fmt"
	"math"
	"sort"
)

type OptimizeOption string

const (
	AllOptimizations OptimizeOption = "optimize"
	Reordering       OptimizeOption = "reordering"
	FastEvaluation   OptimizeOption = "fast_evaluation"
	ConstantFolding  OptimizeOption = "constant_folding"
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
	for k, v := range origin.OptimizeOptions {
		conf.OptimizeOptions[k] = v
	}
	for k, v := range origin.CostsMap {
		conf.CostsMap[k] = v
	}
	conf.AllowUnknownSelectors = origin.AllowUnknownSelectors
	return conf
}

func NewCompileConfig() *CompileConfig {
	return &CompileConfig{
		ConstantMap:     make(map[string]Value),
		SelectorMap:     make(map[string]SelectorKey),
		OperatorMap:     make(map[string]Operator),
		OptimizeOptions: make(map[OptimizeOption]bool),
		CostsMap:        make(map[string]int),

		AllowUnknownSelectors: false,
	}
}

type CompileConfig struct {
	ConstantMap     map[string]Value
	SelectorMap     map[string]SelectorKey
	OperatorMap     map[string]Operator
	OptimizeOptions map[OptimizeOption]bool
	CostsMap        map[string]int // cost of performance

	AllowUnknownSelectors bool
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

	expr := compress(ast, res.size)

	setExtraInfo(expr)

	return expr, nil
}

func setExtraInfo(e *Expr) {
	calAndSetParentIndex(e)
	calAndSetStackSize(e)
	calAndSetShortCircuit(e)
}

func calAndSetParentIndex(e *Expr) {
	size := len(e.nodes)
	f := make([]int, size)
	f[0] = -1

	for i := 0; i < size; i++ {
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

	e.parentIndex = f
}

func calAndSetStackSize(e *Expr) {
	var isLeaf = func(e *Expr, idx int) bool {
		n := e.nodes[idx]
		return n.childCnt == 0 || n.flag&nodeTypeMask == fastOperator
	}

	var isCondNode = func(e *Expr, idx int) bool {
		return e.nodes[idx].flag&nodeTypeMask == cond
	}

	var isFirstChild = func(e *Expr, idx int) bool {
		parentIdx := e.parentIndex[idx]
		return int(e.nodes[parentIdx].childIdx) == idx

	}

	size := len(e.nodes)
	f1 := make([]int, size) // for stack frame
	f2 := make([]int, size) // for operator stack
	f1[0] = 1
	for i := 1; i < size; i++ {
		parentIdx := e.parentIndex[i]

		// f1
		if isLeaf(e, parentIdx) {
			f1[i] = f1[parentIdx]
		} else {
			siblingCount := int(e.nodes[parentIdx].childIdx) + int(e.nodes[parentIdx].childCnt) - 1 - i
			// f[i] = f[pIdx] + right sibling count + 1

			if isCondNode(e, parentIdx) {
				if isFirstChild(e, i) {
					f1[i] = f1[parentIdx] + 2 // add cond expr node and end node
				} else {
					f1[i] = f1[parentIdx] + 1 // push branch expr
				}
			} else {
				f1[i] = f1[parentIdx] + siblingCount + 1
			}
		}

		// f2
		if isLeaf(e, parentIdx) {
			f2[i] = f2[parentIdx]
			continue
		}

		if isCondNode(e, parentIdx) {
			if isLeaf(e, i) {
				f2[i] = f2[parentIdx] + 1
			} else {
				f2[i] = f2[parentIdx]
			}
			continue
		}

		if isLeaf(e, i) {
			// f[i] = f[pIdx] + left sibling count + 1
			siblingCount := i - int(e.nodes[parentIdx].childIdx)
			f2[i] = f2[parentIdx] + siblingCount + 1
		} else {
			// f[i] = f[pIdx] + left sibling count
			siblingCount := i - int(e.nodes[parentIdx].childIdx)
			f2[i] = f2[parentIdx] + siblingCount
		}
	}

	res := 1
	for i := 0; i < size; i++ {
		res = max(res, f1[i])
		res = max(res, f2[i])
	}

	e.maxStackSize = int16(res)

	e.sfSize = f1
	e.osSize = f2
}

func calAndSetShortCircuit(e *Expr) {
	var isLastChild = func(n *node) bool {
		idx := int(n.idx)
		parentIdx := e.parentIndex[idx]
		if parentIdx == -1 {
			return false
		}

		cnt := int(e.nodes[parentIdx].childCnt)
		childIdx := int(e.nodes[parentIdx].childIdx)
		if childIdx+cnt-1 == idx {
			return true
		}
		return false
	}

	var parentNode = func(n *node) *node {
		parentIdx := e.parentIndex[int(n.idx)]
		if parentIdx == -1 {
			return nil
		}
		return e.nodes[parentIdx]
	}

	size := len(e.nodes)

	f := make([]int, size)
	for i := 1; i < size; i++ {
		n := e.nodes[i]
		p := parentNode(n)
		pIdx := int(p.idx)

		if !isBoolOpNode(p) {
			f[i] = i
			continue
		}
		var flag uint8
		switch {
		case isLastChild(n):
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
			p = parentNode(p)
			pIdx = int(p.idx)
		}
	}

	e.scIdx = f
}

func optimize(cc *CompileConfig, root *astNode) {
	if enabled, exist := cc.OptimizeOptions[ConstantFolding]; enabled || !exist {
		_ = optimizeConstantFolding(cc, root)
	}

	if enabled, exist := cc.OptimizeOptions[FastEvaluation]; enabled || !exist {
		optimizeFastEvaluation(cc, root)
	}

	if enabled, exist := cc.OptimizeOptions[Reordering]; enabled || !exist {
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
		baseCost      int64 = 0
		operationCost int64 = 0
		childrenCost  int64 = 0
	)

	n := root.node
	nodeType := n.flag & nodeTypeMask

	// base cost
	switch nodeType {
	case value:
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
			if child.node.getNodeType() != value {
				continue
			}

			b, ok := child.node.value.(bool)
			if !ok {
				return ParamTypeError(n.value.(string), "bool", child.node.value)
			}

			if (b && isOrOpNode(n)) || (!b && isAndOpNode(n)) {
				root.node = &node{
					flag:  value,
					value: b,
				}
				root.children = nil
				return nil
			}
		}
	}

	params := make([]Value, len(root.children))
	for i, child := range root.children {
		if child.node.getNodeType() != value {
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
		flag:  value,
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
	if (n.flag & nodeTypeMask) != operator {
		return
	}

	for _, child := range root.children {
		typ := child.node.getNodeType()
		if typ == value || typ == selector {
			continue
		}
		return
	}

	otherPartMask := nodeTypeMask ^ uint8(0b11111111)

	root.node.flag = fastOperator | (root.node.flag & otherPartMask)
}

type checkRes struct {
	size int
	err  error
}

func check(root *astNode) checkRes {
	if len(root.children) > math.MaxInt8 {
		return checkRes{
			err: fmt.Errorf("expression is too long, operators cannot exceed a maximum of 127 parameters, got: [%d]", len(root.children)),
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
			err: fmt.Errorf("expression is too long, expression cannot exceed a maximum of 32767 nodes, got: [%d]", size),
		}
	}

	return checkRes{
		size: size,
	}
}

func compress(root *astNode, size int) *Expr {
	e := &Expr{
		nodes: make([]*node, 0, size),
	}
	queue := make([]*astNode, 0, size)
	queue = append(queue, root)

	idx := 0
	for idx < len(queue) {
		curt := queue[idx]
		e.appendNode(curt.node, len(queue), len(curt.children))
		for _, child := range curt.children {
			queue = append(queue, child)
		}
		idx++
	}
	return e
}

func (e *Expr) appendNode(n *node, childIndex int, childCnt int) {
	n.idx = int16(len(e.nodes))
	n.childCnt = int8(childCnt)
	switch n.getNodeType() {
	case value, selector:
		n.childIdx = -1
	default:
		n.childIdx = int16(childIndex)
	}
	e.nodes = append(e.nodes, n)
}
