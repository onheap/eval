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

func (cc *CompileConfig) getCosts(nodeType int16, nodeName string) int {
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

	expr := buildExpr(ast, res.size)

	return expr, nil
}

func buildExpr(ast *astNode, size int) *Expr {
	e := &Expr{
		bytecode: make([]int16, size*4),

		// extra info
		parentIdx: make([]int, size),
		scIdx:     make([]int, size),
		sfSize:    make([]int, size),
		osSize:    make([]int, size),
		nodes:     make([]*node, size),
	}

	calAndSetNodes(e, ast)
	calAndSetParentIndex(e)
	calAndSetStackSize(e)
	calAndSetShortCircuit(e)
	calAndSetBytecode(e)
	return e
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
		n.idx = idx
		n.childCnt = childCnt
		switch n.getNodeType() {
		case constant, selector:
			n.childIdx = -1
		default:
			n.childIdx = childIdx
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
	size := len(e.nodes)
	f := make([]int, size)
	f[0] = -1

	for i := 0; i < size; i++ {
		n := e.nodes[i]
		cCnt := n.childCnt
		if cCnt == 0 {
			continue
		}
		cIdx := n.childIdx
		for j := cIdx; j < cIdx+cCnt; j++ {
			f[j] = i
		}
	}

	//e.parentIdx = f
	copy(e.parentIdx, f)
}

func calAndSetStackSize(e *Expr) {
	var isLeaf = func(e *Expr, idx int) bool {
		n := e.nodes[idx]
		return n.childCnt == 0 || n.flag&nodeTypeMask == fastOperator
	}

	var isCondNode = func(e *Expr, idx int) bool {
		return e.nodes[idx].flag&nodeTypeMask == cond
	}

	var isEndNode = func(e *Expr, idx int) bool {
		return e.nodes[idx].flag&nodeTypeMask == end
	}

	var isFirstChild = func(e *Expr, idx int) bool {
		parentIdx := e.parentIdx[idx]
		return e.nodes[parentIdx].childIdx == idx

	}

	size := len(e.nodes)
	f1 := make([]int, size) // for stack frame
	f2 := make([]int, size) // for operator stack
	f1[0] = 1
	for i := 1; i < size; i++ {
		parentIdx := e.parentIdx[i]

		// f1
		if isLeaf(e, parentIdx) || isEndNode(e, i) {
			f1[i] = f1[parentIdx]
		} else {
			siblingCount := e.nodes[parentIdx].childIdx + e.nodes[parentIdx].childCnt - 1 - i
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
		if isLeaf(e, parentIdx) || isEndNode(e, i) {
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
			siblingCount := i - e.nodes[parentIdx].childIdx
			f2[i] = f2[parentIdx] + siblingCount + 1
		} else {
			// f[i] = f[pIdx] + left sibling count
			siblingCount := i - e.nodes[parentIdx].childIdx
			f2[i] = f2[parentIdx] + siblingCount
		}
	}

	res := 1
	for i := 0; i < size; i++ {
		res = max(res, f1[i])
		res = max(res, f2[i])
	}

	e.maxStackSize = int16(res)

	//e.sfSize = f1
	//e.osSize = f2

	copy(e.sfSize, f1)
	copy(e.osSize, f2)
}

func calAndSetShortCircuit(e *Expr) {
	var isLastChild = func(n *node) bool {
		idx := n.idx
		parentIdx := e.parentIdx[idx]
		if parentIdx == -1 {
			return false
		}

		cnt := e.nodes[parentIdx].childCnt
		childIdx := e.nodes[parentIdx].childIdx
		if childIdx+cnt-1 == idx {
			return true
		}
		return false
	}

	var parentNode = func(n *node) *node {
		parentIdx := e.parentIdx[n.idx]
		if parentIdx == -1 {
			return nil
		}
		return e.nodes[parentIdx]
	}

	const mask = scIfTrue | scIfFalse

	size := len(e.nodes)

	f := make([]int, size)
	for i := 1; i < size; i++ {
		n := e.nodes[i]
		p := parentNode(n)
		pIdx := p.idx

		if n.getNodeType() == end {
			f[i] = f[pIdx]
			n.flag |= p.flag & mask
			continue
		}

		if !isBoolOpNode(p) {
			f[i] = i
			continue
		}
		var flag int16
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
			pIdx = p.idx
		}
	}

	copy(e.scIdx, f)
}

func calAndSetBytecode(e *Expr) {
	var (
		setConstant = func(v Value) int16 {
			for i, c := range e.constants {
				if c == v {
					return int16(i)
				}
			}
			i := int16(len(e.constants))
			e.constants = append(e.constants, v)
			return i
		}

		setOperator = func(operator Operator) int16 {
			for i, op := range e.operators {
				if op == operator {
					return int16(i)
				}
			}
			i := int16(len(e.operators))
			e.operators = append(e.operators, operator)
			return i
		}
	)

	for i, n := range e.nodes {
		var third int16
		var forth int16
		switch n.getNodeType() {
		case constant:
			third = setConstant(n.value)
		case selector:
			third = setConstant(n.value)
			forth = int16(n.selKey)
		case operator, fastOperator:
			setOperator(n.operator)
		}

		i = i * 4
		e.bytecode[i+0] = int16(n.childCnt)<<8 | n.flag // child count and flag
		e.bytecode[i+1] = int16(n.childIdx)             // child index
		e.bytecode[i+2] = third                         // index of constants, index of operator, selectorKey
		e.bytecode[i+3] = forth                         // node index
	}
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
	if (n.flag & nodeTypeMask) != operator {
		return
	}

	for _, child := range root.children {
		typ := child.node.getNodeType()
		if typ == constant || typ == selector {
			continue
		}
		return
	}

	otherPartMask := nodeTypeMask ^ int16(0b11111111)

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
