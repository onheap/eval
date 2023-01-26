package eval

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"unicode"
)

var empty = struct{}{}

type GenExprType int

const (
	GenBool GenExprType = iota
	GenNumber
)

type GenExprConfig struct {
	EnableVariable  bool
	EnableCondition bool
	EnableTryEval   bool
	WantError       bool
	GenType         GenExprType
	NumVariables    []GenExprResult
	BoolVariables   []GenExprResult
	DneVariables    []GenExprResult
}

type GenExprResult struct {
	Expr string
	Res  Value
}

type GenExprOption func(conf *GenExprConfig)

var (
	EnableVariable GenExprOption = func(c *GenExprConfig) {
		c.EnableVariable = true
	}
	EnableCondition GenExprOption = func(c *GenExprConfig) {
		c.EnableCondition = true
	}
	EnableTryEval GenExprOption = func(c *GenExprConfig) {
		c.EnableTryEval = true
	}

	GenType = func(genType GenExprType) GenExprOption {
		return func(c *GenExprConfig) {
			c.GenType = genType
		}
	}

	GenVariables = func(m map[string]interface{}) GenExprOption {
		return func(c *GenExprConfig) {
			var (
				numVars  []GenExprResult
				boolVars []GenExprResult
				dneVars  []GenExprResult
			)
			for k, v := range m {
				if v == DNE {
					dneVars = append(dneVars, GenExprResult{Expr: k, Res: v})
					continue
				}
				v = UnifyType(v)
				switch v.(type) {
				case int64:
					numVars = append(numVars, GenExprResult{Expr: k, Res: v})
				case bool:
					boolVars = append(boolVars, GenExprResult{Expr: k, Res: v})
				}
			}
			c.NumVariables = append(c.NumVariables, numVars...)
			c.BoolVariables = append(c.BoolVariables, boolVars...)
			c.DneVariables = append(c.DneVariables, dneVars...)
		}
	}
)

// GenerateRandomExpr generate random expression
func GenerateRandomExpr(level int, random *rand.Rand, opts ...GenExprOption) GenExprResult {
	c := new(GenExprConfig)
	for _, opt := range opts {
		opt(c)
	}

	var (
		// bool atomic expressions
		boolExprTrue  = GenExprResult{Expr: "(= 0 0)", Res: true}
		boolExprFalse = GenExprResult{Expr: "(!= 0 0)", Res: false}

		// bool operators
		boolMultiOps = []string{"and", "or", "eq"}
		boolUnaryOps = []string{"not"}

		// number operators
		numSafeOps = []string{"+", "-", "*"}
		numAllOps  = []string{"+", "-", "*", "/", "%"}

		execOp = func(op string, param ...Value) Value {
			switch {
			case op == "and" && contains(param, false):
				return false
			case op == "or" && contains(param, true):
				return true
			case contains(param, DNE):
				return DNE
			}

			fn := builtinOperators[op]
			res, _ := fn(nil, param)
			return res
		}
	)

	var helper func(typ GenExprType, n int) GenExprResult
	helper = func(genType GenExprType, n int) GenExprResult {
		r := random.Intn(10)
		if n == 0 {
			v := random.Intn(100)
			if genType == GenBool {
				switch {
				case r == 1 && c.EnableTryEval && len(c.DneVariables) != 0:
					idx := (v) % len(c.DneVariables)
					return c.DneVariables[idx]
				case r < 4 && c.EnableVariable && len(c.BoolVariables) != 0:
					idx := (v) % len(c.BoolVariables)
					return c.BoolVariables[idx]
				default:
					if v < 50 {
						return boolExprTrue
					} else {
						return boolExprFalse
					}
				}
			}

			if genType == GenNumber {
				switch {
				case r == 1 && c.EnableTryEval && len(c.DneVariables) != 0:
					idx := (v) % len(c.DneVariables)
					return c.DneVariables[idx]
				case r < 4 && c.EnableVariable && len(c.NumVariables) != 0:
					idx := (v) % len(c.NumVariables)
					return c.NumVariables[idx]
				default:
					v = v - 50
					return GenExprResult{
						Expr: strconv.Itoa(v),
						Res:  int64(v),
					}
				}
			}
		}

		if genType == GenBool && r < 3 {
			// unary operator
			op := boolUnaryOps[r%len(boolUnaryOps)]
			genRes := helper(genType, n-1)
			return GenExprResult{
				Expr: fmt.Sprintf(`(%s %s)`, op, genRes.Expr),
				Res:  execOp(op, genRes.Res),
			}
		}

		if c.EnableCondition && r == 3 {
			// if node
			condExpr := helper(GenBool, random.Intn(n))
			trueBranch := helper(genType, random.Intn(n))
			falseBranch := helper(genType, random.Intn(n))

			var res Value
			switch condExpr.Res {
			case true:
				res = trueBranch.Res
			case false:
				res = falseBranch.Res
			case DNE:
				res = DNE
			}
			return GenExprResult{
				Expr: fmt.Sprintf(`(if %s %s %s)`, condExpr.Expr, trueBranch.Expr, falseBranch.Expr),
				Res:  res,
			}
		}

		l := random.Intn(3) + 2
		childExpr, childRes := make([]string, l), make([]Value, l)
		for i := 0; i < l; i++ {
			genRes := helper(genType, random.Intn(n))
			childExpr[i], childRes[i] = genRes.Expr, genRes.Res
		}

		var op string
		if genType == GenBool {
			op = boolMultiOps[r%len(boolMultiOps)]
		} else {
			safe := true
			for _, res := range childRes[1:] {
				if res == int64(0) {
					safe = false
				}
			}
			if safe {
				op = numAllOps[r%len(numAllOps)]
			} else {
				op = numSafeOps[r%len(numSafeOps)]
			}
		}

		return GenExprResult{
			Expr: fmt.Sprintf(`(%s %s)`, op, strings.Join(childExpr, " ")),
			Res:  execOp(op, childRes...),
		}
	}

	return helper(c.GenType, level)
}

func GenerateTestCase(expr string, want Value, valMap map[string]interface{}) string {
	var valStr = func(val Value) string {
		switch v := val.(type) {
		case bool:
			if v {
				return "true"
			} else {
				return "false"
			}
		case int64:
			return fmt.Sprintf("int64(%d)", v)
		case string:
			if strings.ContainsAny(v, `\n"`) {
				return fmt.Sprintf("`%s`", v)
			}
			return fmt.Sprintf(`"%s"`, v)
		default:
			if v == DNE {
				return "DNE"
			}
			panic("unsupported type")
		}
	}

	var mapStr strings.Builder

	const (
		tab   = 4
		space = " "
	)

	var maxKeyLen int
	for key := range valMap {
		maxKeyLen = max(maxKeyLen, len(key))
	}

	if valMap != nil {
		mapStr.WriteString("map[string]interface{}{\n")
		for key, val := range valMap {
			mapStr.WriteString(strings.Repeat(space, tab*4))
			mapStr.WriteString(fmt.Sprintf(`"%s": `, key))
			mapStr.WriteString(strings.Repeat(space, maxKeyLen-len(key)))
			mapStr.WriteString(valStr(val))
			mapStr.WriteRune(',')
			mapStr.WriteRune('\n')
		}
		mapStr.WriteString(strings.Repeat(space, tab*3))
		mapStr.WriteString("}")
	} else {
		mapStr.WriteString("       nil")
	}

	expr = IndentByParentheses(expr)
	if strings.ContainsRune(expr, '\n') {
		expr = fmt.Sprintf("`\n%s`", expr)
	} else {
		expr = fmt.Sprintf(`            %s`, valStr(expr))
	}

	return fmt.Sprintf(`
        {
            want:          %s,
            optimizeLevel: disable,
            s: %s,
            valMap: %s,
        },`, valStr(want), expr, mapStr.String())
}

func IndentByParentheses(s string) string {
	type syntax int8
	const (
		leftPar syntax = iota
		rightPar
		space
		comment
		normal
	)

	left := make(map[rune]bool)
	right := make(map[rune]bool)
	for _, pair := range []string{"[]", "()"} {
		left[rune(pair[0])] = true
		right[rune(pair[1])] = true
	}

	A := []rune(s)
	var sb strings.Builder
	sb.Grow(len(A) * 2)
	var (
		appendSpace = func() {
			sb.WriteRune(' ')
		}
		appendIndent = func(indent int) {
			for i := 0; i < indent*2; i++ {
				appendSpace()
			}
		}
		appendNewLine = func(indent int) {
			sb.WriteRune('\n')
			appendIndent(indent)
		}

		appendLeft = func(c rune, prev syntax, indent int) {
			if prev == comment {
				appendIndent(indent)
			} else {
				appendNewLine(indent)
			}
			sb.WriteRune(c)
		}

		appendRight = func(c rune, prev syntax, indent int) {
			if prev == comment {
				appendIndent(indent)
			}
			sb.WriteRune(c)
		}

		appendRune = func(c rune, prev syntax, indent int) {
			if prev == comment {
				appendIndent(indent)
			}
			if prev == space || prev == rightPar {
				appendSpace()
			}
			sb.WriteRune(c)
		}
	)

	var indent int
	var prev = normal
	for i := 0; i < len(A); i++ {
		c := A[i]
		switch {
		case left[c]:
			appendLeft(c, prev, indent)
			indent++
			prev = leftPar
		case right[c]:
			indent--
			appendRight(c, prev, indent)
			prev = rightPar
		case unicode.IsSpace(c):
			if prev != comment {
				prev = space
			}
		case c == ';':
			if prev == comment {
				appendIndent(indent)
			} else {
				for j := i - 1; j >= 0; j-- {
					if !unicode.IsSpace(A[j]) {
						appendSpace()
						break
					}
					if A[j] == '\n' { // add a new comment line
						appendNewLine(indent)
						break
					}
				}
			}

			for ; i < len(A); i++ {
				sb.WriteRune(A[i])
				if A[i] == '\n' {
					break
				}
			}
			prev = comment
		default:
			appendRune(c, prev, indent)
			prev = normal
		}
	}

	return strings.TrimSpace(sb.String())
}

func Dump(e *Expr) string {
	var getChildIdxes = func(idx int16) (res []int16) {
		for i, p := range e.parentIdx {
			if p == idx && e.nodes[i].getNodeType() != event {
				res = append(res, int16(i))
			}
		}

		if e.nodes[idx].getNodeType() == cond {
			res = []int16{
				res[0], // condition node
				res[1], // true branch
				res[3], // false branch
			}
		}
		return
	}

	var helper func(int16) (string, bool)

	helper = func(idx int16) (string, bool) {
		n := e.nodes[idx]
		if n.childCnt == 0 {
			return dumpLeafNode(n)
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("(%v", n.value))

		childIdxes := getChildIdxes(idx)

		for _, cIdx := range childIdxes {
			cc, isLeaf := helper(cIdx)
			if isLeaf {
				sb.WriteString(fmt.Sprintf(" %s", cc))
				continue
			}

			for _, cs := range strings.Split(cc, "\n") {
				sb.WriteString(fmt.Sprintf("\n  %s", cs))
			}
		}
		sb.WriteString(")")
		return sb.String(), false
	}

	var rootIdx int16
	for idx, pIdx := range e.parentIdx {
		if pIdx == -1 {
			rootIdx = int16(idx)
		}
	}

	res, _ := helper(rootIdx)
	return res
}

func dumpLeafNode(node *node) (string, bool) {
	switch node.getNodeType() {
	case event:
		return "eventNode", false
	case variable:
		return fmt.Sprint(node.value), true
	case operator, fastOperator:
		return fmt.Sprintf("(%v)", node.value), false
	}

	var res string
	switch v := node.value.(type) {
	case string:
		res = strconv.Quote(v)
	case []string:
		var sb strings.Builder
		sb.WriteRune('(')
		for idx, s := range v {
			if idx != 0 {
				sb.WriteRune(' ')
			}
			sb.WriteString(strconv.Quote(s))
		}
		sb.WriteRune(')')
		res = sb.String()
	case []int64:
		var sb strings.Builder
		sb.WriteRune('(')
		for idx, i := range v {
			if idx != 0 {
				sb.WriteRune(' ')
			}
			sb.WriteString(strconv.FormatInt(i, 10))
		}
		sb.WriteRune(')')
		res = sb.String()
	default:
		res = fmt.Sprint(v)
	}
	return res, true
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func maxInt16(a, b int16) int16 {
	if a > b {
		return a
	} else {
		return b
	}
}

func contains(params []Value, target Value) bool {
	for _, v := range params {
		if v == target {
			return true
		}
	}
	return false
}

func DumpTable(expr *Expr, skipEventNode bool) string {

	var (
		width  = 5
		format = fmt.Sprintf("|%%%dv", width)
	)

	type fetcher struct {
		name string
		fn   func(e *Expr, i int) Value
	}

	type field int

	const (
		idx field = iota
		node
		pIdx
		flag
		cCnt
		scIdx
		scVal
		osTop
	)

	var getFlag = func(f uint8) Value {
		res := ""
		switch f & nodeTypeMask {
		case operator:
			res = "OP"
		case fastOperator:
			res = "OPf"
		case variable:
			res = "V"
		case constant:
			res = "C"
		case cond:
			res = "COND"
		case event:
			res = "EVNT"
		}

		if f&scIfTrue == scIfTrue {
			res += "T"
		}
		if f&scIfFalse == scIfFalse {
			res += "F"
		}
		return res
	}

	fetchers := [...]fetcher{
		idx: {
			name: "idx",
			fn: func(_ *Expr, i int) Value {
				return i
			},
		},
		node: {
			name: "node",
			fn: func(e *Expr, i int) Value {
				var v Value
				if n := e.nodes[i]; n.getNodeType() == event {
					v = n.value.(LoopEventData).NodeValue
				} else {
					v = n.value
				}
				res := fmt.Sprintf("%v", v)
				if l := len(res); l > width {
					res = res[:width-1] + "…"
				}
				return res
			},
		},
		pIdx: {
			name: "pIdx",
			fn: func(e *Expr, i int) Value {
				return e.parentIdx[i]
			},
		},
		flag: {
			name: "flag",
			fn: func(e *Expr, i int) Value {
				return getFlag(e.nodes[i].flag & nodeTypeMask)
			},
		},
		cCnt: {
			name: "cCnt",
			fn: func(e *Expr, i int) Value {
				return e.nodes[i].childCnt
			},
		},
		scIdx: {
			name: "scIdx",
			fn: func(e *Expr, i int) Value {
				return e.nodes[i].scIdx
			},
		},
		scVal: {
			name: "scVal",
			fn: func(e *Expr, i int) Value {
				return getFlag(e.nodes[i].flag & (scIfFalse | scIfTrue))
			},
		},
		osTop: {
			name: "osTop",
			fn: func(e *Expr, i int) Value {
				return e.nodes[i].osTop
			},
		},
	}

	size := len(expr.nodes)
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("node  size: %4d\n", len(expr.nodes)))
	sb.WriteString(fmt.Sprintf("stack size: %4d\n", expr.maxStackSize))

	for f, n := range fetchers {
		sb.WriteString(fmt.Sprintf("%5s: ", n.name))
		for j := 0; j < size; j++ {
			if expr.nodes[j].flag&nodeTypeMask == event && skipEventNode {
				continue
			}

			sb.WriteString(fmt.Sprintf(format, fetchers[f].fn(expr, j)))
		}
		sb.WriteString("|\n")
	}

	return sb.String()
}

func HandleDebugEvent(e *Expr) {
	go func() {
		var prev LoopEventData
		for ev := range e.EventChan {
			switch ev.EventType {
			case OpExecEvent:
				data := ev.Data.(OpEventData)
				fmt.Printf(
					"%13s: op: %s, isFast: %v, params: %v, res: %v, err: %v\n",
					"Exec Operator", data.OpName, data.IsFastOp, data.Params, data.Res, data.Err)
			case LoopEvent:
				var (
					sb   strings.Builder
					curt = ev.Data.(LoopEventData)
				)

				var minSteps int16 = 2
				if prev.NodeType == FastOperatorNode {
					minSteps = 4
				}

				if curt.CurtIdx-prev.CurtIdx > minSteps {
					sb.WriteString(fmt.Sprintf("%13s: [%v] jump to [%v]\n\n", "Short Circuit", prev.NodeValue, curt.NodeValue))
				} else {
					sb.WriteString(fmt.Sprintf("\n"))
				}

				sb.WriteString(fmt.Sprintf("%13s: [%v], type:[%s], idx:[%d]\n", "Current Node", curt.NodeValue, curt.NodeType.String(), curt.CurtIdx))

				sb.WriteString(fmt.Sprintf("%13s: ", "Operand Stack"))
				for i := len(ev.Stack) - 1; i >= 0; i-- {
					sb.WriteString(fmt.Sprintf("|%4v", ev.Stack[i]))
				}
				sb.WriteString("|")
				fmt.Println(sb.String())

				prev = curt
			default:
				fmt.Printf("Unknown event: %+v\n", ev)
			}
		}

		fmt.Println("Event channel closed")
	}()
}
