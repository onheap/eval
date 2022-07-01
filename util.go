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
	Bool GenExprType = iota
	Number
	Hybrid
)

type GenExprConfig struct {
	EnableSelector  bool
	EnableCondition bool
	WantError       bool
	GenType         GenExprType
	NumSelectors    []GenExprResult
	BoolSelectors   []GenExprResult
}

type GenExprResult struct {
	Expr string
	Res  Value
}

type GenExprOption func(conf *GenExprConfig)

var (
	EnableSelector GenExprOption = func(c *GenExprConfig) {
		c.EnableSelector = true
	}
	EnableCondition GenExprOption = func(c *GenExprConfig) {
		c.EnableCondition = true
	}

	GenType = func(genType GenExprType) GenExprOption {
		return func(c *GenExprConfig) {
			c.GenType = genType
		}
	}

	GenSelectors = func(m map[string]Value) GenExprOption {
		return func(c *GenExprConfig) {
			var ns []GenExprResult
			var bs []GenExprResult
			for k, v := range m {
				v = unifyType(v)
				switch v.(type) {
				case int64:
					ns = append(ns, GenExprResult{Expr: k, Res: v})
				case bool:
					bs = append(bs, GenExprResult{Expr: k, Res: v})
				}
			}
			c.NumSelectors = ns
			c.BoolSelectors = bs
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
			if genType == Bool {
				switch {
				case r < 4 && c.EnableSelector && len(c.BoolSelectors) != 0:
					idx := (v) % len(c.BoolSelectors)
					return c.BoolSelectors[idx]
				default:
					if v < 50 {
						return boolExprTrue
					} else {
						return boolExprFalse
					}
				}
			}

			if genType == Number {
				switch {
				case r < 4 && c.EnableSelector && len(c.NumSelectors) != 0:
					idx := (v) % len(c.NumSelectors)
					return c.NumSelectors[idx]
				default:
					v = v - 50
					return GenExprResult{
						Expr: strconv.Itoa(v),
						Res:  int64(v),
					}
				}
			}
		}

		if genType == Bool && r < 3 {
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
			condExpr := helper(Bool, random.Intn(n))
			trueBranch := helper(genType, random.Intn(n))
			falseBranch := helper(genType, random.Intn(n))

			var res Value
			if condExpr.Res == true {
				res = trueBranch.Res
			} else {
				res = falseBranch.Res
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
		if genType == Bool {
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

func GenerateTestCase(res GenExprResult, valMap map[string]Value) string {
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
		mapStr.WriteString("map[string]Value{\n")
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

	expr := IndentByParentheses(res.Expr)
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
        },`, valStr(res.Res), expr, mapStr.String())
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

func PrintCode(e *Expr) string {
	var helper func(*node) (string, bool)

	helper = func(root *node) (string, bool) {
		idx := root.idx
		if e.childCounts[idx] == 0 {
			return printLeafNode(root)
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("(%v", root.value))
		for i := 0; i < int(e.childCounts[idx]); i++ {
			childIdx := int(e.childStartIndex[idx]) + i
			child := e.nodes[childIdx]
			cc, isLeaf := helper(child)
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

	res, _ := helper(e.nodes[0])
	return res
}

func printLeafNode(node *node) (string, bool) {
	if node.getNodeType() == selector {
		return fmt.Sprint(node.value), true
	}
	if typ := node.getNodeType(); typ == operator || typ == fastOperator {
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

func PrintExpr(expr *Expr, fields ...string) string {
	type fetcher func(e *Expr, i int) Value
	const (
		idx   = "idx"
		node  = "node"
		pIdx  = "pIdx"
		flag  = "flag"
		cCnt  = "cCnt"
		cIdx  = "cIdx"
		scIdx = "scIdx"
		scVal = "scVal"
		sfTop = "sfTop"
		osTop = "osTop"
	)

	fetchers := map[string]fetcher{
		idx: func(_ *Expr, i int) Value {
			return i
		},
		node: func(e *Expr, i int) Value {
			return e.nodes[i].value
		},
		pIdx: func(e *Expr, i int) Value {
			return e.parentIndex[i]
		},
		flag: func(e *Expr, i int) Value {
			f := e.nodes[i].flag
			res := ""
			switch f & nodeTypeMask {
			case operator:
				res = "OP"
			case fastOperator:
				res = "OPf"
			case selector:
				res = "S"
			case value:
				res = "V"
			case cond:
				res = "IF"
			}
			return res
		},
		cCnt: func(e *Expr, i int) Value {
			return e.childCounts[i]
		},
		cIdx: func(e *Expr, i int) Value {
			return e.childStartIndex[i]
		},
		scIdx: func(e *Expr, i int) Value {
			return e.nodes[i].scIdx
		},
		scVal: func(e *Expr, i int) Value {
			f := e.nodes[i].flag
			res := ""
			if f&scIfTrue == scIfTrue {
				res += "T"
			}
			if f&scIfFalse == scIfFalse {
				res += "F"
			}
			return res
		},
		sfTop: func(e *Expr, i int) Value {
			return e.sfSize[i] - 1
		},
		osTop: func(e *Expr, i int) Value {
			return e.osSize[i] - 1
		},
	}

	var allFields = []string{idx, node, pIdx, flag, cCnt, cIdx, scIdx, scVal, sfTop, osTop}

	if len(fields) == 0 {
		fields = allFields
	}

	set := make(map[string]bool)
	for _, field := range fields {
		set[field] = true
	}

	size := len(expr.nodes)
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("node  size: %4d\n", len(expr.nodes)))
	sb.WriteString(fmt.Sprintf("stack size: %4d\n", expr.maxStackSize))

	for i, field := range allFields {
		if !set[field] && i >= 2 {
			continue
		}

		sb.WriteString(fmt.Sprintf("%5s: ", field))
		for j := 0; j < size; j++ {
			sb.WriteString(fmt.Sprintf("|%4v", fetchers[field](expr, j)))
		}
		sb.WriteString("|\n")
	}

	return sb.String()
}
