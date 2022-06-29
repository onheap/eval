package eval

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type tokenType string

const (
	integer tokenType = "integer"
	str     tokenType = "str"
	ident   tokenType = "ident"
	lParen  tokenType = "lParen"
	rParen  tokenType = "rParen"
	comment tokenType = "comment"
)

func (t tokenType) String() string {
	return string(t)
}

type token struct {
	typ tokenType
	val string
}

func lex(expr string) ([]token, error) {
	var (
		nextToken = func(A []rune, i int) (string, int) {
			j := i
			for ; j < len(A); j++ {
				if c := A[j]; unicode.IsSpace(c) || c == '(' || c == ')' || c == ';' {
					break
				}
			}
			return string(A[i:j]), j
		}

		lexParen = func(A []rune, i int) (token, int) {
			const parens = "()[]"
			if idx := strings.IndexRune(parens, A[i]); idx != -1 {
				t := token{val: string(A[i])}
				if idx%2 == 0 {
					t.typ = lParen
				} else {
					t.typ = rParen
				}
				return t, i + 1
			}
			return token{}, i
		}

		lexInteger = func(A []rune, i int) (token, int) {
			s, j := nextToken(A, i)
			if _, err := strconv.ParseInt(s, 10, 64); err == nil {
				return token{
					typ: integer,
					val: s,
				}, j
			}
			return token{}, i
		}

		lexStr = func(A []rune, i int) (token, int) {
			const quote = '"'
			if A[i] != quote {
				return token{}, i
			}
			j := i + 1
			for ; j < len(A); j++ {
				if A[j] == quote {
					return token{
						typ: str,
						val: string(A[i+1 : j]),
					}, j + 1
				}
			}
			return token{}, i
		}

		lexIdent = func(A []rune, i int) (token, int) {
			s, j := nextToken(A, i)

			for idx, c := range []rune(s) {
				if unicode.IsNumber(c) {
					if idx != 0 {
						continue
					}
				}
				if unicode.IsLetter(c) {
					continue
				}
				if c == '_' {
					continue
				}

				// if the code execute to here, it means
				// the ident contains special character
				// check if it's builtin operators
				// only builtin operators can have special character
				if _, exist := builtinOperators[s]; exist {
					break
				}

				return token{}, i
			}

			if i != j {
				return token{
					typ: ident,
					val: s,
				}, j
			}
			return token{}, i
		}
		lexComment = func(A []rune, i int) (token, int) {
			if A[i] != ';' {
				return token{}, i
			}
			j := i
			for ; j < len(A); j++ {
				if A[j] == '\n' {
					break
				}
			}

			return token{
				typ: comment,
				val: string(A[i:j]),
			}, j + 1

		}

		lexers = []func([]rune, int) (token, int){
			lexParen,
			lexInteger,
			lexStr,
			lexIdent,
			lexComment,
		}
	)

	var tokens []token
	A := []rune(expr)
	for i := 0; i < len(A); {
		c := A[i]
		if unicode.IsSpace(c) {
			i++
			continue
		}

		found := false
		for _, lexer := range lexers {
			token, j := lexer(A, i)
			if i != j {
				found = true
				tokens = append(tokens, token)
				i = j
				break
			}
		}
		if !found {
			length := 30
			var left, right string
			if l := i - length; l < 0 {
				left = string(A[0:i])
			} else {
				left = "..." + string(A[l:i])
			}
			if r := i + length; r > len(A)-1 {
				right = string(A[i+1:])
			} else {
				right = string(A[i+1:r]) + "..."
			}
			return nil, fmt.Errorf("can not parse token at, %s[%c]%s", left, c, right)
		}
	}
	return tokens, nil
}

type compileCtx struct {
	conf *CompileConfig
	T    []token
}

func (c *compileCtx) invalidExprErr(pos int) error {
	return c.errWithPos(errors.New("invalid expression error"), pos)
}

func (c *compileCtx) unknownTokenError(pos int) error {
	return c.errWithPos(errors.New("unknown token error"), pos)
}

func (c *compileCtx) tokenTypeError(want tokenType, pos int) error {
	err := fmt.Errorf("token type unexpected error (want: %s, got: %s)", want, c.T[pos].typ)
	return c.errWithPos(err, pos)
}

func (c *compileCtx) parenUnmatchedErr(pos int) error {
	return c.errWithPos(errors.New("parentheses unmatched error"), pos)
}

func (c *compileCtx) paramsCountErr(want, got int, pos int) error {
	err := fmt.Errorf("%s parameters count error (want: %d, got: %d)", c.T[pos].val, want, got)
	return c.errWithPos(err, pos)
}

func (c *compileCtx) errWithPos(err error, idx int) error {
	return fmt.Errorf("%w occurs at %s", err, c.pos(idx))
}

func (c *compileCtx) printPosMsg(msg string, idx int) {
	fmt.Println(msg, c.pos(idx))
}

func (c *compileCtx) printPos(idx int) {
	fmt.Println(c.pos(idx))
}

func (c *compileCtx) pos(idx int) string {
	length := 5
	var sb strings.Builder
	l := idx - length
	if l <= 0 {
		l = 0
	} else {
		sb.WriteString("...")
	}
	for l < idx {
		sb.WriteString(c.T[l].val)
		sb.WriteRune(' ')
		l++
	}
	sb.WriteString(fmt.Sprintf("[%s]", c.T[idx].val))
	idx++

	r := idx + length

	for idx <= r && r < len(c.T)-1 {
		sb.WriteRune(' ')
		sb.WriteString(c.T[idx].val)
		idx++
	}

	if r < len(c.T)-1 {
		sb.WriteString("...")
	}
	return sb.String()
}

// ast
type astNode struct {
	node     *node
	children []*astNode
	cost     int
}

func parseListNode(ctx *compileCtx, T []token, i int) (*astNode, int, error) {
	if T[i].typ != lParen {
		return nil, i, nil
	}
	typ := T[i+1].typ
	if typ != rParen && typ != integer && typ != str {
		return nil, i, nil
	}
	strs := []string{}
	for j := i + 1; j < len(T); j++ {
		if T[j].typ == rParen {
			i = j
			break
		}
		if T[j].typ != typ {
			err := fmt.Errorf("mismatched list element types error, want: %+v, got: %+v", typ, T[j])
			return nil, 0, ctx.errWithPos(err, j)
		}
		strs = append(strs, T[j].val)
	}

	// todo: return error when list is empty

	n := &node{flag: value}
	if typ == integer {
		ints := make([]int64, 0, len(strs))
		for _, s := range strs {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, 0, err
			}
			ints = append(ints, v)
		}
		n.value = ints
	} else {
		n.value = strs
	}
	return &astNode{
		node: n,
	}, i + 1, nil
}

func parseSingleNode(ctx *compileCtx, T []token, i int) (*astNode, int, error) {
	var (
		parseInt = func(_ *compileCtx, t token) (*node, error) {
			if t.typ != integer {
				return nil, nil
			}
			v, err := strconv.ParseInt(t.val, 10, 64)
			if err != nil {
				return nil, err
			}
			return &node{
				flag:  value,
				value: v,
			}, nil
		}

		parseStr = func(_ *compileCtx, t token) (*node, error) {
			if t.typ != str {
				return nil, nil
			}
			return &node{
				flag:  value,
				value: t.val,
			}, nil
		}

		parseConst = func(ctx0 *compileCtx, t token) (*node, error) {
			if t.typ != ident {
				return nil, nil
			}

			if val, ok := builtinConstants[t.val]; ok {
				return &node{
					flag:  value,
					value: unifyType(val),
				}, nil
			}

			if val, ok := ctx0.conf.ConstantMap[t.val]; ok {
				return &node{
					flag:  value,
					value: val,
				}, nil
			}
			return nil, nil
		}

		parseSelector = func(ctx0 *compileCtx, t token) (*node, error) {
			if t.typ != ident {
				return nil, nil
			}
			if key, ok := ctx0.conf.SelectorMap[t.val]; ok {
				return &node{
					flag:   selector,
					value:  t.val,
					selKey: key,
				}, nil
			}
			return nil, nil
		}

		singleTokenParser = []func(*compileCtx, token) (*node, error){
			parseInt, parseStr, parseConst, parseSelector,
		}
	)

	for _, parser := range singleTokenParser {
		node, err := parser(ctx, T[i])
		if err != nil {
			return nil, 0, err
		}
		if node != nil {
			return &astNode{
				node: node,
			}, i + 1, nil
		}
	}
	return nil, i, nil
}

type parser func(*compileCtx, []token, int) (*astNode, int, error)

func parseConfig(originConf *CompileConfig, tokens []token) (*CompileConfig, error) {
	const prefix = ";;;;" // prefix of compile config
	const separator = "," // separator of compile config

	confCopy := CopyCompileConfig(originConf)

	for i := 0; i < len(tokens); i++ { // parse config
		if tokens[i].typ != comment {
			break
		}
		cmt := strings.TrimSpace(tokens[i].val)
		if !strings.HasPrefix(cmt, prefix) {
			continue
		}
		// trim compile config prefix and spaces
		cmt = strings.TrimPrefix(cmt, prefix)
		for _, s := range strings.Split(cmt, separator) {
			pair := strings.Split(s, ":")
			if len(pair) != 2 {
				return nil, fmt.Errorf("invalid compile format %s", s)
			}

			for i := range pair {
				pair[i] = strings.TrimSpace(pair[i])
			}

			enabled, err := strconv.ParseBool(pair[1])
			if err != nil {
				return nil, fmt.Errorf("invalid config value %s, err %w", s, err)
			}
			switch option := OptimizeOption(pair[0]); option {
			case AllOptimizations: // switch all optimizations
				for _, option := range []OptimizeOption{Reordering, FastEvaluation, ConstantFolding} {
					confCopy.OptimizeOptions[option] = enabled
				}
			case Reordering, FastEvaluation, ConstantFolding:
				confCopy.OptimizeOptions[option] = enabled
			default:
				return nil, fmt.Errorf("unsupported compile config %s", s)
			}
		}
	}

	return confCopy, nil
}

func parseAstTree(cc *CompileConfig, tokens []token) (*astNode, error) {
	cc = CopyCompileConfig(cc) // to make sure all config maps are all initialized
	temp := make([]token, 0, len(tokens))
	var parenCnt int // check parentheses
	for _, t := range tokens {
		if t.typ == comment {
			continue // remove comments token
		}

		if t.typ == lParen {
			parenCnt++
		} else if t.typ == rParen {
			parenCnt--
		}

		temp = append(temp, t)
	}
	tokens = temp

	ctx := &compileCtx{
		conf: cc,
		T:    tokens,
	}

	if parenCnt != 0 || tokens[0].typ != lParen || tokens[len(tokens)-1].typ != rParen {
		return nil, ctx.parenUnmatchedErr(0)
	}

	root, i, err := parseAstHelper(ctx, tokens, 0)
	if err != nil {
		return nil, err
	}

	if i != len(tokens) {
		return nil, ctx.invalidExprErr(i)
	}
	return root, nil
}

func parseAstHelper(ctx *compileCtx, T []token, i int) (*astNode, int, error) {
	for _, p := range []parser{parseSingleNode, parseListNode} {
		n, j, err := p(ctx, T, i)
		if n != nil || err != nil {
			return n, j, err
		}
	}

	if T[i].typ != lParen {
		return nil, i, ctx.unknownTokenError(i)
	}

	car := T[i+1]
	if car.typ != ident {
		return nil, i, ctx.tokenTypeError(ident, i+1)
	}

	var children []*astNode
	for j := i + 2; j < len(T); {
		if T[j].typ == rParen {
			node, err := buildNode(ctx, car, i+1, children)
			if err != nil {
				return nil, j, err
			}
			return node, j + 1, err
		}

		child, k, err := parseAstHelper(ctx, T, j)
		if err != nil {
			return nil, k, err
		}
		children = append(children, child)
		j = k
	}
	return nil, i, ctx.parenUnmatchedErr(i)
}

func buildNode(ctx *compileCtx, car token, pos int, children []*astNode) (*astNode, error) {
	treeNode := &astNode{
		node:     &node{value: car.val},
		children: children,
	}

	if car.val == "if" {
		if len(children) != 3 {
			return nil, ctx.paramsCountErr(3, len(children), pos)
		}
		treeNode.node.flag = cond
		return treeNode, nil
	}
	// parse op node
	op, exist := builtinOperators[car.val]
	if !exist {
		op, exist = ctx.conf.OperatorMap[car.val]
	}
	if !exist {
		return nil, fmt.Errorf("unknown operator error, operator not found: %s", car.val)
	}
	flag := operator
	treeNode.node.operator = op
	treeNode.node.flag = flag
	return treeNode, nil
}
