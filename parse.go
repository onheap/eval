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
	pos int
}

// ast
type astNode struct {
	node     *node
	children []*astNode
	cost     int
}

type parser struct {
	source string
	conf   *CompileConfig
	tokens []token
	idx    int
}

func newParser(cc *CompileConfig, source string) *parser {
	return &parser{
		source: source,
		conf:   cc,
	}
}

func (p *parser) lex() error {
	var (
		nextToken = func(A []rune, i int) (string, int) {
			j := i
			for ; j < len(A); j++ {
				if r := A[j]; unicode.IsSpace(r) || r == '(' || r == ')' || r == ';' {
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

			for idx, r := range []rune(s) {
				if unicode.IsNumber(r) {
					if idx != 0 {
						continue
					}
				}
				if unicode.IsLetter(r) {
					continue
				}
				if r == '_' {
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
	A := []rune(p.source)
	for i := 0; i < len(A); {
		r := A[i]
		if unicode.IsSpace(r) {
			i++
			continue
		}

		found := false
		for _, lexer := range lexers {
			t, j := lexer(A, i)
			if i != j {
				found = true
				t.pos = i
				tokens = append(tokens, t)
				i = j
				break
			}
		}
		if !found {
			return p.errWithPos(errors.New("can not parse token"), i)
		}
	}
	p.tokens = tokens
	return nil
}

func (p *parser) parseAstTree() (*astNode, error) {
	n := 0
	for _, t := range p.tokens {
		if t.typ != comment {
			p.tokens[n] = t
			n++
		}
	}
	p.tokens = p.tokens[:n]

	if err := p.checkParentheses(); err != nil {
		return nil, err
	}

	root, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	if p.idx != n {
		return nil, p.invalidExprErr(p.idx)
	}
	return root, nil
}

func (p *parser) checkParentheses() error {
	last := len(p.tokens) - 1
	if p.tokens[0].typ != lParen || p.tokens[last].typ != rParen {
		return p.parenUnmatchedErr(0)
	}

	var parenCnt int // check parentheses
	for i, t := range p.tokens {
		switch t.typ {
		case lParen:
			parenCnt++
		case rParen:
			parenCnt--
		}
		if parenCnt < 0 || (parenCnt == 0 && i != last) {
			return p.parenUnmatchedErr(t.pos)
		}
	}

	return nil
}

func (p *parser) parse() (*astNode, *CompileConfig, error) {
	err := p.lex()
	if err != nil {
		return nil, nil, err
	}
	err = p.parseConfig()
	if err != nil {
		return nil, nil, err
	}
	ast, err := p.parseAstTree()
	if err != nil {
		return nil, nil, err
	}
	return ast, p.conf, nil
}

func (p *parser) peek() token {
	return p.tokens[p.idx]
}

func (p *parser) next() token {
	t := p.tokens[p.idx]
	p.idx++
	return t
}

func (p *parser) eat(expectTypes ...tokenType) error {
	t := p.next()
	if len(expectTypes) == 0 {
		return nil
	}

	for _, expectType := range expectTypes {
		if t.typ == expectType {
			return nil
		}
	}
	return p.tokenTypeError(expectTypes[0], t)
}

func (p *parser) walk() {
	p.idx++
}

func (p *parser) invalidExprErr(pos int) error {
	return p.errWithPos(errors.New("invalid expression error"), pos)
}

func (p *parser) unknownTokenError(t token) error {
	return p.errWithToken(errors.New("unknown token error"), t)
}

func (p *parser) tokenTypeError(want tokenType, t token) error {
	err := fmt.Errorf("token type unexpected error (want: %s, got: %s)", want, t.typ)
	return p.errWithToken(err, t)
}

func (p *parser) parenUnmatchedErr(pos int) error {
	return p.errWithPos(errors.New("parentheses unmatched error"), pos)
}

func (p *parser) paramsCountErr(want, got int, t token) error {
	err := fmt.Errorf("%s parameters count error (want: %d, got: %d)", t.val, want, got)
	return p.errWithToken(err, t)
}

func (p *parser) errWithToken(err error, t token) error {
	return p.errWithPos(err, t.pos)
}

func (p *parser) errWithPos(err error, idx int) error {
	return fmt.Errorf("%w occurs at %s", err, p.pos(idx))
}

func (p *parser) printPosMsg(msg string, idx int) {
	fmt.Println(msg, p.pos(idx))
}

func (p *parser) printPos(idx int) {
	fmt.Println(p.pos(idx))
}

func (p *parser) pos(i int) string {
	A := []rune(p.source)
	length := 30
	var left, right string
	if l := i - length; l < 0 {
		left = string(A[0:i])
	} else {
		left = "..." + string(A[l:i])
	}
	if r := i + length; r > len(A)-1 {
		if i >= len(A)-1 {
			right = ""
		} else {
			right = string(A[i+1:])
		}
	} else {
		right = string(A[i+1:r]) + "..."
	}
	return fmt.Sprintf(" %s[%c]%s", left, A[i], right)
}

func (p *parser) valNode(v Value) *astNode {
	return &astNode{
		node: &node{
			flag:  constant,
			value: v,
		},
	}
}

func (p *parser) parseList() (*astNode, error) {
	i := p.idx
	T := p.tokens
	if T[i].typ != lParen {
		return nil, nil
	}
	typ := T[i+1].typ
	if typ != rParen && typ != integer && typ != str {
		return nil, nil
	}
	strs := []string{}
	for j := i + 1; j < len(T); j++ {
		if T[j].typ == rParen {
			i = j
			break
		}
		if T[j].typ != typ {
			return nil, p.tokenTypeError(typ, T[j])
		}
		strs = append(strs, T[j].val)
	}

	// todo: return error when list is empty

	n := &node{flag: constant}
	if typ == integer {
		ints := make([]int64, 0, len(strs))
		for _, s := range strs {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, err
			}
			ints = append(ints, v)
		}
		n.value = ints
	} else {
		n.value = strs
	}
	p.idx = i + 1
	return &astNode{
		node: n,
	}, nil
}

func (p *parser) parseInt() (*astNode, error) {
	t := p.peek()
	if t.typ != integer {
		return nil, nil
	}
	v, err := strconv.ParseInt(t.val, 10, 64)
	if err != nil {
		return nil, err
	}
	p.walk()
	return p.valNode(v), nil
}
func (p *parser) parseStr() (*astNode, error) {
	t := p.peek()
	if t.typ != str {
		return nil, nil
	}
	p.walk()
	return p.valNode(t.val), nil
}
func (p *parser) parseConst() (*astNode, error) {
	t := p.peek()
	if t.typ != ident {
		return nil, nil
	}

	if val, ok := builtinConstants[t.val]; ok {
		p.walk()
		return p.valNode(val), nil
	}

	if val, ok := p.conf.ConstantMap[t.val]; ok {
		p.walk()
		return p.valNode(val), nil
	}
	return nil, nil
}

func (p *parser) parseSelector() (*astNode, error) {
	t := p.peek()
	if t.typ != ident {
		return nil, nil
	}
	if key, ok := p.conf.SelectorMap[t.val]; ok {
		p.walk()
		return &astNode{
			node: &node{
				flag:   selector,
				value:  t.val,
				selKey: key,
			},
		}, nil
	}
	return nil, nil
}

func (p *parser) parseExpression() (*astNode, error) {
	fns := []func() (*astNode, error){
		p.parseInt, p.parseStr, p.parseConst, p.parseSelector, p.parseList}
	for _, fn := range fns {
		n, err := fn()
		if n != nil || err != nil {
			return n, err
		}
	}

	if t := p.peek(); t.typ == ident {
		if p.conf.CompileOptions[AllowUnknownSelectors] {
			p.walk()
			return &astNode{
				node: &node{
					flag:   selector,
					value:  t.val,
					selKey: UndefinedSelKey,
				},
			}, nil
		}
		return nil, p.unknownTokenError(p.peek())
	}

	err := p.eat(lParen)
	if err != nil {
		return nil, err
	}

	car := p.next()
	if car.typ != ident {
		return nil, p.tokenTypeError(ident, car)
	}

	var children []*astNode
	for p.peek().typ != rParen {
		child, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}

	p.walk()

	var n *astNode
	if p.isKeyword(car) {
		n, err = p.buildKeywordNode(car, children)
	} else {
		n, err = p.buildNode(car, children)
	}

	return n, err
}

func (p *parser) isKeyword(car token) bool {
	keywords := []string{"if", "let", "map", "filter", "any", "all", "collect", "reduce"}
	for _, keyword := range keywords {
		if car.val == keyword {
			return true
		}
	}
	return false
}

func (p *parser) buildKeywordNode(car token, children []*astNode) (*astNode, error) {
	if car.val != "if" {
		return nil, p.errWithToken(fmt.Errorf("[%s] is not currently supported", car.val), car)
	}

	n := &astNode{
		node: &node{value: car.val},
	}

	if len(children) != 3 {
		return nil, p.paramsCountErr(3, len(children), car)
	}

	// append an end node
	children = append(children, &astNode{
		node: &node{
			flag:  end,
			value: "end",
		},
	})

	n.node.flag = cond
	n.children = children

	return n, nil
}

func (p *parser) buildNode(car token, children []*astNode) (*astNode, error) {
	treeNode := &astNode{
		node:     &node{value: car.val},
		children: children,
	}

	// parse op node
	op, exist := builtinOperators[car.val]
	if !exist {
		op, exist = p.conf.OperatorMap[car.val]
	}
	if !exist {
		return nil, p.unknownTokenError(car)
	}
	flag := operator
	treeNode.node.operator = op
	treeNode.node.flag = flag
	return treeNode, nil
}

func (p *parser) parseConfig() error {
	const prefix = ";;;;" // prefix of compile config
	const separator = "," // separator of compile config

	confCopy := CopyCompileConfig(p.conf)

	for i := 0; i < len(p.tokens); i++ { // parse config
		t := p.tokens[i]
		if t.typ != comment {
			break
		}
		cmt := strings.TrimSpace(t.val)
		if !strings.HasPrefix(cmt, prefix) {
			continue
		}
		// trim compile config prefix and spaces
		cmt = strings.TrimPrefix(cmt, prefix)
		for _, s := range strings.Split(cmt, separator) {
			pair := strings.Split(s, ":")
			if len(pair) != 2 {
				return p.errWithToken(fmt.Errorf("invalid compile format %s", s), t)
			}

			for i := range pair {
				pair[i] = strings.TrimSpace(pair[i])
			}

			enabled, err := strconv.ParseBool(pair[1])
			if err != nil {
				return p.errWithToken(fmt.Errorf("invalid config value %s, err %w", s, err), t)
			}
			switch option := Option(pair[0]); option {
			case AllOptimizations: // switch all optimizations
				for _, opt := range []Option{Reordering, FastEvaluation, ConstantFolding} {
					confCopy.CompileOptions[opt] = enabled
				}
			case Reordering, FastEvaluation, ConstantFolding:
				confCopy.CompileOptions[option] = enabled
			default:
				return p.errWithToken(fmt.Errorf("unsupported compile config %s", s), t)
			}
		}
	}

	p.conf = confCopy
	return nil
}
