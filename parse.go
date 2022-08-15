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
	comma   tokenType = "comma"
)

type keyword string

const (
	keywordIf      keyword = "if"
	keywordLet     keyword = "let"
	keywordAny     keyword = "any"
	keywordAll     keyword = "all"
	keywordMap     keyword = "map"
	keywordFilter  keyword = "filter"
	keywordReduce  keyword = "reduce"
	keywordCollect keyword = "collect"
)

var keywords = [...]keyword{keywordIf, keywordLet, keywordAny,
	keywordAll, keywordMap, keywordFilter, keywordReduce, keywordCollect}

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
	node      *node
	children  []*astNode
	cost      int
	idx       int
	parentIdx int
}

type parser struct {
	source    string
	conf      *CompileConfig
	tokens    []token
	idx       int
	direction int // walk direction
}

func newParser(cc *CompileConfig, source string) *parser {
	return &parser{
		source:    source,
		conf:      cc,
		direction: 1,
	}
}

func (p *parser) lex() error {
	var (
		nextToken = func(A []rune, i int) (string, int) {
			j := i
			for ; j < len(A); j++ {
				if r := A[j]; unicode.IsSpace(r) || r == '(' || r == ')' || r == ',' || r == ';' {
					break
				}
			}
			return string(A[i:j]), j
		}

		lexComma = func(A []rune, i int) (token, int) {
			if A[i] == ',' {
				return token{
					typ: comma,
					val: string(A[i]),
				}, i + 1
			}
			return token{}, i
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
			lexComma,
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

func (p *parser) parseAstTree() (root *astNode, err error) {
	n := 0
	for _, t := range p.tokens {
		if t.typ != comment {
			p.tokens[n] = t
			n++
		}
	}
	p.tokens = p.tokens[:n]

	if err = p.check(); err != nil {
		return nil, err
	}

	if p.isInfixNotation() {
		root, err = p.parseInfixExpression()
	} else {
		root, err = p.parseExpression()
	}

	if err != nil {
		return nil, err
	}

	if p.hasNext() {
		return nil, p.invalidExprErr(p.idx)
	}
	return root, nil
}

func (p *parser) check() error {
	prefixNotation := !p.isInfixNotation()

	last := len(p.tokens) - 1
	if prefixNotation &&
		(p.tokens[0].typ != lParen || p.tokens[last].typ != rParen) {
		return p.parenUnmatchedErr(0)
	}

	var parenCnt int // check parentheses
	for i, t := range p.tokens {
		switch t.typ {
		case lParen:
			parenCnt++
		case rParen:
			parenCnt--
		case comma:
			if prefixNotation { // commas can be used in infix expressions only
				return p.unknownTokenError(t)
			}
		}
		if parenCnt < 0 {
			return p.parenUnmatchedErr(t.pos)
		}

		if prefixNotation && parenCnt == 0 && i != last {
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

func (p *parser) allowUnknownSelectors() bool {
	return p.conf.CompileOptions[AllowUnknownSelectors]
}

func (p *parser) isInfixNotation() bool {
	return p.conf.CompileOptions[InfixNotation]
}

func (p *parser) peek() token {
	return p.tokens[p.idx]
}

func (p *parser) hasNext() bool {
	if p.direction > 0 {
		return p.idx < len(p.tokens)
	} else {
		return p.idx >= 0
	}
}

func (p *parser) next() token {
	t := p.tokens[p.idx]
	p.walk()
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
	p.idx += p.direction
}

func (p *parser) traverseBackward() {
	p.direction = -1
	p.idx = len(p.tokens) - 1
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

	// todo: return error when list is empty?

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
	key, ok := p.conf.SelectorMap[t.val]
	if !ok {
		return nil, nil
	}

	p.walk()
	return &astNode{
		node: &node{
			flag:   selector,
			value:  t.val,
			selKey: key,
		},
	}, nil
}

func (p *parser) mustParseUnknownSelector() (*astNode, error) {
	if !p.allowUnknownSelectors() {
		return nil, p.unknownTokenError(p.peek())
	}

	t := p.peek()
	if t.typ != ident {
		return nil, p.tokenTypeError(ident, t)
	}
	p.walk()
	return &astNode{
		node: &node{
			flag:   selector,
			value:  t.val,
			selKey: UndefinedSelKey,
		},
	}, nil
}

func (p *parser) parseSingleNode() (ast *astNode, err error) {
	fns := []func() (*astNode, error){
		p.parseInt, p.parseStr, p.parseConst, p.parseSelector, p.parseList}
	for _, fn := range fns {
		ast, err = fn()
		if ast != nil || err != nil {
			return ast, err
		}
	}
	return ast, err
}

func (p *parser) parseExpression() (ast *astNode, err error) {
	ast, err = p.parseSingleNode()
	if ast != nil || err != nil {
		return ast, err
	}

	if t := p.peek(); t.typ == ident {
		return p.mustParseUnknownSelector()
	}

	err = p.eat(lParen)
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

	err = p.eat(rParen)
	if err != nil {
		return nil, err
	}

	if p.isKeyword(car) {
		ast, err = p.buildKeywordNode(car, children)
	} else {
		ast, err = p.buildOperatorNode(car, children)
	}

	return ast, err
}

func (p *parser) parseInfixExpression() (ast *astNode, err error) {
	p.traverseBackward()
	var (
		operatorStack []token
		outputStack   []*astNode
	)

	var (
		push = func(n *astNode) {
			outputStack = append(outputStack, n)
		}
		pop = func() (res *astNode) {
			l := len(outputStack)
			res, outputStack = outputStack[l-1], outputStack[:l-1]
			return res
		}

		comparePrecedence = func(top token, car token) int {
			precedence := map[string]int{
				"add": 1000,
				"mod": 1000,
				"*":   100,
				"/":   100,
				"+":   50,
				"-":   50,
				"=":   40,
				"&":   30,
				"|":   20,
				")":   1,
				"(":   1,
				",":   1,
			}

			return precedence[top.val] - precedence[car.val]
		}

		getChildCount = func(car token) int {
			return 2
		}

		buildTopOperators = func(car token) error {
			for l := len(operatorStack); l != 0; l = len(operatorStack) {
				top := operatorStack[l-1]
				if car.typ == lParen && top.typ == rParen {
					operatorStack = operatorStack[:l-1]
					break
				}

				if comparePrecedence(top, car) <= 0 {
					break
				}

				operatorStack = operatorStack[:l-1]
				var children []*astNode
				for i := 0; i < getChildCount(top); i++ {
					children = append(children, pop())
				}
				opNode, err := p.buildOperatorNode(top, children)
				if err != nil {
					return err
				}

				push(opNode)
			}
			return nil
		}
	)

	for p.hasNext() {
		fns := []func() (*astNode, error){p.parseInt, p.parseStr, p.parseConst, p.parseSelector}
		for _, fn := range fns {
			ast, err = fn()
			if ast != nil || err != nil {
				break
			}
		}
		if err != nil {
			return nil, err
		}
		if ast != nil {
			push(ast)
			continue
		}

		car := p.next()
		switch car.typ {
		case ident:
			err = buildTopOperators(car)
			if err != nil {
				return nil, err
			}
			operatorStack = append(operatorStack, car)
		case rParen:
			operatorStack = append(operatorStack, car)
		case lParen:
			err = buildTopOperators(car)
			if err != nil {
				return nil, err
			}
		case comma:
			err = buildTopOperators(car)
			if err != nil {
				return nil, err
			}
		default:
			return nil, p.tokenTypeError(ident, car)
		}
	}

	err = buildTopOperators(token{})
	if err != nil {
		return nil, err
	}

	return pop(), nil
}

func (p *parser) isKeyword(car token) bool {
	for _, kw := range keywords {
		if car.val == string(kw) {
			return true
		}
	}
	return false
}

func (p *parser) buildKeywordNode(car token, children []*astNode) (*astNode, error) {
	if car.val != string(keywordIf) {
		return nil, p.errWithToken(fmt.Errorf("[%s] is not currently supported", car.val), car)
	}

	if len(children) != 3 {
		return nil, p.paramsCountErr(3, len(children), car)
	}

	return &astNode{
		node: &node{
			flag:  cond,
			value: keywordIf,
			// trigger short circuit when cond node returns false
			operator: func(_ *Ctx, params []Value) (Value, error) {
				if b, ok := params[0].(bool); ok {
					return !b, nil
				}

				return nil, fmt.Errorf("condition node returns a non bool result: [%v]", params[0])
			},
		},

		// append an end if node
		children: append(children, &astNode{
			node: &node{
				flag:  cond,
				value: "fi",
				operator: func(_ *Ctx, _ []Value) (Value, error) {
					return true, nil
				},
			},
		}),
	}, nil
}

func (p *parser) buildOperatorNode(car token, children []*astNode) (*astNode, error) {
	// parse op node
	op, exist := builtinOperators[car.val]
	if !exist {
		op, exist = p.conf.OperatorMap[car.val]
	}
	if !exist {
		return nil, p.unknownTokenError(car)
	}
	return &astNode{
		children: children,
		node: &node{
			flag:     operator,
			value:    car.val,
			operator: op,
		},
	}, nil
}

func (p *parser) parseConfig() error {
	const prefix = ";;;;" // prefix of compile config
	const separator = "," // separator of compile config

	confCopy := CopyCompileConfig(p.conf)

	// parse config
	for _, t := range p.tokens {
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
			case Optimize: // switch all optimizations
				for _, opt := range AllOptimizations {
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
