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
	integer  tokenType = "integer"
	str      tokenType = "str"
	ident    tokenType = "ident"
	lParen   tokenType = "lParen"
	rParen   tokenType = "rParen"
	lBracket tokenType = "lBracket"
	rBracket tokenType = "rBracket"
	comment  tokenType = "comment"
	comma    tokenType = "comma"
)

func (t tokenType) String() string {
	return string(t)
}

type token struct {
	typ tokenType
	val string
	pos int
}

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

// ast
type astNode struct {
	node      *node
	children  []*astNode
	cost      int
	idx       int
	parentIdx int
}

type parser struct {
	source string
	conf   *CompileConfig
	tokens []token
	idx    int

	leafNodeParser []func() (*astNode, error)
}

func newParser(cc *CompileConfig, source string) *parser {
	return &parser{
		source: source,
		conf:   CopyCompileConfig(cc),
	}
}

func (p *parser) lex() error {
	A, i := []rune(p.source), 0

	var (
		lexComment = func() (string, error) {
			start := i
			for ; i < len(A); i++ {
				if A[i] == '\n' {
					break
				}
			}
			return string(A[start:i]), nil
		}

		lexString = func() (string, error) {
			start := i
			i += 1
			for ; i < len(A); i++ {
				if A[i] == '"' {
					i++
					return string(A[start:i]), nil
				}
			}
			return "", errors.New("unclosed quotes")
		}

		nextToken = func() (string, error) {
			start := i
			for ; i < len(A); i++ {
				r := A[i]
				if i == start && r == ';' {
					return lexComment()
				}
				if i == start && r == '"' {
					return lexString()
				}
				if unicode.IsSpace(r) {
					if i == start {
						start = i + 1
						continue
					} else {
						break
					}
				}
				if strings.ContainsRune("()[];,", r) {
					break
				}
			}

			if start == len(A) {
				return "", nil
			}

			if i == start {
				i += 1
			}

			return string(A[start:i]), nil
		}

		isValidInt = func(s string) bool {
			_, err := strconv.ParseInt(s, 10, 64)
			return err == nil
		}
		isValidIdent = func(s string) bool {
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
				// check if it's a builtin operator
				// only builtin operators can have special character
				_, exist := builtinOperators[s]
				return exist
			}
			return true
		}
	)

	for {
		t, err := nextToken()
		if err != nil {
			return p.errWithPos(err, i-len(t))
		}

		if t == "" {
			break
		}

		if p.isInfixNotation() && strings.HasPrefix(t, "!") {
			if isValidIdent(t) {
				p.tokens = append(p.tokens, token{typ: ident, val: t})
				continue
			}

			if next := t[1:]; isValidIdent(next) {
				p.tokens = append(p.tokens, token{typ: ident, val: "!"})
				p.tokens = append(p.tokens, token{typ: ident, val: next})
				continue
			}
		}

		tk := token{val: t}
		switch {
		case t == "(":
			tk.typ = lParen
		case t == ")":
			tk.typ = rParen
		case t == "[":
			tk.typ = lBracket
		case t == "]":
			tk.typ = rBracket
		case t == ",":
			tk.typ = comma
		case strings.HasPrefix(t, ";"):
			tk.typ = comment
		case strings.HasPrefix(t, `"`):
			tk.val = t[1 : len(t)-1] // remove quotes
			tk.typ = str
		case isValidInt(t):
			tk.typ = integer
		case isValidIdent(t):
			tk.typ = ident
		default:
			return p.errWithPos(errors.New("can not parse token"), i-len(t))
		}

		p.tokens = append(p.tokens, tk)
	}

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

	p.setLeafNodeParsers()

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

func (p *parser) setLeafNodeParsers() {
	fns := []func() (*astNode, error){
		p.parseInt, p.parseStr, p.parseConst, p.parseSelector, p.parseUnknownSelector}

	if p.isInfixNotation() {
		// For infix expressions only lists with brackets are supported
		fns = append(fns, p.parseList(lBracket, rBracket))
	} else {
		// For prefix expressions, lists with brackets or parentheses both are supported
		fns = append(fns, p.parseList(lBracket, rBracket), p.parseList(lParen, rParen))
	}

	p.leafNodeParser = fns
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
	return p.idx < len(p.tokens)
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
	p.idx++
}

func (p *parser) getOperator(opName string) (Operator, bool) {
	op, exist := builtinOperators[opName]
	if !exist {
		op, exist = p.conf.OperatorMap[opName]
	}
	return op, exist
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

	if i < 0 || i >= len(A) {
		i = 0
	}

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

func (p *parser) parseList(leftType, rightType tokenType) func() (*astNode, error) {
	return func() (*astNode, error) {
		i := p.idx
		T := p.tokens
		if T[i].typ != leftType {
			return nil, nil
		}
		typ := T[i+1].typ
		if typ != rightType && typ != integer && typ != str {
			return nil, nil
		}
		strs := make([]string, 0)
		for j := i + 1; j < len(T); j++ {
			if T[j].typ == rightType {
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

func (p *parser) parseUnknownSelector() (*astNode, error) {
	if !p.allowUnknownSelectors() {
		return nil, nil
	}

	t := p.peek()
	if t.typ != ident {
		return nil, nil
	}

	if p.isKeyword(t) {
		return nil, nil
	}

	_, exist := p.getOperator(t.val)
	if exist {
		return nil, nil
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

func (p *parser) buildLeafNode() (ast *astNode, err error) {
	for _, fn := range p.leafNodeParser {
		ast, err = fn()
		if ast != nil || err != nil {
			return ast, err
		}
	}
	return ast, err
}

func (p *parser) parseExpression() (ast *astNode, err error) {
	ast, err = p.buildLeafNode()
	if ast != nil || err != nil {
		return ast, err
	}

	if t := p.peek(); t.typ == ident {
		return nil, p.unknownTokenError(t)
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

	return p.buildParentNode(car, children)
}

func (p *parser) parseInfixExpression() (*astNode, error) {
	type op struct {
		t token // token
		l int   // output stack size
	}

	var (
		operatorStack []op
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
		comparePrecedence = func(car token, top token) int {
			p1 := p.getInfixOpInfo(car.val).precedence
			p2 := p.getInfixOpInfo(top.val).precedence
			if p1 == funcPrecedence {
				return funcPrecedence
			}
			return p1 - p2
		}

		buildTopOperators = func(car token) error {
			for l := len(operatorStack); l != 0; l = len(operatorStack) {
				top := operatorStack[l-1]
				if car.typ == rParen && top.t.typ == lParen {
					operatorStack = operatorStack[:l-1]
					break
				}

				if comparePrecedence(car, top.t) > 0 {
					break
				}

				operatorStack = operatorStack[:l-1]

				cnt := p.getInfixOpInfo(top.t.val).childCount
				if cnt == -1 {
					cnt = len(outputStack) - top.l
				}

				children := make([]*astNode, cnt)
				for i := cnt - 1; i >= 0; i-- {
					children[i] = pop()
				}

				ast, err := p.buildParentNode(top.t, children)
				if err != nil {
					return err
				}

				push(ast)
			}
			return nil
		}
	)

	for p.hasNext() {
		ast, err := p.buildLeafNode()
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
			operatorStack = append(operatorStack, op{t: car, l: len(outputStack)})
		case lParen:
			operatorStack = append(operatorStack, op{t: car, l: len(outputStack)})
		case rParen:
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

	err := buildTopOperators(token{})
	if err != nil {
		return nil, err
	}

	if len(outputStack) != 1 {
		return nil, p.invalidExprErr(0)
	}

	return pop(), nil
}

type infixOpInfo struct {
	precedence int
	childCount int
}

const funcPrecedence = 100

func (p *parser) getInfixOpInfo(op string) infixOpInfo {
	switch op {
	case "*", "/", "%":
		return infixOpInfo{precedence: 8, childCount: 2}
	case "+", "-":
		return infixOpInfo{precedence: 7, childCount: 2}
	case "!":
		return infixOpInfo{precedence: 6, childCount: 1}
	case "=", "==", "!=", "<", ">", "<=", ">=":
		return infixOpInfo{precedence: 5, childCount: 2}
	case "&", "&&":
		return infixOpInfo{precedence: 4, childCount: 2}
	case "|", "||":
		return infixOpInfo{precedence: 3, childCount: 2}
	case ",":
		return infixOpInfo{precedence: 2, childCount: 0}
	case "(", ")":
		return infixOpInfo{precedence: 1, childCount: 0}
	case "":
		return infixOpInfo{precedence: -1, childCount: 0}
	default:
		return infixOpInfo{precedence: funcPrecedence, childCount: -1}
	}
}

func (p *parser) isInfixOp(op string) bool {
	return p.getInfixOpInfo(op).precedence == funcPrecedence
}

func (p *parser) isKeyword(car token) bool {
	for _, kw := range keywords {
		if car.val == string(kw) {
			return true
		}
	}
	return false
}

func (p *parser) buildParentNode(car token, children []*astNode) (*astNode, error) {
	if p.isKeyword(car) {
		return p.buildKeywordNode(car, children)
	} else {
		return p.buildOperatorNode(car, children)
	}
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
	op, exist := p.getOperator(car.val)
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
					p.conf.CompileOptions[opt] = enabled
				}
			case Reordering, FastEvaluation, ConstantFolding:
				p.conf.CompileOptions[option] = enabled
			default:
				return p.errWithToken(fmt.Errorf("unsupported compile config %s", s), t)
			}
		}
	}

	return nil
}
