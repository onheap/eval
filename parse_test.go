package eval

import (
	"reflect"
	"testing"
	"time"
)

type verifyNode struct {
	tpy       uint8
	data      Value
	cost      int
	selectKey SelectorKey
	children  []verifyNode
}

var assertAstTreeIdentical = func(t *testing.T, got *astNode, want verifyNode, msg ...any) {
	var q1 []*astNode
	var q2 []verifyNode

	q1 = append(q1, got)
	q2 = append(q2, want)

	for len(q1) != 0 {
		// pull from head
		got, q1 = q1[0], q1[1:]
		want, q2 = q2[0], q2[1:]

		// check node value
		if !reflect.DeepEqual(got.node.value, want.data) {
			t.Fatalf("node value mismatched, got: %+v, want: %+v, msg: %+v", got.node.value, want.data, msg)
		}

		// check node type
		nodeType := got.node.getNodeType()
		if want.tpy != nodeType {
			t.Fatalf("node type mismatched, got: %+v, want: %+v, msg: %+v", nodeType, want.tpy, msg)
		}

		if want.cost != 0 && want.cost != got.cost {
			t.Fatalf("node cost mismatched, got: %+v, want: %+v, msg: %+v", got.cost, want.cost, msg)
		}

		if want.selectKey != SelectorKey(0) && want.selectKey != got.node.selKey {
			t.Fatalf("node selKey mismatched, got: %+v, want: %+v, msg: %+v", got.node.selKey, want.selectKey, msg)
		}

		// check children
		if len(want.children) != len(got.children) {
			t.Fatalf("node children mismatched, got: %+v, want: %+v, msg: %+v", len(got.children), len(want.children), msg)
		}

		for i := 0; i < len(want.children); i++ {
			q2 = append(q2, want.children[i])
			q1 = append(q1, got.children[i])
		}
	}
}

func TestLex(t *testing.T) {
	testCases := []struct {
		expr   string
		tokens []token
		errMsg string
	}{
		{
			expr: `(+ 1 1)`,
			tokens: []token{
				{typ: lParen, val: "("},
				{typ: ident, val: "+"},
				{typ: integer, val: "1"},
				{typ: integer, val: "1"},
				{typ: rParen, val: ")"},
			},
		},
		{
			expr: `
;; expr start
(+ ;; add
1 1 ;; numbers
) ;; expr end
;; new line comment
`,
			tokens: []token{
				{typ: comment, val: ";; expr start"},
				{typ: lParen, val: "("},
				{typ: ident, val: "+"},
				{typ: comment, val: ";; add"},
				{typ: integer, val: "1"},
				{typ: integer, val: "1"},
				{typ: comment, val: ";; numbers"},
				{typ: rParen, val: ")"},
				{typ: comment, val: ";; expr end"},
				{typ: comment, val: ";; new line comment"},
			},
		},
		{
			expr: `
(<
 (+ 1
   (- 2 v3) (/ -6 3) 4)
 (* 5 6 7)
)`,
			tokens: []token{
				{typ: lParen, val: "("},
				{typ: ident, val: "<"},
				{typ: lParen, val: "("},
				{typ: ident, val: "+"},
				{typ: integer, val: "1"},
				{typ: lParen, val: "("},
				{typ: ident, val: "-"},
				{typ: integer, val: "2"},
				{typ: ident, val: "v3"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "/"},
				{typ: integer, val: "-6"},
				{typ: integer, val: "3"},
				{typ: rParen, val: ")"},
				{typ: integer, val: "4"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "*"},
				{typ: integer, val: "5"},
				{typ: integer, val: "6"},
				{typ: integer, val: "7"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"}},
		},
		{
			expr: `
(if
    (<= age 3)
    "👋~ 👶"  ;; we can use emoji in string and comments 🤪
    (if
        (or
            (in language ("zh" "zh-CN"))
            (= country "CN"))
        "你好"
        "hello"))`,
			tokens: []token{
				{typ: lParen, val: "("},
				{typ: ident, val: "if"},
				{typ: lParen, val: "("},
				{typ: ident, val: "<="},
				{typ: ident, val: "age"},
				{typ: integer, val: "3"},
				{typ: rParen, val: ")"},
				{typ: str, val: "👋~ 👶"},
				{typ: comment, val: ";; we can use emoji in string and comments 🤪"},
				{typ: lParen, val: "("},
				{typ: ident, val: "if"},
				{typ: lParen, val: "("},
				{typ: ident, val: "or"},
				{typ: lParen, val: "("},
				{typ: ident, val: "in"},
				{typ: ident, val: "language"},
				{typ: lParen, val: "("},
				{typ: str, val: "zh"},
				{typ: str, val: "zh-CN"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: ident, val: "country"},
				{typ: str, val: "CN"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: str, val: "你好"},
				{typ: str, val: "hello"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
			},
		},

		{
			expr: `(=(now)123)`,
			tokens: []token{
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: lParen, val: "("},
				{typ: ident, val: "now"},
				{typ: rParen, val: ")"},
				{typ: integer, val: "123"},
				{typ: rParen, val: ")"},
			},
		},
		{
			expr: `(now)`,
			tokens: []token{
				{typ: lParen, val: "("},
				{typ: ident, val: "now"},
				{typ: rParen, val: ")"},
			},
		},

		{
			expr:   `(< age 18.0)`,
			errMsg: "can not parse token",
		},
		{
			expr:   `(+ 1 1.0)`,
			errMsg: "can not parse token",
		},
		{
			expr:   `(add* 1)`, // `add*` contains special character
			errMsg: "can not parse token",
		},
		{
			expr:   `(= abc 0cc)`, // `0cc` is invalid
			errMsg: "can not parse token",
		},

		{
			expr: `test abc abc_1`,
			tokens: []token{
				{typ: ident, val: "test"},
				{typ: ident, val: "abc"},
				{typ: ident, val: "abc_1"},
			},
		},

		{
			expr: `""`,
			tokens: []token{
				{typ: str, val: ""},
			},
		},

		{
			expr:   `"`,
			errMsg: "can not parse token",
		},

		// complicated expression with a messy format
		{
			expr: `

;;;; optimize:false
;;;; hhhh
(or  ;; test
(eq 
  (= 1 1)
   (= 1 2)
   (eq 
    (= 1 1)
     (= 1 2)
     (= 1 1)
     (= 1 1)))
	(and
     ;; hhhhh3
	(between age 18 -80)

    (eq (+ 1 1)        (- 3 1   ) 2)
       
	(eq gender "male")  ;; heheda
	(between;;hhhh4
(t_version app_version) (    t_version "1.2.3") (t_version "4.5")
)
	)
(= (now) 123)
(in "" ())
(=(now)123)
(now)
(overlap () (1 2 3))
(overlap ("a" ) (""))

    ;; hhhh5
	(overlap groups (1234 7680 )) ;; todo
    (;; hehehe
    overlap
    ;; heheh6
    ;; hhh 7
    tags ( "bbb" "aaa"))
   ;; hhhh8
) ;; hhh9

;; hhh0


`,
			tokens: []token{
				{typ: comment, val: ";;;; optimize:false"},
				{typ: comment, val: ";;;; hhhh"},
				{typ: lParen, val: "("},
				{typ: ident, val: "or"},
				{typ: comment, val: ";; test"},
				{typ: lParen, val: "("},
				{typ: ident, val: "eq"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: integer, val: "1"},
				{typ: integer, val: "1"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: integer, val: "1"},
				{typ: integer, val: "2"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "eq"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: integer, val: "1"},
				{typ: integer, val: "1"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: integer, val: "1"},
				{typ: integer, val: "2"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: integer, val: "1"},
				{typ: integer, val: "1"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: integer, val: "1"},
				{typ: integer, val: "1"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "and"},
				{typ: comment, val: ";; hhhhh3"},
				{typ: lParen, val: "("},
				{typ: ident, val: "between"},
				{typ: ident, val: "age"},
				{typ: integer, val: "18"},
				{typ: integer, val: "-80"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "eq"},
				{typ: lParen, val: "("},
				{typ: ident, val: "+"},
				{typ: integer, val: "1"},
				{typ: integer, val: "1"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "-"},
				{typ: integer, val: "3"},
				{typ: integer, val: "1"},
				{typ: rParen, val: ")"},
				{typ: integer, val: "2"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "eq"},
				{typ: ident, val: "gender"},
				{typ: str, val: "male"},
				{typ: rParen, val: ")"},
				{typ: comment, val: ";; heheda"},
				{typ: lParen, val: "("},
				{typ: ident, val: "between"},
				{typ: comment, val: ";;hhhh4"},
				{typ: lParen, val: "("},
				{typ: ident, val: "t_version"},
				{typ: ident, val: "app_version"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "t_version"},
				{typ: str, val: "1.2.3"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "t_version"},
				{typ: str, val: "4.5"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: lParen, val: "("},
				{typ: ident, val: "now"},
				{typ: rParen, val: ")"},
				{typ: integer, val: "123"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "in"},
				{typ: str, val: ""},
				{typ: lParen, val: "("},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "="},
				{typ: lParen, val: "("},
				{typ: ident, val: "now"},
				{typ: rParen, val: ")"},
				{typ: integer, val: "123"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "now"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "overlap"},
				{typ: lParen, val: "("},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: integer, val: "1"},
				{typ: integer, val: "2"},
				{typ: integer, val: "3"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: ident, val: "overlap"},
				{typ: lParen, val: "("},
				{typ: str, val: "a"},
				{typ: rParen, val: ")"},
				{typ: lParen, val: "("},
				{typ: str, val: ""},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: comment, val: ";; hhhh5"},
				{typ: lParen, val: "("},
				{typ: ident, val: "overlap"},
				{typ: ident, val: "groups"},
				{typ: lParen, val: "("},
				{typ: integer, val: "1234"},
				{typ: integer, val: "7680"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: comment, val: ";; todo"},
				{typ: lParen, val: "("},
				{typ: comment, val: ";; hehehe"},
				{typ: ident, val: "overlap"},
				{typ: comment, val: ";; heheh6"},
				{typ: comment, val: ";; hhh 7"},
				{typ: ident, val: "tags"},
				{typ: lParen, val: "("},
				{typ: str, val: "bbb"},
				{typ: str, val: "aaa"},
				{typ: rParen, val: ")"},
				{typ: rParen, val: ")"},
				{typ: comment, val: ";; hhhh8"},
				{typ: rParen, val: ")"},
				{typ: comment, val: ";; hhh9"},
				{typ: comment, val: ";; hhh0"},
			},
		},
	}

	for _, c := range testCases {

		t.Run(c.expr, func(t *testing.T) {
			p := &parser{source: c.expr}
			err := p.lex()
			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg, c)
				return
			}

			assertNil(t, err, c)

			assertEquals(t, len(p.tokens), len(c.tokens))

			for i := range p.tokens {
				t1 := p.tokens[i]
				t2 := c.tokens[i]
				assertEquals(t, t1.typ, t2.typ)
				assertEquals(t, t1.val, t2.val)
			}
		})
	}
}

func TestParseConfig(t *testing.T) {
	testCases := []struct {
		expr   string
		origin map[Option]bool
		want   map[Option]bool
		errMsg string
	}{
		{
			expr:   `(+ 1 1)`,
			origin: map[Option]bool{},
			want:   map[Option]bool{},
		},
		{
			expr:   `;;;; constant_folding: true, reordering: true`,
			origin: map[Option]bool{},
			want: map[Option]bool{
				Reordering:      true,
				ConstantFolding: true,
			},
		},
		{
			expr: `;;;; optimize:false`,
			origin: map[Option]bool{
				Reordering:      true,
				FastEvaluation:  true,
				ConstantFolding: true,
			},
			want: map[Option]bool{
				Reordering:      false,
				FastEvaluation:  false,
				ConstantFolding: false,
			},
		},
		{
			expr: `
;; disable all optimization first
;;;; optimize: false
;; then only enable reordering and constant_folding
;;;; reordering: true, constant_folding: true
`,
			origin: map[Option]bool{
				Reordering:      true,
				FastEvaluation:  true,
				ConstantFolding: true,
			},
			want: map[Option]bool{
				Reordering:      true,
				FastEvaluation:  false,
				ConstantFolding: true,
			},
		},

		{
			expr: `;;;;unsupported_option:false`,
			origin: map[Option]bool{
				Reordering:      true,
				FastEvaluation:  true,
				ConstantFolding: true,
			},
			errMsg: "unsupported compile config",
		},

		{
			expr: `;;;;reordering:disabled`,
			origin: map[Option]bool{
				Reordering:      true,
				FastEvaluation:  true,
				ConstantFolding: true,
			},
			errMsg: "invalid config value",
		},

		{
			expr: `;;;;reordering:false:false`,
			origin: map[Option]bool{
				Reordering:      true,
				FastEvaluation:  true,
				ConstantFolding: true,
			},
			errMsg: "invalid compile format",
		},
	}

	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			p := newParser(&CompileConfig{CompileOptions: c.origin}, c.expr)
			err := p.lex()
			assertNil(t, err, c)
			err = p.parseConfig()
			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg, c)
				return
			}
			assertNil(t, err, c)
			updatedOption := p.conf.CompileOptions

			for option, want := range c.want {
				got, exist := updatedOption[option]
				assertEquals(t, exist, true)
				assertEquals(t, got, want)
			}
		})
	}
}

func TestParseAstTree(t *testing.T) {
	testCases := []struct {
		cc     *CompileConfig
		expr   string
		ast    verifyNode
		errMsg string
	}{
		{
			expr: `(+ 1 1)`,
			ast: verifyNode{
				tpy:  operator,
				data: "+",
				children: []verifyNode{
					{tpy: constant, data: int64(1)},
					{tpy: constant, data: int64(1)},
				},
			},
		},
		{
			expr: `
(<
 (+ 1
   (- 2 v3) (/ -6 3) 4)
 (* 5 6 7)
)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v3": SelectorKey(3),
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "<",
				children: []verifyNode{
					{
						tpy:  operator,
						data: "+",
						children: []verifyNode{
							{tpy: constant, data: int64(1)},
							{
								tpy:  operator,
								data: "-",
								children: []verifyNode{
									{tpy: constant, data: int64(2)},
									{tpy: selector, data: "v3", selectKey: SelectorKey(3)},
								},
							},
							{
								tpy:  operator,
								data: "/",
								children: []verifyNode{
									{tpy: constant, data: int64(-6)},
									{tpy: constant, data: int64(3)},
								},
							},
							{tpy: constant, data: int64(4)},
						},
					},
					{
						tpy:  operator,
						data: "*",
						children: []verifyNode{
							{tpy: constant, data: int64(5)},
							{tpy: constant, data: int64(6)},
							{tpy: constant, data: int64(7)},
						},
					},
				},
			},
		},

		{
			// with special character (emoji, chinese character)
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"age":      SelectorKey(1),
					"language": SelectorKey(2),
					"country":  SelectorKey(3),
				},
			},
			expr: `
(if
    (<= age 3)
    "👋~ 👶"  ;; we can use emoji in string and comments 🤪
    (if
        (or
            (in language ("zh" "zh-CN"))
            (= country "CN"))
        "你好"
        "hello"))`,
			ast: verifyNode{
				tpy:  cond,
				data: keywordIf,
				children: []verifyNode{
					{
						tpy:  operator,
						data: "<=",
						children: []verifyNode{
							{tpy: selector, data: "age", selectKey: SelectorKey(1)},
							{tpy: constant, data: int64(3)},
						},
					},
					{tpy: constant, data: "👋~ 👶"},
					{
						tpy:  cond,
						data: keywordIf,
						children: []verifyNode{
							{
								tpy:  operator,
								data: "or",
								children: []verifyNode{
									{
										tpy:  operator,
										data: "in",
										children: []verifyNode{
											{tpy: selector, data: "language", selectKey: SelectorKey(2)},
											{tpy: constant, data: []string{"zh", "zh-CN"}},
										},
									},
									{
										tpy:  operator,
										data: "=",
										children: []verifyNode{
											{tpy: selector, data: "country", selectKey: SelectorKey(3)},
											{tpy: constant, data: "CN"},
										},
									},
								},
							},
							{tpy: constant, data: "你好"},
							{tpy: constant, data: "hello"},
							{tpy: cond, data: "fi"},
						},
					},
					{tpy: cond, data: "fi"},
				},
			},
		},
		// with custom operator and selector and constant
		{
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"birthday": SelectorKey(3),
				},
				ConstantMap: map[string]Value{
					"birthdate_format": "Jan 02, 2006",
				},
				OperatorMap: map[string]Operator{
					"is_child": func(_ *Ctx, params []Value) (Value, error) {
						const (
							op       = "is_child"
							timeYear = time.Hour * 24 * 365
						)
						if len(params) != 1 {
							return nil, ParamsCountError(op, 1, len(params))
						}

						birthday, ok := params[0].(string)
						if !ok {
							return nil, ParamTypeError(op, typeStr, params[0])
						}

						layout, ok := params[1].(string)
						if !ok {
							return nil, ParamTypeError(op, typeStr, params[1])
						}

						birthTime, err := time.Parse(layout, birthday)
						if err != nil {
							return nil, OpExecError(op, err)
						}

						age := int64(time.Now().Sub(birthTime) / timeYear)
						return age < 18, nil
					},
				},
			},
			expr: `(is_child birthday birthdate_format)`,
			ast: verifyNode{
				tpy:  operator,
				data: "is_child",
				children: []verifyNode{
					{tpy: selector, data: "birthday", selectKey: SelectorKey(3)},
					{tpy: constant, data: "Jan 02, 2006"}, // constant nodes will be replaced directly with the value
				},
			},
		},
		{
			expr: `(now)`,
			cc: &CompileConfig{
				OperatorMap: map[string]Operator{
					"now": func(_ *Ctx, _ []Value) (Value, error) {
						return time.Now().Unix(), nil
					},
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "now",
			},
		},
		{
			expr: `()`,
			ast: verifyNode{
				tpy:  constant,
				data: []string{},
			},
		},
		{
			expr: `(1)`,
			ast: verifyNode{
				tpy:  constant,
				data: []int64{1},
			},
		},
		{
			expr: `(overlap () (1 2 4))`,
			ast: verifyNode{
				tpy:  operator,
				data: "overlap",
				children: []verifyNode{
					{tpy: constant, data: []string{}},
					{tpy: constant, data: []int64{1, 2, 4}},
				},
			},
		},
		{
			expr: `(in "" ())`,
			ast: verifyNode{
				tpy:  operator,
				data: "in",
				children: []verifyNode{
					{tpy: constant, data: ""},
					{tpy: constant, data: []string{}},
				},
			},
		},
		{
			cc:     NewCompileConfig(),
			expr:   `(< age 18)`,
			errMsg: "unknown token error",
		},
		// return an error when expr use unregister selector
		{
			cc:   NewCompileConfig(EnableStringSelectors),
			expr: `(< age 18)`,
			ast: verifyNode{
				tpy:  operator,
				data: "<",
				children: []verifyNode{
					{tpy: selector, data: "age"},
					{tpy: constant, data: int64(18)},
				},
			},
		},
		// return an error when expr use unregister operator
		{
			expr:   `(is_child 18)`,
			errMsg: "unknown token error",
		},
		// mismatched element types in the list
		{
			expr:   `(17 age 18)`,
			errMsg: "token type unexpected error",
		},

		// mismatched element types in the list
		{
			expr:   `(17 18 "19")`,
			errMsg: "token type unexpected error",
		},

		{
			expr:   `(())`,
			errMsg: "token type unexpected error",
		},

		{
			expr:   `(if (= 1 1))`,
			errMsg: "if parameters count error",
		},

		{
			expr:   `(< 12 18`,
			errMsg: "parentheses unmatched error",
		},

		{
			expr:   `(+ 1 1 ))`,
			errMsg: "parentheses unmatched error",
		},

		{
			expr:   `(+ 1 1) (+ 1 1)`,
			errMsg: "parentheses unmatched error",
		},
	}

	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			ast, _, err := newParser(c.cc, c.expr).parse()
			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg, c)
				return
			}

			assertNil(t, err)
			assertAstTreeIdentical(t, ast, c.ast, c)
		})
	}
}
