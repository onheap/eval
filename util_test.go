package eval

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestIndentByParentheses(t *testing.T) {
	s := `

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
	(between age 18 80)

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
	(overlap groups (1234 7680 )) ;; todo remove extra space
    (;; hehehe
    overlap
    ;; heheh6
    ;; hhh 7
    tags ( "bbb" "aaa"))  ;; todo remove extra space
   ;; hhhh8
) ;; hhh9

;; hhh0


`

	res := IndentByParentheses(s)
	fmt.Println(res)
}

func TestPrintCode(t *testing.T) {
	s := `

;;;; optimize:false
;; hhhh
(or  ;; test

	(and
     ;; hhhhh3
	(between age 18 80)

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
	(overlap groups (1234 7680 ))
    (;; hehehe
    overlap
    ;; heheh6
    ;; hhh 7
    tags ("bbb" "aaa"))
   ;; hhhh8
) ;; hhh9

;; hhh0


`

	cc := &CompileConfig{
		SelectorMap: map[string]SelectorKey{
			"age":         SelectorKey(1),
			"gender":      SelectorKey(2),
			"tags":        SelectorKey(3),
			"groups":      SelectorKey(4),
			"app_version": SelectorKey(5),
		},

		OperatorMap: map[string]Operator{
			"now": func(_ *Ctx, _ []Value) (Value, error) {
				return time.Now().Unix(), nil
			},
		},
	}

	expr, err := Compile(cc, s)
	assertNil(t, err)

	res := PrintCode(expr)

	assertEquals(t, res, `(or
  (and
    (between age 18 80)
    (eq
      (+ 1 1)
      (- 3 1) 2)
    (eq gender "male")
    (between
      (t_version app_version)
      (t_version "1.2.3")
      (t_version "4.5")))
  (=
    (now) 123)
  (in "" ())
  (=
    (now) 123)
  (now)
  (overlap () (1 2 3))
  (overlap ("a") (""))
  (overlap groups (1234 7680))
  (overlap tags ("bbb" "aaa")))`)
}

func TestGenerateRandomExpr_Bool(t *testing.T) {
	const size = 50
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	cc := &CompileConfig{
		SelectorMap: map[string]SelectorKey{
			//"select_true":    SelectorKey(1),
			//"select_false":   SelectorKey(2),
			//"select_true_1":  SelectorKey(3),
			//"select_false_1": SelectorKey(4),
			"T": SelectorKey(1),
			"F": SelectorKey(2),
		},
		OptimizeOptions: map[OptimizeOption]bool{
			ConstantFolding: false,
		},
	}
	valMap := map[string]Value{
		//"select_true":    true,
		//"select_false":   false,
		//"select_true_1":  true,
		//"select_false_1": true,
		"T": true,
		"F": false,
	}
	ctx := NewCtxWithMap(cc, valMap)

	for i := 1; i < size; i++ {
		expr := GenerateRandomExpr(i, r, GenType(Bool), EnableSelector, EnableCondition, GenSelectors(valMap))

		got, err := Eval(cc, expr.Expr, ctx)
		if err != nil {
			fmt.Println(GenerateTestCase(expr, valMap))
			t.Fatalf("assertNil failed, got: %+v\n", err)
		}

		if got != expr.Res {
			fmt.Println(GenerateTestCase(expr, valMap))
			t.Fatalf("assertEquals failed, got: %+v, want: %+v\n", got, expr.Res)
		}
	}
}

func TestGenerateRandomExpr_Number(t *testing.T) {
	const (
		selectorSize = 10
		size         = 50
	)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	keyMap := make(map[string]SelectorKey, selectorSize)
	valMap := make(map[string]Value, selectorSize)
	for i := 0; i < selectorSize; i++ {
		v := r.Intn(200) - 100
		var k string
		if v < 0 {
			k = "select_neg_" + strconv.Itoa(-v)
		} else {
			k = "select_" + strconv.Itoa(v)
		}
		keyMap[k] = SelectorKey(i)
		valMap[k] = int64(v)
	}

	cc := &CompileConfig{
		SelectorMap: keyMap,
		OptimizeOptions: map[OptimizeOption]bool{
			ConstantFolding: false,
		},
	}
	ctx := NewCtxWithMap(cc, valMap)

	for i := 0; i < size; i++ {
		expr := GenerateRandomExpr(size, r, GenType(Number), EnableCondition, EnableSelector, GenSelectors(valMap))
		//fmt.Println(IndentByParentheses(expr.Expr))
		//fmt.Println(expr.Res)

		got, err := Eval(cc, expr.Expr, ctx)
		if err != nil {
			fmt.Println(GenerateTestCase(expr, valMap))
			t.Fatalf("assertNil failed, got: %+v\n", err)
		}

		if got != expr.Res {
			fmt.Println(GenerateTestCase(expr, valMap))
			t.Fatalf("assertEquals failed, got: %+v, want: %+v\n", got, expr.Res)
		}
	}
}

func TestGenerateTestCase(t *testing.T) {
	testCases := []struct {
		expr GenExprResult
		want string
		vals map[string]Value
	}{
		{
			expr: GenExprResult{
				Res:  false,
				Expr: "(not (eq select_false select_false (!= 0 0)))",
			},
			want: `
        {
            want:          false,
            optimizeLevel: disable,
            s: ` + "`" + `
(not
  (eq select_false select_false
    (!= 0 0)))` + "`" + `,
            valMap: map[string]Value{
                "select_false": false,
            },
        },`,
			vals: map[string]Value{
				"select_false": false,
			},
		},
		{
			expr: GenExprResult{
				Expr: "(if less -1 1)",
				Res:  int64(-1),
			},
			want: `
        {
            want:          int64(-1),
            optimizeLevel: disable,
            s:             "(if less -1 1)",
            valMap: map[string]Value{
                "less": true,
            },
        },`,
			vals: map[string]Value{
				"less": true,
			},
		},
		{
			expr: GenExprResult{
				Expr: "(+ 1 1)",
				Res:  int64(2),
			},
			want: `
        {
            want:          int64(2),
            optimizeLevel: disable,
            s:             "(+ 1 1)",
            valMap:        nil,
        },`,
		},
		{
			expr: GenExprResult{
				Expr: `(if (< age 18) "Child" "Adult")`,
				Res:  "Adult",
			},
			want: `
        {
            want:          "Adult",
            optimizeLevel: disable,
            s: ` + "`" + `
(if
  (< age 18) "Child" "Adult")` + "`" + `,
            valMap: map[string]Value{
                "age": int64(18),
            },
        },`,
			vals: map[string]Value{
				"age": int64(18),
			},
		},
	}

	for _, c := range testCases {
		got := GenerateTestCase(c.expr, c.vals)
		assertEquals(t, got, c.want)
	}
}
