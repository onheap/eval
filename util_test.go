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
	assertEquals(t, res, `;;;; optimize:false
;;;; hhhh
(or ;; test
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
    (eq
      (+ 1 1)
      (- 3 1) 2)
    (eq gender "male") ;; heheda
    (between ;;hhhh4
      (t_version app_version)
      ( t_version "1.2.3")
      (t_version "4.5")))
  (=
    (now) 123)
  (in ""
    ())
  (=
    (now) 123)
  (now)
  (overlap
    ()
    (1 2 3))
  (overlap
    ("a")
    (""))
  ;; hhhh5
  (overlap groups
    (1234 7680)) ;; todo remove extra space
  ( ;; hehehe
    overlap
    ;; heheh6
    ;; hhh 7
    tags
    ( "bbb" "aaa")) ;; todo remove extra space
  ;; hhhh8
) ;; hhh9
;; hhh0`)
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

	cc := &Config{
		VariableKeyMap: map[string]VariableKey{
			"age":         VariableKey(1),
			"gender":      VariableKey(2),
			"tags":        VariableKey(3),
			"groups":      VariableKey(4),
			"app_version": VariableKey(5),
		},

		OperatorMap: map[string]Operator{
			"now": func(_ *Ctx, _ []Value) (Value, error) {
				return time.Now().Unix(), nil
			},
		},
	}

	expr, err := Compile(cc, s)
	assertNil(t, err)

	res := Dump(expr)

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
	cc := &Config{
		VariableKeyMap: map[string]VariableKey{
			"T": VariableKey(1),
			"F": VariableKey(2),
		},
		CompileOptions: map[CompileOption]bool{
			ConstantFolding: false,
		},
	}
	valMap := map[string]interface{}{
		"T": true,
		"F": false,
	}

	for i := 1; i < size; i++ {
		expr := GenerateRandomExpr(i, r, GenType(GenBool), EnableVariable, EnableCondition, GenVariables(valMap))

		got, err := Eval(expr.Expr, valMap, ExtendConf(cc))
		if err != nil {
			fmt.Println(GenerateTestCase(expr.Expr, expr.Res, valMap))
			t.Fatalf("assertNil failed, got: %+v\n", err)
		}

		if got != expr.Res {
			fmt.Println(GenerateTestCase(expr.Expr, expr.Res, valMap))
			t.Fatalf("assertEquals failed, got: %+v, want: %+v\n", got, expr.Res)
		}
	}
}

func TestGenerateRandomExpr_Number(t *testing.T) {
	const (
		variablesSize int = 10
		size          int = 50
	)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	keyMap := make(map[string]VariableKey, variablesSize)
	valMap := make(map[string]interface{}, variablesSize)
	for i := 0; i < variablesSize; i++ {
		v := r.Intn(200) - 100
		var k string
		if v < 0 {
			k = "var_neg_" + strconv.Itoa(-v)
		} else {
			k = "var_" + strconv.Itoa(v)
		}
		keyMap[k] = VariableKey(i)
		valMap[k] = int64(v)
	}

	cc := &Config{
		VariableKeyMap: keyMap,
		CompileOptions: map[CompileOption]bool{
			ConstantFolding: false,
		},
	}

	for i := 0; i < size; i++ {
		expr := GenerateRandomExpr(size, r, GenType(GenNumber), EnableCondition, EnableVariable, GenVariables(valMap))
		//fmt.Println(IndentByParentheses(expr.Expr))
		//fmt.Println(expr.Res)

		got, err := Eval(expr.Expr, valMap, ExtendConf(cc))
		if err != nil {
			fmt.Println(GenerateTestCase(expr.Expr, expr.Res, valMap))
			t.Fatalf("assertNil failed, got: %+v\n", err)
		}

		if got != expr.Res {
			fmt.Println(GenerateTestCase(expr.Expr, expr.Res, valMap))
			t.Fatalf("assertEquals failed, got: %+v, want: %+v\n", got, expr.Res)
		}
	}
}

func TestGenerateRandomExpr_RCO(t *testing.T) {
	const size = 50
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	cc := &Config{
		VariableKeyMap: map[string]VariableKey{
			"var_true":    VariableKey(1),
			"var_false":   VariableKey(2),
			"var_true_1":  VariableKey(3),
			"var_false_1": VariableKey(4),
		},
		CompileOptions: map[CompileOption]bool{
			ConstantFolding:       false,
			AllowUnknownVariables: true,
		},
	}
	valMap := map[string]interface{}{
		"var_true":    true,
		"var_false":   false,
		"var_true_1":  true,
		"var_false_1": true,
	}

	dneMap := map[string]interface{}{
		"var_dne_1": DNE,
		"var_dne_2": DNE,
		"var_dne_3": DNE,
	}

	ctx := NewCtxFromVars(cc, valMap)

	for i := 1; i < size; i++ {
		genRes := GenerateRandomExpr(i, r,
			GenType(GenBool), EnableCondition, EnableVariable, GenVariables(valMap), EnableRCO, GenVariables(dneMap))

		expr, err := Compile(cc, genRes.Expr)

		assertNil(t, err)

		got, err := expr.TryEval(ctx)
		if err != nil {
			fmt.Println(GenerateTestCase(genRes.Expr, genRes.Res, valMap))
			t.Fatalf("assertNil failed, got: %+v\n", err)
		}

		if got != genRes.Res {
			fmt.Println(GenerateTestCase(genRes.Expr, genRes.Res, valMap))
			t.Fatalf("assertEquals failed, got: %+v, want: %+v\n", got, genRes.Res)
		}
	}
}

func TestGenerateTestCase(t *testing.T) {
	testCases := []struct {
		expr GenExprResult
		want string
		vals map[string]interface{}
	}{
		{
			expr: GenExprResult{
				Res:  false,
				Expr: "(not (eq var_false var_false (!= 0 0)))",
			},
			want: `
        {
            want:          false,
            optimizeLevel: disable,
            s: ` + "`" + `
(not
  (eq var_false var_false
    (!= 0 0)))` + "`" + `,
            valMap: map[string]interface{}{
                "var_false": false,
            },
        },`,
			vals: map[string]interface{}{
				"var_false": false,
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
            valMap: map[string]interface{}{
                "less": true,
            },
        },`,
			vals: map[string]interface{}{
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
            valMap: map[string]interface{}{
                "age": int64(18),
            },
        },`,
			vals: map[string]interface{}{
				"age": int64(18),
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.expr.Expr, func(t *testing.T) {
			got := GenerateTestCase(c.expr.Expr, c.expr.Res, c.vals)
			assertEquals(t, got, c.want)
		})
	}
}
