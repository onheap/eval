package eval

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExpr_Eval(t *testing.T) {
	const debugMode bool = false

	type optimizeLevel int
	const (
		all optimizeLevel = iota
		onlyFast
		disable
	)

	cs := []struct {
		s             string
		valMap        map[string]interface{}
		optimizeLevel optimizeLevel // default: all
		want          Value
	}{
		{
			want:          true,
			optimizeLevel: onlyFast,
			s: `
(not
  (and
    (if T
      (!= 3 3)
      (= 4 4))
    (= 5 5)
    (= 6 6)))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(or
  (if
    (= 1 1)
    (< 4 2)
    (!= 5 6))
  (eq 8 -8))`,
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(if
  (= 1 2)
  (not F)
  (and
    (!= 3 4) T1 T2))`,
			valMap: map[string]interface{}{
				"T1": true,
				"T2": true,
				"F":  false,
			},
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(eq
  (if T1 F T2)
  (not T3))`,
			valMap: map[string]interface{}{
				"T1": true,
				"T2": true,
				"T3": true,
				"F":  false,
			},
		},
		{
			want: true,
			s: `
(<
 (+ 1
   (- 2 v3) (/ 6 3) 4)
 (* 5 6 7)
)`,
			valMap: map[string]interface{}{
				"v3": 3,
			},
		},

		{
			want:          int64(-1),
			optimizeLevel: disable,
			s:             "(if less -1 1)",
			valMap: map[string]interface{}{
				"less": true,
			},
		},

		{
			want:          int64(2),
			optimizeLevel: disable,
			s:             "(+ 1 1)",
			valMap:        nil,
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(not
  (and
    (if T
      (!= 0 0)
      (= 0 0))
    (= 0 0)
    (= 0 0)))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{

			want:          int64(1),
			optimizeLevel: disable,
			s:             `(if T 1 0)`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},

		{
			want:          false,
			optimizeLevel: disable,
			s: `
(and
  (if T F T)
  (or T
    (!= 0 0)))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(if T
  (or
    (eq 1 2) T)
  (= 3 4))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(if
  (and
    (= 0 0) T)
  (not
    (= 0 0))
  (!= 0 0))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(if
  (= 1 2)
  (not F)
  (and
    (!= 3 4) T1 T2))`,
			valMap: map[string]interface{}{
				"T1": true,
				"T2": true,
				"F":  false,
			},
		},
		{
			want:          int64(2),
			optimizeLevel: disable,
			s: `
(if
  (and  
    true
    (!= 0 0))
  1
  2)
`,
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(if
  (not
    (and true
      (!= 0 0)
      (!= 0 0)))
  (eq
    (or true true false)
    (!= 0 0)) 
  true)
`,
			valMap: map[string]interface{}{
				"var_true_1":  true,
				"var_false_1": false,
				"var_false":   false,
			},
		},
		{

			want: true,
			s:    `(= 1 1)`,
		},

		{
			want:          true,
			optimizeLevel: disable,
			s: `
(and
  (= 0 0)
  (or
    (&  ;; ok
      (ne 1 1)
      (!= 0 0)
      (!= 0 0))
    (not
      (!= 0 0))
    (not
      (!= 0 0))))`,
		},

		{

			want:          true,
			optimizeLevel: onlyFast,
			s: `
(not
  (and
    (= 0 0)
    (or
      (!= 0 0)
      (!= 0 0))
    (= 0 0)))`,
		},
		{

			want:          true,
			optimizeLevel: onlyFast,
			s: `
(not
  (&
    (and
      (= 0 0)
      (= 0 0))
    (!= 1 1)
    (= 0 0)
    (and
      (= 0 0)
      (= 0 0))))`,
		},
		{

			want:          true,
			optimizeLevel: onlyFast,
			s: `
(and
  (not
    (and
      (!= 0 0)
      (= 0 0)
      (= 0 0)
      (= 0 0)))
  (or
    (!= 0 0)
    (!= 0 0)
    (= 0 0)
    (= 0 0)))`,
		},
		{
			want: true,
			s: `
(and
  (|
    (eq Origin "MOW")
    (= Country "RU"))
  (or
    (>= Value 100)
    (<= Adults 1)))`,
			valMap: map[string]interface{}{
				"Origin":  "MOW",
				"Country": "RU",
				"Value":   100,
				"Adults":  1,
			},
			optimizeLevel: disable,
		},
		{
			want: false,
			s: `
			(and
			  (= Origin "MOW1")
			  (= Country "RU")
			  (>= Value 100)
			  (= Adults 1))`,
			valMap: map[string]interface{}{
				"Origin":  "MOW",
				"Country": "RU",
				"Value":   100,
				"Adults":  1,
			},
		},
		{

			want: false,
			s: `
;;;;optimize:false
(and
  (and
    (not
      (!= 1 1))
    (or
      (!= 2 2)
      (!= 3 3)
      (= 4 4)
      (!= 5 5)))
  (= 6 6)
  (!= 7 7))`,
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(and
  (if T
    (= 0 0)
    (= 0 0))
  (not
    (= 0 0)))`,
			valMap: map[string]interface{}{
				"T": true,
			},
		},
		{
			want:          int64(471240),
			optimizeLevel: disable,
			s: `
(-
  (/ 48 -36 9)
  (* 1 -26 28 -45)
  (* 32
    (% 1 22)
    (/ 37 28)
    (* 15 -3 -7 -50)))`,
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(not
  (and
    (= 0 0)
    (= 0 0)))`,
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(or               ;; false
  (if             ;; false
    (not          ;; true
  	  (!= 0 0))
      (if T       ;; false
  	    (!= 0 0)
  	    (= 0 0)) T)
  (eq             ;; false
    (!= 0 0) T))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(or
  (if            ;; true
    (not         ;; true 
      (!= 0 0))  ;; false
    (if F        ;; true
      (!= 0 0)
      (eq 1 1)) F)
  (eq
    (!= 0 0) T))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          int64(42),
			optimizeLevel: onlyFast,
			s: `
(%
  (* s_79 14 1 29)
  (* s_12 s_n56)
  (if
    (eq
      (= 0 0) T T T) s_56
    (* s_n56 -9))
  (-
    (* 10 47 s_78) -14 13))`,
			valMap: map[string]interface{}{
				"T":     true,
				"s_n56": int64(-56),
				"s_12":  int64(12),
				"s_56":  int64(56),
				"s_79":  int64(79),
				"s_78":  int64(78),
			},
		},
	}

	for _, c := range cs {
		t.Run(c.s, func(t *testing.T) {
			var options []Option
			options = append(options, WithEnv(c.valMap))

			if debugMode {
				options = append(options, EnableDebug)
			}

			switch c.optimizeLevel {
			case all:
				options = append(options, Optimizations(true))
			case disable:
				options = append(options, Optimizations(false))
			case onlyFast:
				// disable all optimizations and enable fast evaluation
				options = append(options, Optimizations(false), Optimizations(true, FastEvaluation))
			}

			cc := NewConfig(options...)

			ctx := NewCtxFromVars(cc, c.valMap)

			expr, err := Compile(cc, c.s)
			assertNil(t, err)
			if debugMode {
				expr.EventChan = make(chan Event)
				HandleDebugEvent(expr)
				fmt.Println(Dump(expr))
				fmt.Println()
				fmt.Println(DumpTable(expr, false))
			}

			res, err := expr.Eval(ctx)

			assertNil(t, err)

			if debugMode {
				fmt.Println(res)
			}

			if c.want != nil {
				assertEquals(t, res, c.want)
			}

			// eval with remote call optimization
			rcoRes, err := expr.TryEval(ctx)
			assertNil(t, err)

			if c.want != nil {
				assertEquals(t, rcoRes, c.want)
			}

			if debugMode {
				close(expr.EventChan)
			}
		})
	}
}

func TestEval_Infix(t *testing.T) {
	testCases := []struct {
		expr   string
		want   Value
		errMsg string
		vals   map[string]interface{}
	}{
		{
			expr: `1 + 1`,
			want: int64(2),
		},
		{
			expr: `a + b`,
			want: int64(3),
			vals: map[string]interface{}{
				"a": 1,
				"b": 2,
			},
		},
		{
			expr: `a + b + mod(7, 3)`,
			want: int64(4),
			vals: map[string]interface{}{
				"a": 1,
				"b": 2,
			},
		},
		{
			expr: `a && b && mod(c + 1, 10) == 0`,
			want: true,
			vals: map[string]interface{}{
				"a": true,
				"b": true,
				"c": 99,
			},
		},
		{
			expr: `a >= 8 && !(b && !e) && mod(c + 6 * f, 10) == 7`,
			want: true,
			vals: map[string]interface{}{
				"a": 8,
				"b": true,
				"e": true,
				"c": 3,
				"f": 9,
			},
		},
		{
			expr: `if(a > 0, a, 0 - a)`,
			want: int64(32),
			vals: map[string]interface{}{
				"a": 32,
			},
		},
		{
			expr: `if(a > 0, a, 0 - a)`,
			want: int64(232),
			vals: map[string]interface{}{
				"a": -232,
			},
		},
		{
			expr: `if(in(a, ["aa" "bb" "cc"]), add(b, c, d, e), mul(b, c, d, e))`,
			want: int64(56),
			vals: map[string]interface{}{
				"a": "cc",
				"b": 11,
				"c": 17,
				"d": 23,
				"e": 5,
			},
		},
		{
			expr: `if(mod(c - d, 6) < 3, c * 2, d + 7) + 3 * if(if(a > 0, a, 0 - a) != 0, b / a, (b + 1) / (e + 1))`,
			want: int64(40),
			vals: map[string]interface{}{
				"a": 0,
				"b": 11,
				"c": 17,
				"d": 23,
				"e": 5,
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			got, err := Eval(c.expr, c.vals,
				Optimizations(false),
				EnableInfixNotation,
				WithEnv(c.vals))

			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg)
				return
			}

			assertNil(t, err)
			assertEquals(t, got, c.want)
		})
	}
}

func TestEval_AllowUnknownVariables(t *testing.T) {
	testCases := []struct {
		cc     *Config
		expr   string
		want   Value
		errMsg string
		vals   map[string]interface{}
	}{
		{
			expr:   `(< age 18)`,
			errMsg: "unknown token error",
		},
		{
			want: false,
			expr: `(< age 18)`,
			cc:   NewConfig(EnableUnknownVariables),
			vals: map[string]interface{}{
				"age": int64(20),
			},
		},
		{
			expr:   `(< not_exist_key 18)`,
			cc:     NewConfig(EnableUnknownVariables),
			errMsg: "variableKey not exist",
		},
		{
			expr: `
(-
 (+ 1
   (- 2 v3) (/ 6 3) 4)
 (* 5 -6 7)
)`,
			errMsg: "unknown token error",
		},
		{
			want: int64(216),
			expr: `
(-
 (+ 1
   (- 2 v3) (/ 6 3) 4)
 (* 5 -6 7)
)`,
			cc: NewConfig(EnableUnknownVariables),
			vals: map[string]interface{}{
				"v3": int64(3),
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			got, err := Eval(c.expr, c.vals, ExtendConf(c.cc))

			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg)
				return
			}

			assertNil(t, err)
			assertEquals(t, got, c.want)
		})
	}
}

func TestExpr_TryEval(t *testing.T) {
	const debugMode bool = false

	type optimizeLevel int
	const (
		all optimizeLevel = iota
		onlyFast
		disable
	)

	cs := []struct {
		s             string
		valMap        map[string]interface{}
		optimizeLevel optimizeLevel // default: all
		want          Value
	}{
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(not
  (and
    (if dne
      (!= 3 3)
      (eq 4 4))
    (= 5 5)
    (= 6 6)))`,
		},
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(or
  (if
    (= 1 1)
    (< 4 2)
    (!= 5 6))
  (eq dne -8))`,
		},
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(if
  (= 1 2)
  (not dne)
  (and
    (!= dne 3 4) T1 T2))`,
			valMap: map[string]interface{}{
				"T1": true,
				"T2": true,
				"F":  false,
			},
		},
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(eq
  (if (= T1 dne) F T2)
  (not T3))`,
			valMap: map[string]interface{}{
				"T1": true,
				"T2": true,
				"T3": true,
				"F":  false,
			},
		},
		{
			want: DNE,
			s: `
(<
 (+ 1
   (- 2 dne) (/ 6 3) 4)
 (* 5 6 7)
)`,
			valMap: map[string]interface{}{
				"v3": 3,
			},
		},

		{
			want:          int64(-1),
			optimizeLevel: disable,
			s:             "(if less -1 dne)",
			valMap: map[string]interface{}{
				"less": true,
			},
		},

		{
			want:          DNE,
			optimizeLevel: disable,
			s:             "(+ 1 dne)",
			valMap:        nil,
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(not
  (and
    (if T
      (!= 0 0)
      (= 0 0))
    (= dne 0)
    (= 1 0)))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{

			want:          DNE,
			optimizeLevel: disable,
			s:             `(if T dne 0)`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},

		{
			want:          true,
			optimizeLevel: disable,
			s: `
(and
  (if T T T)
  (or dne T
    (!= 0 0)))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(and F
  (= 0 0)
  (!= 0 0))`,
			valMap: map[string]interface{}{
				"F": false,
				"T": true,
			},
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(if T
  (or
    (eq 1 2 dne) T)
  (= 3 4))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(if
  (and
    (= 0 0) dne)
  (not
    (= 0 0))
  (!= 0 0))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(if
  (= 1 2)
  (not F)
  (or
    (!= 3 dne) dne T2))`,
			valMap: map[string]interface{}{
				"T1": true,
				"T2": true,
				"F":  false,
			},
		},
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(if
  (and  
    true
    (!= 0 0))
  dne
  dne)
`,
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(if
  (not
    (and true
      (!= 0 0)
      (!= dne dne)))
  (eq
    (or true true false)
    (!= 0 0)) 
  true)
`,
			valMap: map[string]interface{}{
				"var_true_1":  true,
				"var_false_1": false,
				"var_false":   false,
			},
		},
		{

			want: DNE,
			s:    `(= dne dne)`,
		},

		{
			want:          true,
			optimizeLevel: disable,
			s: `
(and
  (= 0 0)
  (or
    (&  ;; ok
      (ne 1 1)
      (!= 0 dne)
      (!= dne 0))
    (not
      (!= 0 0))
    (not
      (!= 0 0))))`,
		},

		{

			want:          DNE,
			optimizeLevel: onlyFast,
			s: `
(not
  (and
    (= dne 0)
    (or
      (!= 0 0)
      (!= dne 0))
    (= 0 0)))`,
		},
		{

			want:          true,
			optimizeLevel: onlyFast,
			s: `
(not
  (&
    (and
      (= 0 0)
      (= dne 0))
    (!= 1 1)
    (= dne 0)
    (and
      (= 0 0)
      (= 0 0))))`,
		},
		{

			want:          true,
			optimizeLevel: onlyFast,
			s: `
(and
  (not
    (and
      (!= 0 0)
      (= 0 0)
      (= dne 0)
      (= 0 0)))
  (or
    (!= dne 0)
    (!= dne dne)
    (= dne dne)
    (= 0 0)))`,
		},
		{
			want: true,
			s: `
(and
  (|
    (eq Origin "MOW")
    (= Country "RU"))
  (or
    (>= Value 100)
    (<= Adults 1)))`,
			valMap: map[string]interface{}{
				"Country": "RU",
				"Adults":  1,
			},
			optimizeLevel: disable,
		},
		{
			want: DNE,
			s: `
			(and
			  (= Origin "MOW")
			  (= Country "RU")
			  (>= Value 100)
			  (= Adults 1))`,
			valMap: map[string]interface{}{
				"Country": "RU",
				"Value":   100,
				"Adults":  1,
			},
		},
		{

			want: false,
			s: `
;;;;optimize:false
(and
  (and
    (not
      (!= 1 1))
    (or
      (!= 2 2)
      (!= dne 3)
      (= 4 4)
      (!= 5 5)))
  (= 6 dne)
  (!= 7 7))`,
		},
		{
			want:          false,
			optimizeLevel: disable,
			s: `
(and
  (if T
    (= 0 0)
    (= dne 0))
  (not
    (= 0 0)))`,
			valMap: map[string]interface{}{
				"T": true,
			},
		},
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(-
  (/ 48 -36 9)
  (* 1 -26 28 -45)
  (* 32
    (% dne 22)
    (/ 37 28)
    (* 15 -3 -7 -50)))`,
		},
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(or
  (if          
    (not       
      (!= 0 0))
    (if T    
      (!= dne 0)
      (eq 1 1)) F)
  (eq
    (!= 0 0) T))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
			},
		},
		{
			want:          true,
			optimizeLevel: all,
			s:             `(or T dne)`,
			valMap: map[string]interface{}{
				"T": true,
			},
		},
		{
			want:          DNE,
			optimizeLevel: disable,
			s: `
(if
  (if dne
    (!= 0 0)
    (= 0 0)) -30 v_5)`,
			valMap: map[string]interface{}{
				"v_5": int64(5),
			},
		},
	}

	for _, c := range cs {
		t.Run(c.s, func(t *testing.T) {
			var options []Option
			options = append(options, EnableUnknownVariables)
			if debugMode {
				options = append(options, EnableDebug)
			}

			switch c.optimizeLevel {
			case all:
				options = append(options, Optimizations(true))
			case disable:
				options = append(options, Optimizations(false))
			case onlyFast:
				// disable all optimizations and enable fast evaluation
				options = append(options, Optimizations(false), Optimizations(true, FastEvaluation))
			}

			cc := NewConfig(options...)

			ctx := NewCtxFromVars(cc, c.valMap)

			expr, err := Compile(cc, c.s)
			assertNil(t, err)
			if debugMode {
				expr.EventChan = make(chan Event)
				HandleDebugEvent(expr)
				fmt.Println(Dump(expr))
				fmt.Println()
				fmt.Println(DumpTable(expr, true))
			}

			res, err := expr.TryEval(ctx)
			if debugMode {
				close(expr.EventChan)
			}
			assertNil(t, err)

			if debugMode {
				fmt.Println(res)
			}

			if c.want != nil {
				assertEquals(t, res, c.want)
			}
		})
	}
}

func TestRandomExpressions(t *testing.T) {
	const (
		size          = 30000
		level         = 53
		step          = size / 100
		showSample    = false
		printProgress = false
	)

	const (
		producerCount = 400
		consumerCount = 2000
		bufferSize    = 10000
	)

	var random = rand.New(rand.NewSource(time.Now().UnixNano()))

	conf := NewConfig()
	conf.VariableKeyMap = map[string]VariableKey{
		"var_true":  VariableKey(1),
		"var_false": VariableKey(2),
	}

	valMap := map[string]interface{}{
		"var_true":  true,
		"var_false": false,
	}
	for i := 0; i < 20; i++ {
		v := random.Intn(200) - 100
		var k string
		if v < 0 {
			k = "var_neg_" + strconv.Itoa(-v)
		} else {
			k = "var_" + strconv.Itoa(v)
		}
		valMap[k] = int64(v)
		_ = GetOrRegisterKey(conf, k)
	}

	dneMap := map[string]interface{}{
		"var_dne_1": DNE,
		"var_dne_2": DNE,
		"var_dne_3": DNE,
	}

	type testCase struct {
		level int
		rco   bool
		expr  string
		want  Value

		cc  *Config
		got Value
		err error
	}

	exprChan := make(chan testCase, bufferSize)
	verifyChan := make(chan testCase, bufferSize)
	eventChan := make(chan Event, bufferSize)

	go func() {
		for e := range eventChan {
			_ = e // do nothing
		}
	}()

	var pwg sync.WaitGroup

	var genCnt int32
	for cnt := 0; cnt < producerCount; cnt++ {
		pwg.Add(1)
		go func(r *rand.Rand) {
			defer pwg.Done()
			for atomic.LoadInt32(&genCnt) < size {
				i := int(atomic.AddInt32(&genCnt, 1))
				options := make([]GenExprOption, 0, 4)
				v := random.Intn(0b10000)

				if v&0b0001 != 0 {
					options = append(options, GenType(GenBool))
				} else {
					options = append(options, GenType(GenNumber))
				}

				if v&0b0010 != 0 {
					options = append(options, EnableCondition)
				}

				if v&0b0100 != 0 {
					options = append(options, EnableVariable, GenVariables(valMap))
				}

				if v&0b1100 == 0b1100 {
					options = append(options, EnableRCO, GenVariables(dneMap))
				}

				l := (i % level) + 1
				expr := GenerateRandomExpr(l, r, options...)
				exprChan <- testCase{
					level: l,
					rco:   v&0b1100 == 0b1100,
					expr:  expr.Expr,
					want:  expr.Res,
				}
			}
		}(rand.New(rand.NewSource(random.Int63())))
	}

	go func() {
		pwg.Wait()
		close(exprChan)
	}()

	var cwg sync.WaitGroup
	for cnt := 0; cnt < consumerCount; cnt++ {
		cwg.Add(1)
		go func(r *rand.Rand) {
			defer cwg.Done()
			for c := range exprChan {
				v := r.Intn(0b10000)
				// combination of optimizations
				cc := CopyConfig(conf)
				cc.CompileOptions[Reordering] = v&0b1 != 0
				cc.CompileOptions[FastEvaluation] = v&0b10 != 0
				cc.CompileOptions[ConstantFolding] = v&0b100 != 0
				cc.CompileOptions[AllowUnknownVariables] = c.rco
				cc.CompileOptions[ReportEvent] = v&0b1000 != 0 && c.level <= level/2

				expr, err := Compile(cc, c.expr)
				if err != nil {
					c.err = err
					verifyChan <- c
					continue
				}

				if cc.CompileOptions[ReportEvent] {
					expr.EventChan = eventChan
				}

				c.cc = cc
				ctx := NewCtxFromVars(cc, valMap)
				if c.rco {
					c.got, c.err = safeExec(expr.TryEval, ctx)
				} else {
					c.got, c.err = safeExec(expr.Eval, ctx)
				}
				verifyChan <- c

			}
		}(rand.New(rand.NewSource(random.Int63())))
	}

	go func() {
		cwg.Wait()
		close(verifyChan)
	}()

	const line = "----------"

	prev := time.Now()
	var i int
	for res := range verifyChan {
		i++
		expr, want, got, err := res.expr, res.want, res.got, res.err
		if err != nil {
			fmt.Println(GenerateTestCase(expr, want, valMap))
			t.Fatalf("assertNil failed, got: %+v\n", err)
		}

		if got != want {
			fmt.Println(GenerateTestCase(expr, want, valMap))
			t.Fatalf("assertEquals failed, got: %+v, want: %+v\n", got, want)
		}

		if i%step == 0 {
			if showSample {
				fmt.Println(GenerateTestCase(expr, want, valMap))
			}
			if printProgress {
				if i == step {
					fmt.Printf("|%10s|%10s|%10s|%10s|%10s|%10s|%10s|\n", "gen progs", "exec progs", "gen count", "exec count", "gen chan", "exec chan", "time")
					fmt.Printf("|%10s|%10s|%10s|%10s|%10s|%10s|%10s|\n", line, line, line, line, line, line, line)
				}

				j := atomic.LoadInt32(&genCnt)
				fmt.Printf("|%8d %%|%8d %%|%10d|%10d|%10d|%10d|%10s|\n", (j*100)/size, (i*100)/size, j, i, len(exprChan), len(verifyChan), time.Since(prev).Truncate(time.Millisecond))
				prev = time.Now()
			}
		}
	}
}

func TestReportEvent(t *testing.T) {
	vals := map[string]interface{}{
		"v2": 2,
		"v3": 3,
	}

	cc := NewConfig(EnableReportEvent, WithEnv(vals))

	s := `(+ 1 v2 v3)`

	e, err := Compile(cc, s)
	assertNil(t, err)
	e.EventChan = make(chan Event)

	var events []Event
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for ev := range e.EventChan {
			events = append(events, ev)
		}
		wg.Done()
	}()

	res, err := e.Eval(NewCtxFromVars(cc, vals))
	close(e.EventChan)

	wg.Wait()
	assertNil(t, err)
	assertEquals(t, res, int64(6))
	assertEquals(t, events, []Event{
		{
			EventType: LoopEvent,
			Stack:     []Value{},
			Data: LoopEventData{
				NodeValue: int64(1),
				NodeType:  ConstantNode,
				CurtIdx:   1,
			},
		},
		{
			EventType: LoopEvent,
			Stack:     []Value{int64(1)},
			Data: LoopEventData{
				NodeValue: "v2",
				NodeType:  VariableNode,
				CurtIdx:   3,
			},
		},
		{
			EventType: LoopEvent,
			Stack:     []Value{int64(1), int64(2)},
			Data: LoopEventData{
				NodeValue: "v3",
				NodeType:  VariableNode,
				CurtIdx:   5,
			},
		},
		{
			EventType: LoopEvent,
			Stack:     []Value{int64(1), int64(2), int64(3)},
			Data: LoopEventData{
				NodeValue: "+",
				NodeType:  OperatorNode,
				CurtIdx:   7,
			},
		},
		{
			EventType: OpExecEvent,
			Data: OpEventData{
				IsFastOp: false,
				OpName:   "+",
				Params:   []Value{int64(1), int64(2), int64(3)},
				Res:      int64(6),
				Err:      nil,
			},
		},
	})
}

func TestStatelessOperators(t *testing.T) {
	cc := &Config{
		OperatorMap: map[string]Operator{
			"to_set": func(_ *Ctx, params []Value) (Value, error) {
				if len(params) != 1 {
					return nil, ParamsCountError("to_set", 1, len(params))
				}
				switch list := params[0].(type) {
				case []int64:
					set := make(map[int64]struct{}, len(list))
					for _, i := range list {
						set[i] = empty
					}
					return set, nil
				case []string:
					set := make(map[string]struct{}, len(list))
					for _, s := range list {
						set[s] = empty
					}
					return set, nil
				default:
					return nil, ParamTypeError("to_set", "list", list)
				}
			},
		},
		VariableKeyMap: map[string]VariableKey{
			"num": VariableKey(1),
		},
		StatelessOperators: []string{"to_set"},
	}

	s := `
  (in 
    num
    (to_set 
      (2 3 5 7 11 13 17 19 23 29 31 37 41
       43 47 53 59 61 67 71 73 79 83 89 97 
       101 103 107 109 113 127 131 137 139 
       149 151 157 163 167 173 179 181 191 
       193 197 199 211 223 227 229 233 239 
       241 251 257 263 269 271 277 281 283 
       293 307 311 313 317 331 337 347 349 
       353 359 367 373 379 383 389 397 401 
       409 419 421 431 433 439 443 449 457 
       461 463 467 479 487 491 499 503 509 
       521 523 541 547 557 563 569 571 577 
       587 593 599 601 607 613 617 619 631 
       641 643 647 653 659 661 673 677 683 
       691 701 709 719 727 733 739 743 751 
       757 761 769 773 787 797 809 811 821 
       823 827 829 839 853 857 859 863 877 
       881 883 887 907 911 919 929 937 941 
       947 953 967 971 977 983 991 997)))
`

	expr, err := Compile(cc, s)
	assertNil(t, err)
	res, err := expr.EvalBool(NewCtxFromVars(cc, map[string]interface{}{
		"num": 499,
	}))

	assertNil(t, err)
	assertEquals(t, res, true)
	assertEquals(t, len(expr.nodes), 3)
}

func assertEquals(t *testing.T, got, want any, msg ...any) {
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assertEquals failed, got: %+v, want: %+v, msg: %+v", got, want, msg)
	}
}

func assertFloatEquals(t *testing.T, got, want float64, msg ...any) {
	if math.Abs(got-want) > 0.00001 {
		t.Fatalf("assertFloatEquals failed, got: %+v, want: %+v, msg: %+v", got, want, msg)
	}
}

func assertNil(t *testing.T, val any, msg ...any) {
	if val != nil {
		t.Fatalf("assertNil failed, got: %+v, msg: %+v", val, msg)
	}
}

func assertNotNil(t *testing.T, val any, msg ...any) {
	if val == nil {
		t.Fatalf("assertNotNil failed, got: %+v, msg: %+v", val, msg)
	}
}

func assertErrStrContains(t *testing.T, err error, errMsg string, msg ...any) {
	if err == nil {
		t.Fatalf("assertErrStrContains failed, err is nil, msg: %+v", msg)
	}
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("assertErrStrContains failed, err: %v, want: %s, msg: %+v", err, errMsg, msg)
	}
}

func safeExec(fn func(*Ctx) (Value, error), ctx *Ctx) (res Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("%+v", r))
		}
	}()
	return fn(ctx)
}
