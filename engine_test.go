package eval

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDebugCases(t *testing.T) {
	const onlyAllowListCases = false

	type runThis string
	const ________RunThisOne________ runThis = "________RunThisOne________"

	type optimizeLevel int
	const (
		all optimizeLevel = iota
		onlyFast
		disable
	)

	cs := []struct {
		name          string
		s             string
		valMap        map[string]interface{}
		optimizeLevel optimizeLevel // default: all
		fields        []string
		want          Value
		run           runThis
	}{
		{
			run:           ________RunThisOne________,
			want:          true,
			optimizeLevel: disable,
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
  (if T F T)
  (not T))`,
			valMap: map[string]interface{}{
				"T": true,
				"F": false,
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
				"select_true_1":  true,
				"select_false_1": false,
				"select_false":   false,
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
			//fields: []string{"scIdx", "scVal", "pIdx"},
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
			fields: []string{"scIdx", "scVal", "pIdx"},
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
	}

	for _, c := range cs {
		if onlyAllowListCases && c.run != ________RunThisOne________ {
			continue
		}

		options := []CompileOption{EnableDebug}
		switch c.optimizeLevel {
		case all:
			options = append(options, Optimizations(true))
		case disable:
			options = append(options, Optimizations(false))
		case onlyFast:
			// disable all optimizations and enable fast evaluation
			options = append(options, Optimizations(false), Optimizations(true, FastEvaluation))
		}

		cc := NewCompileConfig(options...)

		ctx := NewCtxWithMap(cc, c.valMap)

		expr, err := Compile(cc, c.s)
		assertNil(t, err)

		fmt.Println(Dump(expr))
		fmt.Println()
		fmt.Println(PrintExpr(expr, c.fields...))

		res, err := expr.Eval(ctx)
		assertNil(t, err)
		fmt.Println(res)
		if c.want != nil {
			assertEquals(t, res, c.want)
		}
	}
}

func TestEval_AllowUnknownSelector(t *testing.T) {
	testCases := []struct {
		cc     *CompileConfig
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
			cc:   NewCompileConfig(EnableStringSelectors),
			vals: map[string]interface{}{
				"age": int64(20),
			},
		},
		{
			expr:   `(< not_exist_key 18)`,
			cc:     NewCompileConfig(EnableStringSelectors),
			errMsg: "selectorKey not exist",
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
			cc: NewCompileConfig(EnableStringSelectors),
			vals: map[string]interface{}{
				"v3": int64(3),
			},
		},
	}

	for _, c := range testCases {
		got, err := Eval(c.expr, c.vals, c.cc)

		if len(c.errMsg) != 0 {
			assertErrStrContains(t, err, c.errMsg)
			continue
		}
		assertNil(t, err)
		assertEquals(t, got, c.want)
	}

}

func TestRandomExpressions(t *testing.T) {
	const (
		size          = 10000
		level         = 53
		step          = size / 100
		showSample    = false
		printProgress = true
	)

	const (
		producerCount = 400
		consumerCount = 2000
		bufferSize    = 10000
	)

	var random = rand.New(rand.NewSource(time.Now().UnixNano()))

	conf := NewCompileConfig()
	conf.SelectorMap = map[string]SelectorKey{
		"select_true":  SelectorKey(1),
		"select_false": SelectorKey(2),
	}

	valMap := map[string]interface{}{
		"select_true":  true,
		"select_false": false,
	}
	for i := 0; i < 20; i++ {
		v := random.Intn(200) - 100
		var k string
		if v < 0 {
			k = "select_neg_" + strconv.Itoa(-v)
		} else {
			k = "select_" + strconv.Itoa(v)
		}
		valMap[k] = int64(v)
		_ = GetOrRegisterKey(conf, k)
	}

	type execRes struct {
		expr GenExprResult
		got  Value
		err  error
	}

	exprChan := make(chan GenExprResult, bufferSize)
	verifyChan := make(chan execRes, bufferSize)

	var pwg sync.WaitGroup

	var genCnt int32
	for p := 0; p < producerCount; p++ {
		pwg.Add(1)
		go func(r *rand.Rand) {
			defer pwg.Done()
			for atomic.LoadInt32(&genCnt) < size {
				i := int(atomic.AddInt32(&genCnt, 1))
				options := make([]GenExprOption, 0, 4)
				v := random.Intn(0b1000)

				if v&0b001 != 0 {
					options = append(options, GenType(Bool))
				} else {
					options = append(options, GenType(Number))
				}

				if v&0b010 != 0 {
					options = append(options, EnableCondition)
				}

				if v&0b100 != 0 {
					options = append(options, EnableSelector, GenSelectors(valMap))
				}

				exprChan <- GenerateRandomExpr((i%level)+1, r, options...)
				if i%step == 0 {
					t.Log("generating... current:", i, (i*100)/size, "%")
				}
			}
		}(rand.New(rand.NewSource(random.Int63())))
	}

	go func() {
		pwg.Wait()
		close(exprChan)
	}()

	var cwg sync.WaitGroup
	for c := 0; c < consumerCount; c++ {
		cwg.Add(1)
		go func(r *rand.Rand) {
			defer cwg.Done()
			for expr := range exprChan {
				v := r.Intn(0b1000)
				// combination of optimizations
				cc := CopyCompileConfig(conf)
				cc.CompileOptions[Reordering] = v&0b1 != 0
				cc.CompileOptions[FastEvaluation] = v&0b10 != 0
				cc.CompileOptions[ConstantFolding] = v&0b100 != 0
				got, err := Eval(expr.Expr, valMap, cc)

				verifyChan <- execRes{
					expr: expr,
					got:  got,
					err:  err,
				}
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
		expr, got, err := res.expr, res.got, res.err
		if err != nil {
			fmt.Println(GenerateTestCase(expr, valMap))
			t.Fatalf("assertNil failed, got: %+v\n", err)
		}

		if got != expr.Res {
			fmt.Println(GenerateTestCase(expr, valMap))
			t.Fatalf("assertEquals failed, got: %+v, want: %+v\n", got, expr.Res)
		}

		if i%step == 0 {
			t.Log("executing.... current:", i, (i*100)/size, "%")
			if showSample {
				fmt.Println(GenerateTestCase(expr, valMap))
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

func assertEquals(t *testing.T, got, want any, msg ...any) {
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assertEquals failed, got: %+v, want: %+v, msg: %+v", got, want, msg)
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
