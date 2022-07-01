package eval

import (
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
		valMap        map[string]Value
		optimizeLevel optimizeLevel // default: all
		fields        []string
		want          Value
		run           runThis
	}{

		{
			want:          int64(-1),
			optimizeLevel: disable,
			s:             "(if less -1 1)",
			valMap: map[string]Value{
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
			run:           ________RunThisOne________,
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
			valMap: map[string]Value{
				"T": true,
				"F": false,
			},
		},
		{

			want:          int64(1),
			optimizeLevel: disable,
			s:             `(if T 1 0)`,
			valMap: map[string]Value{
				"T": true,
				"F": false,
			},
		},

		{
			run:           ________RunThisOne________,
			want:          false,
			optimizeLevel: disable,
			s: `
(and
  (if T F T)
  (or T
    (!= 0 0)))`,
			valMap: map[string]Value{
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
			valMap: map[string]Value{
				"T": true,
				"F": false,
			},
		},
		{
			want:          true,
			optimizeLevel: disable,
			s: `
(eq
  (if T F T)
  (not T))`,
			valMap: map[string]Value{
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
			valMap: map[string]Value{
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
			valMap: map[string]Value{
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
			valMap: map[string]Value{
				"select_true_1":  true,
				"select_false_1": false,
				"select_false":   false,
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
			valMap: map[string]Value{
				"v3": 3,
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
			valMap: map[string]Value{
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
			valMap: map[string]Value{
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

		cc := NewCompileConfig()
		cc.OptimizeOptions = map[OptimizeOption]bool{
			Reordering:      false,
			FastEvaluation:  false,
			ConstantFolding: false,
		}

		switch c.optimizeLevel {
		case all:
			cc.OptimizeOptions[Reordering] = true
			cc.OptimizeOptions[ConstantFolding] = true
			fallthrough
		case onlyFast:
			cc.OptimizeOptions[FastEvaluation] = true
		case disable:
		}

		ctx := NewCtxWithMap(cc, c.valMap)
		ctx.Debug = true

		expr, err := Compile(cc, c.s)
		assertNil(t, err)

		fmt.Println(PrintCode(expr))
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
		vals   map[string]Value
	}{
		{
			expr:   `(< age 18)`,
			errMsg: "unknown token error",
		},
		{
			want: false,
			expr: `(< age 18)`,
			cc:   &CompileConfig{AllowUnknownSelectors: true},
			vals: map[string]Value{
				"age": int64(20),
			},
		},
		{
			expr:   `(< not_exist_key 18)`,
			cc:     &CompileConfig{AllowUnknownSelectors: true},
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
			cc: &CompileConfig{AllowUnknownSelectors: true},
			vals: map[string]Value{
				"v3": int64(3),
			},
		},
	}

	for _, c := range testCases {
		got, err := Eval(c.cc, c.expr, &Ctx{Selector: NewMapSelector(c.vals)})

		if len(c.errMsg) != 0 {
			assertErrStrContains(t, err, c.errMsg)
			continue
		}
		assertNil(t, err)
		assertEquals(t, got, c.want)
	}

}

func TestRandomExpression(t *testing.T) {
	const (
		size       = 100000
		level      = 53
		step       = size / 100
		showSample = false
	)

	const (
		producerCount = 200
		consumerCount = 1000
		bufferSize    = 10000
	)

	var (
		r         = rand.New(rand.NewSource(time.Now().UnixNano()))
		m         sync.Mutex
		randomInt = func(n int) int {
			m.Lock()
			i := r.Intn(n)
			m.Unlock()
			return i
		}
	)

	conf := NewCompileConfig()
	conf.SelectorMap = map[string]SelectorKey{
		"select_true":  SelectorKey(1),
		"select_false": SelectorKey(2),
	}

	valMap := map[string]Value{
		"select_true":  true,
		"select_false": false,
	}
	for i := 0; i < 20; i++ {
		v := randomInt(200) - 100
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
	resChan := make(chan execRes, bufferSize)

	var pwg sync.WaitGroup

	var cnt int32
	for p := 0; p < producerCount; p++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for atomic.LoadInt32(&cnt) < size {
				options := make([]GenExprOption, 0, 4)
				v := randomInt(0b1000)

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

				random := rand.New(rand.NewSource(int64(randomInt(math.MaxInt))))

				i := int(atomic.AddInt32(&cnt, 1))
				exprChan <- GenerateRandomExpr((i%level)+1, random, options...)
				if i%step == 0 {
					t.Log("generating... current:", i, (i*100)/size, "%")
				}
			}
		}()
	}

	go func() {
		pwg.Wait()
		close(exprChan)
	}()

	var cwg sync.WaitGroup
	for c := 0; c < consumerCount; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for expr := range exprChan {
				v := randomInt(0b1000)
				// combination of optimizations
				cc := CopyCompileConfig(conf)
				cc.OptimizeOptions[Reordering] = v&0b1 != 0
				cc.OptimizeOptions[FastEvaluation] = v&0b10 != 0
				cc.OptimizeOptions[ConstantFolding] = v&0b100 != 0
				ctx := NewCtxWithMap(cc, valMap)
				got, err := Eval(cc, expr.Expr, ctx)

				resChan <- execRes{
					expr: expr,
					got:  got,
					err:  err,
				}
			}
		}()
	}

	go func() {
		cwg.Wait()
		close(resChan)
	}()

	var i int
	for res := range resChan {
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
			//t.Logf("Channel status: %% exprChan: %d, resChan: %d\n", len(exprChan), len(resChan))
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
