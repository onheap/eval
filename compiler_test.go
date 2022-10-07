package eval

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCopyCompileConfig(t *testing.T) {
	var res *CompileConfig
	res = CopyCompileConfig(nil)
	assertNotNil(t, res)
	assertNotNil(t, res.OperatorMap)
	assertNotNil(t, res.ConstantMap)
	assertNotNil(t, res.SelectorMap)
	assertNotNil(t, res.CompileOptions)
	assertNotNil(t, res.StatelessOperators)

	res = CopyCompileConfig(&CompileConfig{})
	assertNotNil(t, res)
	assertNotNil(t, res.OperatorMap)
	assertNotNil(t, res.ConstantMap)
	assertNotNil(t, res.SelectorMap)
	assertNotNil(t, res.CompileOptions)
	assertNotNil(t, res.StatelessOperators)

	cc := &CompileConfig{
		ConstantMap: map[string]Value{
			"birthdate_format": "Jan 02, 2006",
		},
		SelectorMap: map[string]SelectorKey{
			"birthday": SelectorKey(3),
		},
		OperatorMap: map[string]Operator{
			"is_child": func(_ *Ctx, params []Value) (Value, error) {
				const (
					op       = "is_child"
					timeYear = time.Hour * 24 * 365
				)
				if len(params) != 2 {
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
			"max": func(_ *Ctx, param []Value) (Value, error) {
				const op = "max"
				if len(param) < 2 {
					return nil, ParamsCountError(op, 2, len(param))
				}

				var m int64
				for i, p := range param {
					v, ok := p.(int64)
					if !ok {
						return nil, ParamTypeError(op, typeInt, p)
					}
					if i == 0 {
						m = v
					} else {
						if v > m {
							m = v
						}
					}
				}
				return m, nil
			},
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
					return nil, ParamTypeError("to_set", "slice", list)
				}
			},
		},
		CostsMap: map[string]float64{
			"selector": 10,
			"operator": 20,
		},
		CompileOptions: map[Option]bool{
			Reordering:      true,
			ConstantFolding: false,
		},
		// max & to_set are both stateless operators
		// but is_child is not, because it varies with time
		StatelessOperators: []string{"max", "to_set"},
	}

	res = CopyCompileConfig(cc)
	assertEquals(t, res.ConstantMap, cc.ConstantMap)
	assertEquals(t, res.SelectorMap, cc.SelectorMap)
	assertEquals(t, res.CompileOptions, cc.CompileOptions)
	assertEquals(t, res.StatelessOperators, cc.StatelessOperators)

	assertEquals(t, len(res.OperatorMap), len(cc.OperatorMap))
	for s := range cc.OperatorMap {
		got := reflect.ValueOf(res.OperatorMap[s])
		want := reflect.ValueOf(cc.OperatorMap[s])
		assertEquals(t, got, want)
	}

	assertEquals(t, len(res.CostsMap), len(cc.CostsMap))
	for s := range cc.CostsMap {
		assertFloatEquals(t, res.CostsMap[s], cc.CostsMap[s])
	}
}

func TestGetCosts(t *testing.T) {
	cc := &CompileConfig{
		SelectorMap: map[string]SelectorKey{
			"birthday": SelectorKey(3),
			"gender":   SelectorKey(4),
		},
		OperatorMap: map[string]Operator{
			"is_child": func(_ *Ctx, _ []Value) (Value, error) { return false, nil },
			"not_null": func(_ *Ctx, _ []Value) (Value, error) { return false, nil },
		},
		CostsMap: map[string]float64{
			"selector": 10, // generally costs for all selectors
			"operator": 20, // generally costs for all operators

			"is_child": 13, // specified costs for operator `is_child`
			"birthday": 11, // specified costs for selector `birthday`
		},
	}

	assertFloatEquals(t, cc.getCosts(selector, "birthday"), 11)
	assertFloatEquals(t, cc.getCosts(selector, "gender"), 10)
	assertFloatEquals(t, cc.getCosts(operator, "is_child"), 13)
	assertFloatEquals(t, cc.getCosts(fastOperator, "is_child"), 13)
	assertFloatEquals(t, cc.getCosts(operator, "not_null"), 20)
	assertFloatEquals(t, cc.getCosts(fastOperator, "not_null"), 20)

	cc = &CompileConfig{
		SelectorMap: map[string]SelectorKey{
			"birthday": SelectorKey(3),
			"gender":   SelectorKey(4),
		},
		OperatorMap: map[string]Operator{
			"is_child": func(_ *Ctx, _ []Value) (Value, error) { return false, nil },
			"not_null": func(_ *Ctx, _ []Value) (Value, error) { return false, nil },
		},
		CostsMap: map[string]float64{},
	}

	assertFloatEquals(t, cc.getCosts(selector, "birthday"), 7)
	assertFloatEquals(t, cc.getCosts(selector, "gender"), 7)
	assertFloatEquals(t, cc.getCosts(operator, "is_child"), 10)
	assertFloatEquals(t, cc.getCosts(fastOperator, "is_child"), 10)
	assertFloatEquals(t, cc.getCosts(operator, "not_null"), 10)
	assertFloatEquals(t, cc.getCosts(fastOperator, "not_null"), 10)
}

func TestOptimizeConstantFolding(t *testing.T) {
	testCases := []struct {
		cc   *CompileConfig
		expr string
		ast  verifyNode
	}{
		{
			expr: `(+ 1 1)`,
			ast: verifyNode{
				tpy:  constant,
				data: int64(2),
			},
		},

		{
			expr: `(= 1 1)`,
			ast: verifyNode{
				tpy:  constant,
				data: true,
			},
		},

		{
			expr: `
(-
 (+ 1
   (- 2 3) (/ 6 3) 4)
 (* 5 6 7)
)
`,
			ast: verifyNode{
				tpy:  constant,
				data: int64(-204),
			},
		},
		{
			expr: `
(<
 (+ 1
   (- 2 3) (/ 6 3) 4)
 (* 5 6 7)
)
`,
			ast: verifyNode{
				tpy:  constant,
				data: true,
			},
		},
		{
			expr: `
(<
 (+ 1
   (- 2 v3) (/ 6 3) 4)
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
									{tpy: selector, data: "v3"},
								},
							},
							{tpy: constant, data: int64(2)},
							{tpy: constant, data: int64(4)},
						},
					},
					{tpy: constant, data: int64(210)},
				},
			},
		},

		{
			expr: `
(>
   (t_version "1.2.1")
   (t_version "1.2.3")
)
`,
			ast: verifyNode{
				tpy:  constant,
				data: false,
			},
		},

		{
			expr: `
(>
   (t_version app_version)
   (t_version "1.2.3")
)
`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"app_version": SelectorKey(1),
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: ">",
				children: []verifyNode{
					{
						tpy:  operator,
						data: "t_version",
						children: []verifyNode{
							{tpy: selector, data: "app_version"},
						},
					},
					{tpy: constant, data: int64(1_0002_0003)},
				},
			},
		},

		{
			expr: `
(and
  (and
    (= 3 v3) 
    (< 
      (/ v6 3) v4))
 (< 5 v6)
 false
)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v3": SelectorKey(3),
					"v4": SelectorKey(4),
					"v6": SelectorKey(6),
				},
			},
			ast: verifyNode{
				tpy:  constant,
				data: false,
			},
		},

		{
			expr: `
(or
  (or
    (= 3 3) 
    (< 
      (/ v6 3) v4))
 (< 5 v6)
 false
)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v3": SelectorKey(3),
					"v4": SelectorKey(4),
					"v6": SelectorKey(6),
				},
			},
			ast: verifyNode{
				tpy:  constant,
				data: true,
			},
		},

		{
			cc: &CompileConfig{
				OperatorMap: map[string]Operator{
					"is_child": func(_ *Ctx, params []Value) (Value, error) {
						const (
							op       = "is_child"
							timeYear = time.Hour * 24 * 365
						)
						if len(params) != 2 {
							return nil, ParamsCountError(op, 2, len(params))
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

			// it won't trigger ConstantFolding for custom operators
			expr: `(is_child "2022-02-02" "2006-01-02")`,
			ast: verifyNode{
				tpy:  operator,
				data: "is_child",
				children: []verifyNode{
					{tpy: constant, data: "2022-02-02"},
					{tpy: constant, data: "2006-01-02"},
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			ast, cc, err := newParser(c.cc, c.expr).parse()
			assertNil(t, err, c)
			optimizeConstantFolding(cc, ast)
			assertAstTreeIdentical(t, ast, c.ast, c)
		})
	}
}

func TestOptimizeFastEvaluation(t *testing.T) {
	testCases := []struct {
		cc     *CompileConfig
		expr   string
		ast    verifyNode
		errMsg string
	}{
		{
			expr: `(+ 1 v1)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
				},
			},
			ast: verifyNode{
				tpy:  fastOperator,
				data: "+",
				children: []verifyNode{
					{tpy: constant, data: int64(1)},
					{tpy: selector, data: "v1"},
				},
			},
		},

		{
			expr: `(= v1 v1)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
				},
			},
			ast: verifyNode{
				tpy:  fastOperator,
				data: "=",
				children: []verifyNode{
					{tpy: selector, data: "v1"},
					{tpy: selector, data: "v1"},
				},
			},
		},

		{
			expr: `
(< 
  (+ 1 v1)
  (* v2 3 v4)
)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
					"v2": SelectorKey(2),
					"v4": SelectorKey(4),
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "<",
				children: []verifyNode{
					{
						tpy:  fastOperator,
						data: "+",
						children: []verifyNode{
							{tpy: constant, data: int64(1)},
							{tpy: selector, data: "v1"},
						},
					},
					{
						tpy:  operator,
						data: "*",
						children: []verifyNode{
							{tpy: selector, data: "v2"},
							{tpy: constant, data: int64(3)},
							{tpy: selector, data: "v4"},
						},
					},
				},
			},
		},

		{
			expr: `
(<
 (+ 1
   (- 2 v3) (/ 6 3) 4)
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
								tpy:  fastOperator,
								data: "-",
								children: []verifyNode{
									{tpy: constant, data: int64(2)},
									{tpy: selector, data: "v3"},
								},
							},
							{
								tpy:  fastOperator,
								data: "/",
								children: []verifyNode{
									{tpy: constant, data: int64(6)},
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
						if len(params) != 2 {
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
				tpy:  fastOperator,
				data: "is_child",
				children: []verifyNode{
					{tpy: selector, data: "birthday"},
					{tpy: constant, data: "Jan 02, 2006"}, // constant nodes will be replaced directly with the value
				},
			},
		},
	}

	for _, c := range testCases {

		t.Run(c.expr, func(t *testing.T) {
			ast, cc, err := newParser(c.cc, c.expr).parse()
			assertNil(t, err)

			optimizeFastEvaluation(cc, ast)
			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg, c)
				return
			}

			assertAstTreeIdentical(t, ast, c.ast, c)
		})
	}
}

func TestReordering(t *testing.T) {
	testCases := []struct {
		fastEval bool // whether to enable FastEvaluation optimization
		cc       *CompileConfig
		expr     string
		ast      verifyNode
		errMsg   string
	}{
		{
			expr: `(+ 1 v1)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "+",
				cost: 31, // 5(base) + 3(loops) + 10(op cost) + 13(children cost)
				children: []verifyNode{
					{tpy: constant, data: int64(1), cost: 1},
					{tpy: selector, data: "v1", cost: 12}, // 7 + 5
				},
			},
		},

		{
			expr:     `(+ 1 v1)`,
			fastEval: true,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
				},
			},
			ast: verifyNode{
				tpy:  fastOperator,
				data: "+",
				cost: 28, // 5(base) + 10(op cost) + 13(children cost)
				children: []verifyNode{
					{tpy: constant, data: int64(1), cost: 1},
					{tpy: selector, data: "v1", cost: 12}, // 7 + 5
				},
			},
		},

		{
			expr: `(= v1 v2)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
					"v2": SelectorKey(2),
				},
				CostsMap: map[string]float64{
					"v1": 10,
					"v2": 20,
					"=":  30,
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "=",
				cost: 78, // 5(base) + 3(loops) + 30(op cost) + 40(children cost)
				children: []verifyNode{
					{tpy: selector, data: "v1", cost: 15},
					{tpy: selector, data: "v2", cost: 25},
				},
			},
		},

		{
			expr:     `(= v1 v2)`,
			fastEval: true,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
					"v2": SelectorKey(2),
				},
				CostsMap: map[string]float64{
					"v1": 10,
					"v2": 20,
					"=":  30,
				},
			},
			ast: verifyNode{
				tpy:  fastOperator,
				data: "=",
				cost: 75, // 5(base) + 30(op cost) + 40(children cost)
				children: []verifyNode{
					{tpy: selector, data: "v1", cost: 15},
					{tpy: selector, data: "v2", cost: 25},
				},
			},
		},

		{
			expr: `
(and 
  (= v1 1)
  (= v2 2)  ;; this line will be reordered to the first line as it costs less
)
`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
					"v2": SelectorKey(2),
				},
				CostsMap: map[string]float64{
					"v1": 20,
					"v2": 10,
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "and",
				cost: 96, // 5(base) + 3(loops) + 10(op cost) + 78(children cost)
				children: []verifyNode{
					{
						tpy:  operator,
						data: "=",
						cost: 34,
						children: []verifyNode{
							{tpy: selector, data: "v2", cost: 15},
							{tpy: constant, data: int64(2), cost: 1},
						},
					},
					{
						tpy:  operator,
						data: "=",
						cost: 44,
						children: []verifyNode{
							{tpy: selector, data: "v1", cost: 25},
							{tpy: constant, data: int64(1), cost: 1},
						},
					},
				},
			},
		},

		{
			expr: `
(or
  (and
    (= 3 v3) 
    (< 
      (/ v6 3) v4))
 (< 5 v6)
 false
)`,
			fastEval: true,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v3": SelectorKey(3),
					"v4": SelectorKey(4),
					"v6": SelectorKey(6),
				},
				CostsMap: map[string]float64{
					"selector": 2,
					"operator": 3,
					"v3":       50,
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "or",
				cost: 138, // 5 + 4*1 + 3 + (1 + 16 + 109)
				children: []verifyNode{
					{tpy: constant, data: false, cost: 1},
					{
						tpy:  fastOperator,
						data: "<",
						cost: 16,
						children: []verifyNode{
							{tpy: constant, data: int64(5), cost: 1},
							{tpy: selector, data: "v6", cost: 7},
						},
					},
					{
						tpy:  operator,
						data: "and",
						cost: 109, // 5 + 3*1 + 3 + (34 + 64)
						children: []verifyNode{
							{
								tpy:  operator,
								data: "<",
								cost: 34, // 5 + 3*1 + 3 + (16 + 7)
								children: []verifyNode{
									{
										tpy:  fastOperator,
										data: "/",
										cost: 16, //  5 + 3 + (7 + 1)
										children: []verifyNode{
											{tpy: selector, data: "v6", cost: 7},
											{tpy: constant, data: int64(3), cost: 1},
										},
									},
									{tpy: selector, data: "v4", cost: 7},
								},
							},
							{
								tpy:  fastOperator,
								data: "=",
								cost: 64, // 5 + 3 + (1 + 55)
								children: []verifyNode{
									{tpy: constant, data: int64(3), cost: 1},
									{tpy: selector, data: "v3", cost: 55},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			ast, cc, err := newParser(c.cc, c.expr).parse()
			assertNil(t, err)

			if c.fastEval {
				optimizeFastEvaluation(cc, ast)
			}

			calculateNodeCosts(cc, ast)
			optimizeReordering(cc, ast)
			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg, c)
				return
			}

			assertAstTreeIdentical(t, ast, c.ast, c)
		})
	}

}

func TestOptimize(t *testing.T) {
	testCases := []struct {
		cc     *CompileConfig
		expr   string
		ast    verifyNode
		errMsg string
	}{
		{
			expr: `(+ 1 v1)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
				},
			},
			ast: verifyNode{
				tpy:  fastOperator,
				data: "+",
				children: []verifyNode{
					{tpy: constant, data: int64(1)},
					{tpy: selector, data: "v1"},
				},
			},
		},

		{
			expr: `(= 1 1)`,
			ast: verifyNode{
				tpy:  constant,
				data: true,
			},
		},

		{
			expr: `
(or
  (and
    (= 3 v3) 
    (< 
      (/ 6 3) v4))
 (< 5 v6)
 (= 1 0)
)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v3": SelectorKey(3),
					"v4": SelectorKey(4),
					"v6": SelectorKey(6),
				},
				CostsMap: map[string]float64{
					"selector": 2,
					"operator": 3,
					"v3":       50,
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "or",
				cost: 120, // 5 + 4*1 + 3 + (1 + 16 + 91)
				children: []verifyNode{
					{tpy: constant, data: false, cost: 1},
					{
						tpy:  fastOperator,
						data: "<",
						cost: 16,
						children: []verifyNode{
							{tpy: constant, data: int64(5), cost: 1},
							{tpy: selector, data: "v6", cost: 7},
						},
					},
					{
						tpy:  operator,
						data: "and",
						cost: 91, // 5 + 3*1 + 3 + (16 + 64)
						children: []verifyNode{
							{
								tpy:  fastOperator,
								data: "<",
								cost: 16, // 5 + 3 + (1 + 7)
								children: []verifyNode{
									{tpy: constant, data: int64(2), cost: 1},
									{tpy: selector, data: "v4", cost: 7},
								},
							},
							{
								tpy:  fastOperator,
								data: "=",
								cost: 64, // 5 + 3 + (1 + 55)
								children: []verifyNode{
									{tpy: constant, data: int64(3), cost: 1},
									{tpy: selector, data: "v3", cost: 55},
								},
							},
						},
					},
				},
			},
		},

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
						if len(params) != 2 {
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
				tpy:  fastOperator,
				data: "is_child",
				children: []verifyNode{
					{tpy: selector, data: "birthday"},
					{tpy: constant, data: "Jan 02, 2006"}, // constant nodes will be replaced directly with the value
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			ast, cc, err := newParser(c.cc, c.expr).parse()
			assertNil(t, err)

			optimize(cc, ast)
			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg, c)
			}

			assertAstTreeIdentical(t, ast, c.ast, c)
		})
	}
}

func TestCheck(t *testing.T) {
	testCases := []struct {
		cc       *CompileConfig
		optimize bool
		expr     string
		size     int
		errMsg   string
	}{
		{
			expr: `(+ 1 1)`,
			size: 3,
		},
		{
			expr:     `(+ 1 1)`,
			optimize: true,
			size:     1,
		},
		{
			expr: `(+ 1 v1)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
				},
			},
			size: 3,
		},
		{
			expr:     `(+ 1 v1)`,
			optimize: true,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(1),
				},
			},
			size: 3,
		},

		{
			expr: `
(<
 (+ 1
   (- 2 3) (/ 6 3) 4)
 (* 5 6 7)
)
`,
			size: 14,
		},
		{
			expr: `
(<
 (+ 1
   (- 2 3) (/ 6 3) 4)
 (* 5 6 7)
)
`,
			optimize: true,
			size:     1,
		},
		{
			expr: fmt.Sprintf(`(+ %s)`, strings.Repeat(`1 `, 127)),
			size: 128,
		},
		{
			expr:   fmt.Sprintf(`(+ %s)`, strings.Repeat(`1 `, 128)),
			errMsg: "operators cannot exceed a maximum of 127 parameters",
		},
		{
			expr: fmt.Sprintf(
				`(and %s)`, strings.Repeat(
					fmt.Sprintf(`(= %s)`, strings.Repeat(`(= 1 1) `, 127)), 127)),
			errMsg: "expression cannot exceed a maximum of 32767 nodes",
		},
	}

	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			ast, cc, err := newParser(c.cc, c.expr).parse()
			assertNil(t, err)

			if c.optimize {
				optimize(cc, ast)
			}

			res := check(ast)

			if len(c.errMsg) != 0 {
				assertErrStrContains(t, res.err, c.errMsg, c)
				return
			}

			assertNil(t, res.err, c)
			assertEquals(t, res.size, c.size, c)
		})
	}
}

func TestCompile(t *testing.T) {
	testCases := []struct {
		cc     *CompileConfig
		expr   string
		nodes  []*node
		errMsg string
	}{
		{
			expr: `(+ 1 2)`,
			cc:   NewCompileConfig(Optimizations(false)),
			nodes: []*node{
				{
					flag:  constant,
					osTop: 0,
					scIdx: 0,
					value: int64(1),
				},
				{
					flag:  constant,
					osTop: 1,
					scIdx: 1,
					value: int64(2),
				},
				{
					flag:     operator,
					childCnt: 2,
					osTop:    0,
					scIdx:    -1,
					value:    "+",
				},
			},
		},
		{
			expr: `(+ 1 2)`,
			nodes: []*node{
				{
					flag:  constant,
					osTop: 0,
					scIdx: -1,
					value: int64(3),
				},
			},
		},
		{
			expr: `(+ 1 v1)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(168),
				},
				CompileOptions: map[Option]bool{
					FastEvaluation: false,
				},
			},
			nodes: []*node{
				{
					flag:  constant,
					osTop: 0,
					scIdx: 0,
					value: int64(1),
				},
				{
					flag:   selector,
					osTop:  1,
					scIdx:  1,
					selKey: SelectorKey(168),
					value:  "v1",
				},
				{
					flag:     operator,
					childCnt: 2,
					osTop:    0,
					scIdx:    -1,
					value:    "+",
				},
			},
		},
		{
			expr: `(+ 1 v1)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"v1": SelectorKey(168),
				},
			},
			nodes: []*node{
				{
					flag:     fastOperator,
					childCnt: 2,
					scIdx:    0,
					osTop:    0,
					value:    "+",
				},
				{
					flag:  constant,
					osTop: 0,
					scIdx: 1,
					value: int64(1),
				},
				{
					flag:   selector,
					osTop:  0,
					scIdx:  -1,
					selKey: SelectorKey(168),
					value:  "v1",
				},
			},
		},

		{
			expr: `(and T1 T2 F)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"T1": SelectorKey(168),
					"T2": SelectorKey(169),
					"F":  SelectorKey(170),
				},
				CostsMap: map[string]float64{
					"T1": 3,
					"T2": 1,
					"F":  5,
				},
			},
			nodes: []*node{
				{
					flag:   selector | scIfFalse | andOp,
					osTop:  0,
					scIdx:  -1,
					selKey: SelectorKey(169),
					value:  "T2",
				},
				{
					flag:   selector | scIfFalse | andOp,
					osTop:  1,
					scIdx:  -1,
					selKey: SelectorKey(168),
					value:  "T1",
				},
				{
					flag:   selector | (scIfFalse | scIfTrue) | andOp,
					osTop:  2,
					scIdx:  -1,
					selKey: SelectorKey(170),
					value:  "F",
				},
				{
					flag:     operator,
					childCnt: 3,
					scIdx:    -1,
					osTop:    0,
					value:    "and",
				},
			},
		},

		{
			expr: `(and A (or B C (= D E)) F)`,
			cc: &CompileConfig{
				SelectorMap: map[string]SelectorKey{
					"A": SelectorKey(168),
					"B": SelectorKey(169),
					"C": SelectorKey(170),
					"D": SelectorKey(171),
					"E": SelectorKey(172),
					"F": SelectorKey(173),
				},
				CompileOptions: map[Option]bool{
					Reordering: false,
				},
			},
			nodes: []*node{
				{
					flag:   selector | scIfFalse | andOp,
					osTop:  0,
					scIdx:  -1,
					selKey: SelectorKey(168),
					value:  "A",
				},
				{
					flag:   selector | scIfTrue | orOp,
					osTop:  1,
					scIdx:  6,
					selKey: SelectorKey(169),
					value:  "B",
				},
				{
					flag:   selector | scIfTrue | orOp,
					osTop:  2,
					scIdx:  6,
					selKey: SelectorKey(170),
					value:  "C",
				},
				{
					flag:     fastOperator | (scIfTrue | scIfFalse) | orOp,
					childCnt: 2,
					scIdx:    6,
					osTop:    3,
					value:    "=",
				},
				{
					flag:   selector,
					osTop:  3,
					scIdx:  4,
					selKey: SelectorKey(171),
					value:  "D",
				},
				{
					flag:   selector,
					osTop:  3,
					scIdx:  5,
					selKey: SelectorKey(172),
					value:  "E",
				},
				{
					flag:     operator | scIfFalse | andOp,
					childCnt: 3,
					osTop:    1,
					scIdx:    -1,
					value:    "or",
				},
				{
					flag:   selector | (scIfTrue | scIfFalse) | andOp,
					osTop:  2,
					scIdx:  -1,
					selKey: SelectorKey(173),
					value:  "F",
				},
				{
					flag:     operator,
					childCnt: 3,
					osTop:    0,
					scIdx:    -1,
					value:    "and",
				},
			},
		},
		{
			expr:   `(and ()`,
			errMsg: "parentheses unmatched error",
		},
		{
			expr:   fmt.Sprintf(`(+ %s)`, strings.Repeat(`1 `, 128)),
			cc:     NewCompileConfig(Optimizations(false)),
			errMsg: "operators cannot exceed a maximum of 127 parameters",
		},
		{
			expr: fmt.Sprintf(
				`(and %s)`, strings.Repeat(
					fmt.Sprintf(`(= %s)`, strings.Repeat(`(= 1 1) `, 127)), 127)),
			cc:     NewCompileConfig(Optimizations(false)),
			errMsg: "expression cannot exceed a maximum of 32767 nodes",
		},
	}

	for _, c := range testCases {
		t.Run(c.expr, func(t *testing.T) {
			e, err := Compile(c.cc, c.expr)

			if len(c.errMsg) != 0 {
				assertErrStrContains(t, err, c.errMsg)
				return
			}

			assertNil(t, err)

			assertEquals(t, len(e.nodes), len(c.nodes))

			maxOsTop := int16(math.MinInt16)
			for i, want := range c.nodes {
				got := e.nodes[i]
				assertEquals(t, got.value, want.value, "value")
				assertEquals(t, got.flag, want.flag, "flag", got.value)
				assertEquals(t, got.childCnt, want.childCnt, "childCnt", got.value)
				assertEquals(t, got.scIdx, want.scIdx, "scIdx", got.value)
				assertEquals(t, got.osTop, want.osTop, "osTop", got.value)
				assertEquals(t, got.selKey, want.selKey, "selKey", got.value)

				maxOsTop = maxInt16(maxOsTop, want.osTop)
			}

			assertEquals(t, e.maxStackSize, maxOsTop+1)
		})
	}
}
