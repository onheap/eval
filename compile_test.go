package eval

import (
	"fmt"
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
	assertNotNil(t, res.OptimizeOptions)

	res = CopyCompileConfig(&CompileConfig{})
	assertNotNil(t, res)
	assertNotNil(t, res.OperatorMap)
	assertNotNil(t, res.ConstantMap)
	assertNotNil(t, res.SelectorMap)
	assertNotNil(t, res.OptimizeOptions)

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
		CostsMap: map[string]int{
			"selectors": 10,
			"operators": 20,
		},
		OptimizeOptions: map[OptimizeOption]bool{
			Reordering:      true,
			ConstantFolding: false,
		},
	}

	res = CopyCompileConfig(cc)
	assertEquals(t, res.ConstantMap, cc.ConstantMap)
	assertEquals(t, res.SelectorMap, cc.SelectorMap)
	assertEquals(t, res.OptimizeOptions, cc.OptimizeOptions)
	assertEquals(t, res.CostsMap, cc.CostsMap)

	assertEquals(t, len(res.OperatorMap), len(cc.OperatorMap))
	for s := range cc.OperatorMap {
		got := reflect.ValueOf(res.OperatorMap[s])
		want := reflect.ValueOf(cc.OperatorMap[s])
		assertEquals(t, got, want)
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
		CostsMap: map[string]int{
			"selectors": 10, // generally costs for all selectors
			"operators": 20, // generally costs for all operators

			"is_child": 13, // specified costs for operator `is_child`
			"birthday": 11, // specified costs for selector `birthday`
		},
	}

	assertEquals(t, cc.getCosts(selector, "birthday"), 11)
	assertEquals(t, cc.getCosts(selector, "gender"), 10)
	assertEquals(t, cc.getCosts(operator, "is_child"), 13)
	assertEquals(t, cc.getCosts(fastOperator, "is_child"), 13)
	assertEquals(t, cc.getCosts(operator, "not_null"), 20)
	assertEquals(t, cc.getCosts(fastOperator, "not_null"), 20)

	cc = &CompileConfig{
		SelectorMap: map[string]SelectorKey{
			"birthday": SelectorKey(3),
			"gender":   SelectorKey(4),
		},
		OperatorMap: map[string]Operator{
			"is_child": func(_ *Ctx, _ []Value) (Value, error) { return false, nil },
			"not_null": func(_ *Ctx, _ []Value) (Value, error) { return false, nil },
		},
		CostsMap: map[string]int{},
	}

	assertEquals(t, cc.getCosts(selector, "birthday"), 7)
	assertEquals(t, cc.getCosts(selector, "gender"), 7)
	assertEquals(t, cc.getCosts(operator, "is_child"), 10)
	assertEquals(t, cc.getCosts(fastOperator, "is_child"), 10)
	assertEquals(t, cc.getCosts(operator, "not_null"), 10)
	assertEquals(t, cc.getCosts(fastOperator, "not_null"), 10)
}

func TestOptimizeConstantFolding(t *testing.T) {
	testCases := []struct {
		cc     *CompileConfig
		expr   string
		ast    verifyNode
		errMsg string
	}{
		{
			expr: `(+ 1 1)`,
			ast: verifyNode{
				tpy:  value,
				data: int64(2),
			},
		},

		{
			expr: `(= 1 1)`,
			ast: verifyNode{
				tpy:  value,
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
				tpy:  value,
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
				tpy:  value,
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
							{tpy: value, data: int64(1)},
							{
								tpy:  operator,
								data: "-",
								children: []verifyNode{
									{tpy: value, data: int64(2)},
									{tpy: selector, data: "v3"},
								},
							},
							{tpy: value, data: int64(2)},
							{tpy: value, data: int64(4)},
						},
					},
					{tpy: value, data: int64(210)},
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
				tpy:  value,
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
					{tpy: value, data: int64(1_0002_0003)},
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
				tpy:  value,
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
				tpy:  value,
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

			// it won't trigger ConstantFolding for custom operators
			expr: `(is_child "2022-02-02" "2006-01-02")`,
			ast: verifyNode{
				tpy:  operator,
				data: "is_child",
				children: []verifyNode{
					{tpy: value, data: "2022-02-02"},
					{tpy: value, data: "2006-01-02"},
				},
			},
		},
	}

	for _, c := range testCases {
		ast, cc, err := newParser(c.cc, c.expr).parse()
		assertNil(t, err, c)
		err = optimizeConstantFolding(cc, ast)
		if len(c.errMsg) != 0 {
			assertErrStrContains(t, err, c.errMsg, c)
			continue
		}

		assertAstTreeIdentical(t, ast, c.ast, c)
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
					{tpy: value, data: int64(1)},
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
							{tpy: value, data: int64(1)},
							{tpy: selector, data: "v1"},
						},
					},
					{
						tpy:  fastOperator,
						data: "*",
						children: []verifyNode{
							{tpy: selector, data: "v2"},
							{tpy: value, data: int64(3)},
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
							{tpy: value, data: int64(1)},
							{
								tpy:  fastOperator,
								data: "-",
								children: []verifyNode{
									{tpy: value, data: int64(2)},
									{tpy: selector, data: "v3"},
								},
							},
							{
								tpy:  fastOperator,
								data: "/",
								children: []verifyNode{
									{tpy: value, data: int64(6)},
									{tpy: value, data: int64(3)},
								},
							},
							{tpy: value, data: int64(4)},
						},
					},
					{
						tpy:  fastOperator,
						data: "*",
						children: []verifyNode{
							{tpy: value, data: int64(5)},
							{tpy: value, data: int64(6)},
							{tpy: value, data: int64(7)},
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
				tpy:  fastOperator,
				data: "is_child",
				children: []verifyNode{
					{tpy: selector, data: "birthday"},
					{tpy: value, data: "Jan 02, 2006"}, // constant nodes will be replaced directly with the value
				},
			},
		},
	}

	for _, c := range testCases {
		ast, cc, err := newParser(c.cc, c.expr).parse()
		assertNil(t, err)

		optimizeFastEvaluation(cc, ast)
		if len(c.errMsg) != 0 {
			assertErrStrContains(t, err, c.errMsg, c)
			continue
		}

		assertAstTreeIdentical(t, ast, c.ast, c)
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
					{tpy: value, data: int64(1), cost: 1},
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
					{tpy: value, data: int64(1), cost: 1},
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
				CostsMap: map[string]int{
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
				CostsMap: map[string]int{
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
				CostsMap: map[string]int{
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
							{tpy: value, data: int64(2), cost: 1},
						},
					},
					{
						tpy:  operator,
						data: "=",
						cost: 44,
						children: []verifyNode{
							{tpy: selector, data: "v1", cost: 25},
							{tpy: value, data: int64(1), cost: 1},
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
				CostsMap: map[string]int{
					"selectors": 2,
					"operators": 3,
					"v3":        50,
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "or",
				cost: 138, // 5 + 4*1 + 3 + (1 + 16 + 109)
				children: []verifyNode{
					{tpy: value, data: false, cost: 1},
					{
						tpy:  fastOperator,
						data: "<",
						cost: 16,
						children: []verifyNode{
							{tpy: value, data: int64(5), cost: 1},
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
											{tpy: value, data: int64(3), cost: 1},
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
									{tpy: value, data: int64(3), cost: 1},
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
		ast, cc, err := newParser(c.cc, c.expr).parse()
		assertNil(t, err)

		if c.fastEval {
			optimizeFastEvaluation(cc, ast)
		}

		calculateNodeCosts(cc, ast)
		optimizeReordering(ast)
		if len(c.errMsg) != 0 {
			assertErrStrContains(t, err, c.errMsg, c)
			continue
		}

		assertAstTreeIdentical(t, ast, c.ast, c)
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
					{tpy: value, data: int64(1)},
					{tpy: selector, data: "v1"},
				},
			},
		},

		{
			expr: `(= 1 1)`,
			ast: verifyNode{
				tpy:  value,
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
				CostsMap: map[string]int{
					"selectors": 2,
					"operators": 3,
					"v3":        50,
				},
			},
			ast: verifyNode{
				tpy:  operator,
				data: "or",
				cost: 120, // 5 + 4*1 + 3 + (1 + 16 + 91)
				children: []verifyNode{
					{tpy: value, data: false, cost: 1},
					{
						tpy:  fastOperator,
						data: "<",
						cost: 16,
						children: []verifyNode{
							{tpy: value, data: int64(5), cost: 1},
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
									{tpy: value, data: int64(2), cost: 1},
									{tpy: selector, data: "v4", cost: 7},
								},
							},
							{
								tpy:  fastOperator,
								data: "=",
								cost: 64, // 5 + 3 + (1 + 55)
								children: []verifyNode{
									{tpy: value, data: int64(3), cost: 1},
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
				tpy:  fastOperator,
				data: "is_child",
				children: []verifyNode{
					{tpy: selector, data: "birthday"},
					{tpy: value, data: "Jan 02, 2006"}, // constant nodes will be replaced directly with the value
				},
			},
		},
	}

	for _, c := range testCases {
		ast, cc, err := newParser(c.cc, c.expr).parse()
		assertNil(t, err)

		optimize(cc, ast)
		if len(c.errMsg) != 0 {
			assertErrStrContains(t, err, c.errMsg, c)
			continue
		}

		assertAstTreeIdentical(t, ast, c.ast, c)
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
			errMsg: "expression is too long, operators cannot exceed a maximum of 127 parameters",
		},
		{
			expr: fmt.Sprintf(
				`(and %s)`, strings.Repeat(
					fmt.Sprintf(`(= %s)`, strings.Repeat(`(= 1 1) `, 127)), 127)),
			errMsg: "expression is too long, expression cannot exceed a maximum of 32767 nodes",
		},
	}

	for _, c := range testCases {
		ast, cc, err := newParser(c.cc, c.expr).parse()
		assertNil(t, err)

		if c.optimize {
			optimize(cc, ast)
		}

		res := check(ast)

		if len(c.errMsg) != 0 {
			assertErrStrContains(t, res.err, c.errMsg, c)
			continue
		}

		assertNil(t, res.err, c)
		assertEquals(t, res.size, c.size, c)
	}
}

func TestCompress(t *testing.T) {

}

func TestCalculateStackSize(t *testing.T) {

}

func TestCompile(t *testing.T) {

}
