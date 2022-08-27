package eval

import "fmt"

func ExampleEval() {
	output, err := Eval(`(+ 1 v1)`, map[string]interface{}{
		"v1": 1,
	})
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}

	fmt.Printf("%v", output)

	// Output: 2
}

func ExampleEval_infix() {

	expr := `1 + v2 * (v3 + v5) / v4 + abs(-6 - v1) - max(1, 3, 2, abs(-8))`

	vals := map[string]interface{}{
		"abs": Operator(func(_ *Ctx, params []Value) (Value, error) {
			if len(params) != 1 {
				return nil, ParamsCountError("abs", 2, len(params))
			}
			i, ok := params[0].(int64)
			if !ok {
				return nil, ParamTypeError("abs", "int", params[0])
			}
			if i < 0 {
				return -i, nil
			}
			return i, nil
		}),
		"max": func(_ *Ctx, params []Value) (Value, error) {
			if len(params) == 0 {
				return nil, ParamsCountError("max", 1, len(params))
			}

			var res int64
			for i, v := range params {
				i64, ok := v.(int64)
				if !ok {
					return nil, ParamTypeError("max", "int64", v)
				}
				if i == 0 {
					res = i64
					continue
				}
				if res < i64 {
					res = i64
				}
			}
			return res, nil
		},
		"v1": 1,
		"v2": 2,
		"v3": 3,
		"v4": 4,
		"v5": 5,
	}
	cc := NewCompileConfig(EnableInfixNotation, RegisterVals(vals))

	output, err := Eval(expr, vals, cc)
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}

	fmt.Printf("%v", output)

	// Output: 4
}
