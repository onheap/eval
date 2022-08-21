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

	expr := `1 + v2 * (v3 + v5) / v4`

	vals := map[string]interface{}{
		"v2": 2,
		"v3": 3,
		"v4": 4,
		"v5": 5,
	}
	cc := NewCompileConfig(EnableInfixNotation, RegisterSelKeys(vals))

	output, err := Eval(expr, vals, cc)
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}

	fmt.Printf("%v", output)

	// Output: 5
}
