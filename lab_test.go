package eval

import (
	"fmt"
	"testing"
)

func TestWithLabExpr(t *testing.T) {
	s := `
(and
 (or
   (eq Origin "MOW")
   (= Country "RU"))
 (or
   (>= Value 100)
   (<= Adults 1)))
`
	params := map[string]interface{}{
		"Origin":  "MOW",
		"Country": "RU",
		"Adults":  1,
		"Value":   100,
	}

	cc := NewCompileConfig(EnableStringSelectors)

	ctx := NewCtxWithMap(cc, params)

	expr, err := Compile(cc, s)
	assertNil(t, err)

	fmt.Print(PrintExpr(expr))

	le := ConvertLabExpr(expr)

	//for i, n := range le.nodes {
	//	pPos := n.parentPos
	//	if pPos == -1 {
	//		fmt.Printf("NO.%2d curt: [%8v], scTarget:[%8v]\n", i, n.value, "RET")
	//	} else {
	//		p := le.nodes[pPos]
	//		fmt.Printf("NO.%2d curt: [%8v], scTarget:[%8v]\n", i, n.value, p.value)
	//	}
	//}

	res, err := le.Eval(ctx)
	assertNil(t, err)
	assertEquals(t, res, true)
}
