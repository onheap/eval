package eval

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type (
	SelectorKey int16
	Value       interface{}
	Operator    func(ctx *Ctx, params []Value) (res Value, err error)
)

type Ctx struct {
	Selector
	Ctx context.Context
}

const (
	// node types flag
	nodeTypeMask = uint8(0b00000111)
	constant     = uint8(0b00000001)
	selector     = uint8(0b00000010)
	operator     = uint8(0b00000011)
	fastOperator = uint8(0b00000100)
	cond         = uint8(0b00000101)
	debug        = uint8(0b00000110)

	// short circuit flag
	scMask    = uint8(0b00011000)
	scIfFalse = uint8(0b00001000)
	scIfTrue  = uint8(0b00010000)
)

type node struct {
	flag     uint8
	childCnt int8
	scIdx    int16
	osTop    int16
	selKey   SelectorKey
	value    Value
	operator Operator
}

func (n *node) getNodeType() uint8 {
	return n.flag & nodeTypeMask
}

type Expr struct {
	maxStackSize int16
	nodes        []*node
	parentIdx    []int16
}

func Eval(expr string, vals map[string]interface{}, confs ...*CompileConfig) (Value, error) {
	var conf *CompileConfig
	if len(confs) > 1 {
		return nil, errors.New("error: too many compile configurations")
	}

	if len(confs) == 1 {
		conf = confs[0]
	} else {
		conf = NewCompileConfig(RegisterSelKeys(vals))
	}

	tree, err := Compile(conf, expr)
	if err != nil {
		return nil, err
	}

	return tree.Eval(NewCtxWithMap(conf, vals))
}

func (e *Expr) Eval(ctx *Ctx) (res Value, err error) {
	var (
		nodes = e.nodes
		size  = int16(len(nodes))
		m     = e.maxStackSize

		os    []Value
		osTop = int16(-1)
	)

	switch {
	case m <= 8:
		os = make([]Value, 8)
	case m <= 16:
		os = make([]Value, 16)
	default:
		os = make([]Value, size)
	}

	var (
		params []Value
		param2 [2]Value
		curt   *node
		prev   int16
	)

	for i := int16(0); i < size; i++ {
		curt = nodes[i]
		switch curt.flag & nodeTypeMask {
		case fastOperator:
			i++
			child := nodes[i]
			res = child.value
			if child.flag&nodeTypeMask == selector {
				res, err = ctx.Get(child.selKey, res.(string))
				if err != nil {
					return
				}
			}
			param2[0] = res

			i++
			child = nodes[i]
			res = child.value
			if child.flag&nodeTypeMask == selector {
				res, err = ctx.Get(child.selKey, res.(string))
				if err != nil {
					return
				}
			}
			param2[1] = res
			res, err = curt.operator(ctx, param2[:])
			if err != nil {
				return
			}
		case selector:
			res, err = ctx.Get(curt.selKey, curt.value.(string))
			if err != nil {
				return
			}
		case constant:
			res = curt.value
		case operator:
			cCnt := int16(curt.childCnt)
			osTop = osTop - cCnt
			if cCnt == 2 {
				param2[0], param2[1] = os[osTop+1], os[osTop+2]
				params = param2[:]
			} else {
				params = make([]Value, cCnt)
				copy(params, os[osTop+1:])
			}

			res, err = curt.operator(ctx, params)
			if err != nil {
				return
			}
		case cond:
			res, osTop = os[osTop], osTop-1
			res, err = curt.operator(ctx, []Value{res})
			if err != nil {
				return
			}
			if res == true {
				osTop = curt.osTop
				i = curt.scIdx
			}
			continue
		default:
			printDebugExpr(e, prev, i, os, osTop)
			continue
		}
		if b, ok := res.(bool); ok {
			for (!b && curt.flag&scIfFalse == scIfFalse) ||
				(b && curt.flag&scIfTrue == scIfTrue) {
				i = curt.scIdx
				if i == -1 {
					return
				}

				curt = nodes[i]
				osTop = curt.osTop - 1
			}
		}

		os[osTop+1], osTop = res, osTop+1
		prev = i
	}
	return os[0], nil
}

func printDebugExpr(e *Expr, prevIdx, curtIdx int16, os []Value, osTop int16) {
	var (
		sb   strings.Builder
		curt = e.nodes[curtIdx].value
	)

	if curtIdx-prevIdx > 2 {
		sb.WriteString(fmt.Sprintf("%13s: [%v] jump to [%v]\n\n", "Short Circuit", e.nodes[prevIdx].value, curt))
	} else {
		sb.WriteString(fmt.Sprintf("\n"))
	}

	sb.WriteString(fmt.Sprintf("%13s: [%v]\n", "Current Node", curt))

	sb.WriteString(fmt.Sprintf("%13s: ", "Operand Stack"))
	for i := osTop; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("|%4v", os[i]))
	}
	sb.WriteString("|")
	fmt.Println(sb.String())
}
