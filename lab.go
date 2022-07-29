package eval

import (
	"fmt"
	"strings"
)

type (
	labNode struct {
		flag      uint8
		child     int8  // child count
		osTop     int16 // os Top when short circuit triggered
		parentPos int16 // pos of labExpr
		selKey    SelectorKey
		value     Value
		operator  Operator
	}
	labExpr struct {
		maxStackSize int16
		nodes        []*labNode
		osSize       []int16
	}
)

func ConvertLabExpr(e *Expr) *labExpr {
	var (
		nodes    = e.nodes
		size     = len(nodes)
		res      = make([]*labNode, 0, size)
		posToIdx = make([]int16, size)
		idxToPos = make([]int16, size)

		helper func(i int16)
	)

	helper = func(i int16) {
		n := nodes[i]
		var pos int16
		switch n.getNodeType() {
		case constant, selector:
			res = append(res, &labNode{
				flag:   n.flag,
				value:  n.value,
				selKey: n.selKey,
			})
			pos = int16(len(res) - 1)
		case operator:
			cCnt := int16(n.childCnt)
			cIdx := n.childIdx
			for j := cIdx; j < cIdx+cCnt; j++ {
				helper(j)
			}
			res = append(res, &labNode{
				flag:     n.flag,
				child:    n.childCnt,
				value:    n.value,
				operator: n.operator,
			})
			pos = int16(len(res) - 1)

		case fastOperator:
			res = append(res, &labNode{
				flag:     n.flag,
				child:    n.childCnt,
				value:    n.value,
				operator: n.operator,
			})

			pos = int16(len(res) - 1)

			cCnt := int16(n.childCnt)
			cIdx := n.childIdx
			for j := cIdx; j < cIdx+cCnt; j++ {
				helper(j)
			}
		}

		posToIdx[pos] = i
		idxToPos[i] = pos
	}

	helper(0)
	for pos, n := range res {
		idx := posToIdx[pos]
		pIdx := e.nodes[idx].scIdx
		if pIdx == -1 {
			n.parentPos = -1
		} else {
			n.parentPos = idxToPos[pIdx]
		}
		n.osTop = e.osSize[idx] - 1
	}

	return &labExpr{
		maxStackSize: e.maxStackSize,
		nodes:        res,
		osSize:       e.osSize,
	}
}

func (e *labExpr) Eval(ctx *Ctx) (res Value, err error) {
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
		param  []Value
		param2 [2]Value
		curt   *labNode
		prev   int16
	)

	for i := int16(0); i < size; i++ {
		curt = nodes[i]

		//fmt.Printf("curt: %v\n", curt.value)

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
			//fmt.Printf("exec, op:[%v], param:[%v], res:[%v], err:[%v]\n", curt.value, param2, res, err)
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
			cCnt := int16(curt.child)
			osTop = osTop - cCnt
			if cCnt == 2 {
				param2[0], param2[1] = os[osTop+1], os[osTop+2]
				param = param2[:]
			} else {
				param = make([]Value, cCnt)
				copy(param, os[osTop+1:])
			}

			res, err = curt.operator(ctx, param)
			//fmt.Printf("exec, op:[%v], param:[%v], res:[%v], err:[%v]\n", curt.value, param2, res, err)
			if err != nil {
				return nil, err
			}
		case cond:
			res, osTop = os[osTop+1], osTop-1
			switch res {
			case true:
			case false:
				i += curt.parentPos
			default:
				return nil, fmt.Errorf("eval error, result type of if condition should be bool, got: [%v]", res)
			}
			continue
		default:
			printDebug(prev, i, os, osTop, nodes)
			continue
		}

		if b, ok := res.(bool); ok {
			for (!b && curt.flag&scIfFalse == scIfFalse) ||
				(b && curt.flag&scIfTrue == scIfTrue) {
				i = curt.parentPos
				if i == -1 {
					return res, nil
				}
				curt = nodes[i]
				osTop = curt.osTop
			}
		}

		os[osTop+1], osTop = res, osTop+1
		prev = i
	}
	return os[0], nil
}

func printDebug(prevIdx, curtIdx int16, os []Value, osTop int16, nodes []*labNode) {
	var (
		sb   strings.Builder
		curt = nodes[curtIdx].value
	)

	if curtIdx-prevIdx > 1 {
		sb.WriteString(fmt.Sprintf("[%v] short circuit to [%v]\n", curt, nodes[prevIdx].value))
	}

	sb.WriteString(fmt.Sprintf("%10v", curt))

	sb.WriteString(fmt.Sprintf("%15s", "Operand Stack: "))
	for i := osTop; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("|%4v", os[i]))
	}
	sb.WriteString("|\n")
	fmt.Println(sb.String())
}
