package eval

type (
	labNode struct {
		flag      uint8
		child     int8  // child count
		index     int16 // index of original expr
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
		switch n.getNodeType() {
		case constant, selector:
			res = append(res, &labNode{
				flag:   n.flag,
				index:  i,
				value:  n.value,
				selKey: n.selKey,
			})
		case operator, fastOperator:
			cCnt := int16(n.childCnt)
			cIdx := n.childIdx
			for j := cIdx; j < cIdx+cCnt; j++ {
				helper(j)
			}
			res = append(res, &labNode{
				flag:     n.flag,
				index:    i,
				child:    n.childCnt,
				value:    n.value,
				operator: n.operator,
			})
		}
		pos := int16(len(res) - 1)
		posToIdx[pos] = i
		idxToPos[i] = pos
	}

	helper(0)
	for _, n := range res {
		pIdx := e.nodes[n.index].scIdx
		if pIdx == -1 {
			n.parentPos = -1
		} else {
			n.parentPos = idxToPos[pIdx]
		}

	}

	return &labExpr{
		maxStackSize: e.maxStackSize,
		nodes:        res,
		osSize:       e.osSize,
	}
}

func (e *labExpr) Eval(ctx *Ctx) (Value, error) {
	var (
		nodes = e.nodes
		size  = int16(len(nodes))
		m     = e.maxStackSize

		os    []Value
		osTop = int16(-1)

		res    Value
		err    error
		param  []Value
		param2 [2]Value
		curt   *labNode
	)

	switch {
	case m <= 8:
		os = make([]Value, 8)
	case m <= 16:
		os = make([]Value, 16)
	default:
		os = make([]Value, size)
	}

	for i := int16(0); i < size; i++ {
		curt = nodes[i]
		switch curt.flag & nodeTypeMask {
		case constant:
			res = curt.value
		case selector:
			res, err = ctx.Get(curt.selKey, curt.value.(string))
			if err != nil {
				return nil, err
			}
		case operator, fastOperator:
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
			if err != nil {
				return nil, err
			}

		}

		if b, ok := res.(bool); ok {
			for (!b && curt.flag&scIfFalse == scIfFalse) ||
				(b && curt.flag&scIfTrue == scIfTrue) {
				i = curt.parentPos
				if i == -1 {
					return res, nil
				}
				curt = nodes[i]
				osTop = e.osSize[curt.index] - 1
			}
		}

		os[osTop+1], osTop = res, osTop+1
	}
	return os[0], nil
}
