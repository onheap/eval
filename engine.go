package eval

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type (
	SelectorKey int16
	Value       interface{}
	Operator    func(ctx *Ctx, params []Value) (res Value, err error)
)

const (
	// node types
	nodeTypeMask = uint16(0b111)
	value        = uint16(0b001)
	selector     = uint16(0b010)
	operator     = uint16(0b011)
	fastOperator = uint16(0b100)
	cond         = uint16(0b101)
	end          = uint16(0b110)

	// short circuit flag
	scIfFalse = uint16(0b001000)
	scIfTrue  = uint16(0b010000)
)

type node struct {
	flag     uint16
	idx      int16
	scIdx    int16
	selKey   SelectorKey
	value    Value
	operator Operator
}

func (n *node) getNodeType() uint16 {
	return n.flag & nodeTypeMask
}

type Expr struct {
	maxStackSize    int16
	nodes           []*node
	childCounts     []int8
	childStartIndex []int16

	// extra info
	parentIndex []int
	sfSize      []int
	osSize      []int
}

func EvalBool(conf *CompileConfig, expr string, ctx *Ctx) (bool, error) {
	res, err := Eval(conf, expr, ctx)
	if err != nil {
		return false, err
	}
	b, ok := res.(bool)
	if !ok {
		return false, fmt.Errorf("invalid result type: %v", res)
	}
	return b, nil
}

func Eval(conf *CompileConfig, expr string, ctx *Ctx) (Value, error) {
	tree, err := Compile(conf, expr)
	if err != nil {
		return nil, err
	}
	return tree.Eval(ctx)
}

func (e *Expr) EvalInt(ctx *Ctx) (int64, error) {
	res, err := e.Eval(ctx)
	if err != nil {
		return 0, err
	}
	v, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid result type: %v", res)
	}
	return v, nil
}

func (e *Expr) EvalBool(ctx *Ctx) (bool, error) {
	res, err := e.Eval(ctx)
	if err != nil {
		return false, err
	}
	v, ok := res.(bool)
	if !ok {
		return false, fmt.Errorf("invalid result type: %v", res)
	}
	return v, nil
}

func (e *Expr) Eval(ctx *Ctx) (Value, error) {
	var (
		size   = e.maxStackSize
		debug  = ctx.Debug
		maxIdx = -1

		sf    []*node // stack frame
		sfTop = -1

		os    []Value // operand stack
		osTop = -1
	)

	// ensure that variables do not escape to the heap in most cases
	switch {
	case size <= 4:
		os = make([]Value, 4)
		sf = make([]*node, 4)
	case size <= 8:
		os = make([]Value, 8)
		sf = make([]*node, 8)
	case size <= 16:
		os = make([]Value, 16)
		sf = make([]*node, 16)
	default:
		os = make([]Value, size)
		sf = make([]*node, size)
	}

	var (
		curt    *node
		curtIdx int

		res Value // result of current stack frame
		err error

		param  []Value
		param2 [2]Value
	)

	// push the root node to the stack frame
	sf[sfTop+1], sfTop = e.nodes[0], sfTop+1

	for sfTop != -1 { // while stack frame is not empty
		if debug {
			printStacks(maxIdx, os, osTop, sf, sfTop)
		}
		curt, sfTop = sf[sfTop], sfTop-1
		curtIdx = int(curt.idx)

		switch curt.flag & nodeTypeMask {
		case fastOperator:
			cnt := int(e.childCounts[curtIdx])
			startIdx := int(e.childStartIndex[curtIdx])
			if cnt == 2 {
				param2[0], err = getNodeValue(ctx, e.nodes[startIdx])
				if err != nil {
					return nil, err
				}
				param2[1], err = getNodeValue(ctx, e.nodes[startIdx+1])
				if err != nil {
					return nil, err
				}
				param = param2[:]
			} else {
				param = make([]Value, cnt)
				for i := 0; i < cnt; i++ {
					child := e.nodes[startIdx+i]
					param[i], err = getNodeValue(ctx, child)
					if err != nil {
						return nil, err
					}
				}
			}

			res, err = curt.operator(ctx, param)
			if debug {
				printOperatorFunc(curt.value, param, res, err)
			}
			if err != nil {
				return nil, fmt.Errorf("eval error [%w], Operator: %v", err, curt.value)
			}
		case operator:
			cnt := int(e.childCounts[curtIdx])
			if curtIdx > maxIdx {
				// the node has never been visited before
				maxIdx = curtIdx
				sf[sfTop+1], sfTop = curt, sfTop+1
				startIdx := int(e.childStartIndex[curtIdx])
				// push child nodes into the stack frame
				// the back nodes is on top
				if cnt == 2 {
					sf[sfTop+1], sfTop = e.nodes[startIdx+1], sfTop+1
					sf[sfTop+1], sfTop = e.nodes[startIdx], sfTop+1
				} else {
					sfTop = sfTop + cnt
					for i := 0; i < cnt; i++ {
						sf[sfTop-i] = e.nodes[startIdx+i]
					}
				}
				continue
			}

			// current node has been visited
			maxIdx = curtIdx
			osTop = osTop - cnt
			if cnt == 2 {
				param2[0], param2[1] = os[osTop+1], os[osTop+2]
				param = param2[:]
			} else {
				param = make([]Value, cnt)
				copy(param, os[osTop+1:])
			}
			res, err = curt.operator(ctx, param)
			if debug {
				printOperatorFunc(curt.value, param, res, err)
			}
			if err != nil {
				return nil, fmt.Errorf("eval error [%w], Operator: %v", err, curt.value)
			}
		case selector:
			res, err = getSelectorValue(ctx, curt)
			if err != nil {
				return nil, err
			}
		case value:
			res = curt.value
		case cond:
			startIdx := int(e.childStartIndex[curtIdx])
			if curtIdx > maxIdx {
				maxIdx = curtIdx
				// push the end node to the stack frame
				sf[sfTop+1], sfTop = endNode(curt), sfTop+1
				sf[sfTop+1], sfTop = curt, sfTop+1
				sf[sfTop+1], sfTop = e.nodes[startIdx], sfTop+1
			} else {
				res, osTop = os[osTop], osTop-1
				condRes, ok := res.(bool)
				if !ok {
					return nil, fmt.Errorf("eval error, result type of if condition should be bool, got: [%v]", res)
				}
				if condRes {
					sf[sfTop+1], sfTop = e.nodes[startIdx+1], sfTop+1
				} else {
					sf[sfTop+1], sfTop = e.nodes[startIdx+2], sfTop+1
				}
			}
			continue
		case end:
			maxIdx = curtIdx
			res, osTop = os[osTop], osTop-1
		}

		// short circuit
		if b, ok := res.(bool); ok {
			for (!b && curt.flag&scIfFalse == scIfFalse) ||
				(b && curt.flag&scIfTrue == scIfTrue) {
				if debug {
					//printShortCircuit(curt)
				}

				if curt.scIdx == 0 {
					return res, nil
				}

				scIdx := int(curt.scIdx)

				maxIdx = scIdx
				sfTop = e.sfSize[scIdx] - 2
				osTop = e.osSize[scIdx] - 1
				curt = e.nodes[scIdx]
			}
		}

		// push the result of current frame to operator stack
		os[osTop+1], osTop = res, osTop+1
	}
	return os[0], nil
}

type Ctx struct {
	Selector
	Ctx   context.Context
	Debug bool
}

func endNode(n *node) *node {
	const scMask = scIfTrue | scIfFalse
	return &node{
		value: "end",
		idx:   n.idx,
		scIdx: n.scIdx,
		flag:  end | (n.flag & scMask),
	}
}

func unifyType(val Value) Value {
	switch v := val.(type) {
	case int:
		return int64(v)
	case time.Time:
		return v.Unix()
	case time.Duration:
		return int64(v / time.Second)
	case []int:
		temp := make([]int64, len(v))
		for i, iv := range v {
			temp[i] = int64(iv)
		}
		return temp
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint64:
		return int64(v)
	case uint32:
		return int64(v)
	case uint16:
		return int64(v)
	case uint8:
		return int64(v)
	}
	return val
}

func getNodeValue(ctx *Ctx, n *node) (res Value, err error) {
	if n.flag&nodeTypeMask == value {
		res = n.value
	} else {
		res, err = getSelectorValue(ctx, n)
	}
	return
}

func getSelectorValue(ctx *Ctx, n *node) (Value, error) {
	res, err := ctx.Get(n.selKey, n.value.(string))
	if err != nil {
		return nil, err
	}

	switch res.(type) {
	case bool, string, int64, []int64, []string:
		return res, nil
	default:
		return unifyType(res), nil
	}
}

func printStacks(maxId int, os []Value, osTop int, sf []*node, sfTop int) {
	var sb strings.Builder

	fmt.Printf("maxId:%d, sfTop:%d, osTop:%d\n", maxId, sfTop, osTop)
	sb.WriteString(fmt.Sprintf("%15s", "Stack Frame: "))
	for i := sfTop; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("|%4v", sf[i].value))
	}
	sb.WriteString("|\n")

	sb.WriteString(fmt.Sprintf("%15s", "Operand Stack: "))
	for i := osTop; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("|%4v", os[i]))
	}
	sb.WriteString("|\n")
	fmt.Println(sb.String())
}

func printOperatorFunc(op Value, params []Value, ret Value, err error) {
	fmt.Printf("invoke operator, op: %v, params: %v, ret: %v, err: %v\n\n", op, params, ret, err)
}

func printShortCircuit(n *node) {
	fmt.Printf("short circuit triggered, curt: %v, scIdx: %d\n\n", n.value, n.scIdx)
}
