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
	nodeTypeMask = int16(0b111)
	constant     = int16(0b001)
	selector     = int16(0b010)
	operator     = int16(0b011)
	fastOperator = int16(0b100)
	cond         = int16(0b101)
	end          = int16(0b110)

	// short circuit flag
	scMask    = int16(0b011000)
	scIfFalse = int16(0b001000)
	scIfTrue  = int16(0b010000)
)

type node struct {
	flag     int16
	idx      int
	childCnt int
	childIdx int
	selKey   SelectorKey
	value    Value
	operator Operator
}

func (n *node) getNodeType() int16 {
	return n.flag & nodeTypeMask
}

type Expr struct {
	maxStackSize int16

	// Although the field name is bytecode,
	// here we use []int16 for convenience
	bytecode  []int16
	constants []Value
	operators []Operator

	// extra info
	scIdx     []int
	sfSize    []int
	osSize    []int
	parentIdx []int
	nodes     []*node
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
		size  = e.maxStackSize
		debug = ctx.Debug

		bytecode  = e.bytecode
		constants = e.constants
		operators = e.operators

		maxIdx = -1

		sf    []int // stack frame
		sfTop = -1

		os    []Value // operand stack
		osTop = -1
	)

	// ensure that variables do not escape to the heap in most cases
	switch {
	case size <= 4:
		os = make([]Value, 4)
		sf = make([]int, 4)
	case size <= 8:
		os = make([]Value, 8)
		sf = make([]int, 8)
	case size <= 16:
		os = make([]Value, 16)
		sf = make([]int, 16)
	default:
		os = make([]Value, size)
		sf = make([]int, size)
	}

	var (
		curt    int
		curtIdx int

		res Value // result of current stack frame
		err error

		param  []Value
		param2 [2]Value
	)

	// push the root node to the stack frame
	sf[sfTop+1], sfTop = 0, sfTop+1

	for sfTop != -1 { // while stack frame is not empty
		if debug {
			e.printStacks(maxIdx, os, osTop, sf, sfTop)
		}
		curt, sfTop = sf[sfTop], sfTop-1
		curtIdx = curt << 2 // curt * 4

		switch bytecode[curtIdx] & nodeTypeMask {
		case fastOperator:
			cnt := int(bytecode[curtIdx] >> 8)
			childIdx := int(bytecode[curtIdx+1])
			if cnt == 2 {
				if idx := childIdx << 2; bytecode[idx]&nodeTypeMask == constant {
					param2[0] = constants[bytecode[idx+2]]
				} else {
					param2[0], err = e.getNodeValue(ctx, childIdx)
					if err != nil {
						return nil, err
					}
				}
				if idx := (childIdx + 1) << 2; bytecode[idx]&nodeTypeMask == constant {
					param2[1] = constants[bytecode[idx+2]]
				} else {
					param2[1], err = e.getNodeValue(ctx, childIdx)
					if err != nil {
						return nil, err
					}
				}

				param = param2[:]
			} else {
				param = make([]Value, cnt)
				for i := 0; i < cnt; i++ {
					param[i], err = e.getNodeValue(ctx, childIdx+i)
					if err != nil {
						return nil, err
					}
				}
			}

			res, err = operators[bytecode[curtIdx+2]](ctx, param)
			if debug {
				e.printOperator(curt, param, res, err)
			}
			if err != nil {
				return nil, fmt.Errorf("operator execution error, operator: %v, error: %w", curt, err)
			}
		case operator:
			cnt := int(bytecode[curtIdx] >> 8)
			if curt > maxIdx {
				// the node has never been visited before
				maxIdx = curt
				sf[sfTop+1], sfTop = curt, sfTop+1
				childIdx := int(bytecode[curtIdx+1])
				// push child nodes into the stack frame
				// the back nodes is on top
				if cnt == 2 {
					sf[sfTop+1], sfTop = childIdx+1, sfTop+1
					sf[sfTop+1], sfTop = childIdx, sfTop+1
				} else {
					sfTop = sfTop + cnt
					for i := 0; i < cnt; i++ {
						sf[sfTop-i] = childIdx + i
					}
				}
				continue
			}

			// current node has been visited
			maxIdx = curt
			osTop = osTop - cnt
			if cnt == 2 {
				param2[0], param2[1] = os[osTop+1], os[osTop+2]
				param = param2[:]
			} else {
				param = make([]Value, cnt)
				copy(param, os[osTop+1:])
			}
			if debug {
				e.printOperator(curt, param, res, err)
			}
			res, err = operators[bytecode[curtIdx+2]](ctx, param)

			if err != nil {
				return nil, fmt.Errorf("operator execution error, operator: %v, error: %w", curt, err)
			}
		case selector:
			res, err = ctx.Get(SelectorKey(bytecode[curtIdx+3]), "")
			if err != nil {
				return nil, err
			}
		case constant:
			res = e.constants[bytecode[curtIdx+2]]
		case cond:
			childIdx := int(bytecode[curtIdx+1])
			if curt > maxIdx {
				maxIdx = curt
				cnt := int(bytecode[curtIdx] >> 8)

				// push the end node to the stack frame
				sf[sfTop+1], sfTop = curt+cnt-1, sfTop+1
				sf[sfTop+1], sfTop = curt, sfTop+1
				sf[sfTop+1], sfTop = childIdx, sfTop+1
			} else {
				res, osTop = os[osTop], osTop-1
				condRes, ok := res.(bool)
				if !ok {
					return nil, fmt.Errorf("eval error, result type of if condition should be bool, got: [%v]", res)
				}
				if condRes {
					sf[sfTop+1], sfTop = childIdx+1, sfTop+1
				} else {
					sf[sfTop+1], sfTop = childIdx+2, sfTop+1
				}
			}
			continue
		case end:
			maxIdx = e.parentIdx[curt]
			res, osTop = os[osTop], osTop-1
		}

		// short circuit
		if b, ok := res.(bool); ok {
			flag := bytecode[curtIdx] & scMask
			for (flag == scMask) || (!b && flag&scIfFalse == scIfFalse) ||
				(b && flag&scIfTrue == scIfTrue) {

				if debug {
					e.printShortCircuit(curt)
				}

				curt = e.scIdx[curt]
				if curt == 0 {
					return res, nil
				}

				maxIdx = curt
				sfTop = e.sfSize[curt] - 2
				osTop = e.osSize[curt] - 1
				curtIdx = curt << 2
				flag = bytecode[curtIdx] & scMask
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

func (e *Expr) getNodeValue(ctx *Ctx, i int) (res Value, err error) {
	if e.bytecode[i<<2]&nodeTypeMask == constant {
		res = e.constants[e.bytecode[i<<2+2]]
	} else {
		res, err = e.getSelectorValue(ctx, i)
	}
	return
}

func (e *Expr) getSelectorValue(ctx *Ctx, i int) (Value, error) {
	i = i << 2
	var (
		strKey = e.constants[e.bytecode[i+2]].(string)
		selKey = SelectorKey(e.bytecode[i+3])
	)
	res, err := ctx.Get(selKey, strKey)
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

func (e *Expr) printStacks(maxId int, os []Value, osTop int, sf []int, sfTop int) {
	var sb strings.Builder

	fmt.Printf("maxId:%d, sfTop:%d, osTop:%d\n", maxId, sfTop, osTop)
	sb.WriteString(fmt.Sprintf("%15s", "Stack Frame: "))
	for i := sfTop; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("|%4v", e.nodes[sf[i]].value))
	}
	sb.WriteString("|\n")

	sb.WriteString(fmt.Sprintf("%15s", "Operand Stack: "))
	for i := osTop; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("|%4v", os[i]))
	}
	sb.WriteString("|\n")
	fmt.Println(sb.String())
}

func (e *Expr) printOperator(idx int, params []Value, res Value, err error) {
	fmt.Printf("execute operator, op: %v, params: %v, res: %v, err: %v\n\n", e.nodes[idx].value, params, res, err)
}

func (e *Expr) printShortCircuit(idx int) {
	fmt.Printf("short circuit triggered, node: %v\n\n", e.nodes[idx].value)
}
