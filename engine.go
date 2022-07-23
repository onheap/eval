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

type Ctx struct {
	Selector
	Ctx context.Context
}

const (
	// node types
	nodeTypeMask = int16(0b111)
	constant     = int16(0b001)
	selector     = int16(0b010)
	operator     = int16(0b011)
	fastOperator = int16(0b100)
	cond         = int16(0b101)
	end          = int16(0b110)
	debug        = int16(0b111)

	// short circuit flag
	scMask    = int16(0b011000)
	scIfFalse = int16(0b001000)
	scIfTrue  = int16(0b010000)
)

const (
	align = 8

	flag = 0
	rIdx = 1
	selK = 2
	cIdx = 2
	pIdx = 2
	cCnt = 3

	fSfTop = 4
	fOsTop = 5
	tSfTop = 6
	tOsTop = 7
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
		size   = e.maxStackSize
		maxIdx = int16(-1)

		sf    []int16 // stack frame
		sfTop = int16(-1)

		os    []Value // operand stack
		osTop = int16(-1)

		scTriggered bool

		bytecode  = e.bytecode
		constants = e.constants
		operators = e.operators
	)

	// ensure that variables do not escape to the heap in most cases
	switch {
	case size <= 4:
		os = make([]Value, 4)
		sf = make([]int16, 4)
	case size <= 8:
		os = make([]Value, 8)
		sf = make([]int16, 8)
	case size <= 16:
		os = make([]Value, 16)
		sf = make([]int16, 16)
	default:
		os = make([]Value, size)
		sf = make([]int16, size)
	}

	var (
		curt int16

		res Value // result of current stack frame
		err error

		param  []Value
		param2 [2]Value
	)

	// push the root node to the stack frame
	//sf[sfTop+1], sfTop = 0, sfTop+1
	sfTop = 0

	for sfTop != -1 { // while stack frame is not empty
		curt, sfTop = sf[sfTop], sfTop-1

		switch bytecode[curt] & nodeTypeMask {
		case fastOperator:
			childIdx := bytecode[curt+cIdx]
			res = constants[bytecode[childIdx+rIdx]]
			if bytecode[childIdx]&nodeTypeMask == selector {
				res, err = ctx.Get(SelectorKey(bytecode[childIdx+selK]), res.(string))
				if err != nil {
					return nil, err
				}
			}

			param2[0] = res
			res = constants[bytecode[childIdx+align+rIdx]]
			if bytecode[childIdx+align]&nodeTypeMask == selector {
				res, err = ctx.Get(SelectorKey(bytecode[childIdx+align+selK]), res.(string))
				if err != nil {
					return nil, err
				}
			}

			param2[1] = res
			param = param2[:]
			res, err = operators[bytecode[curt+rIdx]](ctx, param)
			if err != nil {
				return nil, fmt.Errorf("operator execution error, operator: %v, error: %w", curt, err)
			}
		case operator:
			cnt := bytecode[curt+cCnt]
			if curt > maxIdx {
				// the node has never been visited before
				maxIdx = curt
				sf[sfTop+1], sfTop = curt, sfTop+1
				childIdx := bytecode[curt+cIdx]
				// push child nodes into the stack frame
				// the back nodes is on top
				if cnt == 2 {
					sf[sfTop+1], sfTop = childIdx+align, sfTop+1
					sf[sfTop+1], sfTop = childIdx, sfTop+1
				} else {
					sfTop = sfTop + cnt
					for i := int16(0); i < cnt; i++ {
						sf[sfTop-i] = childIdx + i*align
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

			res, err = operators[bytecode[curt+rIdx]](ctx, param)
			if err != nil {
				return nil, fmt.Errorf("operator execution error, operator: %v, error: %w", curt, err)
			}
		case selector:
			res, err = ctx.Get(SelectorKey(bytecode[curt+selK]), constants[bytecode[curt+rIdx]].(string))
			if err != nil {
				return nil, err
			}
		case constant:
			res = constants[bytecode[curt+rIdx]]
		case cond:
			childIdx := bytecode[curt+cIdx]
			if curt > maxIdx {
				maxIdx = curt
				// push the end node to the stack frame
				sf[sfTop+1], sfTop = childIdx+align*3, sfTop+1
				sf[sfTop+1], sfTop = curt, sfTop+1
				sf[sfTop+1], sfTop = childIdx, sfTop+1
			} else {
				res, osTop = os[osTop], osTop-1
				condRes, ok := res.(bool)
				if !ok {
					return nil, fmt.Errorf("eval error, result type of if condition should be bool, got: [%v]", res)
				}
				if condRes {
					sf[sfTop+1], sfTop = childIdx+align, sfTop+1
				} else {
					sf[sfTop+1], sfTop = childIdx+align<<1, sfTop+1
				}
			}
			continue
		case end:
			maxIdx = bytecode[curt+pIdx]
			res, osTop = os[osTop], osTop-1
		default:
			// only debug node will enter this branch
			offset := int16(len(e.nodes) / 2)
			debugStackFrame(sf, sfTop, offset)

			// push the real node to print stacks
			sf[sfTop+1], sfTop = curt+offset, sfTop+1

			e.printStacks(scTriggered, maxIdx, os, osTop, sf, sfTop)
			scTriggered = false
			//_ = scTriggered
			continue
		}

		// short circuit
		if b, ok := res.(bool); ok {
			f := bytecode[curt]
			if (!b && f&scIfFalse == scIfFalse) ||
				(b && f&scIfTrue == scIfTrue) {
				scTriggered = true

				if !b {
					sfTop = bytecode[curt+fSfTop]
					osTop = bytecode[curt+fOsTop]
				} else {
					sfTop = bytecode[curt+tSfTop]
					osTop = bytecode[curt+tOsTop]
				}

				if sfTop == -1 {
					return res, nil
				}
				maxIdx = sf[sfTop+1]
			}
		}

		// push the result of current frame to operator stack
		os[osTop+1], osTop = res, osTop+1
	}
	return os[0], nil
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

func debugStackFrame(sf []int16, sfTop, offset int16) {
	// replace with debug node
	for i := int16(0); i < sfTop; i++ {
		if sf[i] >= offset {
			sf[i] -= offset
		}
	}
}

func (e *Expr) printStacks(scTriggered bool, maxIdx int16, os []Value, osTop int16, sf []int16, sfTop int16) {
	if scTriggered {
		fmt.Printf("short circuit triggered\n\n")
	}

	var sb strings.Builder

	offset := int16(len(e.nodes) / 2)

	fmt.Printf("maxIdx:%d, sfTop:%d, osTop:%d\n", maxIdx-offset, sfTop, osTop)
	sb.WriteString(fmt.Sprintf("%15s", "Stack Frame: "))
	for i := sfTop; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("|%4v", e.nodes[sf[i]/align].value))
	}
	sb.WriteString("|\n")

	sb.WriteString(fmt.Sprintf("%15s", "Operand Stack: "))
	for i := osTop; i >= 0; i-- {
		sb.WriteString(fmt.Sprintf("|%4v", os[i]))
	}
	sb.WriteString("|\n")
	fmt.Println(sb.String())
}
