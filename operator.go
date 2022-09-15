package eval

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func RegisterOperator(cc *CompileConfig, name string, op Operator) error {
	if _, exist := builtinOperators[name]; exist {
		return fmt.Errorf("operator already exist %s", name)
	}

	if _, exist := cc.OperatorMap[name]; exist {
		return fmt.Errorf("operator already exist %s", name)
	}

	cc.OperatorMap[name] = op
	return nil
}

var (
	builtinOperators = map[string]Operator{
		// arithmetic
		"add": arithmetic{mode: add}.execute,
		"sub": arithmetic{mode: sub}.execute,
		"mul": arithmetic{mode: mul}.execute,
		"div": arithmetic{mode: div}.execute,
		"mod": arithmetic{mode: mod}.execute,
		"+":   arithmetic{mode: add}.execute,
		"-":   arithmetic{mode: sub}.execute,
		"*":   arithmetic{mode: mul}.execute,
		"/":   arithmetic{mode: div}.execute,
		"%":   arithmetic{mode: mod}.execute,

		// logic
		"and": logic{mode: and}.execute,
		"or":  logic{mode: or}.execute,
		"xor": logic{mode: xor}.execute,
		"not": logicNot,
		"&":   logic{mode: and}.execute,
		"|":   logic{mode: or}.execute,
		"!":   logicNot,

		// comparison
		"eq":      comparisonEquals,
		"ne":      comparisonNotEquals,
		"gt":      comparison{mode: greater}.execute,
		"lt":      comparison{mode: less}.execute,
		"ge":      comparison{mode: greaterEquals}.execute,
		"le":      comparison{mode: lessEquals}.execute,
		"=":       comparisonEquals,
		"!=":      comparisonNotEquals,
		">":       comparison{mode: greater}.execute,
		"<":       comparison{mode: less}.execute,
		">=":      comparison{mode: greaterEquals}.execute,
		"<=":      comparison{mode: lessEquals}.execute,
		"between": comparisonBetween,

		// list
		"in":      listIn,
		"overlap": listOverlap,

		// time
		"date":     timeConvert{mode: date, layout: defaultDateLayout}.execute,
		"datetime": timeConvert{mode: datetime, layout: defaultDatetimeLayout}.execute,

		"t_time":  timeConvert{mode: toTime}.execute,
		"t_date":  timeConvert{mode: toDate}.execute,
		"td_time": timeConvert{mode: toDefaultTime, layout: defaultDatetimeLayout}.execute,
		"td_date": timeConvert{mode: toDefaultDate, layout: defaultDateLayout}.execute,

		// version
		"version":   versionConvert{mode: version, validLen: 3}.execute,
		"t_version": versionConvert{mode: toVersion, validLen: 3}.execute,

		// infix notation patch
		"==": comparisonEquals,
		"&&": logic{mode: and}.execute,
		"||": logic{mode: or}.execute,
	}
)

type mode int

const (
	// arithmetic
	add mode = iota
	sub
	mul
	div
	mod

	// logical
	and
	or
	xor
	not

	// comparison
	equals
	notEquals
	greater
	less
	greaterEquals
	lessEquals
	between

	// list
	in
	overlap

	// time
	date
	datetime
	toTime
	toDate
	toDefaultTime
	toDefaultDate

	version
	toVersion
)

var modeNames = [...]string{
	// arithmetic
	add: "add",
	sub: "sub",
	mul: "mul",
	div: "div",
	mod: "mod",

	// logical
	and: "and",
	or:  "or",
	xor: "xor",
	not: "not",

	// comparison
	equals:        "eq",
	notEquals:     "ne",
	greater:       "gt",
	less:          "lt",
	greaterEquals: "ge",
	lessEquals:    "le",
	between:       "between",

	// list
	in:      "in",
	overlap: "overlap",

	// time
	date:          "date",
	datetime:      "datetime",
	toTime:        "t_time",
	toDate:        "t_date",
	toDefaultTime: "td_time",
	toDefaultDate: "td_date",

	// version
	version:   "version",
	toVersion: "toVersion",
}

const (
	typeBool    = "bool"
	typeInt     = "int64"
	typeStr     = "string"
	typeIntList = "[]int64"
	typeStrList = "[]string"
)

type arithmetic struct {
	mode mode
}

func (a arithmetic) execute(_ *Ctx, params []Value) (Value, error) {
	if len(params) < 2 {
		return nil, errCnt2(a.mode, params)
	}

	var res int64
	for i, p := range params {
		v, ok := p.(int64)
		if !ok {
			return nil, errTypeInt(a.mode, p)
		}

		if i == 0 {
			res = v
		} else {
			switch a.mode {
			case add:
				res += v
			case sub:
				res -= v
			case mul:
				res *= v
			case div:
				if v == 0 {
					return nil, OpExecError("div", errors.New("divide by zero"))
				}
				res /= v
			case mod:
				if v == 0 {
					return nil, OpExecError("mod", errors.New("divide by zero"))
				}
				res %= v
			default:
				return 0, errInvalidMode(a.mode, "arithmetic")
			}
		}
	}
	return res, nil
}

type logic struct {
	mode mode
}

func (c logic) execute(_ *Ctx, params []Value) (Value, error) {
	if len(params) < 2 {
		return nil, errCnt2(c.mode, params)
	}

	var res bool
	for i, p := range params {
		v, ok := p.(bool)
		if !ok {
			return nil, errTypeBool(c.mode, p)
		}
		if i == 0 {
			res = v
		} else {
			switch c.mode {
			case and:
				res = res && v
			case or:
				res = res || v
			case xor:
				res = res != v
			default:
				return false, errInvalidMode(c.mode, "logic")
			}
		}
	}
	return res, nil
}

func logicNot(_ *Ctx, params []Value) (Value, error) {
	const op = "not"
	if len(params) != 1 {
		return nil, ParamsCountError(op, 1, len(params))
	}
	v, ok := params[0].(bool)
	if !ok {
		return nil, ParamTypeError(op, typeBool, params[0])
	}
	return !v, nil
}

type comparison struct {
	mode mode
}

func (c comparison) execute(_ *Ctx, params []Value) (Value, error) {
	if len(params) != 2 {
		return nil, errCnt2(c.mode, params)
	}

	i, ok := params[0].(int64)
	if !ok {
		return nil, errTypeInt(c.mode, params[0])
	}

	j, ok := params[1].(int64)
	if !ok {
		return nil, errTypeInt(c.mode, params[1])
	}

	switch c.mode {
	case greater:
		return i > j, nil
	case less:
		return i < j, nil
	case greaterEquals:
		return i >= j, nil
	case lessEquals:
		return i <= j, nil
	default:
		return false, errInvalidMode(c.mode, "comparison")
	}
}

func comparisonEquals(_ *Ctx, params []Value) (Value, error) {
	if len(params) < 2 {
		return nil, errCnt2(equals, params)
	}

	if len(params) == 2 {
		return params[0] == params[1], nil
	}

	v := params[0]
	for _, p := range params {
		if v != p {
			return false, nil
		}
	}
	return true, nil
}

func comparisonNotEquals(_ *Ctx, params []Value) (Value, error) {
	if len(params) != 2 {
		return nil, errCnt2(notEquals, params)
	}

	return params[0] != params[1], nil
}

func comparisonBetween(_ *Ctx, params []Value) (Value, error) {
	const op = "between"
	if len(params) != 3 {
		return nil, ParamsCountError(op, 3, len(params))
	}

	v, ok := params[0].(int64)
	if !ok {
		return nil, errTypeInt(between, params[0])
	}
	a, ok := params[1].(int64)
	if !ok {
		return nil, errTypeInt(between, params[1])
	}
	b, ok := params[2].(int64)
	if !ok {
		return nil, errTypeInt(between, params[2])
	}
	return a <= v && v <= b, nil
}

func listIn(_ *Ctx, params []Value) (Value, error) {
	const op = "in"
	if len(params) != 2 {
		return nil, errCnt2(in, params)
	}
	switch v := params[0].(type) {
	case string:
		switch coll := params[1].(type) {
		case []string:
			for _, i := range coll {
				if i == v {
					return true, nil
				}
			}
			return false, nil
		case map[string]struct{}:
			_, exist := coll[v]
			return exist, nil
		default:
			return nil, ParamTypeError(op, typeStrList, params[1])
		}
	case int64:
		switch coll := params[1].(type) {
		case []int64:
			for _, i := range coll {
				if i == v {
					return true, nil
				}
			}
			return false, nil
		case []string: // the empty list is parsed to a string list
			if len(coll) == 0 {
				return false, nil
			}
		case map[int64]struct{}:
			_, exist := coll[v]
			return exist, nil
		}
		return nil, ParamTypeError(op, typeIntList, params[1])
	}
	return nil, OpExecError(op, errors.New("unsupported list type"))
}

func listOverlap(_ *Ctx, params []Value) (Value, error) {
	const op = "overlap"
	if len(params) != 2 {
		return nil, errCnt2(overlap, params)
	}

	switch A := params[0].(type) {
	case []string:
		B, ok := params[1].([]string)
		if !ok {
			return nil, ParamTypeError(op, typeStrList, params[1])
		}
		if len(A)+len(B) < 100 {
			for _, i := range A {
				for _, j := range B {
					if i == j {
						return true, nil
					}
				}
			}
			return false, nil
		}
		if len(A) > len(B) {
			A, B = B, A
		}
		set := make(map[string]struct{}, len(A))
		for _, i := range A {
			set[i] = empty
		}
		for _, i := range B {
			if _, exist := set[i]; exist {
				return true, nil
			}
		}
		return false, nil
	case []int64:
		switch B := params[1].(type) {
		case []int64:
			if len(A)+len(B) < 100 {
				for _, i := range A {
					for _, j := range B {
						if i == j {
							return true, nil
						}
					}
				}
				return false, nil
			}
			if len(A) > len(B) {
				A, B = B, A
			}
			set := make(map[int64]struct{}, len(A))
			for _, i := range A {
				set[i] = empty
			}
			for _, i := range B {
				if _, exist := set[i]; exist {
					return true, nil
				}
			}
			return false, nil
		case []string:
			// the empty list is parsed to a string list
			if len(B) != 0 {
				return nil, ParamTypeError(op, typeStrList, params[1])
			}
			return false, nil
		default:
			return nil, ParamTypeError(op, typeStrList, params[1])
		}
	}
	return nil, ParamTypeError(op, typeStrList, params[0])
}

const (
	defaultDatetimeLayout = "2006-01-02 15:04:05"
	defaultDateLayout     = "2006-01-02"
)

type timeConvert struct {
	mode   mode
	layout string
}

func (c timeConvert) execute(_ *Ctx, params []Value) (Value, error) {
	var layout string
	switch c.mode {
	case datetime, date:
		switch len(params) {
		case 1:
			layout = c.layout
		case 2:
			temp, ok := params[1].(string)
			if !ok {
				return nil, errTypeStr(c.mode, params[1])
			}
			layout = temp
		default:
			return nil, ParamsCountError(modeNames[c.mode], 1, len(params))
		}
	case toTime, toDate:
		if len(params) != 2 {
			return nil, errCnt2(c.mode, params)
		}
		temp, ok := params[1].(string)
		if !ok {
			return nil, errTypeStr(c.mode, params[1])
		}
		layout = temp
	case toDefaultTime, toDefaultDate:
		if len(params) != 1 {
			return nil, ParamsCountError(modeNames[c.mode], 1, len(params))
		}
		layout = c.layout
	}

	v, ok := params[0].(string)
	if !ok {
		return nil, errTypeStr(c.mode, params[0])
	}
	t, err := time.Parse(layout, v)
	if err != nil {
		return nil, OpExecError(modeNames[c.mode], err)
	}
	return t.Unix(), nil
}

type versionConvert struct {
	mode     mode
	validLen int
}

func (c versionConvert) execute(_ *Ctx, params []Value) (Value, error) {
	var validLen int
	switch len(params) {
	case 1:
		validLen = c.validLen
	case 2:
		temp, ok := params[1].(int64)
		if !ok {
			return nil, errTypeInt(c.mode, params[1])
		}
		if temp > 4 || temp < 1 {
			return nil, OpExecError(modeNames[c.mode], fmt.Errorf("version valid length out of range: %d", temp))
		}
		validLen = int(temp)
	default:
		return nil, errCnt2(c.mode, params)
	}
	s, ok := params[0].(string)
	if !ok {
		return nil, errTypeStr(c.mode, params[0])
	}

	arr := strings.Split(s, ".")
	var res int64
	for i := 0; i < validLen; i++ {
		if i < len(arr) {
			v, err := strconv.ParseInt(arr[i], 10, 64)
			if err != nil {
				return nil, OpExecError(modeNames[c.mode], fmt.Errorf("version layout error, %s", s))
			}
			if v >= 10000 {
				return nil, OpExecError(modeNames[c.mode], fmt.Errorf("version layout error, %s", s))
			}
			res = res*10000 + v
		} else {
			res = res * 10000
		}
	}
	return res, nil
}

func OpExecError(opName string, err error) error {
	return fmt.Errorf("operator execuation error, operator: %s, error: %w", opName, err)
}

func ParamsCountError(opName string, want, got int) error {
	return fmt.Errorf("unexpected params count, operator: %s, expected: %d, got: %d", opName, want, got)
}

func ParamTypeError(opName string, want string, got Value) error {
	return fmt.Errorf("unexpected param type, operator: %s, expected: %s, got: %+v", opName, want, got)
}

func errCnt2(m mode, params []Value) error {
	return ParamsCountError(modeNames[m], 2, len(params))
}

func errTypeInt(m mode, p Value) error {
	return ParamTypeError(modeNames[m], typeInt, p)
}

func errTypeBool(m mode, p Value) error {
	return ParamTypeError(modeNames[m], typeBool, p)
}

func errTypeStr(m mode, p Value) error {
	return ParamTypeError(modeNames[m], typeStr, p)
}

func errInvalidMode(m mode, c string) error {
	return fmt.Errorf("invalid op: %s for category:%s", modeNames[m], c)
}
