package eval

import (
	"testing"
	"time"
)

const (
	paramsCntErrMsg = "unexpected params count"
	paramTypeErrMsg = "unexpected param type"
)

func TestRegisterOperator(t *testing.T) {
	var maxOp = func(_ *Ctx, param []Value) (Value, error) {
		const op = "max"
		if len(param) < 2 {
			return nil, ParamsCountError(op, 2, len(param))
		}

		var res int64
		for i, p := range param {
			v, ok := p.(int64)
			if !ok {
				return nil, ParamTypeError(op, typeInt, p)
			}
			if i == 0 {
				res = v
			} else {
				if v > res {
					res = v
				}
			}
		}
		return res, nil
	}

	cc := NewCompileConfig()
	err := RegisterOperator(cc, "max", maxOp)
	assertNil(t, err)

	res, err := Eval(`(max 1 5 3)`, nil, cc)
	assertNil(t, err)
	assertEquals(t, res, int64(5))

	// register operator error
	// duplicate
	err = RegisterOperator(cc, "max", maxOp)
	assertErrStrContains(t, err, "operator already exist")

	// register a builtin operator
	testOp := func(_ *Ctx, _ []Value) (Value, error) {
		return nil, nil
	}
	err = RegisterOperator(cc, "add", testOp)
	assertErrStrContains(t, err, "operator already exist")
}

func TestBuiltinOperators(t *testing.T) {
	toParams := func(vs []int64) []Value {
		params := make([]Value, len(vs))
		for i, v := range vs {
			params[i] = v
		}
		return params
	}

	toTime := func(layout, value string) time.Time {
		v, _ := time.Parse(layout, value)
		return v
	}

	testCases := []struct {
		op     string
		params []Value
		res    Value
		errMsg string
	}{
		// arithmetic
		// add
		{
			op:     "add",
			params: toParams([]int64{1, 1}),
			res:    int64(2),
		},

		{
			op:     "add",
			params: toParams([]int64{1, 2, 3}),
			res:    int64(6),
		},

		{
			op:     "add",
			params: toParams([]int64{1, 2, -3, 0}),
			res:    int64(0),
		},

		{
			op:     "add",
			params: toParams([]int64{1, 2, 3, -10}),
			res:    int64(-4),
		},

		{
			op:     "add",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "add",
			params: []Value{1, 1},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		{
			op:     "add",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// sub
		{
			op:     "sub",
			params: toParams([]int64{1, 1}),
			res:    int64(0),
		},

		{
			op:     "sub",
			params: toParams([]int64{3, 2}),
			res:    int64(1),
		},

		{
			op:     "sub",
			params: toParams([]int64{2, -2, -3}),
			res:    int64(7),
		},

		{
			op:     "sub",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "sub",
			params: []Value{1, 1},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		{
			op:     "sub",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// mul
		{
			op:     "mul",
			params: toParams([]int64{1, 1}),
			res:    int64(1),
		},

		{
			op:     "mul",
			params: toParams([]int64{3, 2}),
			res:    int64(6),
		},

		{
			op:     "mul",
			params: toParams([]int64{2, -2, -3}),
			res:    int64(12),
		},

		{
			op:     "mul",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "mul",
			params: []Value{1, 1},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		{
			op:     "mul",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// div
		{
			op:     "div",
			params: toParams([]int64{1, 1}),
			res:    int64(1),
		},

		{
			op:     "div",
			params: toParams([]int64{3, 2}),
			res:    int64(1),
		},

		{
			op:     "div",
			params: toParams([]int64{2, -2, -3}),
			res:    int64(0),
		},

		{
			op:     "div",
			params: toParams([]int64{12, -2, 3}),
			res:    int64(-2),
		},

		{
			op:     "div",
			params: toParams([]int64{12, -2, 0}),
			errMsg: "divide by zero",
		},

		{
			op:     "div",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "div",
			params: []Value{1, 1},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		{
			op:     "div",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// mod
		{
			op:     "mod",
			params: toParams([]int64{1, 1}),
			res:    int64(0),
		},

		{
			op:     "mod",
			params: toParams([]int64{3, 2}),
			res:    int64(1),
		},

		{
			op:     "mod",
			params: toParams([]int64{2, -2, -3}),
			res:    int64(0),
		},

		{
			op:     "mod",
			params: toParams([]int64{12, -2, 3}),
			res:    int64(0),
		},

		{
			op:     "mod",
			params: toParams([]int64{12, -2, 0}),
			errMsg: "divide by zero",
		},

		{
			op:     "mod",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "mod",
			params: []Value{1, 1},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		{
			op:     "mod",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// logic
		// and
		{
			op:     "and",
			params: []Value{true, true},
			res:    true,
		},
		{
			op:     "and",
			params: []Value{false, false},
			res:    false,
		},
		{
			op:     "and",
			params: []Value{true, false},
			res:    false,
		},
		{
			op:     "and",
			params: []Value{true, true, false, true, true, false},
			res:    false,
		},
		{
			op:     "and",
			params: []Value{true},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "and",
			params: []Value{1, 1, "true"},
			errMsg: paramTypeErrMsg,
		},

		// "or"
		{
			op:     "or",
			params: []Value{true, true},
			res:    true,
		},
		{
			op:     "or",
			params: []Value{false, false},
			res:    false,
		},
		{
			op:     "or",
			params: []Value{true, false},
			res:    true,
		},
		{
			op:     "or",
			params: []Value{true, true, false, true, true, false},
			res:    true,
		},
		{
			op:     "or",
			params: []Value{true},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "or",
			params: []Value{1, 1, "true"},
			errMsg: paramTypeErrMsg,
		},

		// "xor"
		{
			op:     "xor",
			params: []Value{true, true},
			res:    false,
		},
		{
			op:     "xor",
			params: []Value{false, false},
			res:    false,
		},
		{
			op:     "xor",
			params: []Value{true, false},
			res:    true,
		},
		{
			op:     "xor",
			params: []Value{true, true, false, true, true, false},
			res:    false,
		},
		{
			op:     "xor",
			params: []Value{true},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "xor",
			params: []Value{1, 1, "true"},
			errMsg: paramTypeErrMsg,
		},

		// not
		{
			op:     "not",
			params: []Value{true},
			res:    false,
		},
		{
			op:     "not",
			params: []Value{false},
			res:    true,
		},
		{
			op:     "not",
			params: []Value{},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "not",
			params: []Value{true, true},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "not",
			params: []Value{1},
			errMsg: paramTypeErrMsg,
		},

		// comparison
		// eq
		{
			op:     "eq",
			params: []Value{1, 1, 1, 1},
			res:    true,
		},

		{
			op:     "eq",
			params: []Value{1, 1.0},
			res:    false,
		},

		{
			op:     "eq",
			params: []Value{"1", "1"},
			res:    true,
		},

		{
			op:     "eq",
			params: []Value{nil, nil},
			res:    true,
		},

		{
			op:     "eq",
			params: []Value{false, false},
			res:    true,
		},
		{
			op:     "eq",
			params: []Value{true, false},
			res:    false,
		},
		{
			op:     "eq",
			params: []Value{true, true, false, true, true, false},
			res:    false,
		},
		{
			op:     "eq",
			params: []Value{true},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "eq",
			params: []Value{1, 1, "true"},
			res:    false,
		},

		// ne
		{
			op:     "ne",
			params: []Value{1, 1, 1, 1},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "ne",
			params: []Value{1, 1},
			res:    false,
		},

		{
			op:     "ne",
			params: []Value{1, 1.0},
			res:    true,
		},

		{
			op:     "ne",
			params: []Value{"1", "1"},
			res:    false,
		},

		{
			op:     "ne",
			params: []Value{nil, nil},
			res:    false,
		},

		{
			op:     "ne",
			params: []Value{false, false},
			res:    false,
		},
		{
			op:     "ne",
			params: []Value{true, false},
			res:    true,
		},
		{
			op:     "ne",
			params: []Value{true, true, false, true, true, false},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "ne",
			params: []Value{true},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "ne",
			params: []Value{1, 1, "true"},
			errMsg: paramsCntErrMsg,
		},
		{
			op:     "ne",
			params: []Value{1, "true"},
			res:    true,
		},

		// gt
		{
			op:     "gt",
			params: toParams([]int64{1, 1}),
			res:    false,
		},

		{
			op:     "gt",
			params: toParams([]int64{1, 2}),
			res:    false,
		},

		{
			op:     "gt",
			params: toParams([]int64{1, -3}),
			res:    true,
		},

		{
			op:     "gt",
			params: toParams([]int64{0, 0}),
			res:    false,
		},

		{
			op:     "gt",
			params: toParams([]int64{0, -1}),
			res:    true,
		},

		{
			op:     "gt",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "gt",
			params: []Value{1, 1},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		{
			op:     "gt",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// ge
		{
			op:     "ge",
			params: toParams([]int64{1, 1}),
			res:    true,
		},

		{
			op:     "ge",
			params: toParams([]int64{1, 2}),
			res:    false,
		},

		{
			op:     "ge",
			params: toParams([]int64{1, -3}),
			res:    true,
		},

		{
			op:     "ge",
			params: toParams([]int64{0, 0}),
			res:    true,
		},

		{
			op:     "ge",
			params: toParams([]int64{0, -1}),
			res:    true,
		},

		{
			op:     "ge",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "ge",
			params: []Value{1, 1, 1},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "ge",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// lt
		{
			op:     "lt",
			params: toParams([]int64{1, 1}),
			res:    false,
		},

		{
			op:     "lt",
			params: toParams([]int64{1, 2}),
			res:    true,
		},

		{
			op:     "lt",
			params: toParams([]int64{1, -3}),
			res:    false,
		},

		{
			op:     "lt",
			params: toParams([]int64{0, 0}),
			res:    false,
		},

		{
			op:     "lt",
			params: toParams([]int64{0, -1}),
			res:    false,
		},

		{
			op:     "lt",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "lt",
			params: []Value{1, 1, 1},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "lt",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// le
		{
			op:     "le",
			params: toParams([]int64{1, 1}),
			res:    true,
		},

		{
			op:     "le",
			params: toParams([]int64{1, 2}),
			res:    true,
		},

		{
			op:     "le",
			params: toParams([]int64{1, -3}),
			res:    false,
		},

		{
			op:     "le",
			params: toParams([]int64{0, 0}),
			res:    true,
		},

		{
			op:     "le",
			params: toParams([]int64{0, -1}),
			res:    false,
		},

		{
			op:     "le",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "le",
			params: []Value{1, 1, 1},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "le",
			params: []Value{int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// between
		{
			op:     "between",
			params: toParams([]int64{1, 1, 1}),
			res:    true,
		},

		{
			op:     "between",
			params: toParams([]int64{1, 2, 3}),
			res:    false,
		},

		{
			op:     "between",
			params: toParams([]int64{1, -3, 5}),
			res:    true,
		},

		{
			op:     "between",
			params: toParams([]int64{0, 0, 1}),
			res:    true,
		},

		{
			op:     "between",
			params: toParams([]int64{0, -1, 0}),
			res:    true,
		},

		{
			op:     "between",
			params: []Value{int64(1)},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "between",
			params: []Value{1, 1, 1, 1},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "between",
			params: []Value{1, int64(1), 1.0},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// list
		// in
		{
			op:     "in",
			params: []Value{int64(1), []int64{1, 2, 3}},
			res:    true,
		},

		{
			op:     "in",
			params: []Value{int64(0), []int64{1, 2, 3}},
			res:    false,
		},

		{
			op:     "in",
			params: []Value{"a", []string{"a", "b", "c"}},
			res:    true,
		},

		{
			op:     "in",
			params: []Value{"abc", []string{"a", "b", "c"}},
			res:    false,
		},

		{
			op:     "in",
			params: []Value{int64(1), []int64{1, 1, 1}, []int64{2, 2, 2}},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "in",
			params: []Value{[]int64{1, 1, 1}},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "in",
			params: []Value{int64(1), []string{"a", "b", "c"}},
			errMsg: paramTypeErrMsg, // type of int param should be int64
		},

		// overlap
		{
			op:     "overlap",
			params: []Value{[]int64{0, 1, 2}, []int64{0, -1, -2}},
			res:    true,
		},

		{
			op:     "overlap",
			params: []Value{[]int64{1, 2}, []int64{-1, -2}},
			res:    false,
		},

		{
			op:     "overlap",
			params: []Value{[]int64{1, 2}, []int64{}},
			res:    false,
		},

		{
			op:     "overlap",
			params: []Value{[]int64{}, []int64{}},
			res:    false,
		},

		{
			op:     "overlap",
			params: []Value{[]string{"a", "b", "c"}, []string{"a", "b", "c"}},
			res:    true,
		},

		{
			op:     "overlap",
			params: []Value{[]string{"a", "b", "c"}, []string{}},
			res:    false,
		},

		{
			op:     "overlap",
			params: []Value{[]string{"aa", "bb", "cc"}, []string{"a", "b", "c"}},
			res:    false,
		},

		{
			op:     "overlap",
			params: []Value{[]int64{1, 1, 1}, []int64{1, 1, 1}, []int64{2, 2, 2}},
			errMsg: paramsCntErrMsg,
		},

		{
			op:     "overlap",
			params: []Value{[]string{"a", "b", "c"}, []int64{1, 1, 1}},
			errMsg: paramTypeErrMsg,
		},

		{
			op:     "overlap",
			params: []Value{int64(1), []string{"a", "b", "c"}},
			errMsg: paramTypeErrMsg,
		},

		{
			op:     "overlap",
			params: []Value{[]int64{1, 2}, []float64{1, 2}},
			errMsg: paramTypeErrMsg,
		},

		// time
		// date
		{
			op:     "date",
			params: []Value{"2022-05-06"},
			res:    toTime(defaultDateLayout, "2022-05-06").Unix(),
		},
		{
			op:     "date",
			params: []Value{"2022/05/06", "2006/01/02"}, // set the using layout
			res:    toTime(defaultDateLayout, "2022-05-06").Unix(),
		},
		{
			op:     "date",
			params: []Value{"2022/05/06"},
			errMsg: "cannot parse", // should set the layout for custom date format
		},
		{
			op:     "date",
			params: []Value{int64(20220506)},
			errMsg: paramTypeErrMsg,
		},
		{
			op:     "date",
			params: []Value{"2022-05-06", "2022-05-07", "2022-05-08"},
			errMsg: paramsCntErrMsg,
		},

		// datetime
		{
			op:     "datetime",
			params: []Value{"2022-05-06 03:56:12"},
			res:    toTime(defaultDatetimeLayout, "2022-05-06 03:56:12").Unix(),
		},
		{
			op:     "datetime",
			params: []Value{"03:56:12, 2022/05/06", "15:04:05, 2006/01/02"}, // set the using layout
			res:    toTime(defaultDatetimeLayout, "2022-05-06 03:56:12").Unix(),
		},
		{
			op:     "datetime",
			params: []Value{"15:04:05, 2022/05/06"},
			errMsg: "cannot parse", // should set the layout for custom date format
		},
		{
			op:     "datetime",
			params: []Value{int64(20220506035612)},
			errMsg: paramTypeErrMsg,
		},
		{
			op:     "datetime",
			params: []Value{"2022-05-06 03:56:12", "2022-05-07 03:56:12", "2022-05-08 03:56:12"},
			errMsg: paramsCntErrMsg,
		},

		// t_time
		{
			op:     "t_time",
			params: []Value{"2022-05-06 03:56:12", "2006-01-02 15:04:05"},
			res:    toTime(defaultDatetimeLayout, "2022-05-06 03:56:12").Unix(),
		},
		{
			op:     "t_time",
			params: []Value{"03:56:12, 2022/05/06", "15:04:05, 2006/01/02"}, // set the using layout
			res:    toTime(defaultDatetimeLayout, "2022-05-06 03:56:12").Unix(),
		},
		{
			op:     "t_time",
			params: []Value{"2022-05-06 03:56:12"},
			errMsg: paramsCntErrMsg,
		},
		// t_date
		{
			op:     "t_date",
			params: []Value{"2022-05-06", "2006-01-02"},
			res:    toTime(defaultDateLayout, "2022-05-06").Unix(),
		},
		{
			op:     "t_date",
			params: []Value{"2022/05/06", "2006/01/02"}, // set the using layout
			res:    toTime(defaultDateLayout, "2022-05-06").Unix(),
		},
		{
			op:     "t_date",
			params: []Value{"2022-05-06"},
			errMsg: paramsCntErrMsg,
		},

		// td_time
		{
			op:     "td_time",
			params: []Value{"2022-05-06 03:56:12"},
			res:    toTime(defaultDatetimeLayout, "2022-05-06 03:56:12").Unix(),
		},
		{
			op:     "td_time",
			params: []Value{"03:56:12, 2022/05/06"}, // set the using layout
			errMsg: "cannot parse",
		},
		{
			op:     "td_time",
			params: []Value{"2022-05-06 03:56:12", "2006-01-02 15:04:05"},
			errMsg: paramsCntErrMsg,
		},

		// td_date
		{
			op:     "td_date",
			params: []Value{"2022-05-06"},
			res:    toTime(defaultDateLayout, "2022-05-06").Unix(),
		},
		{
			op:     "td_date",
			params: []Value{"2022/05/06"}, // set the using layout
			errMsg: "cannot parse",
		},
		{
			op:     "td_date",
			params: []Value{"2022-05-06", "2006-01-02"},
			errMsg: paramsCntErrMsg,
		},

		// version
		// version
		{
			op:     "version",
			params: []Value{"1.2.3"},
			res:    int64(1_0002_0003),
		},
		{
			op:     "version",
			params: []Value{"1.2.3.4", int64(3)},
			res:    int64(1_0002_0003),
		},
		{
			op:     "version",
			params: []Value{"1.2.3.4", int64(4)},
			res:    int64(1_0002_0003_0004),
		},
		{
			op:     "version",
			params: []Value{"1.2", int64(3)},
			res:    int64(1_0002_0000),
		},
		{
			op:     "version",
			params: []Value{"1.2", int64(4)},
			res:    int64(1_0002_0000_0000),
		},
		{
			op:     "version",
			params: []Value{"0.1", int64(4)},
			res:    int64(1_0000_0000),
		},
		{
			op:     "version",
			params: []Value{"0.1"},
			res:    int64(1_0000),
		},
		{
			op:     "version",
			params: []Value{"0"},
			res:    int64(0),
		},
		{
			op:     "version",
			params: []Value{"1"},
			res:    int64(1_0000_0000),
		},
		{
			op:     "version",
			params: []Value{"0.0.0.1", int64(4)},
			res:    int64(1),
		},
		{
			op:     "version",
			params: []Value{"0.0.0.1", int64(1)},
			res:    int64(0),
		},
		{
			op:     "version",
			params: []Value{"233.2.0.3.1.5", int64(1)},
			res:    int64(233),
		},
		{
			op:     "version",
			params: []Value{"233.2.0.3.1.5", int64(2)},
			res:    int64(233_0002),
		},
		{
			op:     "version",
			params: []Value{"1.2.3.4", int64(0)},
			errMsg: "version valid length out of range",
		},
		{
			op:     "version",
			params: []Value{"1.2.3.4.5", int64(5)},
			errMsg: "version valid length out of range",
		},
		{
			op:     "version",
			params: []Value{"1.abc.3.def"},
			errMsg: "version layout error",
		},
		{
			op:     "version",
			params: []Value{"1.22222.3"},
			errMsg: "version layout error",
		},
		{
			op:     "version",
			params: []Value{1.2},
			errMsg: paramTypeErrMsg,
		},
		{
			op:     "version",
			params: []Value{},
			errMsg: paramsCntErrMsg,
		},

		// t_version
		{
			op:     "t_version",
			params: []Value{"1.2.3"},
			res:    int64(1_0002_0003),
		},
		{
			op:     "t_version",
			params: []Value{"0.0.3"},
			res:    int64(3),
		},
		{
			op:     "t_version",
			params: []Value{"3.0.0"},
			res:    int64(3_0000_0000),
		},
		{
			op:     "t_version",
			params: []Value{"1.abc.3.def"},
			errMsg: "version layout error",
		},
		{
			op:     "t_version",
			params: []Value{"1.22222.3"},
			errMsg: "version layout error",
		},
		{
			op:     "t_version",
			params: []Value{1.2},
			errMsg: paramTypeErrMsg,
		},
		{
			op:     "t_version",
			params: []Value{},
			errMsg: paramsCntErrMsg,
		},
	}

	for _, c := range testCases {
		fn := builtinOperators[c.op]
		assertNotNil(t, fn, c)

		res, err := fn(nil, c.params)
		if len(c.errMsg) != 0 {
			assertErrStrContains(t, err, c.errMsg, c)
			continue
		}

		assertNil(t, err, c)
		assertEquals(t, res, c.res, c)
	}
}
