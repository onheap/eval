package eval

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	builtinConstants = map[string]Value{
		"true":  true,
		"false": false,
	}
)

// UndefinedVarKey means that the current key is undefined in the VariableKey type
// In this case you should use the string type key
const UndefinedVarKey VariableKey = math.MinInt16

// DNE means Does Not Exist, it used in RCO (remote call optimization).
// When executing an expression with RCO, if the variable is not cached,
// the variable fetcher proxy will return DNE as its value
type dne struct{ DoesNotExist string }

func (dne) String() string { return "DNE" }

var DNE = dne{DoesNotExist: "DNE"}

var ErrDNE = errors.New("DNE")

// VariableFetcher is used to fetch values of the expression variables.
// Note that there are two types of keys in each method parameters,
// The varKey is of type VariableKey, the strKey is of type string,
// The varKey offers better performance, the strKey offers more flexibility,
// we can use any of them (or hybrid), as they both are passed in during the expression execution.
// we recommend using varKey (as much as possible) to get better performance.
type VariableFetcher interface {
	// Get gets a value from the variable
	Get(varKey VariableKey, strKey string) (Value, error)
	// Set sets the value and the associated key to the variable
	Set(varKey VariableKey, strKey string, val Value) error
	// Cached returns whether the value of the key has been cached
	Cached(varKey VariableKey, strKey string) bool
}

func GetOrRegisterKey(cc *Config, name string) VariableKey {
	if key, exist := cc.VariableKeyMap[name]; exist {
		return key
	}
	size := len(cc.VariableKeyMap)
	keySet := make(map[VariableKey]bool, size)
	for _, key := range cc.VariableKeyMap {
		keySet[key] = true
	}
	for i := 1; i <= size; i++ {
		key := VariableKey(i)
		if !keySet[key] {
			cc.VariableKeyMap[name] = key
			return key
		}
	}
	key := VariableKey(size + 1)
	cc.VariableKeyMap[name] = key
	return key
}

func ToValueMap(m map[string]interface{}) map[string]Value {
	res := make(map[string]Value)
	for k, v := range m {
		res[k] = unifyType(v)
	}
	return res
}

func NewCtxFromVars(cc *Config, vals map[string]interface{}) *Ctx {
	if cc.CompileOptions[AllowUndefinedVariable] {
		return &Ctx{VariableFetcher: NewMapVarFetcher(vals)}
	}

	var fetcher VariableFetcher
	minKey, maxKey := varKeyRange(cc)
	if minKey <= maxKey && 0 <= minKey && maxKey < 256 {
		fetcher = NewSliceVarFetcher(cc, vals)
	} else {
		fetcher = NewMapVarFetcher(vals)
	}

	return &Ctx{VariableFetcher: fetcher}
}

func varKeyRange(cc *Config) (min, max VariableKey) {
	min, max = math.MaxInt16, math.MinInt16
	for _, key := range cc.VariableKeyMap {
		if key < min {
			min = key
		}
		if key > max {
			max = key
		}
	}
	return
}

type SliceVarFetcher []Value

func NewSliceVarFetcher(cc *Config, vals map[string]interface{}) SliceVarFetcher {
	_, maxKey := varKeyRange(cc)
	fetcher := make([]Value, maxKey+1)

	for name, key := range cc.VariableKeyMap {
		if val, exist := vals[name]; exist {
			fetcher[key] = unifyType(val)
		}
	}

	return fetcher
}

func (s SliceVarFetcher) Get(key VariableKey, _ string) (Value, error) {
	if int(key) >= len(s) {
		return nil, fmt.Errorf("variableKey not exist %d", key)
	}
	return s[key], nil
}

func (s SliceVarFetcher) Set(key VariableKey, _ string, val Value) error {
	if int(key) >= len(s) {
		return fmt.Errorf("variableKey not exist %d", key)
	}
	s[key] = val
	return nil
}

func (s SliceVarFetcher) Cached(key VariableKey, _ string) bool {
	if int(key) >= len(s) {
		return false
	}
	return true
}

type MapVarFetcher map[string]Value

func NewMapVarFetcher(vals map[string]interface{}) MapVarFetcher {
	s := make(map[string]Value, len(vals))
	for name, val := range vals {
		s[name] = unifyType(val)
	}
	return s
}

func (s MapVarFetcher) Get(_ VariableKey, key string) (Value, error) {
	val, exist := s[key]
	if !exist {
		return nil, fmt.Errorf("variableKey not exist %s", key)
	}
	return val, nil
}

func (s MapVarFetcher) Set(_ VariableKey, key string, val Value) error {
	s[key] = val
	return nil
}

func (s MapVarFetcher) Cached(_ VariableKey, key string) bool {
	_, exist := s[key]
	return exist
}

func UnifyType(val Value) Value {
	switch val.(type) {
	case bool, string, int64, []int64, []string:
		return val
	default:
		return unifyType(val)
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
	case []int32:
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
