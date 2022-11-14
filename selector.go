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

// UndefinedSelKey means that the current key is defined in the SelectorKey type
// In this case you should use the string type key
const UndefinedSelKey SelectorKey = math.MinInt16

// DNE means Does Not Exist, it used in RCO (remote call optimization).
// When executing an expression with RCO, if the value of a SelectorKey is not cached,
// the selector proxy will return DNE as its value
type dne struct{ DoesNotExist string }

func (dne) String() string { return "DNE" }

var DNE = dne{DoesNotExist: "DNE"}

var ErrDNE = errors.New("DNE")

// Selector is used to get values of the expression variables.
// Note that there are two types of keys in each method parameters,
// selKey is of type SelectorKey, strKey is of type string,
// selKey offers better performance, strKey offers more flexibility,
// we can use any of them, as they will all be passed in during the expr execution.
// we recommend using selKey (if it satisfies your requirements) to get better performance.
type Selector interface {
	// Get gets a value from the selector
	Get(selKey SelectorKey, strKey string) (Value, error)
	// Set sets the value and the associated key to the selector
	Set(selKey SelectorKey, strKey string, val Value) error
	// Cached returns whether the value of the key has been cached
	Cached(selKey SelectorKey, strKey string) bool
}

func GetOrRegisterKey(cc *CompileConfig, name string) SelectorKey {
	if key, exist := cc.SelectorMap[name]; exist {
		return key
	}
	size := len(cc.SelectorMap)
	keySet := make(map[SelectorKey]bool, size)
	for _, key := range cc.SelectorMap {
		keySet[key] = true
	}
	for i := 1; i <= size; i++ {
		key := SelectorKey(i)
		if !keySet[key] {
			cc.SelectorMap[name] = key
			return key
		}
	}
	key := SelectorKey(size + 1)
	cc.SelectorMap[name] = key
	return key
}

func ToValueMap(m map[string]interface{}) map[string]Value {
	res := make(map[string]Value)
	for k, v := range m {
		res[k] = unifyType(v)
	}
	return res
}

func NewCtxWithMap(cc *CompileConfig, vals map[string]interface{}) *Ctx {
	var sel Selector
	if sliceSelectorAvailable(cc) {
		sel = NewSliceSelector(cc, vals)
	} else {
		sel = NewMapSelector(vals)
	}

	return &Ctx{
		Selector: sel,
	}
}

func sliceSelectorAvailable(cc *CompileConfig) bool {
	const (
		minKeyRange = 0
		maxKeyRange = 256
	)

	if cc.CompileOptions[AllowUnknownSelectors] {
		return false
	}

	if len(cc.SelectorMap) > maxKeyRange-minKeyRange {
		return false
	}

	for _, key := range cc.SelectorMap {
		if key < minKeyRange || key >= maxKeyRange {
			return false
		}
	}

	return true
}

type SliceSelector struct {
	Values []Value
}

func NewSliceSelector(cc *CompileConfig, vals map[string]interface{}) SliceSelector {
	maxKey := 0
	for name := range vals {
		key := GetOrRegisterKey(cc, name)
		if int(key) > maxKey {
			maxKey = int(key)
		}
	}
	size := max(len(cc.SelectorMap), maxKey+1)
	sel := SliceSelector{
		Values: make([]Value, size),
	}
	for name, val := range vals {
		key := cc.SelectorMap[name]
		sel.Values[key] = unifyType(val)
	}
	return sel
}

func (s SliceSelector) Get(key SelectorKey, _ string) (Value, error) {
	if int(key) >= len(s.Values) {
		return nil, fmt.Errorf("selectorKey not exist %d", key)
	}
	return s.Values[key], nil
}

func (s SliceSelector) Set(key SelectorKey, _ string, val Value) error {
	if int(key) >= len(s.Values) {
		return fmt.Errorf("selectorKey not exist %d", key)
	}
	s.Values[key] = val
	return nil
}

func (s SliceSelector) Cached(key SelectorKey, _ string) bool {
	if int(key) >= len(s.Values) {
		return false
	}
	return true
}

type MapSelector struct {
	Values map[string]Value
}

func NewMapSelector(vals map[string]interface{}) MapSelector {
	s := MapSelector{
		Values: make(map[string]Value),
	}
	for name, val := range vals {
		s.Values[name] = unifyType(val)
	}
	return s
}

func (s MapSelector) Get(_ SelectorKey, key string) (Value, error) {
	val, exist := s.Values[key]
	if !exist {
		return nil, fmt.Errorf("selectorKey not exist %s", key)
	}
	return val, nil
}

func (s MapSelector) Set(_ SelectorKey, key string, val Value) error {
	s.Values[key] = val
	return nil
}

func (s MapSelector) Cached(_ SelectorKey, key string) bool {
	_, exist := s.Values[key]
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
