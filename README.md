<img src="docs/images/logo_ladder_white.png" alt="Eval Logo" width="400" align="right">

# Eval

**Eval** is an expression evaluation engine purely written in golang with only go stand libraries.
It takes a string expression, compiles the expression into a highly optimized structure, then efficiently evaluates the
result of the expression.

## Highlights

* Fast, probably **the fastest** expression evaluation engine in the go world (benchmark).
* Easy to use, support register customized operators, variables and constants.
* Useful tools:
    * Debug Panel: a Terminal UI to helps you understand how your expressions are executed.
    * Expression Cost Optimizer: it uses Machine Learning algorithms to optimize your expressions and make them even faster.

## Basic Usage

### Install

```bash
go get github.com/onheap/eval
```

### Example

[Play Online](https://go.dev/play/p/DLp87Sqe8gF)

```go
package main

import (
	"fmt"
	"github.com/onheap/eval"
)

func main() {
	expr := `(and (>= age 30) (= gender "Male"))`

	vars := map[string]interface{}{
		"age":    30,
		"gender": "Male",
	}

	// new config and register variables
	config := eval.NewConfig(eval.RegVarAndOp(vars))

	// compile string expression to program
	program, err := eval.Compile(config, expr)

	// evaluation expression with variables
	res, err := program.Eval(eval.NewCtxFromVars(config, vars))

	if err != nil {
		panic(err)
	}

	fmt.Printf("%v", res)
}
```

### Useful Features
* **TryEval** tries to execute the expression when only partial variables are fetched. It skips sub-expressions where no variables were fetched, tries to find at least one sub-branch that can be fully executed with the currently fetched variables, and returns the final result.
  > It is typically used for the scenarios that fetching variables is expansive and the root operator is bool operators.

  
* **ReportEvent** is a configuration option. If it is enabled, the evaluation engine will send events to the EventChannel for each execution step. We can use this feature to observe the internal execution of the engine and to collect statistics on the execution of expressions.


* **Dump / DumpTable / IndentByParentheses**
  * _Dump_ decompiles the compiled expressions into the corresponding string expressions.
  * _DumpTable_ dumps the compiled expressions into an easy-to-understand format.
  * _IndentByParentheses_ formats string expressions.


### Compile Options
* **ConstantFolding** evaluates constant subexpressions at compile time to reduce the complicity of the expression.
  <details>
  <summary>
  Examples
  </summary>
  </details>


* **ReduceNesting** flattens `and` and `or` operators to reduce the nesting level of the expression. Makes short circuits more efficient.
  <details>
  <summary>
  Examples
  </summary>
  </details>


* **FastEvaluation** optimizes hot path subexpressions to reduce the number of loops and stack operations in the evaluation engine.
  <details>
  <summary>
  Examples
  </summary>
  </details>


* **Reordering** reorders subexpressions based on the execution cost. Priory to execute subexpression with less cost, trigger short circuit earlier.
  <details>
  <summary>
  Examples
  </summary>
  </details>

## Tools
#### Debug Panel

<img src="docs/images/debug_panel.gif" alt="Debug Panel" width="800">

A Terminal UI that shows compiled expressions and step-by-step execution progress. Helps you understand how your expressions are executed.

[Learn more →](https://github.com/onheap/eval_lab/tree/main/tui).

#### Expression Cost Optimizer

It uses [Genetic Algorithms](https://en.wikipedia.org/wiki/Genetic_algorithm) (others are optional) to optimize the over all expressions execution time, generates the best scores for the `CostsMap`. 

As shown in the figure below, the total number of executing all rules over all users decreases from 600398 to 558686, reduced by ~7% after ten generations.

```go
initial execution count: 600398
Best fitness at generation 0: 587977.000000
Best fitness at generation 1: 585488.000000
Best fitness at generation 2: 583040.000000
Best fitness at generation 3: 560880.000000
Best fitness at generation 4: 560880.000000
Best fitness at generation 5: 560880.000000
Best fitness at generation 6: 560880.000000
Best fitness at generation 7: 559697.000000
Best fitness at generation 8: 557820.000000
Best fitness at generation 9: 556075.000000
Best fitness at generation 10: 556075.000000
```
[Learn more →](https://github.com/onheap/eval_lab/tree/main/optimizer).

<details>
<summary>
Generated <code>CostsMap</code> scores
</summary>

```go
{
        `address`: 257.5637323444525
        `address.city`: -29.732067230828733
        `address.country`: -4.445875953501092
        `address.state`: -2.733315237719508
        `age`: 13.534118456114095
        `app_version`: 81.96361572619793
        `balance`: 6.5089373401145805
        `birth_date`: 29.504377681831215
        `created_at`: 1.8939662469501435
        `credit`: -14.994423737587496
        `credit_limit`: -20.952782417744316
        `discount`: 1.516122498612845
        `distance`: -2.461526385425413
        `gender`: -20.00951321901351
        `interests`: -1.9843024344711226
        `is_birthday`: 2.0701165078726405
        `is_student`: -6.213750700033799
        `is_vip`: 222.7708005914785
        `language`: -60.04923908428884
        `now`: 85.7151642404042
        `os_version`: -0.0051749009548118785
        `platform`: -8.66752799417992
        `updated_at`: 36.56643865523681
        `user_id`: 20.934025789111697
        `user_tags`: -6.7672454401690025
}
```
</details>

## Benchmark

Benchmark between different Go Expression Evaluation projects. 
```bash
❯ go test -bench=. -run=none -benchtime=3s -benchmem
goos: darwin
goarch: amd64
pkg: github.com/onheap/eval_lab/benchmark/projects
cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
 Benchmark_bexpr-16              1748103          2033 ns/op           888 B/op          45 allocs/op
 Benchmark_celgo-16             26671797           134.9 ns/op          16 B/op           1 allocs/op
┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
│Benchmark_eval-16              53657851            63.31 ns/op         32 B/op           1 allocs/op│
└────────────────────────────────────────────────────────────────────────────────────────────────────┘
 Benchmark_tryEval-16           27411960           126.4 ns/op          32 B/op           1 allocs/op
 Benchmark_evalfilter-16         2012268          1796 ns/op           848 B/op          24 allocs/op
 Benchmark_expr-16              27877728           122.5 ns/op          32 B/op           1 allocs/op
 Benchmark_goja-16              12890437           283.3 ns/op          96 B/op           2 allocs/op
 Benchmark_govaluate-16         16873670           207.6 ns/op          24 B/op           2 allocs/op
 Benchmark_gval-16               6209001           570.2 ns/op         240 B/op           8 allocs/op
 Benchmark_otto-16               5466194           656.4 ns/op         336 B/op           7 allocs/op
 Benchmark_starlark-16            784425          4467 ns/op          3568 B/op          68 allocs/op
PASS
ok      github.com/onheap/eval_lab/benchmark/projects    45.613s
```

<details>
<summary>
Other Benchmarks
</summary>

Benchmark between Eval and Itoa
```bash
❯ go test -bench=. -benchtime=3s -benchmem
goos: darwin
goarch: amd64
pkg: github.com/onheap/eval_lab/benchmark/itoa
cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
Benchmark_eval-16    	50816451	        63.58 ns/op	      32 B/op	       1 allocs/op
Benchmark_itoa-16    	74626735	        46.48 ns/op	      24 B/op	       2 allocs/op
PASS
ok  	github.com/onheap/eval_lab/benchmark/itoa	6.981s
```
The cost of executing an expression is at the same level as `strconv.Itoa(12345678)`. it should be **worry free** to use this project.


```go
                   200ps - 4.6GHz single cycle time
                1ns      - L1 cache latency
               10ns      - L2/L3 cache SRAM latency
               20ns      - DDR4 CAS, first byte from memory latency
               20ns      - C++ raw hardcoded structs access
               46ns      - Go itoa an 8-digtal number
  ---------->  64ns      - Eval execute an expression
               80ns      - C++ FlatBuffers decode/traverse/dealloc
              150ns      - PCIe bus latency
              171ns      - cgo call boundary, 2015
              200ns      - HFT FPGA
              475ns      - 2020 MLPerf winner recommendation inference time per sample
              800ns      - Go Protocol Buffers Marshal
              837ns      - Go json-iterator/go json unmarshal
           1µs           - Go protocol buffers unmarshal
           3µs           - Go JSON Marshal
           7µs           - Go JSON Unmarshal
          10µs           - PCIe/NVLink startup time
          17µs           - Python JSON encode/decode times
          30µs           - UNIX domain socket; eventfd; fifo pipes
         100µs           - Redis intrinsic latency; KDB+; HFT direct market access
         200µs           - 1GB/s network air latency; Go garbage collector pauses interval 2018
         230µs           - San Francisco to San Jose at speed of light
         500µs           - NGINX/Kong added latency
     10ms                - AWS DynamoDB; WIFI6 "air" latency
     15ms                - AWS Sagemaker latency; "Flash Boys" 300million USD HFT drama
     30ms                - 5G "air" latency
     36ms                - San Francisco to Hong-Kong at speed of light
    100ms                - typical roundtrip from mobile to backend
    200ms                - AWS RDS MySQL/PostgreSQL; AWS Aurora
 10s                     - AWS Cloudfront 1MB transfer time
```
</details>

## License

Released under the [Apache License](LICENSE).