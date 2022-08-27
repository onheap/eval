## Summary

## Test Plan
- [ ] Unit test passed
```
❯ go test

```

- [ ] No degradation in performance
```
❯ go test -bench=BenchmarkEval -run=none -benchtime=3s -benchmem

```

- [ ] Tested on over a million random expressions
```
❯ go test -run='TestRandomExpressions'

```

## Reviewers
@onheap