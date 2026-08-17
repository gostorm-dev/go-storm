# AGENTS.md

## Project Mission

`go-storm` is a high-performance, developer-first load testing engine written in Go.

The goal is not to build "just another load tester".

The goal is:

> **STATE-OF-THE-ART CLI ENGINEERING FOR LOAD TESTING**

Every engineering decision must move the project toward that goal.

---

# 1. Core Engineering Philosophy

The engine must optimize for:

> **Same Test → Predictable Load → Accurate Measurements → Low Generator Overhead → Correct Failure Detection → Reproducible Results**

These principles have higher priority than adding features quickly.

Do NOT sacrifice correctness, measurement accuracy, or architecture quality just to ship a feature faster.

---

# 2. THINK BEFORE YOU CODE

## Mandatory rule

**Never start writing code immediately after receiving a task.**

Before modifying code, first understand:

1. What problem are we solving?
2. Why does this problem exist?
3. What is the expected behavior?
4. What are the edge cases?
5. What existing components are affected?
6. What are the performance implications?
7. What are the concurrency implications?
8. What failure modes can occur?
9. How will this be tested?
10. How will we benchmark or measure the change?

If any of these are unclear, investigate the repository first.

---

# 3. Repository Understanding

Before implementing a non-trivial change:

* Inspect the existing architecture.
* Find related interfaces and abstractions.
* Find existing tests.
* Find existing benchmarks.
* Understand ownership of state.
* Understand goroutine lifecycle.
* Understand channel usage.
* Understand cancellation behavior.
* Understand error propagation.
* Check whether the functionality already partially exists.

**Do not duplicate existing functionality.**

Prefer extending a good existing abstraction over introducing a parallel abstraction.

---

# 4. Design Before Implementation

For every significant change, reason about the design first.

Prefer this mental sequence:

```text
Problem
  ↓
Requirements
  ↓
Constraints
  ↓
Design
  ↓
Trade-offs
  ↓
Implementation
  ↓
Tests
  ↓
Benchmark
  ↓
Review
```

For complex changes, write a short design note before coding.

The design should answer:

* What changes?
* Why?
* Which components are affected?
* What data flows through the system?
* What happens under failure?
* What happens under cancellation?
* What happens under high load?
* What happens when resources are exhausted?

---

# 5. Performance Is a First-Class Requirement

`go-storm` is a load testing engine.

Therefore the load generator itself must not unnecessarily become the bottleneck.

Always consider:

* CPU overhead
* memory usage
* allocations
* garbage collection pressure
* lock contention
* channel contention
* goroutine count
* network overhead
* connection reuse
* synchronization overhead
* scheduler behavior

Do not optimize blindly.

**Measure first. Optimize based on evidence.**

Use:

```bash
go test ./...
go test -race ./...
go test -bench=.
```

When appropriate, use CPU, memory and goroutine profiling.

---

# 6. Load Generation Correctness

Never confuse:

* concurrency
* request rate
* throughput
* latency

The engine must generate the requested workload as accurately as possible.

If the requested rate cannot be achieved because the generator is saturated, the engine must detect and report it.

Never silently produce misleading results.

Example:

```text
Requested: 100,000 RPS
Achieved:   72,000 RPS
Generator:  CPU saturated
```

This must be treated as an important test condition, not hidden.

---

# 7. Measurement Correctness

Metrics are more important than flashy features.

Latency measurements must be trustworthy.

Carefully consider:

* clock usage
* measurement boundaries
* aggregation
* histogram accuracy
* dropped results
* sampling
* concurrent metric updates
* percentile calculation
* overflow
* very high request counts

Never introduce a metric implementation without understanding its accuracy and performance characteristics.

---

# 8. Concurrency Rules

Go makes concurrency easy.

That does NOT mean concurrency is free.

Every goroutine must have a clear lifecycle.

Every channel must have a clear ownership model.

Every goroutine must have a termination path.

Avoid:

* goroutine leaks
* uncontrolled goroutine creation
* unnecessary locks
* unnecessary channels
* shared mutable state
* data races
* deadlocks
* busy waiting

Use `context.Context` for cancellation where appropriate.

Run:

```bash
go test -race ./...
```

for concurrency-related changes.

---

# 9. Error Handling

Errors must never disappear silently.

Do not:

```go
_ = err
```

unless the error is intentionally ignored and there is a documented reason.

Differentiate between:

* request failure
* timeout
* connection failure
* DNS failure
* TLS failure
* server error
* client-side failure
* generator saturation
* configuration error
* internal engine error

The user must be able to understand **why** a test failed.

---

# 10. Resource Management

Every acquired resource must have a clear lifecycle.

Pay attention to:

* HTTP connections
* response bodies
* sockets
* goroutines
* timers
* tickers
* channels
* buffers
* files
* contexts

Avoid leaks.

Be especially careful with timers and tickers in high-throughput loops.

---

# 11. No Premature Abstractions

Do not create abstractions simply because they "might be useful later".

Prefer:

* simple
* explicit
* composable
* testable

Avoid:

* unnecessary interfaces
* excessive generics
* excessive dependency injection
* over-engineered factories
* abstraction layers without a real requirement

Abstraction must solve a real problem.

---

# 12. No Magic

Avoid unexplained constants.

Bad:

```go
if latency > 237 {
```

Prefer meaningful constants or configuration.

Names should communicate intent.

Code should be understandable without reverse-engineering it.

---

# 13. Dependencies

Do not add a dependency without a reason.

Before adding a dependency:

1. Check whether the standard library is sufficient.
2. Check whether the project already has an equivalent.
3. Evaluate maintenance quality.
4. Evaluate performance.
5. Evaluate license compatibility.
6. Evaluate dependency size and transitive dependencies.

Prefer the standard library when it is good enough.

---

# 14. Testing Is Part of Implementation

A feature is NOT complete when the code compiles.

A feature is complete when:

* behavior is correct
* edge cases are handled
* tests exist
* race conditions are checked where relevant
* performance is understood where relevant
* failure behavior is tested

Tests should verify behavior, not implementation details.

Include tests for:

* normal behavior
* boundary conditions
* invalid input
* cancellation
* timeouts
* failures
* concurrent execution
* resource cleanup

---

# 15. Benchmark Important Code

For performance-sensitive code, add benchmarks.

Measure:

* operations/sec
* ns/op
* allocations/op
* bytes/op

Example:

```bash
go test -bench=. -benchmem
```

Do not claim something is "faster" without measurement.

---

# 16. CLI Quality

The CLI is a first-class product.

Commands should be:

* predictable
* discoverable
* scriptable
* CI-friendly
* readable
* deterministic

Errors should be actionable.

Bad:

```text
error occurred
```

Good:

```text
failed to create HTTP client:
TLS configuration is invalid
```

Exit codes must be meaningful.

Machine-readable output should be available where useful.

---

# 17. Backward Compatibility

Do not casually break:

* CLI commands
* configuration format
* output formats
* public APIs
* extension interfaces

If breaking behavior is necessary, explicitly explain:

* what breaks
* why
* migration path

---

# 18. Observability

The engine must be able to explain itself.

Where appropriate expose:

* requests
* RPS
* latency
* errors
* concurrency
* active workers
* generator utilization
* dropped results
* resource usage

The user should be able to distinguish:

```text
Target is slow
```

from:

```text
Load generator is slow
```

This distinction is critical.

---

# 19. Reproducibility

The same test configuration should produce the same intended workload characteristics as consistently as possible.

Record relevant metadata such as:

* configuration
* duration
* target
* load model
* tool version
* environment information
* relevant runtime settings

Performance results should be comparable between runs.

---

# 20. Failure Is a Design Case

Always ask:

> "What happens when this fails?"

Consider:

* target unavailable
* network failure
* worker failure
* context cancellation
* timeout
* malformed configuration
* resource exhaustion
* partial results
* unexpected server behavior
* generator saturation

Do not design only for the happy path.

---

# 21. Code Quality Rules

Prefer code that is:

* readable
* small
* explicit
* testable
* idiomatic Go
* low allocation where it matters
* concurrency-safe
* easy to profile

Avoid:

* clever code
* unnecessary complexity
* giant functions
* hidden global state
* duplicated logic
* premature optimization
* TODO-driven architecture

---

# 22. Before Every Implementation

Before writing code, mentally answer:

```text
[ ] Do I understand the problem?
[ ] Do I understand the existing architecture?
[ ] Is there already code solving part of this?
[ ] What is the simplest correct design?
[ ] What are the concurrency implications?
[ ] What are the failure modes?
[ ] What are the performance implications?
[ ] How will I test it?
[ ] How will I measure it?
[ ] Can this introduce a regression?
```

If these questions cannot be answered, investigate before implementing.

---

# 23. Definition of Done

Never consider a task complete merely because:

```text
"it works on my machine"
```

A task is done when:

```text
Correct
+
Tested
+
Race-safe
+
Measured
+
Performant
+
Failure-aware
+
Maintainable
```

For performance-sensitive work:

```text
Correct
+
Benchmark
+
Profile
+
Optimize
+
Benchmark Again
```

---

# 24. Golden Rule

When choosing between:

```text
Fast implementation
```

and

```text
Correct, measurable, maintainable implementation
```

choose the second.

When choosing between:

```text
More features
```

and

```text
Stronger core engine
```

choose the stronger core engine.

When unsure:

> **Measure. Don't guess.**

When tempted to rush:

> **Understand first. Code second.**

The objective is not to produce a lot of code.

The objective is to build an engine that engineers can trust under extreme load.

---

# Final Standard

Every contribution to `go-storm` should move the project closer to:

> **The strongest, most accurate, efficient, predictable, and developer-friendly load testing CLI possible in Go.**

**Build deliberately. Measure everything important. Never fake confidence.**
