# BENCHMARKS — go-storm vs k6 vs wrk vs hey vs vegeta

> First published run: **2026-08-22** · methodology in [`bench/README.md`](bench/README.md)
>
> Philosophy: numbers are reported as measured. Losses are shown, not hidden.
> A benchmark you cannot reproduce is a benchmark you should not trust.

## Environment

| | |
|---|---|
| Machines | 2 × AWS c6i.large (2 vCPU), same AZ (ap-south-1a) |
| Network | Private IPs between generator and target |
| Target | chi-based HTTP server (Go), identical for all tools |
| go-storm | built from commit `e046a77` |
| Competitors | wrk 4.1.0 · k6 v2.2.0 · hey (latest, Aug 2026) · vegeta v12.13.0 |
| Protocol | warmup + 3 measured runs per scenario → median; 10s cooldown; sar on both machines |

## Results (median of 3)

### S1 — Burst: 100k requests, c=100
| Tool | RPS | p50 | p95 | p99 | Errors |
|---|---|---|---|---|---|
| hey | **42,887** 👑 | 2.0ms | 5.3ms | 7.2ms | 0 |
| go-storm | 35,986 | 2.66ms | 7.55ms | 9.70ms | 0 |

*(wrk/k6/vegeta excluded: no count mode — feature gap, documented)*

### S2 — Sustained soak: 60s, c=100
| Tool | RPS | p50 | p95 | p99 | Gen CPU% |
|---|---|---|---|---|---|
| wrk | **65,621** 👑 | n/a* | n/a* | n/a* | 48.4 |
| hey | 44,237 | 2.0ms | 5.0ms | 6.3ms | 98.2 |
| **go-storm** | **35,870** | 2.65ms | 7.52ms | **9.75ms** | 97.8 |
| k6 | 24,332 | 3.42ms | 9.48ms | 16.03ms | n/a* |

*\*known capture gaps this round — see Known Gaps below*

### S3 — Rate accuracy: target 5000 req/s for 30s
| Tool | Achieved RPS | Deviation | Verdict |
|---|---|---|---|
| k6 | 4999.8 | −0.005% | 👑 perfect |
| hey | 4990.0 | −0.02% | excellent |
| **go-storm** | **5166.4** | **+3.33%** | ⚠️ known bug, fix targeted v0.6.1 |

### S4 — Generator ceiling: c=1000, 30s
| Tool | RPS | p50 | p99 | Gen CPU% |
|---|---|---|---|---|
| wrk | **60,520** 👑 | n/a* | n/a* | 41.3 |
| hey | 38,862 | 24.5ms | 48.8ms | 96.0 |
| **go-storm** | **31,198** | 32.6ms | 96.9ms | 96.3 |
| vegeta | 30,696 | 23,590ms† | 46,528ms† | 96.5 |
| k6 | 18,154 | 46.0ms | 147.6ms | n/a* |

### Head-to-head: go-storm vs k6
- S2 soak: **+47.4% throughput**, p99 39% lower
- S4 ceiling: **+71.8% throughput**

### Efficiency (S2: RPS per generator CPU point)
```
wrk     1356   ← C + epoll; the raw-speed reference
hey      450
go-storm 367
vegeta   341
```

## Honest findings from this round

1. **wrk wins raw throughput everywhere it runs.** It is C with epoll and decades of tuning.
   We publish that because pretending otherwise would make every other number suspicious.
2. **go-storm beats k6 on throughput AND tail latency** in every shared scenario.
3. **We found our own bug:** `-r 5000 -d 30s` sends exactly 155,000 requests instead of
   150,000 (+3.33%, deterministic across runs). Fix targeted for v0.6.1.
4. The target server did not break even at c=1000 (0% errors, all tools):
   S5 needs a heavier endpoint to find real server limits. Generator-side ceilings were hit first.

## Known gaps in this round

- wrk percentile output not captured (`--latency` flag omitted; added to suite after this run)
- k6 generator CPU% missing (sar not started on k6 runs; fixed in suite)
- vegeta latency figures look anomalous under open pacing (under investigation)
- Kernel/tool metadata partially recorded; SETUP.md now mandates full env capture

Reproduce everything yourself: [`bench/`](bench/README.md).
