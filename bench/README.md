# go-storm Benchmark Suite

Two kinds of benchmarks live here. They answer different questions.

| Kind | Files | Question it answers |
|---|---|---|
| **Engine micro-benchmarks** | `go test -bench ./...` (baselines: `bench-*.txt`) | Is this code fast? (ns/op, allocs/op per function) |
| **Cross-tool suite** | `run_bench.sh`, `k6_script.js`, `analyze.py` | Where does go-storm stand against k6/wrk/hey/vegeta on real hardware? |

Published results of the cross-tool suite: [`BENCHMARKS.md`](../BENCHMARKS.md) at repo root.

---

## Cross-tool Suite

### Methodology (the rules that make numbers trustworthy)

- **Machines:** 2 × c6i.large (2 vCPU Intel Cascade Lake) EC2 instances,
  **same availability zone**, traffic over **private IPs only**
  (zero internet latency noise).
- **Target:** a fixed chi-based HTTP server, identical for every tool.
- **Warmup:** every measured run is preceded by one throwaway warmup run (~10% intensity).
- **Measured runs:** 3 per scenario per tool → **median is reported, never best-of**.
- **Cooldown:** 10 seconds between runs.
- **Monitoring:** `sar -u -r 1` runs on BOTH machines during every measured run.
  Generator CPU% comes from sar, not from self-reporting — same ruler for every tool.

### Scenarios

| ID | Scenario | Load | Tests |
|---|---|---|---|
| S1 | Count burst | `-n 100000 -c 100` | generator speed |
| S2 | Sustained soak | `-d 60s -c 100` | stability over time |
| S3 | Rate accuracy | `-r 5000 -d 30s` | does the tool deliver the rate you asked? |
| S4 | Generator ceiling | `-c 1000 -d 30s` | efficiency under extreme concurrency |
| S5 | Server breakpoint | steps c=25→1000 ×10s each | where does the target start failing |

Fairness notes:
- wrk supports neither count mode nor rate limiting → excluded from S1/S3 (feature gap, not hidden).
- Every tool's exact command is visible in `run_bench.sh`. Nothing hidden.

### Formulas

```
RPS            = total_requests / duration_seconds
Deviation %    = |achieved_rps − target_rps| / target_rps × 100      (S3 accuracy)
GenCPU %       = %user + %nice + %system                              (real work only)
Steal %        = reported separately — hypervisor theft is NOT generator effort
Efficiency     = median_RPS / GenCPU %                                (RPS per CPU point)
Spread %       = (max_run − min_run) / median × 100                   (consistency)
Error %        = failed_requests / total_requests × 100
```

> **Why steal is excluded:** on shared-vCPU instances (c6i.large etc.) the
> hypervisor can take a large share of cycles; `100 − idle` then reads as
> "generator busy" while real throughput halves. Steal is therefore reported
> separately per run and excluded from generator effort — see
> [`BENCHMARKS.md`](../BENCHMARKS.md) for the Round 2 investigation.

### Reproduce it yourself

1. Provision two machines per [SETUP.md](SETUP.md)
2. Deploy your target server on machine A
3. Install tools on machine B (storm build, k6, wrk, hey, vegeta)
4. `bash run_bench.sh`
5. `python3 analyze.py` after copying results back

The whole point: **run it before every release, archive results, track regressions.**
Numbers you cannot reproduce are numbers you should not trust.
