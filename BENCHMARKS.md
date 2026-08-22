# BENCHMARKS — go-storm vs k6 vs wrk vs hey vs vegeta

> Two rounds published so far, same day, same lab. Methodology in [`bench/README.md`](bench/README.md).
>
> Philosophy: numbers are reported as measured. Losses and regressions are shown,
> not hidden. A benchmark you cannot reproduce is a benchmark you should not trust.

## ⚠️ Current status (read first)

**v0.5.3 has a known performance regression on sustained unlimited-concurrency load**
(S2 soak −57%, S4 ceiling −54% vs the previous build), caused by allocation pressure /
GC thrash (~662 GC cycles per 30s observed via go-storm's own generator health report).
Short bursts (S1) and rate-limited runs (S3) are unaffected — S3 numbers below are a
record for this project. Fix targeted for v0.5.4.

The regression was caught **by this suite within hours of release**, before any user report.
That is exactly what the suite exists for.

---

## Environment (both rounds)

| | |
|---|---|
| Machines | 2 × AWS c6i.large (2 vCPU), same AZ (ap-south-1a) |
| Network | Private IPs between generator and target |
| Target | chi-based HTTP server (Go), identical for all tools |
| Protocol | warmup + 3 measured runs per scenario → median; 10s cooldown; `sar` on both machines |

---

# Round 2 — 2026-08-22 · go-storm v0.5.3 (commit `eb428d7`)

### Headline: rate-overshoot fix verified ✅

`-r 5000 -d 30s` now dispatches **exactly 150,000 requests** (Round 1 sent 155,000).
Median achieved across 3 runs: **5000.00 RPS (−0.000%)**.

| Tool | Achieved RPS | Deviation | p50 | p95 | p99 | Gen CPU% |
|---|---|---|---|---|---|---|
| **go-storm** | **5000.00** | −0.000% | 0.185ms | 0.257ms | **0.55ms** | **19.0** |
| k6 | 4999.86 | −0.003% | 0.166ms | 0.343ms | 2.99ms | 37.0 |
| hey | 4999.04 | −0.02% | 0.9ms | 1.9ms | 2.3ms | 11.6 |
| vegeta | 5000 | −0.00%† | 192ms† | 275ms† | 374ms† | 17.8 |

Rate accuracy is now at parity with k6 — with **5× better tail latency at roughly half
the generator CPU**. This scenario is go-storm's home turf again.

### Other scenarios (median of 3)

| Scenario | go-storm | k6 | wrk | hey | vegeta |
|---|---|---|---|---|---|
| S1 burst n=100k c=100 | 37,044 ↑ | n/a | n/a | **44,505** | n/a |
| S2 soak 60s c=100 ⚠️ | 15,351 | 25,512 | **66,148** | 46,942 | 34,503 |
| S4 ceiling c=1000 30s ⚠️ | 14,420 | 18,545 | **63,023** | 40,291 | 30,774 |

⚠️ = affected by the v0.5.3 sustained-load regression described above.
↑ = improvement over Round 1 (+3% throughput, p50 2.66→2.06ms).
wrk latency capture worked this round: 66K RPS at p50 1.38ms / p99 ~4.0ms using only ~45% CPU — still the raw-efficiency reference.

### Regression evidence (from go-storm's own health report, 30s diagnostic run)

```
Achieved RPS:     17498            (expected ≈35k on this hardware)
CPU Usage:        89.8%
GC Cycles:        662              ← 22 GC cycles/second
Verdict:          GENERATOR UNDER PRESSURE
Recommendation:   High GC pressure — reduce allocation rate or concurrency
```

Suspects: hot-path allocations introduced with the log-linear histogram /
scheduler restructuring in v0.5.3 under sustained dispatch. Investigation open → v0.5.4.

---

# Round 1 — 2026-08-22 · pre-v0.5.3 build (commit `e046a77`)

### Results (median of 3)

| Scenario | wrk | hey | go-storm | vegeta | k6 |
|---|---|---|---|---|---|
| S1 burst 100k | – | **42,887** 👑 | 35,986 | – | – |
| S2 soak 60s | **65,621** 👑 | 44,235 | 35,870 | 32,887* | 24,332 |
| S3 rate 5000/s | n/a | 4989.9 | **5166.4** ⚠️ bug | 5000 | **4999.8** 👑 |
| S4 ceiling c=1000 | **60,520** 👑 | 38,862 | 31,198 | 30,696 | 18,154 |

### Head-to-head vs k6 (that round)
S2 soak: **+47.4% throughput**, p99 39% lower · S4 ceiling: **+71.8% throughput**

### Efficiency (S2: RPS per generator CPU point)
```
wrk     1356      hey      450      go-storm 367      vegeta   341
```

### Findings that drove changes
1. Rate overshoot bug discovered (`-r 5000` → exactly +5000 requests every run) → fixed in v0.5.3, verified in Round 2.
2. chiserver never broke even at c=1000 (0% errors): real server-breaking-point tests need a heavier endpoint.
3. wrk percentile output was not captured (flag omitted) → fixed for Round 2.

---

## Open items

- [ ] v0.5.4: fix sustained-load allocation/GC regression (S2/S4)
- [ ] Investigate vegeta's anomalous latency figures under open pacing (both rounds)
- [ ] Heavier target endpoint for true server breaking-point measurement
- [ ] Record full kernel/tool metadata per round automatically

Reproduce everything yourself: [`bench/`](bench/README.md) · setup guide: [`bench/SETUP.md`](bench/SETUP.md)
