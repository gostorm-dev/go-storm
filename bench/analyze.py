#!/usr/bin/env python3
import csv, json, re, os, statistics as st

RAW = "raw"; SAR = "sar"

def med(vals):
    return round(st.median([float(v) for v in vals]), 2)

def parse_hey(path):
    t = open(path).read()
    def g(p):
        m = re.search(p, t)
        return float(m.group(1)) if m else None
    rps = g(r"Requests/sec\s*:?\s+([\d.]+)")
    p50 = g(r"50%% in ([\d.]+) secs")
    p95 = g(r"95%% in ([\d.]+) secs")
    p99 = g(r"99%% in ([\d.]+) secs")
    errs = sum(int(m.group(1)) for m in re.finditer(r"(?:connect|timeout|read): (\d+)", t))
    sc = re.search(r"\[(\d+)\]\s+(\d+) responses", t)
    bad = int(sc.group(2)) if sc and sc.group(1) != "200" else 0
    return rps, p50 * 1000 if p50 else None, p95 * 1000 if p95 else None, p99 * 1000 if p99 else None, bad + errs

def parse_veg(path):
    r = json.load(open(path))
    ok = r.get("status_codes", {}).get("200", 0)
    l = r["latencies"]
    return r["throughput"], l["50th"] / 1000, l["95th"] / 1000, l["99th"] / 1000, r["requests"] - ok

def sar_cpu(tag):
    path = f"{SAR}/gen_{tag}.log"
    if not os.path.exists(path):
        return None
    idle = None
    for line in open(path):
        m = re.match(r"Average:\s+all\s+\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+([\d.]+)", line)
        if m:
            idle = float(m.group(1))
    return round(100 - idle, 1) if idle is not None else None

rows = []  # tool, scen, rps, p50, p95, p99, failed, gen_cpu
for f in sorted(os.listdir(RAW)):
    m = re.match(r"(storm|k6|wrk|hey|vegeta)_(S\d)_run(1|2|3)\.(out|json)$", f)
    if not m:
        continue
    tool, scen, run, ext = m.groups()
    path = f"{RAW}/{f}"
    try:
        if tool == "storm":
            r = json.load(open(path))
            vals = (r["requests_per_sec"], r["p50_ms"], r["p95_ms"], r["p99_ms"], r["failed"])
        elif tool == "hey":
            vals = parse_hey(path)
        elif tool == "vegeta":
            vals = parse_veg(path)
        else:
            continue  # wrk/k6 handled from results.csv below
        rows.append([tool, scen] + [round(float(v), 3) for v in vals[:4]] + [vals[4]])
    except Exception as e:
        print(f"WARN {f}: {e}")

# wrk + k6 from suite CSV
suite = list(csv.reader(open("results.csv")))
for row in suite[1:]:
    parts = row[3].split() if len(row) > 3 and " " in row[3] else []
    if row[0] == "wrk" and len(parts) >= 7:
        sock = int(parts[5]) if parts[5].isdigit() else 0
        rows.append(["wrk", row[1], float(parts[0]), None, None, None, sock])
    elif row[0] == "k6":
        jf = f"raw/k6_{row[1]}_run{row[2].replace('run','')}.json"
        try:
            d = json.load(open(jf))["metrics"]
            dur = d["http_req_duration"]; it = d["iterations"]
            fails = round(it.get("count", 0) * d["http_req_failed"].get("rate", 0))
            rows.append(["k6", row[1], it.get("rate"), dur.get("med"), dur.get("p(95)"), dur.get("p(99)"), fails])
        except Exception as e:
            print(f"WARN {jf}: {e}")

# aggregate medians + CPU
tools_order = ["storm", "k6", "wrk", "hey", "vegeta"]
scen_names = {"S1": "Burst n=100k c=100", "S2": "Soak 60s c=100", "S3": "Rate 5000/s 30s", "S4": "Ceiling c=1000 30s"}
print("\n=== MEDIAN TABLE (rps | p50 | p95 | p99 | failed | genCPU%) ===")
final = {}
for scen in ["S1", "S2", "S3", "S4"]:
    print(f"\n--- {scen}: {scen_names[scen]} ---")
    for tool in tools_order:
        rs = [r for r in rows if r[0] == tool and r[1] == scen]
        if len(rs) < 3:
            continue
        rps_m = med([r[2] for r in rs])
        cols = []
        for i in (3, 4, 5):
            vv = [r[i] for r in rs if r[i] is not None]
            cols.append(med(vv) if vv else "NA")
        failed = sum(int(r[6]) for r in rs)
        cpu = sar_cpu(f"{tool}_{scen}_run2")
        spread = (max(r[2] for r in rs) - min(r[2] for r in rs)) / rps_m * 100
        print(f"{tool:8s} {rps_m:>9.0f} | {cols[0]:>7} | {cols[1]:>8} | {cols[2]:>8} | {failed:>4} | {cpu}% | spread ±{spread:.1f}%")
        final[(tool, scen)] = rps_m

print("\n=== HEAD TO HEAD vs k6 (median RPS ratio) ===")
for scen in ["S1", "S2", "S3", "S4"]:
    s = final.get(("storm", scen)); k = final.get(("k6", scen))
    if s and k:
        print(f"{scen}: storm {s:.0f} vs k6 {k:.0f}  -> storm {'+' if s>k else ''}{(s-k)/k*100:.1f}%")
