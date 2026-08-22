#!/usr/bin/env bash
# go-storm benchmark suite v1 — generator machine
set -u
TARGET="http://172.31.33.43:8080/"
DIR="$HOME/bench"; RAW="$DIR/raw"; SARD="$DIR/sar"
CSV="$DIR/results.csv"; BPCSV="$DIR/breakpoint.csv"
export PATH="$PATH:/home/ubuntu/go/bin"
mkdir -p "$RAW" "$SARD"
echo "tool,scenario,run,rps,p50_ms,p95_ms,p99_ms,total_reqs,failed,dur_s" > "$CSV"
echo "tool,c_level,rps,err_pct" > "$BPCSV"

log(){ echo "[$(date +%H:%M:%S)] $*"; }
cooldown(){ log "cooldown 10s"; sleep 10; }
start_sar(){ nohup sar -u -r 1 > "$SARD/$1.log" 2>&1 & echo $! > "$SARD/$1.pid"; sleep 1; }
stop_sar(){ kill -INT "$(cat "$SARD/$1.pid")" 2>/dev/null; sleep 1; }

# ---- parsers: emit "rps p50 p95 p99 total failed dur_s" ----
p_storm(){ python3 -c "
import json;r=json.load(open('$1'))
print(f\"{r['requests_per_sec']:.2f} {r['p50_ms']} {r['p95_ms']} {r['p99_ms']} {r['total_requests']} {r['failed']} {r['total_duration_ms']/1000:.1f}\")"; }

p_veg(){ python3 -c "
import json;r=json.load(open('$1'))
ok=r.get('status_codes',{}).get('200',0)
print(f\"{r['throughput']:.2f} {r["latencies"]["50th"]*1000:.3f} {r["latencies"]["95th"]*1000:.3f} {r["latencies"]["99th"]*1000:.3f} {r['requests']} {r['requests']-ok} {r['duration']}\")"; }

p_wrk(){ python3 -c "
import re;t=open('$1').read()
def g(p):
    m=re.search(p,t);return m.group(1) if m else 'NA'
sock=sum(int(n) for m in re.finditer(r'Socket errors: connect (\d+), timeout (\d+), read (\d+)',t) for n in m.groups())
print(f\"{g(r'Requests/sec:\s+([\d.]+)')} {g(r'50%\s+([\d.]+\w+)')} NA {g(r'99%\s+([\d.]+\w+)')} NA {sock} NA\")"; }

p_hey(){ python3 -c "
import re;t=open('$1').read()
def g(p):
    m=re.search(p,t);return float(m.group(1)) if m else 0
sock=sum(int(m.group(1)) for m in re.finditer(r'(?:connect|timeout|read): (\d+)',t))
sc=re.search(r'\[(\d+)\]\s+(\d+) responses',t)
bad=int(sc.group(2)) if sc and sc.group(1)!='200' else 0
print(f\"{g(r'Requests/sec:\s+([\d.]+)'):.2f} {g(r'50%+% in ([\d.]+) secs')*1000:.3f} {g(r'95%+% in ([\d.]+) secs')*1000:.3f} {g(r'99%+% in ([\d.]+) secs')*1000:.3f} NA {bad+sock} NA\")"; }

p_k6(){ python3 -c "
import json
try:
    d=json.load(open('$1'))['metrics']
    dur=d['http_req_duration'];it=d['iterations']
    f=round(it.get('count',0)*d['http_req_failed'].get('rate',0))
    print(f\"{it.get('rate',0):.2f} {dur.get('med','NA')} {dur.get('p(95)','NA')} {dur.get('p(99)','NA')} {it.get('count',0)} {f} NA\")
except Exception as e: print('PARSE_FAIL')" ; }

emit(){ # TOOL SCEN RUN parser_out
  local o="$4"
  [ -z "$o" ] && { log "!! PARSE_FAIL $1 $2 $3"; return; }
  echo "$1,$2,$3,$o" >> "$CSV"
}

run_case(){ # TOOL SCEN RUN DUR_S CMD...
  local tool=$1 scen=$2 run=$3; shift 3
  local tag="${tool}_${scen}_run${run}"
  start_sar "gen_$tag"
  "$@" > "$RAW/$tag.out" 2> "$RAW/$tag.err"
  local rc=$?
  stop_sar "gen_$tag"
  log "$tag exit=$rc"
}

log "=== GO-STORM BENCHMARK SUITE START $(date) ==="

########## S1: count burst n=100000 c=100 (storm, hey) ##########
for run in warmup 1 2 3; do
  n=100000; [ "$run" = warmup ] && n=10000
  run_case storm S1 "$run" ~/storm-linux run -u "$TARGET" -n "$n" -c 100 --format json
  [ "$run" != warmup ] && emit storm S1 "$run" "$(p_storm "$RAW/storm_S1_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  n=100000; [ "$run" = warmup ] && n=10000
  run_case hey S1 "$run" hey -n "$n" -c 100 "$TARGET"
  [ "$run" != warmup ] && emit hey S1 "$run" "$(p_hey "$RAW/hey_S1_run$run.out")"
  cooldown
done

########## S2: sustained 60s c=100 (all 5 tools) ##########
for run in warmup 1 2 3; do
  z=60s; [ "$run" = warmup ] && z=10s
  run_case storm S2 "$run" ~/storm-linux run -u "$TARGET" -d "$z" -c 100 --format json
  [ "$run" != warmup ] && emit storm S2 "$run" "$(p_storm "$RAW/storm_S2_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  z=60s; [ "$run" = warmup ] && z=10s
  SCEN=s2 k6 run --quiet -u 100 --duration "$z" --summary-export "$RAW/k6_S2_run$run.json" "$DIR/k6_script.js" > "$RAW/k6_S2_run$run.out" 2>&1 || \
  SCEN=s2 k6 run --quiet -u 100 -d "$z" --summary-export "$RAW/k6_S2_run$run.json" "$DIR/k6_script.js" > "$RAW/k6_S2_run$run.out" 2>&1
  log "k6_S2_run$run done"
  [ "$run" != warmup ] && emit k6 S2 "$run" "$(p_k6 "$RAW/k6_S2_run$run.json")"
  cooldown
done
for run in warmup 1 2 3; do
  z=60s; [ "$run" = warmup ] && z=10s
  run_case wrk S2 "$run" wrk -t8 -c100 --latency -d"$z" "$TARGET"
  [ "$run" != warmup ] && emit wrk S2 "$run" "$(p_wrk "$RAW/wrk_S2_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  z=60s; [ "$run" = warmup ] && z=10s
  run_case hey S2 "$run" hey -z "$z" -c 100 "$TARGET"
  [ "$run" != warmup ] && emit hey S2 "$run" "$(p_hey "$RAW/hey_S2_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  z=60s; [ "$run" = warmup ] && z=10s
  run_case vegeta S2 "$run" bash -c "echo GET $TARGET | vegeta attack -duration $z -rate 0 -max-workers 100 | vegeta report -type=json"
  [ "$run" != warmup ] && emit vegeta S2 "$run" "$(p_veg "$RAW/vegeta_S2_run$run.out")"
  cooldown
done

########## S3: rate accuracy r=5000 d=30s (storm,k6,hey,vegeta) ##########
for run in warmup 1 2 3; do
  z=30s; [ "$run" = warmup ] && z=5s
  run_case storm S3 "$run" ~/storm-linux run -u "$TARGET" -r 5000 -d "$z" --format json
  [ "$run" != warmup ] && emit storm S3 "$run" "$(p_storm "$RAW/storm_S3_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  SCEN=s3 k6 run --quiet --summary-export "$RAW/k6_S3_run$run.json" "$DIR/k6_script.js" > "$RAW/k6_S3_run$run.out" 2>&1
  log "k6_S3_run$run done"
  [ "$run" != warmup ] && emit k6 S3 "$run" "$(p_k6 "$RAW/k6_S3_run$run.json")"
  cooldown
done
for run in warmup 1 2 3; do
  run_case hey S3 "$run" hey -z 30s -c 100 -q 50 "$TARGET"
  [ "$run" != warmup ] && emit hey S3 "$run" "$(p_hey "$RAW/hey_S3_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  run_case vegeta S3 "$run" bash -c "echo GET $TARGET | vegeta attack -rate 5000/s -duration 30s | vegeta report -type=json"
  [ "$run" != warmup ] && emit vegeta S3 "$run" "$(p_veg "$RAW/vegeta_S3_run$run.out")"
  cooldown
done

########## S4: generator ceiling c=1000 d=30s (all 5) ##########
for run in warmup 1 2 3; do
  z=30s; [ "$run" = warmup ] && z=5s
  run_case storm S4 "$run" ~/storm-linux run -u "$TARGET" -d "$z" -c 1000 --format json
  [ "$run" != warmup ] && emit storm S4 "$run" "$(p_storm "$RAW/storm_S4_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  SCEN=s4 k6 run --quiet -u 1000 -d 30s --summary-export "$RAW/k6_S4_run$run.json" "$DIR/k6_script.js" > "$RAW/k6_S4_run$run.out" 2>&1
  log "k6_S4_run$run done"
  [ "$run" != warmup ] && emit k6 S4 "$run" "$(p_k6 "$RAW/k6_S4_run$run.json")"
  cooldown
done
for run in warmup 1 2 3; do
  z=30s; [ "$run" = warmup ] && z=5s
  run_case wrk S4 "$run" wrk -t16 -c1000 --latency -d"$z" "$TARGET"
  [ "$run" != warmup ] && emit wrk S4 "$run" "$(p_wrk "$RAW/wrk_S4_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  z=30s; [ "$run" = warmup ] && z=5s
  run_case hey S4 "$run" hey -z "$z" -c 1000 "$TARGET"
  [ "$run" != warmup ] && emit hey S4 "$run" "$(p_hey "$RAW/hey_S4_run$run.out")"
  cooldown
done
for run in warmup 1 2 3; do
  z=30s; [ "$run" = warmup ] && z=5s
  run_case vegeta S4 "$run" bash -c "echo GET $TARGET | vegeta attack -duration $z -rate 0 -max-workers 1000 | vegeta report -type=json"
  [ "$run" != warmup ] && emit vegeta S4 "$run" "$(p_veg "$RAW/vegeta_S4_run$run.out")"
  cooldown
done

########## S5: server breaking point steps 25..1000 x 10s (all 5) ##########
for tool in storm k6 wrk hey vegeta; do
  for c in 25 50 100 250 500 1000; do
    case $tool in
      storm)  out=$(~/storm-linux run -u "$TARGET" -d 10s -c $c --format json 2>/dev/null | python3 -c "
import json,sys;r=json.load(sys.stdin);print(f\"{r['requests_per_sec']:.0f} {(r['failed']/max(r['total_requests'],1))*100:.2f}\")" 2>/dev/null) ;;
      k6)     SCEN=x k6 run --quiet -u $c -d 10s "$DIR/k6_script.js" 2>/dev/null | tee /tmp/o.txt > /dev/null
              out=$(python3 -c "
import re;t=open('/tmp/o.txt').read()
i=re.search(r'iterations.*?: (\d+) .*?(\d+\.\d+)/s',t,re.S)
print(f\"{(i.group(2) if i else 'NA')} NA\")") ;;
      wrk)    raw=$(wrk -t8 -c$c -d10s "$TARGET" 2>/dev/null); echo "$raw" > "$RAW/bp_${tool}_$c.out"
              out=$(python3 -c "
import re,sys;t=sys.stdin.read() if False else open('$RAW/bp_${tool}_$c.out').read()
m=re.search(r'Requests/sec:\s+([\d.]+)',t)
e=sum(int(n) for mm in re.finditer(r'Socket errors: connect (\d+), timeout (\d+), read (\d+)',t) for n in mm.groups())
print(f\"{m.group(1) if m else 'NA'} NA\")" ) ;;
      hey)    out=$(hey -z 10s -c $c "$TARGET" 2>/dev/null | tee "$RAW/bp_${tool}_$c.out" | python3 -c "
import re,sys;t=sys.stdin.read()
m=re.search(r'Requests/sec:\s+([\d.]+)',t)
e=sum(int(mm.group(1)) for mm in re.finditer(r'(?:connect|timeout|read): (\d+)',t))
sc=re.search(r'\[(\d+)\]\s+(\d+) responses',t); bad=int(sc.group(2)) if sc and sc.group(1)!='200' else 0
tot=g=float(m.group(1))*10 if m else 0
print(f\"{m.group(1) if m else 'NA'} {(bad+e)/max(tot,1)*100:.2f}\")") ;;
      vegeta) out=$(echo GET $TARGET | vegeta attack -duration 10s -rate 0 -max-workers $c 2>/dev/null | vegeta report -type=json | python3 -c "
import json,sys;r=json.load(sys.stdin);ok=r.get('status_codes',{}).get('200',0)
print(f\"{r['throughput']:.0f} {(r['requests']-ok)/max(r['requests'],1)*100:.2f}\")") ;;
    esac
    echo "$tool,$c,${out% *},${out#* }" >> "$BPCSV"
    log "S5 $tool c=$c -> $out"
    sleep 3
  done
  cooldown
done

log "=== SUITE COMPLETE $(date) ==="
