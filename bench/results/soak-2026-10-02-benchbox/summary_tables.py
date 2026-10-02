import json, statistics as S, collections, bisect, sys
D=sys.argv[1]
rows=[json.loads(l) for l in open(D+"/samples.jsonl")]
ev=[json.loads(l) for l in open(D+"/events.jsonl")]
ops=[e for e in ev if e["type"] in ("restart","reshard")]
# calm = no busy op, >30s after any op end, not first 300 s of each window
ends=sorted(e["t"] for e in ops); starts=sorted(e["t"] for e in ev if e["type"]=="start")
def calm(r):
    if r.get("busy"): return False
    i=bisect.bisect_right(ends,r["t"])
    if i and r["t"]-ends[i-1]<30+10: return False
    j=bisect.bisect_right(starts,r["t"])
    if j and r["t"]-starts[j-1]<300: return False
    return r["phase"] in ("calm","restart","reshard","mixed")
def med(xs):
    xs=[x for x in xs if x is not None]; return round(S.median(xs),2) if xs else None
def p90(xs):
    xs=sorted(x for x in xs if x is not None); return round(xs[int(.9*(len(xs)-1))],1) if xs else None
out=[]
P=lambda s: out.append(s)
P("### Per-hour medians of calm samples (no op in progress or within 40 s of one; 5 min warmup after each start)\n")
keys=[("lat.w.p50","write p50 ms"),("lat.w.p99","write p99 ms"),("lat.r_getRecord.p99","getRecord p99 ms"),("lat.r_listRecords.p99","listRecords p99 ms"),("lat.r_listRecords_dh.p99","listRecords delete-heavy p99 ms"),("req.per_commit","obj-store req/commit"),("req.state_reads_per_read","state SST GET+HEAD / read"),("req.ctl_assign_get_s","assign/ GETs/s"),("fh.lag_p99","firehose lag p99 ms")]
P("| soak h | phase(s) | n | "+" | ".join(k[1] for k in keys)+" |")
P("|"+"---|"*(len(keys)+3))
B=collections.defaultdict(list)
for r in rows:
    if calm(r): B[int(r["soak_s"]//3600)].append(r)
for h in sorted(B):
    rs=B[h]; ph=",".join(sorted({r["phase"] for r in rs}))
    P(f"| {h}-{h+1} | {ph} | {len(rs)} | "+" | ".join(str(med([r.get(k) for r in rs])) for k,_ in keys)+" |")
P("\n### Stall windows (10 s samples with write p50 > 200 ms or p99 > 1 s), per soak hour and phase\n")
P("| soak h | samples | stalls in restart | in reshard | in calm | in mixed | of which within 30 s of an op |")
P("|---|---|---|---|---|---|---|")
SB=collections.defaultdict(lambda: collections.Counter())
for r in rows:
    h=int(r["soak_s"]//3600); SB[h]["n"]+=1
    if (r.get("lat.w.p50") or 0)>200 or (r.get("lat.w.p99") or 0)>1000:
        SB[h][r["phase"]]+=1
        i=bisect.bisect_right(ends,r["t"]+10)
        near=any(abs(r["t"]-e)<40 for e in ends[max(0,i-3):i+2])
        if near: SB[h]["near"]+=1
for h in sorted(SB):
    c=SB[h]; P(f"| {h}-{h+1} | {c['n']} | {c['restart']} | {c['reshard']} | {c['calm']} | {c['mixed']} | {c['near']} |")
P("\n### Restart cost by restart ordinal (bins of ~20)\n")
P("| restarts # | soak h | SIGTERM exit s (med) | serving s (med) | converged s (med) | kill -9 share | max write p99 in 40 s after (med, SIGTERM) | req/commit in 40 s after (med) |")
P("|---|---|---|---|---|---|---|---|")
ts=[r["t"] for r in rows]
rs=[e for e in ev if e["type"]=="restart"]
for i in range(0,len(rs),20):
    b=rs[i:i+20]; imp=[]; rq=[]
    for e in b:
        j=bisect.bisect_left(ts,e["t"]-e.get("total_s",0)); w=rows[j:j+5]
        if e["kind"]=="sigterm":
            imp.append(max((x.get("lat.w.p99") or 0) for x in w)); rq.append(S.mean([(x.get("req.per_commit") or 0) for x in w]))
    sig=[e["exit_s"] for e in b if e["kind"]=="sigterm"]
    P(f"| {i+1}-{i+len(b)} | {b[0]['soak_s']/3600:.2f}-{b[-1]['soak_s']/3600:.2f} | {med(sig)} | {med([e.get('start_s') for e in b])} | {med([e.get('converge_s') for e in b])} | {sum(e['kind']=='kill9' for e in b)}/{len(b)} | {med(imp)} | {med(rq)} |")
P("\n### Reshard ops by ordinal (bins of 10)\n")
P("| reshards # | soak h | op secs (med / max) | converge s (med) | retired dirs after | retired GB after | pinned GB after | assign/ objects after | assign/ LIST B after |")
P("|---|---|---|---|---|---|---|---|---|")
rh=[e for e in ev if e["type"]=="reshard" and e.get("done")]
for i in range(0,len(rh),10):
    b=rh[i:i+10]; t=b[-1]["t"]; j=min(bisect.bisect_left(ts,t+65),len(rows)-1); r=rows[j]
    P(f"| {i+1}-{i+len(b)} | {b[0]['soak_s']/3600:.2f}-{b[-1]['soak_s']/3600:.2f} | {med([e['secs'] for e in b])} / {max(e['secs'] for e in b)} | {med([e.get('converge_s') for e in b])} | {r.get('state.retired_dirs')} | {round((r.get('state.retired_bytes') or 0)/1e9,2)} | {round((r.get('state.pinned_bytes') or 0)/1e9,2)} | {r.get('assign.keys')} | {r.get('assign.list_bytes')} |")
P("\n### n1 memory (n1 is restarted only by the window-2 resume at 4.83 h)\n")
P("| soak h | RSS MB | jemalloc allocated MB | repo cache MB | in-memory caches MB | tokio tasks | /metrics series |")
P("|---|---|---|---|---|---|---|")
last=-1
for r in rows:
    h=round(r["soak_s"]/3600*2)/2
    if h!=last and r.get("node.n1.rss_mb"):
        last=h; P(f"| {r['soak_s']/3600:.2f} | {r.get('node.n1.rss_mb')} | {r.get('node.n1.jemalloc_allocated_mb')} | {r.get('node.n1.repo_cache_mb')} | {r.get('node.n1.cache_mb')} | {r.get('node.n1.tasks')} | {r.get('node.n1.series')} |")
# errors
P("\n### Client errors after retries, by phase\n")
E=collections.Counter(); F={}
for r in rows:
    for k,v in r.items():
        if k.startswith("err."): E[(r["phase"],k[4:])]+=v
    for k,v in (r.get("first_err") or {}).items(): F[k]=v[:150]
P("| phase | op | errors |"); P("|---|---|---|")
for (ph,k),v in sorted(E.items()): P(f"| {ph} | {k} | {v} |")
P("\nFirst error texts: " + "; ".join(f"`{k}`: {v}" for k,v in F.items()))
print("\n".join(out))
