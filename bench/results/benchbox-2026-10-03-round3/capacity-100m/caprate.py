import json,sys,collections
# usage: caprate.py sampler.jsonl t_from t_to  -> per node rates of key families
f,a,b=sys.argv[1],float(sys.argv[2]),float(sys.argv[3])
first={};last={}
for l in open(f):
    r=json.loads(l)
    if not r.get('up') or not (a<=r['t']<=b): continue
    first.setdefault(r['node'],r); last[r['node']]=r
tot=collections.Counter(); dt=None
for n in sorted(last):
    x,y=first[n],last[n]; dt=y['t']-x['t']
    d={k:y['m'].get(k,0)-x['m'].get(k,0) for k in y['m']}
    g=lambda pred: sum(v for k,v in d.items() if pred(k))
    sstb=g(lambda k:k.startswith('vlpds_object_store_bytes_total') and 'state_sst' in k and 'dir="down"' in k)
    sstn=g(lambda k:k.startswith('vlpds_object_store_requests_total') and 'state_sst' in k and 'op="get' in k)
    mc={k.split('{')[1].rstrip('}'):v for k,v in d.items() if k.startswith('vlpds_meta_cache_loads_total') and v}
    print(f"{n}: dt {dt:.0f}s SST GET {sstb/dt/1e6:.1f} MB/s {sstn/dt:.0f}/s; meta loads/s {{{', '.join(f'{k}: {v/dt:.1f}' for k,v in mc.items())}}}; meta_cache_bytes {y['m'].get('vlpds_meta_cache_bytes',0)/1e6:.0f} MB / cap {y['m'].get('vlpds_meta_cache_capacity_bytes',0)/1e6:.0f}; sst_meta_bytes {sum(v for k,v in y['m'].items() if k.startswith('vlpds_sst_meta_bytes'))/1e6:.0f} MB")
    tot['sstb']+=sstb
print(f"total SST GET {tot['sstb']/1e9:.2f} GB over {dt:.0f}s")
