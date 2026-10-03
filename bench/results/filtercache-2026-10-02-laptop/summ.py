import sys, json

for f in sys.argv[1:]:
    for l in open(f):
        try:
            d = json.loads(l)
        except Exception:
            print(l[:300])
            continue
        c = d['cache']
        g = lambda k, r: int(c.get('entry_kind="%s",result="%s"' % (k, r), 0))
        m = d['meta']
        mm = lambda k: m.get(k, 0)
        print(d['name'], d['at'], 'acct/s', d['acct_s'], 'getMB', d['sst_get_mb'], 'upMB', d['sst_up_mb'], 'KB/acct', d['sst_get_kb_per_acct'],
              'fmiss', g('filter', 'miss'), 'imiss', g('index', 'miss'), 'bmiss', g('data_block', 'miss'), 'cpu', d['cpu_s'], 'sst', d['ssts'], 'sr', d['srs'],
              'footMB', round((mm('vlpds_sst_meta_bytes{kind="filter"}') + mm('vlpds_sst_meta_bytes{kind="index"}')) / 1e6, 1),
              'cacheMB', round(mm('vlpds_meta_cache_bytes') / 1e6, 1))
