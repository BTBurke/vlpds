# Usage: python3 lexicons/bundle.py <atproto>/lexicons lexicons/records.json
# Bundles every record lexicon from the atproto lexicons dir, plus the
# lexicons they reference (transitively), into one JSON object {nsid: doc}.
import json, os, sys
root = sys.argv[1]; out = sys.argv[2]
docs = {}
for dp, _, fs in os.walk(root):
    for f in fs:
        if f.endswith('.json'):
            d = json.load(open(os.path.join(dp, f)))
            docs[d['id']] = d
def refs(node, base, acc):
    if isinstance(node, dict):
        t = node.get('type')
        if t == 'ref':
            acc.add(node['ref'] if not node['ref'].startswith('#') else base + node['ref'])
        if t == 'union':
            for r in node.get('refs', []):
                acc.add(r if not r.startswith('#') else base + r)
        for v in node.values(): refs(v, base, acc)
    elif isinstance(node, list):
        for v in node: refs(v, base, acc)
want = [i for i, d in docs.items() if d.get('defs', {}).get('main', {}).get('type') == 'record']
seen = set(); stack = list(want)
while stack:
    i = stack.pop()
    if i in seen or i not in docs: continue
    seen.add(i)
    acc = set(); refs(docs[i], i, acc)
    for r in acc:
        n = r.split('#')[0]
        if n not in seen: stack.append(n)
bundle = {i: docs[i] for i in sorted(seen)}
json.dump(bundle, open(out, 'w'), separators=(',', ':'), sort_keys=True)
print(len(want), 'records,', len(bundle), 'lexicons,', os.path.getsize(out), 'bytes')
