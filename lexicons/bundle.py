# Usage: python3 lexicons/bundle.py <atproto>/lexicons lexicons/spaces-alpha lexicons/bundle.json
# Bundles every record lexicon from the lexicon dirs, every com.atproto.*
# query/procedure (their params, inputs and outputs are validated:
# src/lexicon.rs), plus the lexicons they reference (transitively), into
# one JSON object {nsid: doc}. A later dir's document replaces an earlier
# one's: lexicons/spaces-alpha holds the Spaces lexicons, vendored from
# the reference's permissioned-data branch (lexicons/spaces-alpha/SOURCE).
import json, os, sys
roots = sys.argv[1:-1]; out = sys.argv[-1]
docs = {}
for root in roots:
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
def main_type(d): return d.get('defs', {}).get('main', {}).get('type')
want = [i for i, d in docs.items() if main_type(d) == 'record']
methods = [i for i, d in docs.items()
           if i.startswith('com.atproto.') and main_type(d) in ('query', 'procedure')]
seen = set(); stack = want + methods
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
print(len(want), 'records,', len(methods), 'methods,', len(bundle), 'lexicons,', os.path.getsize(out), 'bytes')
