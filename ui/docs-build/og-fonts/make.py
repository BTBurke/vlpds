# Regenerates the link-card fonts from the site's variable woff2 files:
#   uv run --with fonttools --with brotli python ui/docs-build/og-fonts/make.py
# resvg-js reads neither woff2 nor variable axes, so the cards get static TTF
# instances, plus each one's advance widths (metrics.json) for line wrapping.
# Both families are SIL OFL 1.1 (the copyright notices are in the fonts).
import json
import os
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont

HERE = os.path.dirname(os.path.abspath(__file__))
FONTS = os.path.join(HERE, '..', '..', 'public', 'fonts')
OUT = [
    ('schibsted-grotesk.woff2', 400, 'grotesk-400'),
    ('schibsted-grotesk.woff2', 700, 'grotesk-700'),
    ('jetbrains-mono.woff2', 500, 'mono-500'),
]

metrics = {}
for src, wght, name in OUT:
    f = instantiateVariableFont(TTFont(os.path.join(FONTS, src)), {'wght': wght})
    f.flavor = None
    f['OS/2'].usWeightClass = wght
    f.save(os.path.join(HERE, f'{name}.ttf'))
    hmtx, upm = f['hmtx'], f['head'].unitsPerEm
    metrics[name] = {chr(cp): round(hmtx[g][0] / upm, 4) for cp, g in f.getBestCmap().items()}
with open(os.path.join(HERE, 'metrics.json'), 'w') as fh:
    json.dump(metrics, fh, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
