# vlpds docs: style guide

Not published (files starting with `_` are skipped). Read this before writing or editing a page.

The docs site is built from the Markdown files in this directory and served by the vlpds binary at
`/docs`. `docs/foo.md` is `/docs/foo`, `docs/operations/bar.md` is `/docs/operations/bar`,
`docs/operations/index.md` is `/docs/operations`, and `/docs` itself is `overview.md`.

## How it is built

- `ui/docs-build/` is a Vite plugin. At build time it reads every page, renders the Markdown
  (markdown-it), highlights code (highlight.js) and draws the diagrams as inline SVG. The browser
  gets finished HTML, one lazily loaded chunk per page: no Markdown parser, no diagram library and
  no external host (the page CSP is `'self'` only).
- The sidebar is generated from front matter. Nobody maintains a nav list.
- The build **fails** on a problem: missing front matter, a page that doesn't start with a hero, a
  bad diagram spec, a link to a page or heading that doesn't exist. Check without a full build:

  ```bash
  cd ui && npm run check-docs     # validates and prints the nav
  just dev-ui                     # live preview on :5620, reloads when a page changes
  just ui                         # what the server serves (ui/dist)
  ```

- `tests/all/docs_site.rs` checks that the server serves `/docs/…` with the UI's CSP and that the
  bundle holds the pages.

## Adding a page

1. Create `docs/<slug>.md` (or `docs/operations/<slug>.md`) with front matter:

   ```yaml
   ---
   title: Firehose                 # sidebar and page title
   section: vlPDS                  # "vlPDS" for docs/*.md, "Operations" for docs/operations/*.md
   order: 6                        # position in its section (vlPDS 1-99, Operations 100+); unique
   status: ready                   # stub (an outline; grey dot in the nav) | draft | ready (default)
   summary: "One sentence under the title. Quote it if it contains ': '."
   ---
   ```

2. Start the body with a ```` ```hero ```` block (below), then `##` sections. No `#` heading: the
   title comes from front matter.
3. Link to other pages by file: `[leases](architecture.md#leases)`,
   `[deploy](operations/deploy.md)`, `[overview](../overview.md)` from inside `operations/`.
   Anchors are the heading text, lowercased, with runs of other characters turned into `-`.
4. Run `npm run check-docs`.

## Page template

Every page has the same shape, so a reader can tell what's up from the top of the page:

1. **Hero**: one diagram of the whole topic and 3-5 stat tiles (round numbers, the key defaults, the
   one thing to remember). Someone who reads only the hero should come away with the right picture.
2. **One or two short paragraphs** saying what the page covers and why an operator cares.
3. **Sections** (`##`), each opening with its own diagram, steps or table when the idea has a shape,
   then the explanation. A section explains its diagram: name the boxes and arrows the reader just
   saw, in the same words.
4. **Links out** at the end of a section ("Details: …") rather than repeating another page.

```markdown
---
title: …
section: vlPDS
order: 6
summary: "…"
---

```hero
diagram: { … }
facts: [ … ]
```

What this page covers, in two or three sentences.

## First idea

```diagram
…
```

Explanation of the diagram.
```

`overview.md` is the worked example of everything here.

## Visual blocks

All visual blocks are fenced code blocks whose body is YAML. In YAML flow maps (`{ a: 1, b: 2 }`),
**quote any string that contains a comma** (`label: "65,536 slots"`) or `: `; an unquoted comma
starts a new key, and the build reports it as an unknown key.

### hero

```yaml
diagram: { …a diagram spec, below… }
facts:
  - { value: "~60k", unit: commits/s, label: per 16-core node, note: "measured; ~350/s is Bluesky's average", tone: amber }
```

### facts (stat tiles)

A list of `{ value, unit?, label, note?, tone? }`. `value` is short and big (`~60k`, `$0`, `64`,
`10 s`); `label` says what it is; `note` gives the basis or the caveat. Tones: `accent` (default),
`amber`, `blue`, `violet`, `rust`, `muted`. Use 3-5 tiles; four fit one row on a laptop.

### steps (numbered sequence)

A list of `{ title, body? }`; `body` is Markdown. For ordered processes: a takeover, a deploy,
the life of a write.

### pages (section index)

```` ```pages ```` with an empty body (`{}`) lists the cards of every page under the current
directory; `{ dir: operations }` lists another one. Used by `operations/index.md`.

### diagram

Boxes on a grid, with groups behind them and arrows between them. One grid unit is 20 px;
a default box is 8 × 3 units (160 × 60 px). The drawing is sized to its content and scales down
with the column (on a phone, diagrams wider than 560 px scroll sideways instead).

```yaml
caption: One or two sentences under the figure. `code` works here.
nodes:
  - { id: n1, label: vlpds node 1, sub: "shards 0–21 · log 1", at: [21, 1], size: [9, 3], tone: accent }
  - { id: log, label: "`log/`", sub: segments, at: [35, 1], size: [10, 2.6], shape: store, tone: amber }
groups:
  - { label: vlpds cluster, around: [n1], tone: accent }       # or at: [x, y], size: [w, h]
edges:
  - "n1 -> log: append, then ack"                              # string form
  - "n1 <-> n2"                                                # both ends
  - "n1 ~> log"                                                # dashed
  - "n1 -- log"                                                # no arrow
  - { from: n1.b25, to: appview.t, label: app.bsky.*, dash: true, tone: blue }
notes:
  - { at: [21, 8.6], text: split / merge online, align: start }
```

- **Nodes**: `id`, `label` (`\n` for a second line; backticks for code), `sub` (smaller, muted),
  `at: [x, y]` in grid units (fractions are fine), `size: [w, h]` (default `[8, 3]`),
  `tone`, `shape` (`box` default, `store` = cylinder for anything in the bucket, `pill`, `note`
  = dashed), `stack: true` (drawn as several), `badge: ×3`.
- **Edges** connect node ids. Without sides, aligned boxes get a straight line through their
  overlap; otherwise a diagonal between their edges. Pin the ends to sides to get clean right-angle
  routes: `id.t`, `id.b`, `id.l`, `id.r`, optionally a percentage along the side (`n3.b25` is a
  quarter of the way along the bottom). `via: [[x, y], …]` forces waypoints. `arrow`: `end`
  (default), `start`, `both`, `none`. Labels sit on the longest segment of the route; `labelAt: [x, y]`
  moves one.
- **Groups** are drawn behind the nodes with a small caps label: a cluster, a process, the bucket.
  `around: [ids]` wraps those nodes (`pad`, default 1 unit).
- **Tones** (theme-aware, the same in light and dark):

  | tone | use it for |
  |---|---|
  | `ink` (default) | clients, generic components |
  | `accent` | vlpds itself: nodes, workers, the write path |
  | `amber` | the object store and anything durable in it |
  | `blue` | the firehose and its consumers, reads |
  | `violet`, `cyan`, `rust` | a third or fourth kind of thing, when needed |
  | `muted` | external services (AppView, PLC, KMS), optional parts |
  | `solid` | the one outcome a diagram is about (an ack, "serving") |
  | `danger`, `ok` | failure and recovery states |

  Keep to these meanings across pages so color means the same thing everywhere.

Diagram rules:

- **Accurate to the code.** Every box is a real component, key prefix or process, named as the code or
  the bucket names it. If you simplify (three nodes stand for N), say so in the caption.
- **One idea per diagram.** If a diagram needs a legend, split it.
- **Arrows are data or control flow, in the direction it moves**, labeled with the verb or the
  payload (`append, then ack`, `lease CAS`). Dashed = background or optional.
- **Don't let lines cross boxes.** Pin sides (`n3.b15 -> appview.t`) and leave a grid unit or two
  between rows for routes. Check the result in both themes (the theme toggle is in the top bar).
- **Short labels.** A box label is a noun of one to three words; details go in `sub` or the text.
  A box is 160 px wide by default: a `sub` longer than ~24 characters needs a wider box.

## Tone and content

- **Operator-focused.** Write for someone running a server: what it does, what it costs, what to
  watch, what to do. Design history and rejected alternatives stay in `DESIGN.md`.
- **Concise.** Short sentences, plain words, active voice. Lead with the point.
- **Round numbers** with their basis: "~60k commits/s per 16-core node (measured)", "~$1.7k/mo on S3".
  Say whether a number is measured, modeled or a design target. Link the benchmark directory
  (`bench/results/…`) in a note or sentence rather than copying tables.
- **Defaults with their flag**: "a 10 s lease (`--lease-ttl-ms`)". Name metrics and alerts exactly
  (`vlpds_lease_renew_ttl_ratio`, `VlpdsShardsUnowned`).
- **Code paths sparingly**, as inline code (`src/nodelog.rs`), when an operator would actually go
  read it. Not as links.
- **Say what isn't built.** If something is design only (backups, planet scale), say so plainly.
- Callouts: `> [!NOTE]`, `> [!TIP]`, `> [!WARNING]`, `> [!DANGER]` as the first line of a
  blockquote. Use them rarely: one warning a page reads as a warning; five read as noise.

## Avoiding duplication

- `DESIGN.md` remains the deep design log: every mechanism, measurement and rejected alternative.
  The docs are the curated, current view: what is true now and what an operator needs.
- `ops/RUNBOOK.md` stays the per-alert reference that `ops/alerts.yml` `runbook_url`s point at. The
  Operations pages explain and organize; for an alert's exact steps, link the runbook section rather
  than copying it (copying means two places to update).
- Each fact has one home page. Other pages state it in a clause and link there.
- When the code changes a default or a mechanism, update the page that owns it in the same change.
