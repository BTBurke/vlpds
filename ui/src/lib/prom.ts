// Prometheus text exposition parsing and rate/quantile derivation between
// consecutive scrapes, all client-side.

export type Sample = { name: string; labels: Record<string, string>; value: number }
export type Scrape = { t: number; samples: Sample[] }

const LINE = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)/
const LABEL = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g

export function parseProm(text: string, t = Date.now()): Scrape {
  const samples: Sample[] = []
  for (const line of text.split('\n')) {
    if (!line || line[0] === '#') continue
    const m = LINE.exec(line)
    if (!m) continue
    const labels: Record<string, string> = {}
    if (m[2]) {
      LABEL.lastIndex = 0
      let lm: RegExpExecArray | null
      while ((lm = LABEL.exec(m[2]))) labels[lm[1]] = lm[2].replace(/\\(.)/g, (_, c) => (c === 'n' ? '\n' : c))
    }
    const v = m[3] === '+Inf' ? Infinity : m[3] === '-Inf' ? -Infinity : Number(m[3])
    samples.push({ name: m[1], labels, value: v })
  }
  return { t, samples }
}

type Filter = (l: Record<string, string>) => boolean

/** Sum of a metric's samples (optionally filtered). */
export function sum(s: Scrape, name: string, f?: Filter): number | undefined {
  let found = false
  let total = 0
  for (const x of s.samples) {
    if (x.name === name && (!f || f(x.labels))) {
      found = true
      total += x.value
    }
  }
  return found ? total : undefined
}

/** Per-second rate of a counter between two scrapes (resets read as 0). */
export function rate(prev: Scrape, cur: Scrape, name: string, f?: Filter): number | undefined {
  const a = sum(prev, name, f)
  const b = sum(cur, name, f)
  if (a === undefined || b === undefined) return undefined
  const dt = (cur.t - prev.t) / 1000
  if (dt <= 0) return undefined
  return Math.max(0, b - a) / dt
}

/** Per-label-value rates of a counter, e.g. HTTP requests by method. */
export function rateBy(prev: Scrape, cur: Scrape, name: string, label: string): Map<string, number> {
  const agg = (s: Scrape) => {
    const m = new Map<string, number>()
    for (const x of s.samples) if (x.name === name) m.set(x.labels[label] ?? '', (m.get(x.labels[label] ?? '') ?? 0) + x.value)
    return m
  }
  const a = agg(prev)
  const b = agg(cur)
  const dt = (cur.t - prev.t) / 1000
  const out = new Map<string, number>()
  for (const [k, v] of b) out.set(k, dt > 0 ? Math.max(0, v - (a.get(k) ?? 0)) / dt : 0)
  return out
}

function buckets(s: Scrape, name: string, f?: Filter): Map<number, number> {
  const m = new Map<number, number>()
  for (const x of s.samples) {
    if (x.name !== `${name}_bucket` || (f && !f(x.labels))) continue
    const le = x.labels.le === '+Inf' ? Infinity : Number(x.labels.le)
    m.set(le, (m.get(le) ?? 0) + x.value)
  }
  return m
}

/**
 * histogram_quantile over the interval between two scrapes (bucket deltas,
 * summed across label sets), linear within a bucket. undefined when no
 * observations landed in the interval.
 */
export function quantile(prev: Scrape, cur: Scrape, name: string, q: number, f?: Filter): number | undefined {
  const a = buckets(prev, name, f)
  const b = buckets(cur, name, f)
  const les = [...b.keys()].sort((x, y) => x - y)
  if (!les.length) return undefined
  const counts = les.map((le) => Math.max(0, (b.get(le) ?? 0) - (a.get(le) ?? 0)))
  const total = counts[counts.length - 1]
  if (!total) return undefined
  const rank = q * total
  for (let i = 0; i < les.length; i++) {
    if (counts[i] >= rank) {
      const hi = les[i]
      const lo = i === 0 ? 0 : les[i - 1]
      if (!isFinite(hi)) return lo
      const below = i === 0 ? 0 : counts[i - 1]
      const inBucket = counts[i] - below
      return inBucket > 0 ? lo + ((hi - lo) * (rank - below)) / inBucket : hi
    }
  }
  return les[les.length - 2]
}
