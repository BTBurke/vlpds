// Scenario bookkeeping: each step of a configuration ends pass, fail,
// not-implemented (a method answered 501), blocked (a step it needs didn't
// pass) or skip (not applicable to this placement). Results go to
// out/<mode>.json and a short out/<mode>.md.
import { writeFileSync } from 'node:fs'
import { notImplemented, timingByHost, timingRows } from './http.mjs'
import { OUT, log } from './env.mjs'

export class StepFailed extends Error {}

export class Report {
  constructor(mode, meta = {}) {
    this.mode = mode
    this.meta = { started: new Date().toISOString(), ...meta }
    this.rows = [] // { config, step, status, detail, ms, checks }
    this.notes = []
    this.metrics = {}
    this.sections = [] // extra markdown: [title, lines]
  }

  section(title, lines) {
    this.sections.push([title, lines])
  }

  note(s) {
    this.notes.push(s)
    log(`  note: ${s}`)
  }

  /** Run `fn(check)` as one step; `needs` are earlier steps of the same config that must have passed. */
  async step(config, step, fn, { needs = [], skip } = {}) {
    const row = { config, step, status: 'pass', detail: '', ms: 0, checks: [] }
    this.rows.push(row)
    if (skip) {
      row.status = 'skip'
      row.detail = skip
      log(`[${config}] ${step}: skip (${skip})`)
      return row
    }
    const missing = needs.filter((n) => this.status(config, n) !== 'pass')
    if (missing.length) {
      row.status = 'blocked'
      row.detail = `needs ${missing.map((n) => `${n} (${this.status(config, n) ?? 'not run'})`).join(', ')}`
      log(`[${config}] ${step}: blocked (${row.detail})`)
      return row
    }
    const check = (ok, what, detail = '') => {
      row.checks.push({ ok: !!ok, what, detail: ok ? '' : String(detail).slice(0, 500) })
      if (!ok) log(`    FAIL ${what} ${String(detail).slice(0, 300)}`)
      return !!ok
    }
    const t0 = performance.now()
    log(`[${config}] ${step}`)
    try {
      await fn(check)
      const failed = row.checks.filter((c) => !c.ok)
      if (failed.length) {
        row.status = 'fail'
        row.detail = failed.map((c) => `${c.what}${c.detail ? `: ${c.detail}` : ''}`).join('; ')
      }
    } catch (e) {
      const ni = notImplemented(e)
      if (ni) {
        row.status = 'ni'
        row.detail = ni.message
      } else {
        row.status = 'fail'
        row.detail = `${e?.message ?? e}${e?.error ? ` [${e.error}]` : ''}`
        if (process.env.DEBUG) console.error(e)
      }
    }
    row.ms = Math.round(performance.now() - t0)
    const n = row.checks.length
    log(`[${config}] ${step}: ${row.status.toUpperCase()} (${n} checks, ${row.ms} ms)${row.status !== 'pass' ? ` ${row.detail.slice(0, 300)}` : ''}`)
    return row
  }

  status(config, step) {
    return this.rows.find((r) => r.config === config && r.step === step)?.status
  }

  failed() {
    return this.rows.filter((r) => r.status === 'fail')
  }

  write() {
    this.meta.finished = new Date().toISOString()
    const json = {
      mode: this.mode,
      meta: this.meta,
      rows: this.rows,
      notes: this.notes,
      metrics: this.metrics,
      client_ms: { by_config_host: timingRows(summarize), by_host_ok: timingByHost(summarize) },
    }
    writeFileSync(`${OUT}${this.mode}.json`, JSON.stringify(json, null, 2))
    writeFileSync(`${OUT}${this.mode}.md`, this.markdown())
    return `${OUT}${this.mode}.md`
  }

  markdown() {
    const configs = [...new Set(this.rows.map((r) => r.config))]
    const steps = [...new Set(this.rows.map((r) => r.step))]
    const cell = (c, s) => {
      const r = this.rows.find((x) => x.config === c && x.step === s)
      if (!r) return ''
      return { pass: 'pass', fail: '**FAIL**', ni: 'not impl.', blocked: 'blocked', skip: '–' }[r.status]
    }
    const lines = [
      `# Spaces ${this.mode}: ${this.meta.started}`,
      '',
      Object.entries(this.meta)
        .filter(([k]) => !['started', 'finished'].includes(k))
        .map(([k, v]) => `- ${k}: ${typeof v === 'object' ? JSON.stringify(v) : v}`)
        .join('\n'),
      '',
      `| step | ${configs.join(' | ')} |`,
      `|---|${configs.map(() => '---').join('|')}|`,
      ...steps.map((s) => `| ${s} | ${configs.map((c) => cell(c, s)).join(' | ')} |`),
      '',
    ]
    const counts = {}
    for (const r of this.rows) counts[r.status] = (counts[r.status] ?? 0) + 1
    lines.push(`Totals: ${Object.entries(counts).map(([k, v]) => `${k} ${v}`).join(', ')}`, '')
    const bad = this.rows.filter((r) => r.status === 'fail' || r.status === 'ni')
    if (bad.length) {
      lines.push('## Failures and gaps', '')
      for (const r of bad) lines.push(`- **${r.config} / ${r.step}** (${r.status}): ${r.detail}`)
      lines.push('')
    }
    if (Object.keys(this.metrics).length) {
      lines.push('## Metrics', '', '```json', JSON.stringify(this.metrics, null, 2), '```', '')
    }
    for (const [title, body] of this.sections) lines.push(`## ${title}`, '', ...body, '')
    lines.push(...timingMarkdown())
    if (this.notes.length) lines.push('## Notes', '', ...this.notes.map((n) => `- ${n}`), '')
    return lines.join('\n')
  }
}

export function pct(arr, p) {
  if (!arr.length) return null
  const s = [...arr].sort((a, b) => a - b)
  return +s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))].toFixed(3)
}

export function summarize(arr) {
  return arr.length ? { n: arr.length, p50: pct(arr, 50), p90: pct(arr, 90), p99: pct(arr, 99), max: pct(arr, 100) } : { n: 0 }
}

const short = (m) => m.replace(/^com\.atproto\./, '')
const f = (x) => (x == null ? '' : x < 10 ? x.toFixed(1) : Math.round(x))

/**
 * The driver's client-side latency (ms) as markdown: successful calls by host
 * pooled over every config, then every config / host / outcome. A call that
 * retried (DPoP nonce, token refresh) counts its final attempt only.
 */
function timingMarkdown() {
  const byHost = timingByHost(summarize)
  if (!byHost.length) return []
  const lines = [
    '## Client latency by host (ok calls, ms)',
    '',
    '| method | host | n | p50 | p90 | p99 | max |',
    '|---|---|---|---|---|---|---|',
    ...byHost.map((r) => `| ${short(r.method)} | ${r.host} | ${r.n} | ${f(r.p50)} | ${f(r.p90)} | ${f(r.p99)} | ${f(r.max)} |`),
    '',
    '## Client latency by config, host and outcome (ms)',
    '',
    '| config | host | method | outcome | n | p50 | p90 | p99 | max | retried |',
    '|---|---|---|---|---|---|---|---|---|---|',
    ...timingRows(summarize).map(
      (r) => `| ${r.config} | ${r.host} | ${short(r.method)} | ${r.outcome} | ${r.n} | ${f(r.p50)} | ${f(r.p90)} | ${f(r.p99)} | ${f(r.max)} | ${r.retried || ''} |`,
    ),
    '',
  ]
  return lines
}
