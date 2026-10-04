// Link-preview cards (Open Graph / Twitter images): 1200×630 PNGs rendered
// at build time from an SVG template with resvg. Card fetchers don't run JS
// and most can't read SVG, so the server points og:image at these.
// SVG has no line wrapping: lines are broken here with the fonts' advance
// widths (og-fonts/metrics.json, written with the fonts by og-fonts/make.py).

import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import { fileURLToPath } from 'node:url'
import { Resvg } from '@resvg/resvg-js'

const FONT_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'og-fonts')
const FONT_FILES = ['grotesk-400.ttf', 'grotesk-700.ttf', 'mono-500.ttf'].map((f) => path.join(FONT_DIR, f))
const METRICS = JSON.parse(fs.readFileSync(path.join(FONT_DIR, 'metrics.json'), 'utf8'))

export const W = 1200
export const H = 630
const PAD = 80
const TEXT_W = 860

// light palette (styles.css :root); cards are read in feeds, mostly light
const C = { sheet: '#f8f9f6', paper: '#edefea', ink: '#18222d', ink2: '#4d5966', ink3: '#6f7a85', rule: '#dde1db', accent: '#17705f', teal: '#4fbf9f', amber: '#e3a43a', amberInk: '#a86d0a' }

const xml = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')

function width(text, font, size) {
  const m = METRICS[font]
  let w = 0
  for (const ch of text) w += m[ch] ?? 0.6
  return w * size
}

/** Greedy wrap into at most `max` lines; the last one ends in "…" when cut. */
function wrap(text, font, size, max, w = TEXT_W) {
  const words = text.trim().split(/\s+/)
  const lines = []
  let cur = ''
  let i = 0
  for (; i < words.length; i++) {
    const next = cur ? `${cur} ${words[i]}` : words[i]
    if (width(next, font, size) <= w || !cur) {
      cur = next
      continue
    }
    lines.push(cur)
    cur = words[i]
    if (lines.length === max) break
  }
  if (lines.length < max) {
    lines.push(cur)
    return { lines, cut: false }
  }
  let last = lines[max - 1]
  while (last && width(`${last}…`, font, size) > w) last = last.replace(/\s*\S+$/, '')
  lines[max - 1] = `${last.replace(/[\s,.;:–—-]+$/, '')}…`
  return { lines, cut: true }
}

/** The biggest title size that fits in `max` lines without cutting it. */
function fitTitle(title, sizes) {
  for (const [size, max] of sizes) {
    const r = wrap(title, 'grotesk-700', size, max)
    if (!r.cut) return { size, lines: r.lines }
  }
  const [size, max] = sizes[sizes.length - 1]
  return { size, lines: wrap(title, 'grotesk-700', size, max).lines }
}

function text(lines, { x, y, size, lh, weight, fill, family = 'Schibsted Grotesk', spacing = 0 }) {
  return lines
    .map(
      (l, i) =>
        `<text x="${x}" y="${y + i * lh}" font-family="${family}" font-size="${size}" font-weight="${weight}" fill="${fill}"${spacing ? ` letter-spacing="${spacing}"` : ''}>${xml(l)}</text>`,
    )
    .join('')
}

/** The site mark (favicon.svg's three strata), `s` px per favicon unit. */
function mark(x, y, s, solid = true) {
  const bar = (bx, by, bw, fill, op = 1) => `<rect x="${x + bx * s}" y="${y + by * s}" width="${bw * s}" height="${4.5 * s}" rx="${s}" fill="${fill}"${op < 1 ? ` opacity="${op}"` : ''}/>`
  return bar(0, 0, 22, solid ? C.teal : C.accent) + bar(3, 7, 16, solid ? C.teal : C.accent, 0.65) + bar(6, 14, 10, C.amber)
}

/**
 * kicker: small mono line over the title (a docs section); foot: mono text
 * bottom left (the page's path).
 */
export function cardSvg({ kicker, title, summary, foot, where }) {
  const t = fitTitle(title, [
    [76, 2],
    [64, 3],
    [56, 3],
  ])
  const titleLh = Math.round(t.size * 1.1)
  let y = 214
  let body = ''
  if (kicker) {
    body += text([kicker.toUpperCase()], { x: PAD, y, size: 24, lh: 0, weight: 500, fill: C.accent, family: 'JetBrains Mono', spacing: 1.5 })
    y += 32 + t.size
  } else y += t.size - 24
  body += text(t.lines, { x: PAD, y, size: t.size, lh: titleLh, weight: 700, fill: C.ink, spacing: -0.02 * t.size })
  y += (t.lines.length - 1) * titleLh
  if (summary) {
    const room = Math.floor((548 - (y + 28)) / 42)
    if (room > 0) {
      const s = wrap(summary, 'grotesk-400', 30, Math.min(room, 4))
      body += text(s.lines, { x: PAD, y: y + 66, size: 30, lh: 42, weight: 400, fill: C.ink2 })
    }
  }
  const wordmark =
    mark(PAD, 62, 2) +
    `<text x="${PAD + 60}" y="${100}" font-family="Schibsted Grotesk" font-size="40" font-weight="700" fill="${C.ink}" letter-spacing="-0.8">vlpds</text>` +
    (where ? `<text x="${PAD + 60 + width('vlpds', 'grotesk-700', 40) - 0.8 * 5 + 14}" y="100" font-family="Schibsted Grotesk" font-size="40" font-weight="400" fill="${C.ink2}">${xml(where)}</text>` : '')
  return (
    `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">` +
    `<rect width="${W}" height="${H}" fill="${C.sheet}"/>` +
    // the topbar's strata: segment written (verdigris), state applied (amber)
    `<rect width="${W}" height="10" fill="${C.accent}"/><rect y="14" width="${W}" height="5" fill="${C.amber}"/>` +
    // a large faint mark bleeding off the right edge
    `<g opacity="0.22">${mark(985, 150, 9, false)}</g>` +
    wordmark +
    body +
    `<rect x="${PAD}" y="560" width="${W - PAD * 2}" height="2" fill="${C.rule}"/>` +
    (foot ? text([foot], { x: PAD, y: 600, size: 22, lh: 0, weight: 500, fill: C.ink3, family: 'JetBrains Mono' }) : '') +
    `<text x="${W - PAD}" y="600" text-anchor="end" font-family="Schibsted Grotesk" font-size="22" font-weight="400" fill="${C.ink3}">AT Protocol · Bluesky PDS</text>` +
    `</svg>`
  )
}

export function renderPng(svg) {
  const r = new Resvg(svg, {
    fitTo: { mode: 'original' },
    font: { fontFiles: FONT_FILES, loadSystemFonts: false, defaultFontFamily: 'Schibsted Grotesk' },
  })
  return r.render().asPng()
}

const hash = (buf) => crypto.createHash('sha256').update(buf).digest('hex').slice(0, 10)

/**
 * The cards and the manifest the server reads (`og/manifest.json`): per
 * docs page its title, summary, section and card; the site and /migrate
 * cards. Images are content-hashed so the server can cache them forever.
 */
export function buildCards(pages) {
  const files = []
  const card = (name, spec, alt) => {
    const png = renderPng(cardSvg(spec))
    const fileName = `og/${name}-${hash(png)}.png`
    files.push({ fileName, source: png })
    return { image: `/${fileName}`, alt }
  }
  const manifest = {
    width: W,
    height: H,
    site: card(
      'site',
      { title: 'A personal data server for the AT Protocol', summary: 'Your Bluesky posts, likes and follows, kept as a signed repository that any atproto app can read and write with your permission.' },
      'vlpds: a personal data server for the AT Protocol',
    ),
    migrate: card(
      'migrate',
      { kicker: 'Move here', title: 'Move your Bluesky account to this server', summary: 'Keep your handle, followers and posts: your repository, blobs and identity move in one guided flow.', foot: '/migrate' },
      'Move your Bluesky account to this vlpds server',
    ),
    docs: {},
  }
  for (const p of pages) {
    const name = `docs-${p.slug.replace(/\//g, '-')}`
    const c = card(name, { kicker: p.section, title: p.title, summary: p.summary, foot: `/docs/${p.slug}`, where: 'docs' }, `${p.title}: vlpds documentation`)
    manifest.docs[p.slug] = { title: p.title, summary: p.summary, section: p.section, status: p.status, ...c }
  }
  files.push({ fileName: 'og/manifest.json', source: JSON.stringify(manifest) })
  return files
}
