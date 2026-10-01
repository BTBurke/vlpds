// Reference verdicts for identifier syntax, as @atproto/lex-schema's strict
// string formats check them (the formats records are validated with).
// usage: node syntax_oracle.mjs <node_modules dir> <in.tsv: kind\tinput> <out.tsv>
// then: VLPDS_SYNTAX_ORACLE=out.tsv cargo test --test all -- --ignored syntax_reference_oracle
import fs from 'fs'
import path from 'path'
import { pathToFileURL } from 'url'
const [nm, input, output] = process.argv.slice(2)
const imp = (p) => import(pathToFileURL(path.join(nm, p)).href)
const s = await imp('@atproto/syntax/dist/index.js')
const lx = await imp('@atproto/lex-schema/dist/core/string-format.js')
const f = {
  did: lx.isDidString, handle: lx.isHandleString, nsid: lx.isNsidString, rkey: lx.isRecordKeyString,
  aturi: s.isAtUriString, datetime: s.isDatetimeString, tid: lx.isTidString,
  language: lx.isLanguageString, atidentifier: s.isAtIdentifierString,
}
const lines = fs.readFileSync(input, 'utf8').split('\n').filter(Boolean)
fs.writeFileSync(output, lines.map((l) => { const [k, v] = l.split('\t'); return `${l}\t${!!f[k](v)}` }).join('\n') + '\n')
