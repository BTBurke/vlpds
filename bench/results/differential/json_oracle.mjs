// Reference record JSON -> DAG-CBOR: @atproto/lex-json lexParse (strict, as
// record writes parse) + @atproto/lex-cbor encode. Writes `json\tcbor-hex|ERR`.
// usage: node json_oracle.mjs <node_modules dir> <corpus.jsonl> <out.tsv>
import fs from 'fs'
import path from 'path'
import { pathToFileURL } from 'url'
const [nm, input, output] = process.argv.slice(2)
const imp = (p) => import(pathToFileURL(path.join(nm, p)).href)
const { lexParse } = await imp('@atproto/lex-json/dist/index.js')
const { encode } = await imp('@atproto/lex-cbor/dist/index.js')
const out = []
for (const line of fs.readFileSync(input, 'utf8').split('\n').filter(Boolean)) {
  let r
  try { r = Buffer.from(encode(lexParse(line, { strict: true }))).toString('hex') } catch { r = 'ERR' }
  out.push(`${line}\t${r}`)
}
fs.writeFileSync(output, out.join('\n') + '\n')
