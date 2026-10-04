// The bucket table on docs/operations/rate-limits.md ("## The buckets") is
// hand-written: fail the docs check when its key, window or points disagree
// with the `limit!` definitions in src/ratelimit.rs, or a bucket is missing
// or extra.
// Not part of `vite build`, because the Docker UI stage has no src/.
import { existsSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "../..");
const srcFile = join(root, "src/ratelimit.rs");
const docFile = join(root, "docs/operations/rate-limits.md");
if (!existsSync(srcFile)) {
  console.log("check-docs-ratelimits: no src/ratelimit.rs, skipped");
  process.exit(0);
}
const src = readFileSync(srcFile, "utf8");

const consts = { MINUTE: 60_000, HOUR: 3_600_000, DAY: 86_400_000 };
const budget = src.match(/pub const DEFAULT_MAIL_DAILY_BUDGET: u32 = (\d+);/);
if (budget) consts.DEFAULT_MAIL_DAILY_BUDGET = Number(budget[1]);
const num = (expr) =>
  expr.split("*").reduce((acc, f) => {
    const t = f.trim().replace(/_/g, "");
    const v = /^\d+$/.test(t) ? Number(t) : consts[f.trim()];
    if (v === undefined) throw new Error(`check-docs-ratelimits: can't evaluate ${JSON.stringify(expr)}`);
    return acc * v;
  }, 1);

const KEY = { Ip: "IP", IdentifierIp: "identifier + IP", Did: "DID", Node: "node", Cluster: "cluster" };
const fmtWindow = (ms) => (ms % consts.DAY === 0 ? `${ms / consts.DAY} day` : ms % consts.HOUR === 0 ? `${ms / consts.HOUR} h` : `${ms / consts.MINUTE} min`);

const re = /limit!\(\s*\w+,\s*\d+,\s*"([^"]+)",\s*(\w+),\s*"(?:[^"\\]|\\.)*",\s*([^,]+?),\s*([^,]+?),?\s*\);/gs;
const code = new Map([...src.matchAll(re)].map((m) => [m[1], { key: KEY[m[2]], window: fmtWindow(num(m[3])), points: num(m[4]) }]));
const declared = Number(src.match(/pub const BUILTIN: \[&Limit; (\d+)\]/)?.[1]);

const errors = [];
if (code.size !== declared) errors.push(`parsed ${code.size} limit!() definitions but BUILTIN has ${declared}: update this script's pattern`);
const rows = new Map();
const section = readFileSync(docFile, "utf8").split(/^## /m).find((s) => s.startsWith("The buckets\n")) ?? "";
if (!section) errors.push('no "## The buckets" section');
for (const line of section.split("\n")) {
  const cells = line.split("|").map((c) => c.trim());
  const name = cells[1]?.match(/^`([^`]+)`$/)?.[1];
  if (!name || cells.length < 6) continue;
  rows.set(name, { key: cells[2], window: cells[3], points: Number(cells[4].match(/^[\d,]+/)?.[0].replace(/,/g, "")) });
}
for (const [name, want] of code) {
  const got = rows.get(name);
  if (!got) errors.push(`${name}: missing from the table`);
  else for (const k of ["key", "window", "points"]) if (got[k] !== want[k]) errors.push(`${name}: ${k} is ${JSON.stringify(got[k])} in the table, ${JSON.stringify(want[k])} in src/ratelimit.rs`);
}
for (const name of rows.keys()) if (!code.has(name)) errors.push(`${name}: in the table but not a built-in bucket`);

if (errors.length) {
  console.error(`check-docs-ratelimits: docs/operations/rate-limits.md disagrees with src/ratelimit.rs:\n  ${errors.join("\n  ")}`);
  process.exit(1);
}
console.log(`check-docs-ratelimits: ${code.size} buckets ok`);
