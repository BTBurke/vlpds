// The docs site is public: fail the docs check if a published page matches a
// pattern in docs-deny.txt (a deployment's own hosts, networks, accounts or
// domains). Files starting with `_` are not published and are skipped.
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join, relative, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const docsDir = join(dirname(fileURLToPath(import.meta.url)), "../../docs");

const denyFile = join(dirname(fileURLToPath(import.meta.url)), "docs-deny.txt");
if (!existsSync(denyFile)) {
  console.log("check-docs-private: no docs-deny.txt, skipped");
  process.exit(0);
}
const patterns = readFileSync(denyFile, "utf8")
  .split("\n")
  .map((l) => l.trim())
  .filter((l) => l && !l.startsWith("#"))
  .map((l) => new RegExp(l, "i"));

function walk(dir) {
  const out = [];
  for (const ent of readdirSync(dir, { withFileTypes: true })) {
    if (ent.name.startsWith("_") || ent.name.startsWith(".")) continue;
    const p = join(dir, ent.name);
    if (ent.isDirectory()) out.push(...walk(p));
    else if (ent.name.endsWith(".md")) out.push(p);
  }
  return out;
}

const hits = [];
for (const file of walk(docsDir)) {
  readFileSync(file, "utf8")
    .split("\n")
    .forEach((line, i) => {
      for (const re of patterns) {
        const m = line.match(re);
        if (m) hits.push(`  ${relative(docsDir, file)}:${i + 1}: "${m[0]}"  ${line.trim().slice(0, 120)}`);
      }
    });
}

if (hits.length) {
  console.error(
    `check-docs-private: ${hits.length} reference(s) to private infrastructure in published docs.\n` +
      `The docs are public: use placeholders (pds.example.com, <your-bucket>, "a small VPS", "your monitoring stack").\n` +
      hits.join("\n"),
  );
  process.exit(1);
}
console.log("check-docs-private: ok");
