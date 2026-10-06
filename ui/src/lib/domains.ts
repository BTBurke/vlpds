/** The served handle domain (leading dot, as describeServer lists them) a
 * handle is under; the longest wins, since one domain can sit under another. */
export function domainOf(handle: string, domains: string[]): string | undefined {
  let best: string | undefined
  for (const d of domains) if (d && handle.length > d.length && handle.endsWith(d) && (!best || d.length > best.length)) best = d
  return best
}
