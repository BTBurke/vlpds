import { admin } from '../xrpc'
import { isUnsupported } from './live'

// The one place the console reaches admin endpoints that older vlpds builds don't have
// (segment feed, per-node metrics, lockouts, mail log, config, kicking a subscriber). Each call
// answers { supported: false } when the server doesn't know the method, so a page shows a
// "needs a newer vlpds" placeholder instead of an error. When lib/adminApi.ts lands, these
// bodies call it instead; the page-facing shapes below stay.

export type Optional<T> = { supported: true; data: T } | { supported: false; nsid: string }

async function optional<T>(nsid: string, run: () => Promise<T>): Promise<Optional<T>> {
  try {
    return { supported: true, data: await run() }
  } catch (e) {
    if (isUnsupported(e)) return { supported: false, nsid }
    throw e
  }
}

/** Every node's Prometheus text, scraped by the node serving the console (peer fan-out). */
export type NodeMetrics = { node: string; text?: string; error?: string }[]
export const nodeMetrics = () =>
  optional('vlpds.admin.getNodeMetrics', async () => {
    const r: { nodes: { node: string; metrics?: string; text?: string; error?: string }[] } = await admin('vlpds.admin.getNodeMetrics')
    return r.nodes.map((n) => ({ node: n.node, text: n.text ?? n.metrics, error: n.error })) as NodeMetrics
  })

/** Log segments per node since `sinceMs`: when each batch started, when its PUT was durable. */
export type Segment = { ordinal: number; startMs: number; durableMs?: number; events: number; bytes?: number }
export type SegmentFeed = { nodes: { node: string; log: string; segments: Segment[] }[]; time: number }
export const segmentFeed = (sinceMs: number) =>
  optional('vlpds.admin.getSegmentFeed', () => admin<SegmentFeed>('vlpds.admin.getSegmentFeed', { params: { since: sinceMs } }))

/** Accounts and addresses held by a rate-limit bucket or a factor lock right now. */
export type Lockout = { kind: 'account' | 'ip'; did?: string; handle?: string; ip?: string; bucket: string; used: number; limit: number; resetsAt: number; ips?: number }
export const lockouts = () =>
  optional('vlpds.admin.listLockouts', async () => (await admin<{ lockouts: Lockout[] }>('vlpds.admin.listLockouts')).lockouts)

export type MailEntry = { at: number; purpose: string; to: string; did?: string; node: string; result: 'sent' | 'retrying' | 'failed' | 'suppressed'; attempts: number; why?: string }
export const mailLog = (limit = 50) =>
  optional('vlpds.admin.getMailLog', async () => (await admin<{ entries: MailEntry[] }>('vlpds.admin.getMailLog', { params: { limit } })).entries)

export type ConfigEntry = { key: string; value: string; source: 'flag' | 'env' | 'default' | 'stored'; group?: string; secret?: boolean; set?: boolean }
export const getConfig = (node?: string) =>
  optional('vlpds.admin.getConfig', async () => (await admin<{ node: string; entries: ConfigEntry[] }>('vlpds.admin.getConfig', { params: { node } })))

export const kickSubscriber = (node: string, conn: string) =>
  optional('vlpds.admin.kickSubscriber', () => admin('vlpds.admin.kickSubscriber', { body: { node, conn } }))
