import type { ReactNode } from 'react'
import { NeedsVersion, PageHead, Panel } from '../../components/console/kit'
import type { Section } from '../../components/console/sections'

/** A section that hasn't been built yet: says what it will hold and what it needs. */
export function Placeholder({ section, what, nsid, children }: { section: Section; what: ReactNode; nsid?: string; children?: ReactNode }) {
  return (
    <>
      <PageHead title={section.label} sub={<span>{what}</span>} />
      <Panel title="Coming to this console">
        {nsid && <NeedsVersion what={section.label} nsid={nsid} />}
        {children && <div className="cx-pn-b t2">{children}</div>}
      </Panel>
    </>
  )
}

/** An older console page shown inside the new shell, in its own look, until its section is rebuilt. */
export function Legacy({ children }: { children: ReactNode }) {
  return <div className="cx-legacy">{children}</div>
}
