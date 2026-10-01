import qrcode from 'qrcode-generator'
import { useMemo } from 'react'

/** A QR code as one SVG path (always dark-on-white, so phones can read it in dark mode). */
export function QR({ text, label }: { text: string; label: string }) {
  const { d, n } = useMemo(() => {
    const qr = qrcode(0, 'M')
    qr.addData(text)
    qr.make()
    const n = qr.getModuleCount()
    let d = ''
    for (let r = 0; r < n; r++) {
      for (let c = 0; c < n; c++) {
        if (qr.isDark(r, c)) d += `M${c} ${r}h1v1h-1z`
      }
    }
    return { d, n }
  }, [text])
  return (
    <div className="qr">
      <svg viewBox={`-2 -2 ${n + 4} ${n + 4}`} role="img" aria-label={label} shapeRendering="crispEdges">
        <rect x="-2" y="-2" width={n + 4} height={n + 4} fill="#fff" />
        <path d={d} fill="#0d141b" />
      </svg>
    </div>
  )
}
