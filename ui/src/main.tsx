import { StrictMode, Suspense, lazy, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'
import { usePath } from './lib/router'
import { Landing } from './pages/Landing'
import { AccountApp } from './pages/account/AccountApp'
import { AdminApp } from './pages/admin/AdminApp'
import { Migrate } from './pages/migrate/Migrate'

// its own chunk: the nav and page index aren't needed anywhere else
const DocsApp = lazy(() => import('./pages/docs/DocsApp').then((m) => ({ default: m.DocsApp })))

function App() {
  const path = usePath()
  const area = path.startsWith('/account')
    ? 'account'
    : path.startsWith('/admin')
      ? 'admin'
      : path.startsWith('/migrate')
        ? 'migrate'
        : path === '/docs' || path.startsWith('/docs/')
          ? 'docs'
          : 'landing'
  useEffect(() => {
    if (area === 'docs') return // DocsApp titles each page
    document.title = area === 'account' ? 'Account · vlpds' : area === 'admin' ? 'Console · vlpds' : area === 'migrate' ? 'Move here · vlpds' : `${location.hostname} · vlpds`
  }, [area])
  if (area === 'account') return <AccountApp path={path} />
  if (area === 'admin') return <AdminApp path={path} />
  if (area === 'migrate') return <Migrate />
  if (area === 'docs')
    return (
      <Suspense fallback={null}>
        <DocsApp path={path} />
      </Suspense>
    )
  return <Landing />
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
