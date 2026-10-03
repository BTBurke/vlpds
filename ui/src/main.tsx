import { StrictMode, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'
import { usePath } from './lib/router'
import { Landing } from './pages/Landing'
import { AccountApp } from './pages/account/AccountApp'
import { AdminApp } from './pages/admin/AdminApp'
import { Migrate } from './pages/migrate/Migrate'

function App() {
  const path = usePath()
  const area = path.startsWith('/account') ? 'account' : path.startsWith('/admin') ? 'admin' : path.startsWith('/migrate') ? 'migrate' : 'landing'
  useEffect(() => {
    document.title = area === 'account' ? 'Account · vlpds' : area === 'admin' ? 'Console · vlpds' : area === 'migrate' ? 'Move here · vlpds' : `${location.hostname} · vlpds`
  }, [area])
  if (area === 'account') return <AccountApp path={path} />
  if (area === 'admin') return <AdminApp path={path} />
  if (area === 'migrate') return <Migrate />
  return <Landing />
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
