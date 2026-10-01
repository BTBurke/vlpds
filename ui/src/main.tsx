import { StrictMode, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'
import { usePath } from './lib/router'
import { Landing } from './pages/Landing'
import { AccountApp } from './pages/account/AccountApp'
import { AdminApp } from './pages/admin/AdminApp'

function App() {
  const path = usePath()
  const area = path.startsWith('/account') ? 'account' : path.startsWith('/admin') ? 'admin' : 'landing'
  useEffect(() => {
    document.title = area === 'account' ? 'Account · vlpds' : area === 'admin' ? 'Console · vlpds' : `${location.hostname} · vlpds`
  }, [area])
  if (area === 'account') return <AccountApp path={path} />
  if (area === 'admin') return <AdminApp path={path} />
  return <Landing />
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
