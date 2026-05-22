import { useState, useEffect, useCallback } from 'react'
import { infrastructure } from '../api/client'

// ── Quick links ───────────────────────────────────────────────────────────────
const QUICK_LINKS = [
  { label: 'Kafka UI',     url: 'http://65.0.80.152:8085', icon: '📨', desc: 'Topics & consumer groups',  color: 'border-orange-500/40 hover:border-orange-400' },
  { label: 'Flink UI',     url: 'http://65.0.80.152:8081', icon: '⚡', desc: 'Jobs & task managers',       color: 'border-yellow-500/40 hover:border-yellow-400' },
  { label: 'Airflow UI',   url: 'http://3.6.92.19:8080',   icon: '🔄', desc: 'DAG runs & task logs',       color: 'border-green-500/40 hover:border-green-400'  },
  { label: 'Spark Master', url: 'http://3.6.92.19:9090',   icon: '✨', desc: 'Workers & active jobs',      color: 'border-brand-500/40 hover:border-brand-400'  },
  { label: 'Spark Worker', url: 'http://3.6.92.19:8082',   icon: '⚙️', desc: 'Worker cores & memory',     color: 'border-purple-500/40 hover:border-purple-400'},
  { label: 'API Docs',     url: 'http://3.6.92.19:8000/docs', icon: '📖', desc: 'Swagger / OpenAPI',      color: 'border-cyan-500/40 hover:border-cyan-400'    },
]

// ── Service icon map ──────────────────────────────────────────────────────────
const ICONS = { kafka: '📨', flink: '⚡', airflow: '🔄', spark: '✨', db: '🗄️', zk: '🔗', api: '📡', ui: '🖥️' }

// ── Helpers ───────────────────────────────────────────────────────────────────
function overallStatus(data) {
  if (!data) return 'loading'
  const all = [...data.ec21.services, ...data.ec22.services]
  const down = all.filter(s => s.status === 'offline').length
  if (down === 0) return 'operational'
  if (down < all.length) return 'degraded'
  return 'down'
}

function StatusBadge({ status }) {
  const map = {
    online:      'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
    offline:     'bg-red-500/15    text-red-400     border-red-500/30',
    operational: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
    degraded:    'bg-yellow-500/15 text-yellow-400  border-yellow-500/30',
    down:        'bg-red-500/15    text-red-400     border-red-500/30',
    loading:     'bg-slate-500/15  text-slate-400   border-slate-500/30',
  }
  const dot = { online: 'bg-emerald-400', offline: 'bg-red-400', operational: 'bg-emerald-400', degraded: 'bg-yellow-400', down: 'bg-red-400', loading: 'bg-slate-400' }
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border text-xs font-semibold uppercase tracking-wide ${map[status] || map.loading}`}>
      <span className={`w-1.5 h-1.5 rounded-full animate-pulse ${dot[status] || dot.loading}`} />
      {status}
    </span>
  )
}

// ── Server panel ──────────────────────────────────────────────────────────────
function ServerPanel({ title, host, label, services }) {
  const down = services.filter(s => s.status === 'offline').length
  const panelStatus = down === 0 ? 'operational' : down < services.length ? 'degraded' : 'down'

  return (
    <div className="bg-navy-800 rounded-xl border border-navy-700 overflow-hidden">
      {/* Panel header */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-navy-700">
        <div>
          <h2 className="font-semibold text-white">{title}</h2>
          <p className="text-xs text-slate-400 mt-0.5">
            <span className="font-mono text-slate-300">{host}</span>
            <span className="mx-1.5">·</span>
            {label}
          </p>
        </div>
        <StatusBadge status={panelStatus} />
      </div>

      {/* Service rows */}
      <div className="divide-y divide-navy-700/60">
        {services.map((svc, i) => (
          <div key={i} className="flex items-center gap-4 px-5 py-3.5 hover:bg-navy-700/30 transition-colors">
            {/* Icon + name */}
            <span className="text-lg w-7 text-center shrink-0">{ICONS[svc.icon] || '🔧'}</span>
            <span className="text-sm font-medium text-slate-200 w-36 shrink-0">{svc.name}</span>

            {/* Status */}
            <div className="flex-1">
              <StatusBadge status={svc.status} />
            </div>

            {/* Response time */}
            <div className="w-20 text-right shrink-0">
              {svc.response_ms != null
                ? <span className={`text-xs font-mono ${svc.response_ms < 100 ? 'text-emerald-400' : svc.response_ms < 500 ? 'text-yellow-400' : 'text-red-400'}`}>
                    {svc.response_ms} ms
                  </span>
                : <span className="text-xs text-slate-600">—</span>
              }
            </div>

            {/* UI link */}
            <div className="w-28 text-right shrink-0">
              {svc.ui_url
                ? <a
                    href={svc.ui_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={`inline-flex items-center gap-1 text-xs px-3 py-1.5 rounded-lg border transition-colors ${
                      svc.status === 'online'
                        ? 'border-brand-500/50 text-brand-400 hover:bg-brand-500/10'
                        : 'border-navy-700 text-slate-600 cursor-not-allowed pointer-events-none'
                    }`}
                  >
                    {svc.ui_label}
                    <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                    </svg>
                  </a>
                : <span className="text-xs text-slate-600 select-none">Internal</span>
              }
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function Infrastructure() {
  const [data, setData]         = useState(null)
  const [loading, setLoading]   = useState(true)
  const [lastChecked, setLast]  = useState(null)

  const refresh = useCallback(() => {
    setLoading(true)
    infrastructure.status()
      .then(r => { setData(r.data); setLast(new Date()) })
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [])

  // Initial load + auto-refresh every 30s
  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 30_000)
    return () => clearInterval(id)
  }, [refresh])

  const overall = overallStatus(data)
  const overallLabel = { operational: 'All Systems Operational', degraded: 'Partial Outage', down: 'Major Outage', loading: 'Checking…' }

  return (
    <div className="space-y-6">

      {/* Page header */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-white">Infrastructure</h1>
        <div className="flex items-center gap-3">
          {lastChecked && (
            <span className="text-xs text-slate-500">
              Last checked {lastChecked.toLocaleTimeString()}
            </span>
          )}
          <button
            onClick={refresh}
            disabled={loading}
            className="flex items-center gap-2 text-sm px-4 py-2 bg-navy-800 border border-navy-700 rounded-lg text-slate-300 hover:text-white hover:border-brand-500 transition-colors disabled:opacity-40"
          >
            <svg className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Refresh
          </button>
        </div>
      </div>

      {/* Overall status banner */}
      <div className={`flex items-center gap-3 px-5 py-4 rounded-xl border ${
        overall === 'operational' ? 'bg-emerald-500/10 border-emerald-500/30' :
        overall === 'degraded'    ? 'bg-yellow-500/10  border-yellow-500/30'  :
        overall === 'down'        ? 'bg-red-500/10     border-red-500/30'     :
                                    'bg-navy-800       border-navy-700'
      }`}>
        <span className="text-2xl">
          {overall === 'operational' ? '✅' : overall === 'degraded' ? '⚠️' : overall === 'down' ? '🔴' : '⏳'}
        </span>
        <div>
          <p className="font-semibold text-white">{overallLabel[overall]}</p>
          {data && (
            <p className="text-xs text-slate-400 mt-0.5">
              {[...data.ec21.services, ...data.ec22.services].filter(s => s.status === 'online').length}
              {' / '}
              {data.ec21.services.length + data.ec22.services.length}
              {' services online'}
            </p>
          )}
        </div>
      </div>

      {/* Quick links */}
      <div>
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wide mb-3">Quick Access</h2>
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
          {QUICK_LINKS.map(({ label, url, icon, desc, color }) => (
            <a
              key={label}
              href={url}
              target="_blank"
              rel="noopener noreferrer"
              className={`bg-navy-800 border rounded-xl p-4 flex flex-col gap-2 transition-all hover:bg-navy-700/60 group ${color}`}
            >
              <span className="text-2xl">{icon}</span>
              <div>
                <p className="text-sm font-semibold text-white group-hover:text-brand-400 transition-colors">{label}</p>
                <p className="text-xs text-slate-500 mt-0.5">{desc}</p>
              </div>
              <svg className="w-3.5 h-3.5 text-slate-600 group-hover:text-brand-400 transition-colors self-end" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
              </svg>
            </a>
          ))}
        </div>
      </div>

      {/* Server panels */}
      {loading && !data ? (
        <div className="flex items-center justify-center py-16 text-slate-500">
          <svg className="w-6 h-6 animate-spin mr-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Checking all services…
        </div>
      ) : data ? (
        <div className="space-y-4">
          <ServerPanel
            title="EC21"
            host={data.ec21.host}
            label={data.ec21.label}
            services={data.ec21.services}
          />
          <ServerPanel
            title="EC22"
            host={data.ec22.host}
            label={data.ec22.label}
            services={data.ec22.services}
          />
        </div>
      ) : (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-6 text-center text-red-400 text-sm">
          Failed to fetch infrastructure status — check that the API container is running.
        </div>
      )}
    </div>
  )
}
