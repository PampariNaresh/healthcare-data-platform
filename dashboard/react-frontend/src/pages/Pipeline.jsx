import { useEffect, useState } from 'react'
import { pipeline } from '../api/client'

const STATE_COLOR = {
  success:  'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  failed:   'bg-red-500/20 text-red-400 border-red-500/30',
  running:  'bg-blue-500/20 text-blue-400 border-blue-500/30',
  skipped:  'bg-slate-500/20 text-slate-400 border-slate-500/30',
  upstream_failed: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
}

function Badge({ state }) {
  const cls = STATE_COLOR[state] || STATE_COLOR.skipped
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded border text-xs font-medium ${cls}`}>
      {state}
    </span>
  )
}

export default function Pipeline() {
  const [lastRun, setLastRun] = useState(null)
  const [runs, setRuns]       = useState([])
  const [error, setError]     = useState(null)

  useEffect(() => {
    pipeline.lastRun().then(r => setLastRun(r.data)).catch(() => setError('Airflow unreachable'))
    pipeline.runs().then(r => setRuns(r.data)).catch(() => {})
  }, [])

  const fmtDate = (s) => s ? new Date(s).toLocaleString() : '—'
  const duration = (s, e) => {
    if (!s || !e) return '—'
    const secs = Math.round((new Date(e) - new Date(s)) / 1000)
    return secs < 60 ? `${secs}s` : `${Math.floor(secs/60)}m ${secs%60}s`
  }

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Pipeline Status</h1>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-4 text-red-400 text-sm">
          ⚠️ {error} — make sure Airflow is running on EC22
        </div>
      )}

      {/* Last run overview */}
      {lastRun && lastRun.status !== 'no_runs' && (
        <div className="bg-navy-800 rounded-xl p-5 border border-navy-700">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="font-semibold text-white">healthcare_analytics_pipeline</h2>
              <p className="text-xs text-slate-400 mt-0.5">Run: {lastRun.dag_run_id}</p>
            </div>
            <Badge state={lastRun.state} />
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <p className="text-slate-400 text-xs">Execution Date</p>
              <p className="text-white mt-0.5">{fmtDate(lastRun.execution_date)}</p>
            </div>
            <div>
              <p className="text-slate-400 text-xs">Started</p>
              <p className="text-white mt-0.5">{fmtDate(lastRun.start_date)}</p>
            </div>
            <div>
              <p className="text-slate-400 text-xs">Ended</p>
              <p className="text-white mt-0.5">{fmtDate(lastRun.end_date)}</p>
            </div>
            <div>
              <p className="text-slate-400 text-xs">Duration</p>
              <p className="text-white mt-0.5">{duration(lastRun.start_date, lastRun.end_date)}</p>
            </div>
          </div>

          {/* Task instances */}
          {lastRun.task_instances?.length > 0 && (
            <div className="mt-5">
              <p className="text-xs text-slate-400 mb-2 uppercase tracking-wide">Tasks</p>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                {lastRun.task_instances.map((t) => (
                  <div key={t.task_id} className="bg-navy-700/50 rounded-lg p-3">
                    <p className="text-xs text-slate-300 font-medium truncate">{t.task_id}</p>
                    <div className="mt-1.5"><Badge state={t.state || 'none'} /></div>
                    <p className="text-xs text-slate-500 mt-1">{duration(t.start_date, t.end_date)}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Run history */}
      <div className="bg-navy-800 rounded-xl p-4 border border-navy-700">
        <h3 className="text-sm font-semibold text-slate-300 mb-3">Recent Runs</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-slate-400 border-b border-navy-700">
                <th className="pb-2 pr-4">Run ID</th>
                <th className="pb-2 pr-4">Execution Date</th>
                <th className="pb-2 pr-4">Start</th>
                <th className="pb-2 pr-4">Duration</th>
                <th className="pb-2">State</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r, i) => (
                <tr key={i} className="border-b border-navy-700/50 hover:bg-navy-700/30">
                  <td className="py-2 pr-4 text-xs text-slate-400 font-mono truncate max-w-48">{r.dag_run_id}</td>
                  <td className="py-2 pr-4">{fmtDate(r.execution_date)}</td>
                  <td className="py-2 pr-4">{fmtDate(r.start_date)}</td>
                  <td className="py-2 pr-4 text-slate-400">{duration(r.start_date, r.end_date)}</td>
                  <td className="py-2"><Badge state={r.state} /></td>
                </tr>
              ))}
              {runs.length === 0 && !error && (
                <tr><td colSpan={5} className="py-4 text-center text-slate-500">No runs found</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
