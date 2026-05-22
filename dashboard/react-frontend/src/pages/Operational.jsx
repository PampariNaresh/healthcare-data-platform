import { useEffect, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer
} from 'recharts'
import { operational } from '../api/client'
import StatCard from '../components/StatCard'
import ChartCard from '../components/ChartCard'

export default function Operational() {
  const [summary, setSummary]     = useState(null)
  const [workload, setWorkload]   = useState([])
  const [peakHours, setPeakHours] = useState([])
  const [scorecard, setScorecard] = useState([])

  useEffect(() => {
    operational.summary().then(r => setSummary(r.data))
    operational.doctorWorkload().then(r => setWorkload(r.data.slice(0, 10)))
    operational.peakHours().then(r => setPeakHours(r.data))
    operational.topDoctors().then(r => setScorecard(r.data))
  }, [])

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Operational Analytics</h1>

      <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
        <StatCard label="Total Appointments" value={summary?.total_appointments ?? '—'} color="blue" />
        <StatCard label="Completion Rate"    value={summary ? `${summary.completion_rate_pct}%` : '—'} color="green" />
        <StatCard label="No-show Rate"       value={summary ? `${summary.no_show_rate_pct}%` : '—'}    color="red" />
        <StatCard label="Cancellation Rate"  value={summary ? `${summary.cancellation_rate_pct}%` : '—'} color="yellow" />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <ChartCard title="Peak Appointment Hours">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={peakHours}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="hour_of_day" tickFormatter={h => `${h}:00`} tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155' }}
                labelFormatter={h => `Hour ${h}:00`} />
              <Bar dataKey="appointment_count" name="Total"     fill="#3b82f6" radius={[4,4,0,0]} />
              <Bar dataKey="completed_count"   name="Completed" fill="#10b981" radius={[4,4,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Doctor Workload">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={workload} layout="vertical" margin={{ left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <YAxis type="category" dataKey="full_name" width={110} tick={{ fill: '#94a3b8', fontSize: 10 }} />
              <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="completed_appointments" name="Completed" fill="#10b981" stackId="a" />
              <Bar dataKey="no_show_count"          name="No-show"   fill="#ef4444" stackId="a" />
              <Bar dataKey="cancellation_count"     name="Cancelled" fill="#f59e0b" stackId="a" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      <ChartCard title="Top Doctors Scorecard">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-slate-400 border-b border-navy-700">
                <th className="pb-2 pr-4">Rank</th>
                <th className="pb-2 pr-4">Doctor</th>
                <th className="pb-2 pr-4">Specialization</th>
                <th className="pb-2 pr-4">Branch</th>
                <th className="pb-2 pr-4 text-right">Revenue</th>
                <th className="pb-2 pr-4 text-right">Completion %</th>
                <th className="pb-2 text-right">Score</th>
              </tr>
            </thead>
            <tbody>
              {scorecard.map((d, i) => (
                <tr key={i} className="border-b border-navy-700/50 hover:bg-navy-700/30 transition-colors">
                  <td className="py-2 pr-4">
                    <span className={`inline-flex w-6 h-6 items-center justify-center rounded-full text-xs font-bold ${
                      i === 0 ? 'bg-yellow-500 text-black' : i === 1 ? 'bg-slate-400 text-black' : i === 2 ? 'bg-amber-700 text-white' : 'bg-navy-700 text-slate-300'
                    }`}>{i+1}</span>
                  </td>
                  <td className="py-2 pr-4 font-medium text-white">{d.full_name}</td>
                  <td className="py-2 pr-4 text-slate-400">{d.specialization}</td>
                  <td className="py-2 pr-4 text-slate-400">{d.hospital_branch}</td>
                  <td className="py-2 pr-4 text-right text-emerald-400">₹{(d.total_revenue/1000).toFixed(0)}K</td>
                  <td className="py-2 pr-4 text-right">{d.completion_rate_pct}%</td>
                  <td className="py-2 text-right font-bold text-brand-400">{d.overall_score}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ChartCard>
    </div>
  )
}
