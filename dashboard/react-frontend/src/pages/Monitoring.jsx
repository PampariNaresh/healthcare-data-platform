import { useEffect, useState } from 'react'
import {
  BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'
import { monitoring } from '../api/client'
import StatCard from '../components/StatCard'
import ChartCard from '../components/ChartCard'

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#f97316']
const FLAG_COLORS = { normal: '#10b981', low: '#f59e0b', high: '#f97316', critical: '#ef4444' }
const fmt = (n) => n >= 1e6 ? `₹${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `₹${(n / 1e3).toFixed(0)}K` : `₹${n}`

export default function Monitoring() {
  const [summary, setSummary]   = useState(null)
  const [vitals, setVitals]     = useState([])
  const [labs, setLabs]         = useState([])
  const [events, setEvents]     = useState([])
  const [depts, setDepts]       = useState([])
  const [icu, setIcu]           = useState([])

  useEffect(() => {
    monitoring.summary().then(r => setSummary(r.data))
    monitoring.vitalsSummary().then(r => setVitals(r.data.slice(0, 10)))
    monitoring.labTests().then(r => setLabs(r.data))
    monitoring.hospitalEvents().then(r => setEvents(r.data))
    monitoring.departmentActivity().then(r => setDepts(r.data))
    monitoring.icuCodes().then(r => setIcu(r.data))
  }, [])

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Monitoring Analytics</h1>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
        <StatCard label="Total Anomalies"     value={summary ? summary.total_anomalies : '—'}           color="red"    />
        <StatCard label="Avg Anomaly Rate"    value={summary ? `${summary.avg_anomaly_rate}%` : '—'}    color="orange" />
        <StatCard label="Critical Lab Tests"  value={summary ? summary.critical_lab_tests : '—'}        color="purple" />
        <StatCard label="ICU Activations"     value={summary ? summary.icu_activations : '—'}           color="blue"   />
      </div>

      {/* Row 1 — Vitals anomaly + ICU pie */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <ChartCard title="Patient Anomaly Rate (Top 10)">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={vitals} layout="vertical" margin={{ left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" unit="%" tick={{ fill: '#94a3b8', fontSize: 11 }} domain={[0, 100]} />
              <YAxis type="category" dataKey="patient_id" width={50} tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip
                formatter={(v) => [`${v}%`, 'Anomaly Rate']}
                contentStyle={{ background: '#1e293b', border: '1px solid #334155' }}
              />
              <Bar dataKey="anomaly_rate_pct" name="Anomaly Rate %" fill="#ef4444" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="ICU Code Distribution">
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie
                data={icu} dataKey="code_count" nameKey="code_type"
                cx="50%" cy="50%" outerRadius={80}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                labelLine={false}
              >
                {icu.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Pie>
              <Tooltip
                formatter={(v, n, p) => [v, p.payload.code_type]}
                contentStyle={{ background: '#1e293b', border: '1px solid #334155' }}
              />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Row 2 — Lab flags + Hospital events */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <ChartCard title="Lab Test Flag Distribution">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={labs}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="test_name" tick={{ fill: '#94a3b8', fontSize: 10 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="normal_count"   name="Normal"   fill={FLAG_COLORS.normal}   stackId="a" />
              <Bar dataKey="low_count"      name="Low"      fill={FLAG_COLORS.low}      stackId="a" />
              <Bar dataKey="high_count"     name="High"     fill={FLAG_COLORS.high}     stackId="a" />
              <Bar dataKey="critical_count" name="Critical" fill={FLAG_COLORS.critical} stackId="a" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Hospital Events by Type">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={events} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" tickFormatter={fmt} tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <YAxis type="category" dataKey="event_type" width={110} tick={{ fill: '#94a3b8', fontSize: 10 }} />
              <Tooltip
                formatter={(v, n) => [n === 'total_amount' ? fmt(v) : v, n === 'total_amount' ? 'Revenue' : 'Count']}
                contentStyle={{ background: '#1e293b', border: '1px solid #334155' }}
              />
              <Bar dataKey="total_amount" name="total_amount" fill="#3b82f6" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Department Activity Table */}
      <ChartCard title="Department Activity">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-slate-400 border-b border-navy-700">
                <th className="pb-2 pr-4">Department</th>
                <th className="pb-2 pr-4">Branch</th>
                <th className="pb-2 pr-4 text-right">Events</th>
                <th className="pb-2 pr-4 text-right">ICU Codes</th>
                <th className="pb-2 pr-4 text-right">Critical ICU</th>
                <th className="pb-2 pr-4 text-right">Event Rev</th>
                <th className="pb-2 text-right">Total Rev</th>
              </tr>
            </thead>
            <tbody>
              {depts.map((d, i) => (
                <tr key={i} className="border-b border-navy-700/50 hover:bg-navy-700/30 transition-colors">
                  <td className="py-2 pr-4 font-medium text-white">{d.department_name}</td>
                  <td className="py-2 pr-4 text-slate-400">{d.hospital_branch}</td>
                  <td className="py-2 pr-4 text-right">{d.total_events}</td>
                  <td className="py-2 pr-4 text-right">{d.total_icu_codes}</td>
                  <td className="py-2 pr-4 text-right text-red-400 font-medium">{d.critical_icu_count}</td>
                  <td className="py-2 pr-4 text-right text-slate-300">{fmt(d.total_event_amount)}</td>
                  <td className="py-2 text-right text-emerald-400 font-medium">{fmt(d.total_amount)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ChartCard>
    </div>
  )
}
