import { useEffect, useState } from 'react'
import {
  PieChart, Pie, Cell, LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'
import { patients } from '../api/client'
import StatCard from '../components/StatCard'
import ChartCard from '../components/ChartCard'

const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6']

export default function Patients() {
  const [summary, setSummary]     = useState(null)
  const [ageGroups, setAgeGroups] = useState([])
  const [retention, setRetention] = useState([])
  const [trend, setTrend]         = useState([])
  const [spending, setSpending]   = useState([])

  useEffect(() => {
    patients.summary().then(r => setSummary(r.data))
    patients.ageGroups().then(r => setAgeGroups(r.data))
    patients.retention().then(r => setRetention(r.data))
    patients.newTrend().then(r =>
      setTrend(r.data.map(d => ({ ...d, label: `${d.year}-${String(d.month).padStart(2,'0')}` })))
    )
    patients.spending().then(r => setSpending(r.data))
  }, [])

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Patient Analytics</h1>

      <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
        <StatCard label="Total Patients"  value={summary?.total_patients ?? '—'}              color="blue" />
        <StatCard label="New This Month"  value={summary?.new_this_month ?? '—'}              color="green" />
        <StatCard label="Retention Rate"  value={summary ? `${summary.retention_pct}%` : '—'} color="purple" />
        <StatCard label="Avg Spend"       value={summary ? `₹${summary.avg_spend?.toLocaleString()}` : '—'} color="yellow" />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <ChartCard title="Patient Age Distribution">
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={ageGroups} dataKey="patient_count" nameKey="age_group"
                cx="50%" cy="50%" outerRadius={85} label={({ name, percent }) => `${name}: ${(percent*100).toFixed(0)}%`}
                labelLine={false}>
                {ageGroups.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Pie>
              <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Monthly New Patients">
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={trend}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="label" tick={{ fill: '#94a3b8', fontSize: 10 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Line type="monotone" dataKey="new_patients"  name="Total"  stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} />
              <Line type="monotone" dataKey="male_count"    name="Male"   stroke="#60a5fa" strokeWidth={1.5} dot={false} />
              <Line type="monotone" dataKey="female_count"  name="Female" stroke="#f472b6" strokeWidth={1.5} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <ChartCard title="Patient Retention Segments">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={retention}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="visit_segment" tick={{ fill: '#94a3b8', fontSize: 10 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
              <Bar dataKey="patient_count" name="Patients" fill="#8b5cf6" radius={[4,4,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Spending by Insurance Provider">
          <div className="overflow-y-auto max-h-48">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-slate-400 border-b border-navy-700">
                  <th className="pb-2 pr-4">Insurance</th>
                  <th className="pb-2 pr-4 text-right">Patients</th>
                  <th className="pb-2 pr-4 text-right">Avg Age</th>
                  <th className="pb-2 text-right">Avg Spend</th>
                </tr>
              </thead>
              <tbody>
                {spending.map((s, i) => (
                  <tr key={i} className="border-b border-navy-700/50">
                    <td className="py-1.5 pr-4 text-white">{s.insurance_provider}</td>
                    <td className="py-1.5 pr-4 text-right">{s.patient_count}</td>
                    <td className="py-1.5 pr-4 text-right text-slate-400">{s.avg_age}</td>
                    <td className="py-1.5 text-right text-emerald-400">₹{s.avg_spend?.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </ChartCard>
      </div>
    </div>
  )
}
