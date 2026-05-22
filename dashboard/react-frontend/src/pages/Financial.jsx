import { useEffect, useState } from 'react'
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'
import { financial } from '../api/client'
import StatCard from '../components/StatCard'
import ChartCard from '../components/ChartCard'

const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316']

const fmt = (n) => n >= 1e6 ? `₹${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `₹${(n/1e3).toFixed(0)}K` : `₹${n}`

export default function Financial() {
  const [summary, setSummary]   = useState(null)
  const [spec, setSpec]         = useState([])
  const [branch, setBranch]     = useState([])
  const [monthly, setMonthly]   = useState([])
  const [treatment, setTreatment] = useState([])
  const [billing, setBilling]   = useState([])
  const [doctors, setDoctors]   = useState([])

  useEffect(() => {
    financial.summary().then(r => setSummary(r.data))
    financial.revenueBySpec().then(r => setSpec(r.data))
    financial.revenueByBranch().then(r => setBranch(r.data))
    financial.monthlyRevenue().then(r =>
      setMonthly(r.data.map(d => ({ ...d, label: `${d.year}-${String(d.month).padStart(2,'0')}` })))
    )
    financial.treatmentCost().then(r => setTreatment(r.data))
    financial.billingPayment().then(r => setBilling(r.data))
    financial.revenueByDoctor().then(r => setDoctors(r.data.slice(0, 10)))
  }, [])

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Financial Analytics</h1>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
        <StatCard label="Total Revenue"    value={summary ? fmt(summary.total_revenue) : '—'} color="blue" />
        <StatCard label="Paid Bills"       value={summary ? `${summary.paid_pct}%` : '—'}     color="green" />
        <StatCard label="Outstanding"      value={summary ? fmt(summary.total_outstanding) : '—'} color="red" />
        <StatCard label="Total Bills"      value={summary ? summary.total_bills : '—'}          color="purple" />
      </div>

      {/* Row 1 */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <ChartCard title="Revenue by Specialization">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={spec} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" tickFormatter={fmt} tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <YAxis type="category" dataKey="specialization" width={100} tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip formatter={(v) => fmt(v)} contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
              <Bar dataKey="total_revenue" fill="#3b82f6" radius={[0,4,4,0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Revenue by Branch">
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={branch} dataKey="total_revenue" nameKey="hospital_branch"
                cx="50%" cy="50%" outerRadius={80} label={({ name, percent }) => `${name} ${(percent*100).toFixed(0)}%`}
                labelLine={false}>
                {branch.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Pie>
              <Tooltip formatter={(v) => fmt(v)} contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Monthly Revenue */}
      <ChartCard title="Monthly Revenue Trend">
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={monthly}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis dataKey="label" tick={{ fill: '#94a3b8', fontSize: 11 }} />
            <YAxis tickFormatter={fmt} tick={{ fill: '#94a3b8', fontSize: 11 }} />
            <Tooltip formatter={(v) => fmt(v)} contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
            <Line type="monotone" dataKey="total_revenue" stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} />
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Row 2 */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <ChartCard title="Treatment Cost by Type">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={treatment}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="treatment_type" tick={{ fill: '#94a3b8', fontSize: 10 }} />
              <YAxis tickFormatter={fmt} tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip formatter={(v) => fmt(v)} contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
              <Bar dataKey="avg_cost" name="Avg Cost" fill="#f59e0b" radius={[4,4,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Billing by Payment Method">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={billing}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="payment_method" tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
              <Bar dataKey="bill_count" name="Paid"    fill="#10b981" stackId="a" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Doctor Revenue Table */}
      <ChartCard title="Top Doctors by Revenue">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-slate-400 border-b border-navy-700">
                <th className="pb-2 pr-4">Doctor</th>
                <th className="pb-2 pr-4">Specialization</th>
                <th className="pb-2 pr-4">Branch</th>
                <th className="pb-2 pr-4 text-right">Bills</th>
                <th className="pb-2 pr-4 text-right">Revenue</th>
                <th className="pb-2 text-right">Avg Bill</th>
              </tr>
            </thead>
            <tbody>
              {doctors.map((d, i) => (
                <tr key={i} className="border-b border-navy-700/50 hover:bg-navy-700/30 transition-colors">
                  <td className="py-2 pr-4 font-medium text-white">{d.full_name}</td>
                  <td className="py-2 pr-4 text-slate-400">{d.specialization}</td>
                  <td className="py-2 pr-4 text-slate-400">{d.hospital_branch}</td>
                  <td className="py-2 pr-4 text-right">{d.total_bills}</td>
                  <td className="py-2 pr-4 text-right text-emerald-400 font-medium">{fmt(d.total_revenue)}</td>
                  <td className="py-2 text-right text-slate-300">{fmt(d.avg_bill_amount)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ChartCard>
    </div>
  )
}
