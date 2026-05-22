export default function StatCard({ label, value, sub, color = 'blue' }) {
  const accent = {
    blue:   'border-brand-500 text-brand-400',
    green:  'border-emerald-500 text-emerald-400',
    red:    'border-red-500 text-red-400',
    yellow: 'border-yellow-500 text-yellow-400',
    purple: 'border-purple-500 text-purple-400',
  }[color] || 'border-brand-500 text-brand-400'

  return (
    <div className={`bg-navy-800 rounded-xl border-l-4 ${accent.split(' ')[0]} p-4`}>
      <p className="text-xs text-slate-400 uppercase tracking-wide">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${accent.split(' ')[1]}`}>{value}</p>
      {sub && <p className="text-xs text-slate-500 mt-1">{sub}</p>}
    </div>
  )
}
