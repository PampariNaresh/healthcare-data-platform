export default function ChartCard({ title, children, className = '' }) {
  return (
    <div className={`bg-navy-800 rounded-xl p-4 ${className}`}>
      {title && <h3 className="text-sm font-semibold text-slate-300 mb-4">{title}</h3>}
      {children}
    </div>
  )
}
