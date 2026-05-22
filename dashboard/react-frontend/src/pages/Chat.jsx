import { useState, useRef, useEffect } from 'react'

const SUGGESTIONS = [
  'How many patients are in the system?',
  'Which doctor earned the most revenue?',
  'Is the Flink streaming job running?',
  'What is the pipeline status?',
  'Show top 5 doctors by overall score',
  'What are the peak appointment hours?',
  'How many Kafka messages have been processed?',
  'Are all services healthy?',
]

const TOOL_CHIPS = {
  query_analytics_db:          { icon: '🔍', label: 'Querying database...' },
  get_pipeline_status:         { icon: '🔄', label: 'Checking pipeline...' },
  trigger_analytics_pipeline:  { icon: '▶',  label: 'Triggering pipeline...' },
  check_infrastructure_health: { icon: '🖥️', label: 'Checking infrastructure...' },
  get_kafka_topic_info:        { icon: '📨', label: 'Checking Kafka...' },
  get_flink_job_status:        { icon: '⚡', label: 'Checking Flink...' },
  get_mysql_row_counts:        { icon: '🗄️', label: 'Counting rows...' },
}

function ToolChip({ tool }) {
  const chip = TOOL_CHIPS[tool] || { icon: '🔧', label: `Running ${tool}...` }
  return (
    <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium bg-brand-600/20 text-brand-400 border border-brand-500/30 animate-pulse">
      <span>{chip.icon}</span>
      {chip.label}
    </span>
  )
}

function MessageBubble({ msg }) {
  const isUser = msg.role === 'user'
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'} gap-3`}>
      {!isUser && (
        <div className="w-8 h-8 rounded-full bg-brand-600 flex items-center justify-center text-xs font-bold text-white shrink-0 mt-1">
          AI
        </div>
      )}
      <div className={`max-w-[80%] space-y-2`}>
        {/* Tool chips */}
        {msg.tools?.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {msg.tools.map((t, i) => <ToolChip key={i} tool={t} />)}
          </div>
        )}
        {/* Message content */}
        {msg.content && (
          <div className={`rounded-2xl px-4 py-3 text-sm leading-relaxed whitespace-pre-wrap ${
            isUser
              ? 'bg-brand-600 text-white rounded-tr-sm'
              : 'bg-navy-800 text-slate-200 border border-navy-700 rounded-tl-sm'
          }`}>
            {msg.content}
            {msg.streaming && (
              <span className="inline-block w-1.5 h-4 bg-brand-400 ml-1 animate-pulse align-middle" />
            )}
          </div>
        )}
      </div>
      {isUser && (
        <div className="w-8 h-8 rounded-full bg-slate-600 flex items-center justify-center text-xs font-bold text-white shrink-0 mt-1">
          U
        </div>
      )}
    </div>
  )
}

export default function Chat() {
  const [messages, setMessages]   = useState([])
  const [input, setInput]         = useState('')
  const [loading, setLoading]     = useState(false)
  const [activeTools, setActive]  = useState([])
  const bottomRef = useRef(null)
  const inputRef  = useRef(null)
  const sessionId = useRef(`session-${Date.now()}`)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, activeTools])

  const sendMessage = async (text) => {
    const msg = text.trim()
    if (!msg || loading) return

    setInput('')
    setLoading(true)
    setActive([])

    const history = messages.map(m => ({ role: m.role, content: m.content }))
    setMessages(prev => [...prev, { role: 'user', content: msg }])

    // placeholder for streaming AI reply
    const aiIdx = messages.length + 1
    setMessages(prev => [...prev, { role: 'assistant', content: '', tools: [], streaming: true }])

    try {
      const res = await fetch('/api/chat/message', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg, session_id: sessionId.current, history }),
      })

      if (!res.ok) throw new Error(`HTTP ${res.status}`)

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop()

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const raw = line.slice(6).trim()
          if (!raw) continue
          try {
            const event = JSON.parse(raw)
            if (event.type === 'token') {
              setMessages(prev => {
                const updated = [...prev]
                updated[updated.length - 1] = {
                  ...updated[updated.length - 1],
                  content: (updated[updated.length - 1].content || '') + event.text,
                }
                return updated
              })
            } else if (event.type === 'tool_start') {
              setActive(prev => [...prev, event.tool])
              setMessages(prev => {
                const updated = [...prev]
                const last = updated[updated.length - 1]
                updated[updated.length - 1] = {
                  ...last,
                  tools: [...(last.tools || []), event.tool],
                }
                return updated
              })
            } else if (event.type === 'tool_end') {
              setActive([])
            } else if (event.type === 'done') {
              setMessages(prev => {
                const updated = [...prev]
                updated[updated.length - 1] = {
                  ...updated[updated.length - 1],
                  streaming: false,
                }
                return updated
              })
            } else if (event.type === 'error') {
              setMessages(prev => {
                const updated = [...prev]
                updated[updated.length - 1] = {
                  ...updated[updated.length - 1],
                  content: `Error: ${event.text}`,
                  streaming: false,
                }
                return updated
              })
            }
          } catch {
            // ignore malformed SSE lines
          }
        }
      }
    } catch (err) {
      setMessages(prev => {
        const updated = [...prev]
        updated[updated.length - 1] = {
          ...updated[updated.length - 1],
          content: `Failed to connect to AI agent: ${err.message}`,
          streaming: false,
        }
        return updated
      })
    } finally {
      setLoading(false)
      setActive([])
      inputRef.current?.focus()
    }
  }

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage(input)
    }
  }

  return (
    <div className="flex flex-col h-full max-h-[calc(100vh-3rem)]">
      {/* Header */}
      <div className="flex items-center justify-between mb-4 shrink-0">
        <div>
          <h1 className="text-xl font-bold text-white">💬 Ask your data</h1>
          <p className="text-xs text-slate-400 mt-0.5">
            Powered by Claude · 7 live tools · SQL + Pipeline + Infra
          </p>
        </div>
        {messages.length > 0 && (
          <button
            onClick={() => setMessages([])}
            className="text-xs text-slate-500 hover:text-slate-300 px-3 py-1.5 rounded-lg border border-navy-700 hover:border-navy-600 transition-colors"
          >
            Clear chat
          </button>
        )}
      </div>

      {/* Messages or suggestions */}
      <div className="flex-1 overflow-y-auto space-y-4 pb-4 min-h-0">
        {messages.length === 0 ? (
          <div className="space-y-6">
            {/* Welcome */}
            <div className="bg-navy-800 rounded-2xl border border-navy-700 p-6 text-center">
              <div className="text-4xl mb-3">🏥</div>
              <h2 className="text-base font-semibold text-white mb-1">Healthcare Data Assistant</h2>
              <p className="text-sm text-slate-400">
                Ask anything about patients, revenue, doctors, pipeline status, or infrastructure.
                The AI can query your database and check live systems.
              </p>
            </div>

            {/* Tool capabilities */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              {Object.entries(TOOL_CHIPS).map(([key, { icon, label }]) => (
                <div key={key} className="bg-navy-800/60 border border-navy-700 rounded-xl p-3 text-center">
                  <div className="text-xl mb-1">{icon}</div>
                  <p className="text-xs text-slate-400">{label.replace('...', '')}</p>
                </div>
              ))}
            </div>

            {/* Suggestions */}
            <div>
              <p className="text-xs text-slate-500 uppercase tracking-wide mb-2">Try asking</p>
              <div className="flex flex-wrap gap-2">
                {SUGGESTIONS.map((s, i) => (
                  <button
                    key={i}
                    onClick={() => sendMessage(s)}
                    className="text-xs px-3 py-2 bg-navy-800 border border-navy-700 rounded-full text-slate-300 hover:text-white hover:border-brand-500 hover:bg-brand-600/10 transition-colors"
                  >
                    {s}
                  </button>
                ))}
              </div>
            </div>
          </div>
        ) : (
          messages.map((msg, i) => <MessageBubble key={i} msg={msg} />)
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="shrink-0 pt-3 border-t border-navy-700">
        <div className="flex gap-3 items-end">
          <textarea
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            disabled={loading}
            placeholder="Ask anything about your healthcare data..."
            rows={1}
            className="flex-1 bg-navy-800 border border-navy-700 rounded-xl px-4 py-3 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500 resize-none transition disabled:opacity-50"
            style={{ maxHeight: '120px', overflowY: 'auto' }}
          />
          <button
            onClick={() => sendMessage(input)}
            disabled={loading || !input.trim()}
            className="bg-brand-600 hover:bg-brand-500 disabled:opacity-40 disabled:cursor-not-allowed text-white px-4 py-3 rounded-xl text-sm font-medium transition-colors shrink-0"
          >
            {loading ? (
              <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
              </svg>
            )}
          </button>
        </div>
        <p className="text-xs text-slate-600 mt-2">Enter to send · Shift+Enter for new line</p>
      </div>
    </div>
  )
}
