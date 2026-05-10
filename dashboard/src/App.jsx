import { useState, useEffect, useRef } from 'react'

function Sparkline({ history, color }) {
  if (!history || history.length < 2) return null
  const width = 80
  const height = 24
  const max = Math.max(...history)
  const min = Math.min(...history)
  const range = max - min || 0.01
  const points = history.map((v, i) => {
    const x = (i / (history.length - 1)) * width
    const y = height - ((v - min) / range) * height
    return `${x},${y}`
  }).join(' ')
  return (
    <svg width={width} height={height}>
      <polyline points={points} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  )
}

function DriverCard({ driver, history }) {
  return (
    <div style={{ marginBottom: '10px', background: '#111', padding: '14px 16px', borderRadius: '10px', borderLeft: `5px solid ${driver.team_color}` }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <span style={{ color: '#666', fontSize: '13px', width: '28px' }}>P{driver.position}</span>
          <span style={{ fontWeight: 'bold', fontSize: '16px', color: '#ffffff' }}>{driver.driver_name}</span>
          <span style={{ fontSize: '11px', color: '#888', background: '#1a1a1a', padding: '2px 8px', borderRadius: '4px' }}>
            Lap {Math.round(driver.race_completion_pct * 60)}
          </span>
          <span style={{ fontSize: '11px', color: '#888' }}>🔄 {driver.pit_stop_count}</span>
          <span style={{ fontSize: '11px', color: '#888' }}>🏎 {driver.tire_age}</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          <Sparkline history={history} color={driver.team_color} />
          <span style={{ fontWeight: 'bold', fontSize: '18px', color: '#ffffff', width: '56px', textAlign: 'right' }}>
            {(driver.win_probability * 100).toFixed(1)}%
          </span>
        </div>
      </div>
      <div style={{ background: '#222', borderRadius: '4px', height: '5px' }}>
        <div style={{ background: driver.team_color, width: `${Math.min(driver.win_probability * 200, 100)}%`, height: '5px', borderRadius: '4px', transition: 'width 0.8s ease' }} />
      </div>
    </div>
  )
}

function App() {
  const [predictions, setPredictions] = useState([])
  const [timestamp, setTimestamp] = useState(null)
  const [connected, setConnected] = useState(false)
  const historyRef = useRef({})

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws/predictions')
    ws.onopen = () => setConnected(true)
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data)
      data.predictions.forEach(driver => {
        const key = driver.driver_number
        if (!historyRef.current[key]) historyRef.current[key] = []
        historyRef.current[key].push(driver.win_probability)
        if (historyRef.current[key].length > 30) historyRef.current[key].shift()
      })
      setPredictions(data.predictions)
      setTimestamp(data.timestamp)
    }
    ws.onclose = () => setConnected(false)
    return () => ws.close()
  }, [])

  const leader = predictions[0]

  return (
    <div style={{ background: '#0a0a0a', minHeight: '100vh', padding: '32px', fontFamily: 'Inter, system-ui, sans-serif' }}>
      <div style={{ maxWidth: '960px', margin: '0 auto' }}>

        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '32px' }}>
          <div>
            <h1 style={{ fontSize: '26px', fontWeight: '700', color: '#ffffff', margin: '0', letterSpacing: '-0.5px' }}>
              🏁 F1 Win Probability
            </h1>
            <p style={{ color: '#555', fontSize: '13px', margin: '4px 0 0' }}>
              {timestamp ? new Date(timestamp).toLocaleTimeString() : 'Waiting for data...'}
            </p>
          </div>
          <div style={{ color: connected ? '#22c55e' : '#ef4444', fontSize: '13px', background: connected ? '#052e16' : '#2d0707', padding: '6px 16px', borderRadius: '20px', fontWeight: '600' }}>
            {connected ? '● LIVE' : '○ DISCONNECTED'}
          </div>
        </div>

        {/* Leader spotlight */}
        {leader && (
          <div style={{ background: '#111', border: `1px solid ${leader.team_color}44`, borderRadius: '12px', padding: '24px 28px', marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <p style={{ color: '#555', fontSize: '11px', fontWeight: '600', letterSpacing: '1px', margin: '0 0 6px' }}>RACE LEADER</p>
              <h2 style={{ fontSize: '36px', fontWeight: '700', color: '#ffffff', margin: '0' }}>{leader.driver_name}</h2>
              <p style={{ color: '#666', fontSize: '13px', margin: '6px 0 0' }}>
                {Math.round(leader.race_completion_pct * 100)}% race complete · P{leader.position}
              </p>
            </div>
            <div style={{ textAlign: 'right' }}>
              <p style={{ color: '#555', fontSize: '11px', fontWeight: '600', letterSpacing: '1px', margin: '0 0 6px' }}>WIN PROBABILITY</p>
              <h2 style={{ fontSize: '52px', fontWeight: '700', color: '#ffffff', margin: '0', lineHeight: 1 }}>
                {(leader.win_probability * 100).toFixed(1)}%
              </h2>
              <div style={{ height: '3px', background: leader.team_color, borderRadius: '2px', marginTop: '10px' }} />
            </div>
          </div>
        )}

        {/* Driver list */}
        <div>
          {predictions.map((driver) => (
            <DriverCard
              key={driver.driver_number}
              driver={driver}
              history={historyRef.current[driver.driver_number] || []}
            />
          ))}
        </div>

      </div>
    </div>
  )
}

export default App