import { useState, useEffect } from 'react'

function DriverCard({ driver }) {
  return (
    <div style={{
      marginBottom: '10px',
      background: '#111',
      padding: '14px 16px',
      borderRadius: '10px',
      borderLeft: `5px solid ${driver.team_color}`,
      transition: 'all 0.5s ease'
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <span style={{ color: '#666', fontSize: '13px', width: '24px' }}>P{driver.position}</span>
          <span style={{ fontWeight: 'bold', fontSize: '16px' }}>{driver.driver_name}</span>
          <span style={{ fontSize: '11px', color: '#888', background: '#222', padding: '2px 8px', borderRadius: '4px' }}>
            Lap {Math.round(driver.race_completion_pct * 60)}
          </span>
          <span style={{ fontSize: '11px', color: '#888' }}>🔄 {driver.pit_stop_count} stops</span>
          <span style={{ fontSize: '11px', color: '#888' }}>🏎 {driver.tire_age} laps</span>
        </div>
        <span style={{ fontWeight: 'bold', fontSize: '18px', color: '#ffffff' }}>
          {(driver.win_probability * 100).toFixed(1)}%
        </span>
      </div>
      <div style={{ background: '#222', borderRadius: '4px', height: '6px' }}>
        <div style={{
          background: driver.team_color,
          width: `${Math.min(driver.win_probability * 200, 100)}%`,
          height: '6px',
          borderRadius: '4px',
          transition: 'width 0.8s ease'
        }} />
      </div>
    </div>
  )
}

function App() {
  const [predictions, setPredictions] = useState([])
  const [timestamp, setTimestamp] = useState(null)
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws/predictions')
    ws.onopen = () => setConnected(true)
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data)
      setPredictions(data.predictions)
      setTimestamp(data.timestamp)
    }
    ws.onclose = () => setConnected(false)
    return () => ws.close()
  }, [])

  const leader = predictions[0]

  return (
    <div style={{ background: '#0a0a0a', minHeight: '100vh', color: 'white', padding: '32px', fontFamily: 'Inter, sans-serif' }}>
      <div style={{ maxWidth: '900px', margin: '0 auto' }}>

        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '32px' }}>
          <div>
            <h1 style={{ fontSize: '28px', fontWeight: 'bold', margin: 0 }}>🏁 F1 Win Probability</h1>
            <p style={{ color: '#666', fontSize: '13px', margin: '4px 0 0' }}>
              {timestamp ? new Date(timestamp).toLocaleTimeString() : 'Waiting...'}
            </p>
          </div>
          <span style={{
            color: connected ? '#22c55e' : '#ef4444',
            fontSize: '13px',
            background: connected ? '#052e16' : '#2d0707',
            padding: '6px 14px',
            borderRadius: '20px'
          }}>
            {connected ? '● LIVE' : '○ DISCONNECTED'}
          </span>
        </div>

        {/* Leader spotlight */}
        {leader && (
          <div style={{
            background: '#111',
            border: `1px solid ${leader.team_color}`,
            borderRadius: '12px',
            padding: '20px',
            marginBottom: '24px',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center'
          }}>
            <div>
              <p style={{ color: '#666', fontSize: '12px', margin: '0 0 4px' }}>RACE LEADER</p>
              <h2 style={{ fontSize: '32px', fontWeight: 'bold', margin: '0', color: '#ffffff' }}>
                {leader.driver_name}
              </h2>
              <p style={{ color: '#888', margin: '4px 0 0', fontSize: '13px' }}>
                {Math.round(leader.race_completion_pct * 100)}% race complete
              </p>
            </div>
            <div style={{ textAlign: 'right' }}>
              <p style={{ color: '#666', fontSize: '12px', margin: '0 0 4px' }}>WIN PROBABILITY</p>
              <h2 style={{ fontSize: '48px', fontWeight: 'bold', margin: 0, color: '#ffffff' }}>
                {(leader.win_probability * 100).toFixed(1)}%
              </h2>
              <div style={{ height: '4px', background: leader.team_color, borderRadius: '2px', marginTop: '8px' }} />
            </div>
          </div>
        )}

        {/* Driver list */}
        <div>
          {predictions.map((driver) => (
            <DriverCard key={driver.driver_number} driver={driver} />
          ))}
        </div>

      </div>
    </div>
  )
}

export default App