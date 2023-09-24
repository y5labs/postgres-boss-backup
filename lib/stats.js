import inject from 'seacreature/lib/inject.js'
import { Stats } from 'fast-stats'

inject('ctx', () => {
  const stats = new Map()

  const rec = (name, ms) => {
    if (!stats.has(name)) {
      console.log(`${name} recording stats`)
      stats.set(name, {
        s: new Stats({
          bucket_precision: 50,
          store_data: false
        }),
        c: 0
      })
    }
    const s = stats.get(name)
    s.s.push(ms)
    s.c++
  }

  const reset = () => {
    for (const s of stats.values()) {
      s.s.reset()
      s.c = 0
    }
  }

  const print = name => {
    if (!stats.has(name)) return
    const { s, c } = stats.get(name)
    if (c == 0) return
    const p = n => s.percentile(n).toFixed(0).padStart(5)
    console.log(
      `${new Date().toISOString()} ${name.padStart(32).substring(0, 32)} ${c.toString().padStart(5)}∑ ${p(50)}×50 ${p(
        95
      )}×95 ${p(99)}×99`
    )
  }

  const print_all = () => {
    for (const name of stats.keys()) print(name)
  }

  const ms = () => new Date().getTime()

  setInterval(print_all, 3e5) // 5 min
  setInterval(reset, 3.6e6) // 1 hour
  // setInterval(() => { rec('task_update.query', [Math.random() * 1000]) }, 10)

  return {
    stats: { rec, reset, print, print_all, ms }
  }
})