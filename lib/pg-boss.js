import inject from 'seacreature/lib/inject'
import Boss from 'pg-boss'
import pg from 'pg'
const { Pool } = pg

inject('ctx', async ({ startup }) => {
  const release = startup.retain()
  const db = new Pool({
    host: process.env.BOSS_HOST,
    user: process.env.BOSS_USER,
    database: process.env.BOSS_DATABASE,
    password: process.env.BOSS_PASSWORD,
    port: process.env.BOSS_PORT
  })
  const boss = await new Boss({
    db: { executeSql: (...args) => db.query(...args) },
    retryLimit: Math.pow(2, 31) - 1,
    retryDelay: 5, // 5s
    retryBackoff: true,
    expireInMinutes: 60,
    application_name: process.env.BOSS_APPLICATION_NAME,
    schema: process.env.BOSS_SCHEMA
  })
    .on('error', (e) => console.error(e))
    .start()
  release()

  return { boss }
})
