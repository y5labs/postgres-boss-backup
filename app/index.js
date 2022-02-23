import inject from 'seacreature/lib/inject'
import pLimit from 'p-limit'
import { exec } from 'child_process'
import pg from 'pg'
import fs from 'fs/promises'

const assert_dir = async dir => {
  try {
    await fs.access(dir)
  } catch (err) {
    if (err.code === 'ENOENT') {
      await fs.mkdir(dir, { recursive: true })
    } else {
      throw err
    }
  }
}

const { Pool, Client } = pg

const log = (...args) => console.log(new Date().toISOString(), ...args)
const error = (...args) => console.error(new Date().toISOString(), ...args)

const launch = (name, command, options = {}) => new Promise((resolve, reject) => {
  let is_complete = false
  const complete_resolve = (...args) => {
    if (is_complete) return
    is_complete = true
    // log(...args)
    resolve()
  }
  const complete_reject = (...args) => {
    if (is_complete) return
    is_complete = true
    error(...args)
    reject(args[1])
  }
  const p = exec(command, { shell: '/bin/sh', ...options })
  // p.on('spawn', () =>
  //   log(`${name} spawned`))
  p.on('error', async e => {
    complete_reject(`${name} errored`, e)
  })
  p.on('close', async code => {
    complete_resolve(`${name} closed`, code)
  })
  p.on('exit', async code => {
    complete_resolve(`${name} exited`, code)
  })
  // p.stdout.on('data', msg => log(msg))
  // p.stderr.on('data', msg => error(msg))
})

const db_options = name => ({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  database: name || process.env.DB_DATABASE,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT
})

inject('pod', async ({ boss }) => {
  const name = process.env.QUEUE || 'postgres-boss-backup'
  const db_host = `postgresql://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${process.env.DB_HOST}`
  const pgdb = new Pool(db_options())
  pgdb.on('error', error)

  inject('command.now', async ({ boss }) => {
    await boss.send(name)
  })

  await boss.work(name, async () => {
    try {
      log('Discovering databases')
      const { rows: dats } = await pgdb.query('select datname from pg_database where datistemplate = false')
      const databases = dats.map(d => d.datname).filter(d => d == 'whites_link')
      const tasks = []
      const add_task = (d, t) => tasks.push(async () => {
        const cmd = `pg_dump --compress=0 --format=plain --file=${t}.sql --table=${t} ${db_host}/${d}`
        const dir = `${process.env.BK_DIR || `${process.cwd()}/data`}/${d}`
        await assert_dir(dir)
        try {
          await launch(`${d}.${t}`, cmd, {
            cwd: dir,
            env: {
              PATH: process.env.PATH
            }
          })
        }
        catch (e) {
          error(cmd, e)
          throw e
        }
      })
      log('Discovering database tables')
      for (const d of databases) {
        const db = new Client(db_options(d))
        db.on('error', e => console.error(e))
        db.connect()
        const { rows: tab } = await db.query(`
          select concat(table_schema , '.', table_name) as t
          from information_schema.tables
          where table_type = 'BASE TABLE'`)
        db.end()
        const tables = tab.map(t => t.t)
        for (const t of tables) add_task(d, t)
      }

      log('Backing up tables')
      const limit = pLimit(10)
      await Promise.all(tasks.map(t => limit(t)))
      log('Backup complete')
    } catch(e) {
      error(`Error encountered during backup - ${e}`)
      throw e
    }
  })

  await boss.schedule(name, process.env.SCHEDULE, null, {
    retryLimit: Number(process.env.RETRY_LIMIT || 0),
    retryDelay: Number(process.env.RETRY_DELAY || 300),
    retryBackoff: Boolean(process.env.RETRY_BACKOFF || false),
    expireInMinutes: Number(process.env.EXPIRE_MINUTES || 180),
    tz: process.env.TZ || 'UTC'
  })

  setTimeout(() => {
    boss.send(name)
  }, 1000)
})
