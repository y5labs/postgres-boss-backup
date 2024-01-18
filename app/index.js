import inject from 'seacreature/lib/inject'
import pLimit from 'p-limit'
import { exec, execSync } from 'child_process'
import pg from 'pg'
import fsp from 'fs/promises'
import fs from 'fs'
import { format } from 'date-fns'
import zlib from 'zlib'
// import { create } from 'domain'
// import { fileURLToPath } from 'url'

const assert_dir = async dir => {
  try {
    await fsp.access(dir)
  } catch (err) {
    if (err.code === 'ENOENT') {
      await fsp.mkdir(dir, { recursive: true })
    } else {
      throw err
    }
  }
}

const { Pool, Client } = pg

const log = (...args) => console.log(new Date().toISOString(), ...args)
const error = (...args) => console.error(new Date().toISOString(), ...args)

const launch = (name, command, options = {}) =>
  new Promise((resolve, reject) => {
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
    p.stdout.on('data', msg => log(msg))
    p.stderr.on('data', msg => error(msg))
  })

const compress = (input_file, output_file) =>
  new Promise((resolve, reject) => {
    const read_stream = fs.createReadStream(input_file)
    const write_stream = fs.createWriteStream(output_file)
    const gzip = zlib.createGzip()

    read_stream.pipe(gzip).pipe(write_stream)

    write_stream.on('close', () => {
      resolve('file compressed successfully.')
    })

    write_stream.on('error', error => {
      reject(error)
    })
  })

const to_mins_and_seconds = async ms => {
  const minutes = Math.floor(ms / 60000)
  const seconds = ((ms % 60000) / 1000).toFixed(0)
  return minutes + ':' + (seconds < 10 ? '0' : '') + seconds
}

const format_string = async text => {
  const formatted = text.split(' ').map(t => t.charAt(0).toUpperCase() + t.slice(1))
  return formatted.join('')
}

const pgpass_filepath = '/root/.pgpass'

const create_pgpass = function () {
  // hostname:port:database:username:password
  // *:*:*:<DB_USER>:<DB_PASSWORD> we can use this format
  // .pgpass users home dir /root in our case

  if (fs.existsSync(pgpass_filepath)) {
    console.log(
      `.pgpass file exists at ${pgpass_filepath}. If you want to recreate use remove_pgpass then create_pgpass via telnet commands`
    )
    return
  }
  const [hostname, port, database] = ['*', '*', '*']
  const { DB_USER, DB_PASSWORD, DB_DATABASE } = process.env
  const content = `${hostname}:${port}:${database}:${DB_USER}:${DB_PASSWORD}`
  console.log(`Creating .pgpass file - ${content}`)
  try {
    const res = fs.writeFileSync(pgpass_filepath, content + '\n') // not we require the newline in the .pgpass file it seems
    fs.chmodSync(pgpass_filepath, fs.constants.S_IWUSR | fs.constants.S_IRUSR)
    console.log(res)
  } catch (err) {
    console.log(err)
  }
}

const remove_pgpass = function () {
  try {
    const res = fs.unlinkSync(pgpass_filepath)
    console.log(res)
  } catch (err) {
    console.log(err)
  }
}

const check_pgpass = function () {
  // hostname:port:database:username:password
  // .pgpass users home dir /root in our case (container /root)
  const exists = fs.existsSync(pgpass_filepath)
  if (!exists) {
    console.log(`No pgpass file at ${pgpass_filepath}`)
    return
  }
  console.log(`pgpass file exists at ${pgpass_filepath} - ${fs.readFileSync(pgpass_filepath).toString()}`)
}

inject('pod', async ({ boss, minio, discord }) => {
  const {
    BACKUP_DIRECTORY,
    BOSS_APPLICATION_NAME,
    BOSS_SCHEDULE,
    BOSS_RETRY_DELAY,
    BOSS_RETRY_LIMIT,
    BOSS_RETRY_BACKOFF,
    BOSS_EXPIRE_MINUTES,
    BOSS_TZ,
    CONTAINER_NAME,
    DATABASE_BLACKLIST,
    DATABASE_STRUCTURE_ONLY,
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_DATABASE,
    DB_PASSWORD,
    DISCORD_ICON,
    DUMP_LOGGING,
    SERVER_NAME,
    S3_URL,
    S3_BUCKET,
    S3_REGION,
    SAVE_UNCOMPRESSED_BACKUP
  } = process.env

  const job_prefix = 'postgres-backup'

  // check for s3 bucket existence
  const minio_bucket_check = async () => {
    const bucket_name = S3_BUCKET.toLowerCase()
    try {
      const buckets = await minio.listBuckets()
      console.log(buckets)
      const exists = buckets.find(b => b.name === bucket_name)
      if (!exists) {
        await minio.makeBucket(bucket_name, S3_REGION)
        console.log(`s3 bucket '${bucket_name}' created`)
      } else {
        console.log(`s3 bucket '${bucket_name}' already exists`)
      }
    } catch (err) {
      console.log(`something went wrong verifying/creating a s3 bucket for '${bucket_name}'`)
      console.log(err.message)
      return false
    }
    return true
  }

  const s3_bucket_ok = await minio_bucket_check()

  create_pgpass()

  const postgres_backup = async name => {
    try {
      console.log(`queueing backup '${name}'`)
      const tasks = []
      const add_task = c =>
        tasks.push(async () => {
          const verbose = Number(DUMP_LOGGING) ? '-v' : ''
          const structure_only = Number(DATABASE_STRUCTURE_ONLY) ? '-s' : ''
          const blacklist = DATABASE_BLACKLIST ? DATABASE_BLACKLIST.split(',') : []
          const exclusions = blacklist.length ? blacklist.map(db => `--exclude-database=${db}`).join(' ') : ''

          const cmd = `pg_dumpall -h ${DB_HOST} -p ${DB_PORT} ${verbose} ${structure_only} -U ${DB_USER} --file=${c}.sql ${exclusions}`
          const dir = `${process.env.BK_DIR || `${process.cwd()}/data`}`
          console.log('cmd', cmd)
          console.log('directory', dir)
          await assert_dir(dir)
          try {
            // Create the backup sql file
            await launch(`${c}`, cmd, {
              cwd: dir,
              env: {
                PATH: process.env.PATH
              }
            })
            // ---------------------------------------------------------------
            // Fetch the database names from the server hosting the current db
            // We will insert the create database statements at the head of the
            // backup sql file
            // ---------------------------------------------------------------
            console.log('Fetching database names')
            const db_container_name = c
            const databases_output_file = `${dir}/${db_container_name}_databases.txt`
            const databases_cmd_text = 'SELECT datname FROM pg_database WHERE datistemplate = false;'
            const get_databases_cmd = `PGPASSWORD=${DB_PASSWORD} psql -h ${DB_HOST} -p ${DB_PORT} --username ${DB_USER} -d ${DB_DATABASE} -o ${databases_output_file} -c "${databases_cmd_text}"`
            await launch(`${c}`, get_databases_cmd, {
              cwd: dir,
              env: {
                PATH: process.env.PATH
              }
            })

            // ---------------------------------------------------------------
            // Read back the db names from output file, build CREATE db statement
            // lines, insert the statement lines at the head of the master
            // backup/restore script
            // ---------------------------------------------------------------

            console.log('Building create database statements')
            const db_names_content = fs.readFileSync(databases_output_file).toString().trim()
            const db_names = db_names_content.split('\n').slice(2, -1) // ignore first 2 lines of output (col name and sperator lines) and also last line ( row count)
            const create_dbs_text = db_names.map(n => `CREATE DATABASE ${n.trim()};`).join('\n') + '\n'

            const pgdump_filepath = `${dir}/${c}.sql`

            console.log(
              `Inserting create statements at the head of the temp backup file: ${pgdump_filepath} \n${create_dbs_text}`
            )

            const merge_create_statements = async function () {
              // wrap stream read writes in a promise so we can wait on stream to complete
              const p = new Promise((resolve, reject) => {
                // rename orig pgdump to the side
                execSync(`mv ${pgdump_filepath} ${pgdump_filepath}.orig`)
                // write create dbs text to output file - only has create statements at this point
                fs.writeFileSync(pgdump_filepath, create_dbs_text + '\n')
                // get a write stream on the output file
                const output_stream = fs.createWriteStream(pgdump_filepath)
                // a read stream on the orig backup sql
                const pgdump_stream = fs.createReadStream(`${pgdump_filepath}.orig`) // read from original pg dump

                // wait till output streeam closed so we can resolve and free up this func execution
                output_stream.on('close', () => {
                  console.log('output stream closed')
                  resolve('Content merged') // reolve this promise
                })
                // append pgdump to the create dbs
                pgdump_stream.pipe(output_stream)
              })
              return p
            }

            const res = await merge_create_statements()
            console.log(res)
          } catch (e) {
            error(cmd, e)
            throw e
          }
        })

      const container = CONTAINER_NAME || 'postgres-container'
      add_task(container)

      console.log('begin back up process')
      const limit = pLimit(10)
      await Promise.all(tasks.map(t => limit(t)))

      console.log('backup complete')
    } catch (e) {
      console.error(`error encountered during backup - ${e}`)
      throw e
    }
  }

  const compress_backup = async file_name => {
    try {
      console.log(`compressing back up: ${file_name}`)

      const read_file = `${file_name}.sql`
      const write_file = `${file_name}.sql.gz`

      await compress(`./data/${read_file}`, `./data/${write_file}`)
      console.log('compressed backup')
    } catch (err) {
      console.error(`something went wrong compressing the backup '${SERVER_NAME.toLowerCase()}'`)
      console.error(err.message)
      throw err
    }
  }

  const minio_write = async file_name => {
    try {
      console.log('write back ups to s3 bucket')

      const uncompressed_backup_filepath = `./data/${file_name}.sql`
      const uncompressed_backup_name = `${file_name}.sql`

      const compressed_backup_filepath = `./data/${file_name}.sql.gz`
      const compressed_backup_name = `${file_name}.sql.gz`

      const date_directory = format(new Date(), 'yyyy-MM-dd')

      const missing = [uncompressed_backup_filepath, compressed_backup_filepath].filter(p => !fs.existsSync(p))
      if (missing.length) {
        console.error(`Unable to locate all backup files. Missing ${missing}.`)
        throw new Error(`Unable to locate all backup files. Missing ${missing}.`)
      }

      // --------------------------------------------------
      // Bucket object naming - note the object path prefix
      // doesnt include the bucket name - thats later
      // --------------------------------------------------
      const backblaze_target = S3_URL.includes('backblaze')
      const s3_object_path_prefix = backblaze_target ? `database/${DB_DATABASE}/${date_directory}` : `${date_directory}`
      const uncompressed_object_path = `${s3_object_path_prefix}/${uncompressed_backup_name}`
      const compressed_object_path = `${s3_object_path_prefix}/${compressed_backup_name}`
      const write_uncompressed_to_s3 = SAVE_UNCOMPRESSED_BACKUP.toLowerCase() == 'true'

      if (write_uncompressed_to_s3) {
        console.log(
          `writing uncompressed backup file to s3 bucket: ${uncompressed_backup_filepath} -> ${S3_BUCKET}/${uncompressed_object_path}`
        )
        await minio.fPutObject(S3_BUCKET.toLowerCase(), uncompressed_object_path, uncompressed_backup_filepath)
      }

      console.log(
        `writing compressed backup file to s3 bucket: ${compressed_backup_filepath} -> ${S3_BUCKET}/${compressed_object_path}`
      )
      await minio.fPutObject(S3_BUCKET, compressed_object_path, compressed_backup_filepath)
      console.log('written back ups to s3 bucket')

      // keep files on the server until disk space is an issue
      // fs.unlinkSync(backup_path)
      // console.log(`postgres backup '${backup_path}' deleted`)
    } catch (err) {
      console.log(`something went wrong writing a backup to s3 bucket '${S3_BUCKET.toLowerCase()}'`)
      console.log(err.message)
    }
  }

  const SCHEDULE = BOSS_SCHEDULE || '0 0 * * *'
  await boss.schedule(`${job_prefix}.${SERVER_NAME}.${CONTAINER_NAME}`, SCHEDULE, null, {
    singletonKey: `${BOSS_APPLICATION_NAME}.${SERVER_NAME.toLowerCase()}`,
    retryLimit: Number(BOSS_RETRY_LIMIT || 5),
    retryDelay: Number(BOSS_RETRY_DELAY || 300),
    retryBackoff: Boolean(BOSS_RETRY_BACKOFF || false),
    expireInMinutes: Number(BOSS_EXPIRE_MINUTES || 5),
    tz: BOSS_TZ || 'UTC'
  })

  await boss.work(`${job_prefix}.${SERVER_NAME}.${CONTAINER_NAME}`, async job => {
    const job_entry = await boss.getJobById(job.id)
    const formatted_name = await format_string(SERVER_NAME.toLowerCase())

    try {
      const start = Date.now()
      const backup_start = Date.now()
      await postgres_backup(process.env.SERVER_NAME)
      const backup_end = Date.now()
      const compression_start = Date.now()
      await compress_backup(CONTAINER_NAME)
      const compression_end = Date.now()
      const minio_start = Date.now()
      await minio_write(CONTAINER_NAME)
      const minio_end = Date.now()

      const backup = fs.statSync(`./data/${CONTAINER_NAME}.sql`)
      const backup_compressed = fs.statSync(`./data/${CONTAINER_NAME}.sql.gz`)

      const size = {
        backup: (backup.size / 1024 / 1024).toFixed(0),
        compressed: (backup_compressed.size / 1024 / 1024).toFixed(0)
      }

      const end = Date.now()

      const timing = {
        backup: ((backup_end - backup_start) / 1000).toFixed(0),
        compression: ((compression_end - compression_start) / 1000).toFixed(0),
        minio: ((minio_end - minio_start) / 1000).toFixed(0),
        total: ((end - start) / 1000).toFixed(0)
      }

      let timing_output = ''
      for (const [key, value] of Object.entries(timing)) {
        const formatted_key = await format_string(key)
        timing_output += `${formatted_key}: ${value} secs\n`
      }

      let size_output = ''
      for (const [key, value] of Object.entries(size)) {
        const formatted_key = await format_string(key)
        size_output += `${formatted_key}: ${value} MB\n`
      }

      await discord.notification(
        `✅ Postgres Backup → Databse backup for '${DISCORD_ICON} ${formatted_name} - ${CONTAINER_NAME}' completed successfully.`,
        [
          {
            title: `Backup completed successfully in ${timing.total} secs`,
            color: 65280,
            timestamp: new Date(),
            fields: [
              {
                name: 'Timing',
                value: timing_output
              },
              {
                name: 'Size',
                value: size_output
              }
            ]
          }
        ]
      )
      await job.done()
    } catch (e) {
      console.error(`unable to perform postgres backup for '${SERVER_NAME.toLowerCase()}'`)
      if (job_entry.retrycount < 3) {
        await discord.notification(
          `:warning: Postgres Backup → Unable to perform a database backup for '${DISCORD_ICON} ${formatted_name} - ${CONTAINER_NAME}', retrying...`,
          [
            {
              title: 'An error has occured while trying to back up the database.',
              color: 16711680,
              timestamp: new Date(),
              fields: [e]
            }
          ]
        )
      }
      console.error(e)
      await job.done(e)
    }
  })

  inject('command.now', async ({ boss }) => {
    await boss.send(`${job_prefix}.${SERVER_NAME}.${CONTAINER_NAME}`)
  })

  inject('command.create_pgpass', async () => {
    create_pgpass()
  })
  inject('command.check_pgpass', async () => {
    check_pgpass()
  })
  inject('command.remove_pgpass', async () => {
    remove_pgpass()
  })
  inject('command.create_bucket', async () => {
    await minio_bucket_check()
  })
  inject('command.s3_upload', async () => {
    await minio_bucket_check()
    await minio_write()
  })
})
