import { config } from 'dotenv'
config()
import minio from 'minio'
import fs from 'fs'
import path from 'path'
import { constrainedMemory } from 'process'

// const minio = new Client({
//   endPoint: process.env.MINIO_URL,
//   port: parseInt(process.env.MINIO_PORT),
//   useSSL: process.env.MINIO_USE_SSL == 'true',
//   accessKey: process.env.MINIO_ACCESS_KEY,
//   secretKey: process.env.MINIO_SECRET_KEY
// })

const { S3_URL, S3_ACCESS_KEY, S3_SECRET_KEY, S3_PORT, S3_BUCKET, S3_REGION } = process.env

const s3 = new minio.Client({
  endPoint: S3_URL,
  port: parseInt(S3_PORT) || 443,
  useSSL: true,
  accessKey: S3_ACCESS_KEY,
  secretKey: S3_SECRET_KEY
})
const backblaze_bucket_name = S3_BUCKET

console.log(s3)

async function upload(file_path) {
  const date_directory = new Date().toISOString().slice(0, 13)
  const backup_name = file_path.split('/').slice(-1)[0]
  console.log(file_path)
  console.log(backup_name)
  try {
    const res = await s3.fPutObject(
      backblaze_bucket_name,
      process.env.SERVER_NAME.toLowerCase(),
      `${date_directory}/${backup_name}`,
      file_path
    )
    console.log(res)
  } catch (e) {
    console.log(e)
  }
}

const data_dir = 'data'
const file_names = fs.readdirSync(data_dir).filter(f => !f.toLowerCase().startsWith('read'))
const file_paths = file_names.map(f => path.join(data_dir, f))
console.log(file_paths)
// process.exit(0)

const big_file = 'data/srv-captain--postgres-db-mcgrath.sql.gz'
console.log(big_file)
// await upload(big_file)
// for (const file_path of file_paths) {
//     await upload(file_path)
// }

const fPutBigObject = async function (minio_client) {
  const res = await s3.fPutObject(
    'y5-whites-backup-test',
    'mcgrath/srv-captain--postgres-db-mcgrath.sql.gz',
    './data/srv-captain--postgres-db-mcgrath.sql.gz'
  )
  console.log(res)
}

const list_bucket_objects = function (bucket_name) {
  const objectsStream = s3.listObjects(bucket_name, '', true)
  objectsStream.on('data', function (obj) {
    console.log(obj)
  })
  objectsStream.on('error', function (e) {
    console.log(e)
  })
}

const try_create_bucket = async function (bucket_name) {
  await s3.makeBucket(bucket_name.toLowerCase(), S3_REGION)
  console.log(`minio bucket '${bucket_name.toLowerCase()}' created`)
}

const try_minio_bb = async function () {
  console.log('trying to connect')
  const buckets = await s3.listBuckets()
  console.log(buckets)

  //   console.log(await minio.listObjects('y5-whites-backup-test'))
}

// await try_minio_bb()
list_bucket_objects(S3_BUCKET)
// try_create_bucket('database-backups')
