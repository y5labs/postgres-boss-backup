import inject from 'seacreature/lib/inject'
import { Client } from 'minio'

// note we are using the S3_ settings prefix
// to indicate that minio/backblaze are
// just s3 compatable client and stores and
// can be swapped out
inject('ctx', async () => {
  const minio = new Client({
    endPoint: process.env.S3_URL,
    port: parseInt(process.env.S3_PORT),
    useSSL: process.env.S3_USE_SSL == 'true',
    accessKey: process.env.S3_ACCESS_KEY,
    secretKey: process.env.S3_SECRET_KEY
  })
  return { minio }
})
