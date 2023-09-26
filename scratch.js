import { config } from 'dotenv'
config()
import { Client } from 'minio'
import fs from 'fs'
import path from 'path'
import { constrainedMemory } from 'process'


const minio = new Client({
    endPoint: process.env.MINIO_URL,
    port: parseInt(process.env.MINIO_PORT),
    useSSL: process.env.MINIO_USE_SSL == 'true',
    accessKey: process.env.MINIO_ACCESS_KEY,
    secretKey: process.env.MINIO_SECRET_KEY
})
console.log(minio)


async function upload(file_path) {
    const date_directory = new Date().toISOString().slice(0, 13)
    const backup_name = file_path.split("/").slice(-1)[0]
    console.log(file_path)
    console.log(backup_name)
    try {
        const res = await minio.fPutObject(process.env.SERVER_NAME.toLowerCase(), `${date_directory}/${backup_name}`,file_path )
        console.log(res)
    }
    catch (e) {
        console.log(e)
    }
}


const data_dir = 'data'
const file_names = fs.readdirSync(data_dir).filter(f => !f.toLowerCase().startsWith("read"));
const file_paths = file_names.map(f => path.join(data_dir, f))
console.log(file_paths)
// process.exit(0)

const big_file = 'data/srv-captain--postgres-db-mcgrath.sql'
await upload(big_file)
// for (const file_path of file_paths) {
//     await upload(file_path)
// }

