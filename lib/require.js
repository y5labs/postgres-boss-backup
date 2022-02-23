import inject from 'seacreature/lib/inject'
import path from 'path'

inject('command.require', async ({ args, log }) => {
  if (!args.length > 0) return
  try {
    const filename = path.resolve(process.cwd(), args[0])
    delete require.cache[require.resolve(filename)]
    require(filename)
  }
  catch (e) {
    await log.error(e)
  }
})
