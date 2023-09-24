import inject from 'seacreature/lib/inject'
import pjson from '../package.json' assert { type: 'json' }

inject('ctx', async () => {
  const { DISCORD_CHANNEL, DISCORD_TOKEN, DISCORD_NOTIFY } = process.env

  const notification = async (content, embeds = []) => {
    if(!Number(DISCORD_NOTIFY)) return

    try {
      await fetch(`https://discord.com/api/v9/channels/${DISCORD_CHANNEL}/messages`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bot ${DISCORD_TOKEN}`,
          'User-Agent': `Y5 DevOps (https://y5.nz, ${pjson.version})`
        },
        body: JSON.stringify({ content, embeds })
      })

      return
    } catch (e) {
      console.error(e)
    }
  }

  return {
    discord: {
      notification
    }
  }
})
