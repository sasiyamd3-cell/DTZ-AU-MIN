let downloadMediaMessage, getContentType;
try {
  ({ downloadMediaMessage, getContentType } = require('baileys'));
} catch (e) {}
if (!downloadMediaMessage) {
  try { ({ downloadMediaMessage } = require('./msg')); } catch (e) {}
}
if (!getContentType) {
  getContentType = (message) => Object.keys(message || {})[0];
}

let moment;
try { moment = require('moment-timezone'); } catch (e) {}

function getSriLankaTimestamp() {
  if (moment) return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss');
  return new Date().toLocaleString('en-GB', { timeZone: 'Asia/Colombo' }).replace(',', '');
}

function resolveJid(rawJid, altJid) {
  return (rawJid && rawJid.endsWith('@lid') && altJid) ? altJid : rawJid;
}

function resolveWhoFromKey(key) {
  if (!key) return '';
  if (key.participant) return resolveJid(key.participant, key.participantAlt) || '';
  return resolveJid(key.remoteJid, key.remoteJidAlt) || '';
}

const messageCache = new Map();
const CACHE_TTL_MS = 3 * 60 * 60 * 1000;
const CLEANUP_INTERVAL_MS = 30 * 60 * 1000;

setInterval(() => {
  const now = Date.now();
  for (const [key, val] of messageCache.entries()) {
    if (now - val.timestamp > CACHE_TTL_MS) messageCache.delete(key);
  }
}, CLEANUP_INTERVAL_MS).unref?.();

function cacheKey(remoteJid, id) {
  return `${remoteJid}::${id}`;
}

function extractText(message) {
  if (!message) return '';
  return message.conversation
    || message.extendedTextMessage?.text
    || message.imageMessage?.caption
    || message.videoMessage?.caption
    || '';
}

function buildCuteCaption(title, body, botName) {
  return `🌸✨ *${botName}* ✨🌸\n` +
    `━━━━◇ 𝗗𝗘𝗟𝗘𝗧𝗘𝗗 ${title} ◇━━━━\n\n` +
    `${body}\n\n` +
    `┊ ┊ ┊ ┊ ┊ 🌷\n` +
    `┊ ┊ ✧ ˚♡ ⋆｡\n` +
    `┊ ☾ ⋆ 🦋\n` +
    `✿ 𝑫𝒓𝒆𝒂𝒎 • 𝑪𝒓𝒆𝒂𝒕𝒆 • 𝑰𝒏𝒔𝒑𝒊𝒓𝒆 ✿\n` +
    `━━━━━━━━━━━━━━━`;
}

function buildChannelContext(NEWSLETTER_CONTEXT, botName) {
  const newsletterJid = NEWSLETTER_CONTEXT?.forwardedNewsletterMessageInfo?.newsletterJid;
  if (!newsletterJid) return undefined;
  return {
    forwardingScore: 1000,
    isForwarded: true,
    forwardedNewsletterMessageInfo: {
      newsletterJid,
      newsletterName: botName,
      // Must be unique per message. A hardcoded id (e.g. 999) makes WhatsApp's
      // client treat every message that shares this newsletterJid+id as the
      // same forwarded "channel post", visually threading/quoting unrelated
      // messages together (e.g. an antidelete recovery card appearing quoted
      // under an earlier .fb/.song command reply that used the same id).
      serverMessageId: Math.floor(100000 + Math.random() * 900000)
    }
  };
}

async function sendRecovered(socket, target, cached, header, botName, channelContext) {
  const message = cached.message;
  const fakeMsg = { key: cached.key, message };
  const type = getContentType(message) || Object.keys(message || {})[0];
  const dlCtx = { logger: socket.logger || console, reuploadRequest: socket.updateMediaMessage };

  async function download() {
    return downloadMediaMessage(fakeMsg, 'buffer', {}, dlCtx);
  }

  try {
    if (type === 'conversation' || type === 'extendedTextMessage') {
      const text = message.conversation || message.extendedTextMessage?.text || '';
      await socket.sendMessage(target, {
        text: buildCuteCaption('𝗠𝗦𝗚 𝗥𝗘𝗖𝗢𝗩𝗘𝗥𝗬', `${header}\n\n💬 *Recovered Message:*\n\`\`\`${text}\`\`\``, botName),
        contextInfo: channelContext
      });

    } else if (type === 'imageMessage') {
      const buffer = await download();
      const capLine = message.imageMessage?.caption ? `\n\n💬 *Caption:*\n${message.imageMessage.caption}` : '';
      await socket.sendMessage(target, {
        image: buffer,
        caption: buildCuteCaption('𝗜𝗠𝗔𝗚𝗘 𝗥𝗘𝗖𝗢𝗩𝗘𝗥𝗬', `${header}${capLine}`, botName),
        contextInfo: channelContext
      });

    } else if (type === 'videoMessage') {
      const buffer = await download();
      const capLine = message.videoMessage?.caption ? `\n\n💬 *Caption:*\n${message.videoMessage.caption}` : '';
      const isPtv = !!message.videoMessage?.ptv;
      await socket.sendMessage(target, {
        video: buffer,
        ptv: isPtv,
        caption: buildCuteCaption(isPtv ? '𝗩𝗜𝗗𝗘𝗢 𝗡𝗢𝗧𝗘 𝗥𝗘𝗖𝗢𝗩𝗘𝗥𝗬' : '𝗩𝗜𝗗𝗘𝗢 𝗥𝗘𝗖𝗢𝗩𝗘𝗥𝗬', `${header}${capLine}`, botName),
        contextInfo: channelContext
      });

    } else if (type === 'stickerMessage' || type === 'lottieStickerMessage') {
      try {
        const buffer = await download();
        await socket.sendMessage(target, { sticker: buffer, contextInfo: channelContext });
        await socket.sendMessage(target, {
          text: buildCuteCaption('𝗦𝗧𝗜𝗖𝗞𝗘𝗥 𝗥𝗘𝗖𝗢𝗩𝗘𝗥𝗬', header, botName),
          contextInfo: channelContext
        });
      } catch (dlErr) {
        await socket.sendMessage(target, {
          text: buildCuteCaption('𝗦𝗧𝗜𝗖𝗞𝗘𝗥 𝗗𝗘𝗟𝗘𝗧𝗘𝗗', `${header}\n\n⚠️ This animated sticker couldn't be re-downloaded for recovery.`, botName),
          contextInfo: channelContext
        });
      }

    } else if (type === 'audioMessage') {
      const buffer = await download();
      await socket.sendMessage(target, {
        audio: buffer,
        mimetype: message.audioMessage?.mimetype || 'audio/mp4',
        ptt: !!message.audioMessage?.ptt,
        contextInfo: channelContext
      });
      await socket.sendMessage(target, {
        text: buildCuteCaption('𝗩𝗢𝗜𝗖𝗘/𝗔𝗨𝗗𝗜𝗢 𝗥𝗘𝗖𝗢𝗩𝗘𝗥𝗬', header, botName),
        contextInfo: channelContext
      });

    } else if (type === 'documentMessage') {
      const buffer = await download();
      await socket.sendMessage(target, {
        document: buffer,
        mimetype: message.documentMessage?.mimetype,
        fileName: message.documentMessage?.fileName || 'file',
        caption: buildCuteCaption('𝗗𝗢𝗖𝗨𝗠𝗘𝗡𝗧 𝗥𝗘𝗖𝗢𝗩𝗘𝗥𝗬', header, botName),
        contextInfo: channelContext
      });

    } else {
      await socket.sendMessage(target, {
        text: buildCuteCaption('𝗠𝗘𝗦𝗦𝗔𝗚𝗘', `${header}\n\n(Unsupported/unknown message type: ${type})`, botName),
        contextInfo: channelContext
      });
    }
  } catch (e) {
    console.error('[AntiDelete] send-recovered error:', e.message);
    try {
      await socket.sendMessage(target, {
        text: buildCuteCaption('𝗠𝗘𝗦𝗦𝗔𝗚𝗘 𝗗𝗘𝗟𝗘𝗧𝗘𝗗', `${header}\n\n⚠️ Couldn't recover the original content (${e.message || 'download failed'}).`, botName),
        contextInfo: channelContext
      });
    } catch (e2) {}
  }
}

function setupAntiDelete(socket, number, deps) {
  const { loadUserConfigFromMongo, BOT_NAME_FANCY, jidNormalizedUser, NEWSLETTER_CONTEXT, config } = deps;
  const sanitizedNumber = (number || '').replace(/[^0-9]/g, '');

  socket.ev.on('messages.upsert', async ({ messages }) => {
    try {
      const mek = messages?.[0];
      if (!mek || !mek.message) return;

      const remoteJid = mek.key.remoteJid;
      if (!remoteJid || remoteJid === 'status@broadcast') return;

      const proto = mek.message.protocolMessage;

      if (proto && proto.type === 0 && proto.key) {
        let cfg = {};
        try { cfg = (await loadUserConfigFromMongo(sanitizedNumber)) || {}; } catch (e) {}

        const antideleteEnabled = cfg.antidelete !== undefined ? !!cfg.antidelete : (config?.AUTO_ANTIDELETE === 'true');
        if (!antideleteEnabled) return;

        const key = cacheKey(remoteJid, proto.key.id);
        const cached = messageCache.get(key);
        if (!cached) return;

        const botNumber = socket.user.id.split(':')[0];
        const deletedBy = (resolveWhoFromKey(mek.key) || '').split('@')[0];
        const sentBy = (resolveWhoFromKey(cached.key) || '').split('@')[0];

        if (deletedBy.includes(botNumber) || sentBy.includes(botNumber)) return;

        const isGroup = remoteJid.endsWith('@g.us');
        const bodyText = extractText(cached.message);
        if (isGroup && bodyText && bodyText.includes('chat.whatsapp.com')) return;

        const antideleteTarget = cfg.antideleteTarget || config?.AUTO_ANTIDELETE_MODE || 'inbox';
        const target = antideleteTarget === 'chat' ? remoteJid : jidNormalizedUser(socket.user.id);

        const botName = cfg.botName || BOT_NAME_FANCY;
        const deletedAt = getSriLankaTimestamp();
        const header = `🗑️ *Deleted by:* _${deletedBy}_\n📩 *Sent by:* _${sentBy}_\n🕒 *Deleted at:* _${deletedAt}_${isGroup ? `\n👥 *Group:* ${remoteJid}` : ''}`;
        const channelContext = buildChannelContext(NEWSLETTER_CONTEXT, botName);

        await sendRecovered(socket, target, cached, header, botName, channelContext);
        messageCache.delete(key);
        return;
      }

      if (mek.key.fromMe) return;
      const key = cacheKey(remoteJid, mek.key.id);
      messageCache.set(key, {
        key: mek.key,
        message: mek.message,
        sender: mek.key.participant || mek.key.remoteJid,
        pushName: mek.pushName || '',
        timestamp: Date.now()
      });

    } catch (e) {
      console.error('[AntiDelete] handler error:', e.message);
    }
  });
}

module.exports = { setupAntiDelete };
