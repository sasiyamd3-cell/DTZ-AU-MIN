const BOT_NAME_FANCY = '✦ 𝐀𝐍𝐔 𝐌𝐃 ✦';

const config = {
  AUTO_VIEW_STATUS: 'false',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING: 'false',
  AUTO_VV_UNLOCK: 'false',
  AUTO_VV_UNLOCK_MODE: 'inbox', // 'inbox' = bot's own DM, 'direct' = back to the sender
  AUTO_ANTIDELETE: 'false',
  AUTO_ANTIDELETE_MODE: 'inbox', // 'inbox' = bot's own DM, 'chat' = back to the chat it was deleted from
  AUTO_LIKE_EMOJI: [
    '🔥','👍','❤️','💜','💙','💚','🧡','🤍','🖤',
    '💖','💗','💓','💞','💕','💝','💘','💟',
    '✨','🌟','💫','⚡','☀️','🌈','🌙','🌸','🌷','🌼','🌺','🌻',
    '🍓','🍒','🍎','🍉','🍇','🍰','🧁','🍭','🍬','🍫','🍩','🍪',
    '🐣','🐥','🐤','🐰','🐼','🐨','🦊','🧸','🐶','🐱','🐭',
    '🎀','🎁','🎈','🎉','🎊','💎','👑','🏆','🎶','🎵'
  ],
  PREFIX: '.',
  MAX_RETRIES: 3,

  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/KBy93MkplPmGLwbPU3GSnd',
  CHANNEL_LINK: 'https://whatsapp.com/channel/0029Vb6UR8S8fewn0otjcc0g',
  NEWSLETTER_JID: '120363409995383814@newsletter',

  OWNER_NUMBER: process.env.OWNER_NUMBER || '94764014979',
  OWNER_NAME: 'Thihina Anuhas',

  OWNER_CONTACTS: [
    { name: 'Thihina Anuhas🖤 Owner', number: '94764014979' },
    { name: 'Nimeshka Mihiran 👑 No.2', number: '94721584279' },
    { name: 'Nimeshka Mihiran 🌍 No.3', number: '94728304801' },
  ],

  BOT_NAME: 'ANU MD',
  BOT_VERSION: 'V1',
  BOT_FOOTER: 'ᴘᴏᴡᴇʀᴅ ʙʏ ᴀɴᴜ ᴍᴅ',

  RCD_IMAGE_PATH: 'https://raw.githubusercontent.com/NimeshMihiranga-Neno/Mezuka-help/main/IMG-20260704-WA0001.jpg',
  IMAGE_PATH: 'https://raw.githubusercontent.com/NimeshMihiranga-Neno/Mezuka-help/main/IMG-20260704-WA0001.jpg',
  BUTTON_IMAGES: {
    ALIVE: 'https://raw.githubusercontent.com/NimeshMihiranga-Neno/Mezuka-help/main/IMG-20260704-WA0001.jpg'
  },

  OTP_EXPIRY: 300000,

  MODE: process.env.BOT_MODE || 'public'
};

const NEWSLETTER_CONTEXT = {
  forwardingScore: 1,
  isForwarded: true,
  forwardedNewsletterMessageInfo: {
    newsletterJid: '120363409995383814@newsletter',
    newsletterName: '♡⸝⸝> ̫ <⸝⸝♡ 𝐀ɴᴜ 𝐌ᴅ',
    serverMessageId: 999
  }
};

const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://nmihiranga216_db_user:e1SZCOLqGul7XpOp@cluster0.9yyxp20.mongodb.net/';
const MONGO_DB = process.env.MONGO_DB || 'SAKURADB';

module.exports = {
  BOT_NAME_FANCY,
  config,
  NEWSLETTER_CONTEXT,
  MONGO_URI,
  MONGO_DB
};
