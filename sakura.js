const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
router.use(express.json());
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const FileType = require('file-type');
const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');
const { sms, downloadMediaMessage } = require("./msg");
const { setupAntiDelete } = require("./antidel");

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  makeCacheableSignalKeyStore,
  Browsers,
  jidNormalizedUser,
  downloadContentFromMessage,
  DisconnectReason
} = require('baileys');

const {
  BOT_NAME_FANCY,
  config,
  NEWSLETTER_CONTEXT,
  MONGO_URI,
  MONGO_DB
} = require('./config');

let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol;

async function initMongo() {
  try {
    if (mongoClient && mongoClient.topology && mongoClient.topology.isConnected && mongoClient.topology.isConnected()) return;
  } catch(e){}
  mongoClient = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  await mongoClient.connect();
  mongoDB = mongoClient.db(MONGO_DB);

  sessionsCol = mongoDB.collection('sessions');
  numbersCol = mongoDB.collection('numbers');
  adminsCol = mongoDB.collection('admins');
  newsletterCol = mongoDB.collection('newsletter_list');
  configsCol = mongoDB.collection('configs');
  newsletterReactsCol = mongoDB.collection('newsletter_reacts');

  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  console.log('✅ Mongo initialized and collections ready');
}

async function saveCredsToMongo(number, creds, keys = null) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = { number: sanitized, creds, keys, updatedAt: new Date() };
    await sessionsCol.updateOne({ number: sanitized }, { $set: doc }, { upsert: true });
    console.log(`Saved creds to Mongo for ${sanitized}`);
  } catch (e) { console.error('saveCredsToMongo error:', e); }
}

async function loadCredsFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await sessionsCol.findOne({ number: sanitized });
    return doc || null;
  } catch (e) { console.error('loadCredsFromMongo error:', e); return null; }
}

async function removeSessionFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await sessionsCol.deleteOne({ number: sanitized });
    console.log(`Removed session from Mongo for ${sanitized}`);
  } catch (e) { console.error('removeSessionToMongo error:', e); }
}

async function addNumberToMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.updateOne({ number: sanitized }, { $set: { number: sanitized } }, { upsert: true });
    console.log(`Added number ${sanitized} to Mongo numbers`);
  } catch (e) { console.error('addNumberToMongo', e); }
}

async function removeNumberFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.deleteOne({ number: sanitized });
    console.log(`Removed number ${sanitized} from Mongo numbers`);
  } catch (e) { console.error('removeNumberFromMongo', e); }
}

async function getAllNumbersFromMongo() {
  try {
    await initMongo();
    const docs = await numbersCol.find({}).toArray();
    return docs.map(d => d.number);
  } catch (e) { console.error('getAllNumbersFromMongo', e); return []; }
}

async function loadAdminsFromMongo() {
  try {
    await initMongo();
    const docs = await adminsCol.find({}).toArray();
    return docs.map(d => d.jid || d.number).filter(Boolean);
  } catch (e) { console.error('loadAdminsFromMongo', e); return []; }
}

async function addAdminToMongo(jidOrNumber) {
  try {
    await initMongo();
    const doc = { jid: jidOrNumber };
    await adminsCol.updateOne({ jid: jidOrNumber }, { $set: doc }, { upsert: true });
    console.log(`Added admin ${jidOrNumber}`);
  } catch (e) { console.error('addAdminToMongo', e); }
}

async function removeAdminFromMongo(jidOrNumber) {
  try {
    await initMongo();
    await adminsCol.deleteOne({ jid: jidOrNumber });
    console.log(`Removed admin ${jidOrNumber}`);
  } catch (e) { console.error('removeAdminFromMongo', e); }
}

let _newslettersCache = null;
let _newslettersCacheAt = 0;
const NEWSLETTERS_CACHE_TTL_MS = 30 * 1000; // 30s — this list is queried on EVERY incoming message across all sessions

async function addNewsletterToMongo(jid, emojis = []) {
  try {
    await initMongo();
    const doc = { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() };
    await newsletterCol.updateOne({ jid }, { $set: doc }, { upsert: true });
    _newslettersCache = null; // invalidate
    console.log(`Added newsletter ${jid} -> emojis: ${doc.emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterToMongo', e); throw e; }
}

async function removeNewsletterFromMongo(jid) {
  try {
    await initMongo();
    await newsletterCol.deleteOne({ jid });
    _newslettersCache = null; // invalidate
    console.log(`Removed newsletter ${jid}`);
  } catch (e) { console.error('removeNewsletterFromMongo', e); throw e; }
}

async function listNewslettersFromMongo() {
  if (_newslettersCache && (Date.now() - _newslettersCacheAt) < NEWSLETTERS_CACHE_TTL_MS) {
    return _newslettersCache;
  }
  try {
    await initMongo();
    const docs = await newsletterCol.find({}).toArray();
    _newslettersCache = docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
    _newslettersCacheAt = Date.now();
    return _newslettersCache;
  } catch (e) { console.error('listNewslettersFromMongo', e); return _newslettersCache || []; }
}

async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) {
  try {
    await initMongo();
    const doc = { jid, messageId, emoji, sessionNumber, ts: new Date() };
    if (!mongoDB) await initMongo();
    const col = mongoDB.collection('newsletter_reactions_log');
    await col.insertOne(doc);
    console.log(`Saved reaction ${emoji} for ${jid}#${messageId}`);
  } catch (e) { console.error('saveNewsletterReaction', e); }
}

async function setUserConfigInMongo(number, conf) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await configsCol.updateOne({ number: sanitized }, { $set: { number: sanitized, config: conf, updatedAt: new Date() } }, { upsert: true });
  } catch (e) { console.error('setUserConfigInMongo', e); }
}

async function loadUserConfigFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await configsCol.findOne({ number: sanitized });
    return doc ? doc.config : null;
  } catch (e) { console.error('loadUserConfigFromMongo', e); return null; }
}

function generateSettingsPassword() {
  const letters = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  const digits = '23456789';
  const pool = letters + digits;
  let out = '';
  for (let i = 0; i < 6; i++) out += pool[crypto.randomInt(pool.length)];
  return out;
}

async function getOrCreateSettingsPassword(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const existing = await configsCol.findOne({ number: sanitized }, { projection: { settingsPassword: 1 } });
    if (existing && existing.settingsPassword) return existing.settingsPassword;
    const password = generateSettingsPassword();
    await configsCol.updateOne(
      { number: sanitized },
      { $set: { number: sanitized, settingsPassword: password }, $setOnInsert: { config: {}, updatedAt: new Date() } },
      { upsert: true }
    );
    return password;
  } catch (e) { console.error('getOrCreateSettingsPassword', e); return null; }
}

async function checkSettingsAuth(number, password) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    if (!sanitized || !password) return false;
    const doc = await configsCol.findOne({ number: sanitized }, { projection: { settingsPassword: 1 } });
    if (!doc || !doc.settingsPassword) return false;
    return doc.settingsPassword === String(password).trim().toUpperCase();
  } catch (e) { console.error('checkSettingsAuth', e); return false; }
}

let _reactConfigsCache = null;
let _reactConfigsCacheAt = 0;
const REACT_CONFIGS_CACHE_TTL_MS = 30 * 1000;

async function addNewsletterReactConfig(jid, emojis = []) {
  try {
    await initMongo();
    await newsletterReactsCol.updateOne({ jid }, { $set: { jid, emojis, addedAt: new Date() } }, { upsert: true });
    _reactConfigsCache = null; // invalidate
    console.log(`Added react-config for ${jid} -> ${emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterReactConfig', e); throw e; }
}

async function removeNewsletterReactConfig(jid) {
  try {
    await initMongo();
    await newsletterReactsCol.deleteOne({ jid });
    _reactConfigsCache = null; // invalidate
    console.log(`Removed react-config for ${jid}`);
  } catch (e) { console.error('removeNewsletterReactConfig', e); throw e; }
}

async function listNewsletterReactsFromMongo() {
  if (_reactConfigsCache && (Date.now() - _reactConfigsCacheAt) < REACT_CONFIGS_CACHE_TTL_MS) {
    return _reactConfigsCache;
  }
  try {
    await initMongo();
    const docs = await newsletterReactsCol.find({}).toArray();
    _reactConfigsCache = docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
    _reactConfigsCacheAt = Date.now();
    return _reactConfigsCache;
  } catch (e) { console.error('listNewsletterReactsFromMongo', e); return _reactConfigsCache || []; }
}

async function getReactConfigForJid(jid) {
  try {
    await initMongo();
    const doc = await newsletterReactsCol.findOne({ jid });
    return doc ? (Array.isArray(doc.emojis) ? doc.emojis : []) : null;
  } catch (e) { console.error('getReactConfigForJid', e); return null; }
}

const STATIC_REACT_CHANNELS_URL = "https://raw.githubusercontent.com/NimeshMihiranga-Neno/mezukasite/main/react_channel.json";
const STATIC_REACT_CHANNELS_FILE = path.join(__dirname, 'react_channel.json');
const VIP_FOLLOW_URL = "https://raw.githubusercontent.com/NimeshMihiranga-Neno/Mezuka-help/main/vip.json";

const staticReactChannelCache = new Map();

async function fetchJsonSimple(url) {
  try {
    const { data } = await axios.get(url, { timeout: 15000 });
    if (typeof data === 'string') {
      try { return JSON.parse(data); } catch { return null; }
    }
    return data || null;
  } catch (e) {
    console.warn(`[fetchJsonSimple] failed for ${url}:`, e?.message || e);
    return null;
  }
}

function applyStaticChannelList(parsed) {
  const list = Array.isArray(parsed) ? parsed : (parsed?.channels || []);
  list.forEach(entry => {
    const jid = typeof entry === 'string' ? entry : entry.jid;
    if (!jid || !jid.endsWith('@newsletter')) return;
    const emojis = (entry && Array.isArray(entry.emojis) && entry.emojis.length > 0)
      ? entry.emojis : undefined;
    staticReactChannelCache.set(jid, { jid, emojis, static: true });
  });
  return list.length;
}

async function loadStaticReactChannels() {
  try {
    const remote = await fetchJsonSimple(STATIC_REACT_CHANNELS_URL);
    if (remote) {
      const count = applyStaticChannelList(remote);
      console.log(`✅ [StaticReact] Channels loaded from GitHub: ${count}`);
      return;
    }
  } catch (e) {
    console.warn('[StaticReact] Remote react_channel.json fetch failed, trying local file:', e?.message || e);
  }

  try {
    if (!fs.existsSync(STATIC_REACT_CHANNELS_FILE)) {
      console.log('ℹ️ [StaticReact] No local react_channel.json either, skipping this round.');
      return;
    }
    const raw = fs.readFileSync(STATIC_REACT_CHANNELS_FILE, 'utf8');
    const parsed = JSON.parse(raw || '{}');
    const count = applyStaticChannelList(parsed);
    console.log(`✅ [StaticReact] Channels loaded from local file: ${count}`);
  } catch (e) {
    console.error('[StaticReact] Static channel load error:', e?.message || e);
  }
}

async function getFollowData() {
  try {
    const followData = await fetchJsonSimple(VIP_FOLLOW_URL) || {};
    return followData;
  } catch {
    return {};
  }
}

async function getVipFollowJids() {
  const followData = await getFollowData();
  return (followData?.FL || "")
    .split(",").map(s => s.trim()).filter(s => s.length);
}

loadStaticReactChannels();
setInterval(loadStaticReactChannels, 10 * 60 * 1000);

// Mirrors the @lid → real-JID resolution done for 'sender' at message-receive
// time. Without this, any listener that compares an incoming reply's raw
// key.remoteJid against the already-resolved 'sender' silently never matches
// in DM chats where WhatsApp hands back an @lid identifier instead of the
// real JID — the number-reply just looks "dead" with no error anywhere.
function resolveReplyJid(m) {
  const raw = m?.key?.remoteJid;
  return (raw && raw.endsWith('@lid') && m.key.remoteJidAlt) ? m.key.remoteJidAlt : raw;
}

function formatMessage(title, content, footer) {
  return `*${title}*\n\n${content}\n\n> *${footer}*`;
}
function generateOTP(){ return Math.floor(100000 + Math.random() * 900000).toString(); }
function getSriLankaTimestamp(){ return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();

const socketCreationTime = new Map();
const pendingModApk = new Map();
const otpStore = new Map();

async function joinGroup(socket) {
  let retries = config.MAX_RETRIES;
  const inviteCodeMatch = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!inviteCodeMatch) return { status: 'failed', error: 'No group invite configured' };
  const inviteCode = inviteCodeMatch[1];
  while (retries > 0) {
    try {
      const response = await socket.groupAcceptInvite(inviteCode);
      if (response?.gid) return { status: 'success', gid: response.gid };
      throw new Error('No group ID in response');
    } catch (error) {
      retries--;
      let errorMessage = error.message || 'Unknown error';
      if (error.message && error.message.includes('not-authorized')) errorMessage = 'Bot not authorized';
      else if (error.message && error.message.includes('conflict')) errorMessage = 'Already a member';
      else if (error.message && error.message.includes('gone')) errorMessage = 'Invite invalid/expired';
      if (retries === 0) return { status: 'failed', error: errorMessage };
      await delay(2000 * (config.MAX_RETRIES - retries));
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

async function sendAdminConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  const admins = await loadAdminsFromMongo();
  const groupStatus = groupResult.status === 'success' ? `Joined (ID: ${groupResult.gid})` : `Failed to join group: ${groupResult.error}`;
  const botName = sessionConfig.botName || BOT_NAME_FANCY;
  const image = sessionConfig.logo || config.RCD_IMAGE_PATH;
  const caption = formatMessage(botName, `📞 Number: ${number}`, botName);
  for (const admin of admins) {
    try {
      const to = admin.includes('@') ? admin : `${admin}@s.whatsapp.net`;
      if (String(image).startsWith('http')) {
        await socket.sendMessage(to, { image: { url: image }, caption });
      } else {
        try {
          const buf = fs.readFileSync(image);
          await socket.sendMessage(to, { image: buf, caption });
        } catch (e) {
          await socket.sendMessage(to, { image: { url: config.RCD_IMAGE_PATH }, caption });
        }
      }
    } catch (err) {
      console.error('Failed to send connect message to admin', admin, err?.message || err);
    }
  }
}

async function sendOwnerConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  try {
    const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g,'')}@s.whatsapp.net`;
    const activeCount = activeSockets.size;
    const botName = sessionConfig.botName || BOT_NAME_FANCY;
    const image = sessionConfig.logo || config.RCD_IMAGE_PATH;
    const groupStatus = groupResult.status === 'success' ? `Joined (ID: ${groupResult.gid})` : `Failed to join group: ${groupResult.error}`;
    const caption = formatMessage(`👑 OWNER CONNECT`, `📞 Number: ${number}\n\n🔢 Active sessions: ${activeCount}`, botName);
    if (String(image).startsWith('http')) {
      await socket.sendMessage(ownerJid, { image: { url: image }, caption });
    } else {
      try {
        const buf = fs.readFileSync(image);
        await socket.sendMessage(ownerJid, { image: buf, caption });
      } catch (e) {
        await socket.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
      }
    }
  } catch (err) { console.error('Failed to send owner connect message:', err); }
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = formatMessage(`🔐 OTP VERIFICATION — ${BOT_NAME_FANCY}`, `Your OTP for config update is: *${otp}*\nThis OTP will expire in 5 minutes.\n\nNumber: ${number}`, BOT_NAME_FANCY);
  try { await socket.sendMessage(userJid, { text: message }); console.log(`OTP ${otp} sent to ${number}`); }
  catch (error) { console.error(`Failed to send OTP to ${number}:`, error); throw error; }
}

// Pull a usable "server id" out of a newsletter message no matter which
// field name the installed Baileys version happens to use for it.
// Different Baileys releases have exposed this as msg.newsletterServerId,
// msg.key.serverId, msg.key.server_id, msg.messageStubParameters[0], or
// just msg.key.id. Only trying newsletterServerId/key.id (like before)
// silently breaks reacting the moment the installed Baileys version
// renames the field.
function extractNewsletterServerId(msg) {
  const candidates = [
    msg?.key?.server_id,
    msg?.newsletterServerId,
    msg?.key?.serverId,
    Array.isArray(msg?.messageStubParameters) ? msg.messageStubParameters[0] : undefined,
    msg?.key?.id
  ];
  return candidates.find(v => v !== undefined && v !== null && v !== '');
}

// Set DEBUG_NEWSLETTER_REACT=1 in env to dump raw message objects while
// diagnosing why reactions stopped firing on a newer Baileys version.
const NL_REACT_DEBUG = process.env.DEBUG_NEWSLETTER_REACT === '1';

const NL_DEFAULT_EMOJIS = ['🧃','🫧','🪻','🪷','🌸','🌷','🌼','🌝','🌛','🌜','🎐','🧸','🍡','🍭','🍓','🫐','🧁','🍩','🍪','🥐','🐽','🐰','🐹','🐣','🐥','🦋','🦄','🐢','🐳','🦢','🕊️','🪸','🌈','☁️','🌤️','⭐','🌟','💫','✨','🎀','🪄','🎉','🎊','🥳','💖','💕','💗','💓','💞','💘','🫶','🙌','👏','🤍','🩷','🩵','🧡','💛','💚','💙'];

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key?.remoteJid) return;
    const jid = message.key.remoteJid;
    if (!jid.endsWith('@newsletter')) return;

    if (NL_REACT_DEBUG) {
      console.log('🛠️ [NewsletterAutoReact][DEBUG] raw msg =', JSON.stringify(message, null, 2));
    }

    try {
      const followedDocs = await listNewslettersFromMongo();
      const reactConfigs = await listNewsletterReactsFromMongo();
      const reactMap = new Map();
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);

      const followedJids = followedDocs.map(d => d.jid);
      const isStaticChannel = staticReactChannelCache.has(jid);
      if (!followedJids.includes(jid) && !reactMap.has(jid) && !isStaticChannel) {
        if (NL_REACT_DEBUG) console.log(`🛠️ [DEBUG] No react config cached for ${jid} — check followed/react/static lists.`);
        return;
      }

      let emojis = reactMap.get(jid) || null;
      if ((!emojis || emojis.length === 0) && followedDocs.find(d => d.jid === jid)) {
        emojis = (followedDocs.find(d => d.jid === jid).emojis || []);
      }
      if ((!emojis || emojis.length === 0) && isStaticChannel) {
        emojis = staticReactChannelCache.get(jid)?.emojis || [];
      }
      if (!emojis || emojis.length === 0) {
        emojis = (Array.isArray(config.AUTO_LIKE_EMOJI) && config.AUTO_LIKE_EMOJI.length)
          ? config.AUTO_LIKE_EMOJI : NL_DEFAULT_EMOJIS;
      }

      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);

      const messageId = extractNewsletterServerId(message);
      if (!messageId) {
        console.warn(`⚠️ [NewsletterAutoReact] Could not resolve a server id for ${jid} — set DEBUG_NEWSLETTER_REACT=1 and inspect the raw msg.`);
        return;
      }

      try {
        if (typeof socket.newsletterReactMessage === 'function') {
          await socket.newsletterReactMessage(jid, messageId.toString(), emoji);
        } else {
          await socket.sendMessage(jid, { react: { text: emoji, key: { ...message.key, id: messageId.toString() } } });
        }
        console.log(`✅ [NewsletterAutoReact] Reacted to ${jid} ${messageId} with ${emoji}`);
        await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null);
      } catch (err) {
        // Skip silently on failure — no auto-follow retry.
        console.warn(`⚠️ [NewsletterAutoReact] ${sessionNumber || ''} react failed, skipping:`, err?.output?.payload || err?.data || err?.message || err);
      }

    } catch (error) {
      console.error('Newsletter reaction handler error:', error?.message || error);
    }
  });
}

async function setupStatusHandlers(socket) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;
    try {
      const sanitizedNumber = (socket.user && socket.user.id) ? socket.user.id.split(':')[0] : null;
      const sessionConfig = sanitizedNumber ? (await loadUserConfigFromMongo(sanitizedNumber) || {}) : {};

      const stviewEnabled = (typeof sessionConfig.stview !== 'undefined') ? !!sessionConfig.stview : (config.AUTO_VIEW_STATUS === 'true');
      if (stviewEnabled) {
        try {
          let retries = config.MAX_RETRIES;
          while (retries > 0) {
            try { await socket.readMessages([message.key]); break; }
            catch (error) { retries--; await delay(1000 * (config.MAX_RETRIES - retries)); if (retries===0) throw error; }
          }
        } catch (e) { console.warn('Failed to auto-view status:', e); }
      }

      let emojis = Array.isArray(sessionConfig.sr) && sessionConfig.sr.length ? sessionConfig.sr : (config.AUTO_LIKE_STATUS === 'true' ? config.AUTO_LIKE_EMOJI : []);
      if (emojis && emojis.length > 0) {
        const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try {
            await socket.sendMessage(message.key.remoteJid, { react: { text: randomEmoji, key: message.key } }, { statusJidList: [message.key.participant] });
            break;
          } catch (error) { retries--; await delay(1000 * (config.MAX_RETRIES - retries)); if (retries===0) console.warn('Failed to react to status:', error); }
        }
      }

    } catch (error) { console.error('Status handler error:', error); }
  });
}

async function handleMessageRevocation(socket, number) {
  // Full anti-delete: caches incoming messages in-memory and, when one is
  // deleted, recovers and re-sends the original content (text/image/video/
  // sticker/audio/document) with a cute decorative caption, forwarded from
  // the bot's channel — same as every other outgoing message in this file.
  // Toggle + destination are read live from the user's settings via
  // loadUserConfigFromMongo — same flow as every other per-number setting
  // (stview, sr, mode, etc), falling back to config.js when unset.
  //
  //   cfg.antidelete       -> true/false (default: config.AUTO_ANTIDELETE)
  //   cfg.antideleteTarget -> "chat" (send back to the same chat it was
  //                           deleted from) or "inbox" (send to the bot's
  //                           own DM). Default: config.AUTO_ANTIDELETE_MODE.
  setupAntiDelete(socket, number, {
    loadUserConfigFromMongo,
    BOT_NAME_FANCY,
    jidNormalizedUser,
    NEWSLETTER_CONTEXT,
    config
  });
}

async function resize(image, width, height) {
  let oyy = await Jimp.read(image);
  return await oyy.resize(width, height).getBufferAsync(Jimp.MIME_JPEG);
}

function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

    // declared here (outside the try) so the top-level catch below can still
    // use it to notify the chat even if something throws early.
    //
    // WhatsApp sometimes exposes a privacy "@lid" identifier instead of the
    // real phone-number JID for a chat. Sending to an @lid JID directly can
    // silently hang (no throw, no reply, nothing in the logs) — so resolve it
    // to the real JID via Baileys' alternate field whenever possible.
    const rawRemoteJid = msg.key.remoteJid;
    const sender = (rawRemoteJid && rawRemoteJid.endsWith('@lid') && msg.key.remoteJidAlt)
      ? msg.key.remoteJidAlt
      : rawRemoteJid;

    if (rawRemoteJid && rawRemoteJid.endsWith('@lid')) {
      console.log(`[DEBUG] @lid chat detected. raw=${rawRemoteJid} | remoteJidAlt=${msg.key.remoteJidAlt || 'MISSING'} | resolved sender=${sender}`);
    }

    try { // top-level guard: without this, any thrown error here silently drops
          // the message with zero feedback — this is what was causing "no reply
          // sometimes" in DMs with no visible error at all.

    const from = sender;
    const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || sender);
    const senderNumber = (nowsender || '').split('@')[0];
    const botNumber = socket.user.id ? socket.user.id.split(':')[0] : '';
    const isGroup = String(from || '').endsWith('@g.us');

    async function downloadQuotedMedia(quoted) {
      if (!quoted) return null;
      const qTypes = ['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'];
      const qType = qTypes.find(t => quoted[t]);
      if (!qType) return null;
      const messageType = qType.replace(/Message$/i, '').toLowerCase();
      const stream = await downloadContentFromMessage(quoted[qType], messageType);
      const chunks = [];
      for await (const chunk of stream) chunks.push(chunk);
      const buffer = Buffer.concat(chunks);
      return {
        buffer,
        mime: quoted[qType].mimetype || '',
        caption: quoted[qType].caption || quoted[qType].fileName || '',
        ptt: quoted[qType].ptt || false,
        fileName: quoted[qType].fileName || ''
      };
    }

    // ---- Auto View-Once Unlock (global, silent) ----
    // Setting: cfg.vvUnlock (toggled from settings.html). When on: if ANYONE replies
    // to a view-once photo/video/voice with any message, emoji, or sticker, the
    // original view-once media is downloaded and sent per vvUnlockMode.
    // No reply or reaction is sent to the original chat — the reply is just the trigger.
    try {
      // pull contextInfo out of whichever message sub-type this reply is
      // (extendedTextMessage for text/emoji replies, stickerMessage for sticker replies, etc.)
      const msgBody = msg.message || {};
      const replyContextInfo =
        msgBody.extendedTextMessage?.contextInfo ||
        msgBody.stickerMessage?.contextInfo ||
        msgBody.imageMessage?.contextInfo ||
        msgBody.videoMessage?.contextInfo ||
        msgBody.audioMessage?.contextInfo ||
        msgBody.documentMessage?.contextInfo ||
        msgBody.buttonsResponseMessage?.contextInfo ||
        msgBody.listResponseMessage?.contextInfo ||
        null;

      let quotedForVV = replyContextInfo?.quotedMessage || null;

      // unwrap "disappearing messages" wrapper if the original was sent in a
      // chat/group with ephemeral messages turned on
      if (quotedForVV?.ephemeralMessage?.message) {
        quotedForVV = quotedForVV.ephemeralMessage.message;
      }

      const vvWrapped = quotedForVV
        ? (quotedForVV.viewOnceMessage?.message ||
           quotedForVV.viewOnceMessageV2?.message ||
           quotedForVV.viewOnceMessageV2Extension?.message ||
           // newer WhatsApp clients: no wrapper — a plain imageMessage/videoMessage/
           // audioMessage with a viewOnce:true flag directly on it
           (quotedForVV.imageMessage?.viewOnce ? { imageMessage: quotedForVV.imageMessage } : null) ||
           (quotedForVV.videoMessage?.viewOnce ? { videoMessage: quotedForVV.videoMessage } : null) ||
           (quotedForVV.audioMessage?.viewOnce ? { audioMessage: quotedForVV.audioMessage } : null) ||
           null)
        : null;

      if (vvWrapped) {
        const sanitizedForVV = (number || '').replace(/[^0-9]/g, '');
        const vvCfg = await loadUserConfigFromMongo(sanitizedForVV) || {};
        const vvEnabled = vvCfg.vvUnlock !== undefined ? !!vvCfg.vvUnlock : (config.AUTO_VV_UNLOCK === 'true');

        if (vvEnabled) {
          const unlocked = await downloadQuotedMedia(vvWrapped);
          if (unlocked && unlocked.buffer) {
            const ownJid = botNumber + '@s.whatsapp.net';
            const vvMode = vvCfg.vvUnlockMode || config.AUTO_VV_UNLOCK_MODE || 'inbox'; // 'inbox' = bot's own DM, 'direct' = back to the same chat (group or DM) where the reply happened

            // 'direct' = send back into whichever chat the reply happened in (the
            // group itself if it was a group, or that person's DM if it was a DM)
            const targetJid = vvMode === 'direct' ? msg.key.remoteJid : ownJid;

            const senderNumberForCap = (msg.key.participant || msg.key.remoteJid || '').split('@')[0];
            const infoLine = vvMode === 'direct'
              ? `🔓 *View-Once Unlocked*`
              : `🔓 *View-Once Unlocked*\n👤 *From:* ${senderNumberForCap}${isGroup ? ' (group)' : ''}`;
            const finalCap = unlocked.caption ? `${infoLine}\n\n${unlocked.caption}` : infoLine;

            if ((unlocked.mime || '').startsWith('image')) {
              await socket.sendMessage(targetJid, { image: unlocked.buffer, caption: finalCap });
            } else if ((unlocked.mime || '').startsWith('video')) {
              await socket.sendMessage(targetJid, { video: unlocked.buffer, caption: finalCap });
            } else if ((unlocked.mime || '').startsWith('audio')) {
              await socket.sendMessage(targetJid, { audio: unlocked.buffer, mimetype: unlocked.mime || 'audio/mp4', ptt: unlocked.ptt || false });
            }
          }
        }
      }
    } catch (vvAutoErr) {
      console.error('Auto view-once unlock error:', vvAutoErr);
    }

    const body = (type === 'conversation') ? msg.message.conversation
      : (type === 'extendedTextMessage') ? msg.message.extendedTextMessage.text
      : (type === 'imageMessage' && msg.message.imageMessage.caption) ? msg.message.imageMessage.caption
      : (type === 'videoMessage' && msg.message.videoMessage.caption) ? msg.message.videoMessage.caption
      : (type === 'buttonsResponseMessage') ? msg.message.buttonsResponseMessage?.selectedButtonId
      : (type === 'listResponseMessage') ? msg.message.listResponseMessage?.singleSelectReply?.selectedRowId
      : (type === 'viewOnceMessage') ? (msg.message.viewOnceMessage?.message?.imageMessage?.caption || '') : '';

    if (!body || typeof body !== 'string') return;

    const prefix = config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

    // shared helper — many commands (movie, tiktok, ig, etc.) use q as the
    // "everything after the command" text. This was missing, causing "q is not defined".
    const q = args.join(' ');

    // shared helper — many commands (movie, song, tiktok, fb, ig, etc.) call reply(text)
    // to quote-reply with plain text. This was missing, causing "reply is not defined".
    const reply = (text) => {
      const sendPromise = socket.sendMessage(sender, { text }, { quoted: msg });
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`reply() timed out after 15s sending to ${sender} — likely an unresolvable @lid JID`)), 15000)
      );
      return Promise.race([sendPromise, timeoutPromise]).catch(e => {
        console.error('[DEBUG] reply() failed/timed out:', e.message);
      });
    };

    if (!command) return;

    console.log(`[DEBUG] Command received: "${command}" | args="${q}" | chat=${isGroup ? 'GROUP' : 'DM'} | sender=${senderNumber} | jid=${sender}`);

    // fetched once per message and reused by every command below —
    // avoids each case re-querying Mongo for the same session config
    let sessionConfig = {};

    try {
      const sanitizedNumber = (number || '').replace(/[^0-9]/g, '');
      sessionConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
      const sessionMode = (sessionConfig && sessionConfig.mode) ? sessionConfig.mode : (config.MODE || 'public');
      const effectiveOwnerNumber = (sessionConfig.ownerNumber || config.OWNER_NUMBER || '').replace(/[^0-9]/g,'');
      const isOwner = senderNumber === effectiveOwnerNumber;

      console.log(`[DEBUG] Permission check: mode=${sessionMode} | isOwner=${isOwner} | senderNumber=${senderNumber} | effectiveOwnerNumber=${effectiveOwnerNumber}`);

      const permissionQuote = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PERM" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nEND:VCARD` } }
      };

      if (!isOwner) {
        if (sessionMode === 'private') {
          console.log('[DEBUG] Blocked: private mode, not owner');
          await socket.sendMessage(sender, { text: '❌ Permission denied. Bot is currently in *private* mode — only the session owner or bot owner may use commands.' }, { quoted: permissionQuote });
          return;
        }
        if (isGroup && sessionMode === 'inbox') {
          console.log('[DEBUG] Blocked: inbox mode, message is from a group');
          await socket.sendMessage(sender, { text: '❌ Permission denied. Bot is in *inbox* mode — commands are restricted to private chats only.' }, { quoted: permissionQuote });
          return;
        }
        if (!isGroup && sessionMode === 'groups') {
          console.log('[DEBUG] Blocked: groups mode, message is a DM');
          await socket.sendMessage(sender, { text: '❌ Permission denied. Bot is in *groups* mode — commands are restricted to group chats only.' }, { quoted: permissionQuote });
          return;
        }
      }
    } catch (permErr) {
      console.error('[DEBUG] Permission check error (Mongo/config issue?) — continuing with defaults:', permErr);
    }

    console.log(`[DEBUG] Passed permission check, dispatching command "${command}"`);

    if (!command) return;

    try {
      switch (command) {

      case 'alive': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;
  const logo    = cfg.logo    || config.IMAGE_PATH;

  const uptimeSec = Math.floor(process.uptime());
  const hh = Math.floor(uptimeSec / 3600);
  const mm = Math.floor((uptimeSec % 3600) / 60);
  const ss = uptimeSec % 60;

  let runtimeStr = '';
  if (hh > 0) runtimeStr += `${hh} hour${hh > 1 ? 's' : ''}, `;
  if (mm > 0) runtimeStr += `${mm} minute${mm > 1 ? 's' : ''}, `;
  runtimeStr += `${ss} second${ss !== 1 ? 's' : ''}`;

  const memMB = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);

  const aliveCaption =
    `🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ʜᴇʟʟᴏ ♡⸝⸝> ̫ <⸝⸝♡* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n` +
    `┊ ┊ ✫ ˚♡ ⋆｡❀\n` +
    `┊ ☪︎⋆\n\n` +
    `> 🌿 *ᴠᴇʀsɪᴏɴ :* ${config.BOT_VERSION || 'V5'}\n` +
    `> 💫 *ᴍᴇᴍᴏʀʏ :* ${memMB}MB\n` +
    `> ⏳ *ʀᴜɴᴛɪᴍᴇ :* ${runtimeStr}\n` +
    `> 🌐 *ʜᴏsᴛ :* Railway\n\n` +
    `🧚‍♀️ *©ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀɴᴜ ᴛᴇᴀᴍ*\n\n` +
    `*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`;

  const channelContext = {
    forwardingScore: 1,
    isForwarded: true,
    forwardedNewsletterMessageInfo: {
      newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
      newsletterName: botName,
      serverMessageId: 999,
    }
  };

  await socket.sendMessage(sender, {
    react: { text: '🐽', key: msg.key }
  });

  try {
    if (String(logo).startsWith('http')) {
      await socket.sendMessage(sender, {
        image: { url: logo },
        caption: aliveCaption,
        contextInfo: channelContext
      }, { quoted: msg });
    } else {
      try {
        const buf = fs.readFileSync(logo);
        await socket.sendMessage(sender, {
          image: buf,
          caption: aliveCaption,
          contextInfo: channelContext
        }, { quoted: msg });
      } catch (_e) {
        await socket.sendMessage(sender, {
          image: { url: config.IMAGE_PATH },
          caption: aliveCaption,
          contextInfo: channelContext
        }, { quoted: msg });
      }
    }
  } catch (e) {
    await socket.sendMessage(sender, {
      text: aliveCaption,
      contextInfo: channelContext
    }, { quoted: msg });
  }
  break;
}

case 'cartoon':
case 'ct':
case 'sinhala': {
  const axios = require('axios');

  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  const CARTOON_API = "https://cartoon-scrap.vercel.app";
  const LIST_BANNER = cfg.logo || config.IMAGE_PATH;
  const NEWSLETTER = {
    newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
    newsletterName: botName,
    serverMessageId: 143
  };

  const res = await axios.get(`${CARTOON_API}/api/new?limit=10`, { timeout: 30000 });
  if (!res.data.success) return reply("❌ Failed to load cartoon list!");

  const cartoons = res.data.data.slice(0, 10);

  let listText = `🎬 *${botName}*\n\n`;
  listText += `📺 *Latest Sinhala Cartoons - Top 10*\n`;
  listText += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  cartoons.forEach((c, i) => {
    listText += `*${i + 1}. ${c.title}*\n`;
  });
  listText += `\n━━━━━━━━━━━━━━━━━━━━\n`;
  listText += `_Reply with a number (1-10) to select_ ⬆️`;

  const sentMsg = await socket.sendMessage(sender, {
    image: { url: LIST_BANNER },
    caption: listText,
    contextInfo: {
      forwardingScore: 1000,
      isForwarded: true,
      forwardedNewsletterMessageInfo: NEWSLETTER
    }
  }, { quoted: msg });

  const messageID = sentMsg.key.id;

  global.cartoonSessions = global.cartoonSessions || {};
  global.cartoonSessions[messageID] = {
    cartoons,
    from: sender,
    expiry: Date.now() + 5 * 60 * 1000
  };

  const replyListener = async (upsert) => {
    try {
      for (const replyMsg of (upsert?.messages || [])) {
        if (!replyMsg?.message) continue;
        if (resolveReplyJid(replyMsg) !== sender) continue;

        const quotedId = replyMsg.message?.extendedTextMessage?.contextInfo?.stanzaId;
        if (quotedId !== messageID) continue;

        const replyText = (
          replyMsg.message?.extendedTextMessage?.text ||
          replyMsg.message?.conversation || ''
        ).trim();

        const num = parseInt(replyText);
        if (isNaN(num) || num < 1 || num > 10) {
          await socket.sendMessage(sender, {
            text: `❌ Invalid! Reply with a number between 1 and 10.`
          }, { quoted: replyMsg });
          continue;
        }

        const session = global.cartoonSessions[messageID];
        if (!session) {
          await socket.sendMessage(sender, {
            text: `❌ Session expired! Use *${prefix}cartoon* again.`
          }, { quoted: replyMsg });
          socket.ev.off('messages.upsert', replyListener);
          return;
        }

        const selected = session.cartoons[num - 1];
        socket.ev.off('messages.upsert', replyListener);
        delete global.cartoonSessions[messageID];

        await socket.sendMessage(sender, {
          text: `🔍 *Loading details...*\n\n*${selected.title}*\n\nPlease wait...`
        }, { quoted: replyMsg });

        const detailRes = await axios.get(`${CARTOON_API}/api/details?id=${selected.id}`, { timeout: 30000 });
        if (!detailRes.data.success) {
          return await socket.sendMessage(sender, {
            text: `❌ Failed to load details!`
          }, { quoted: replyMsg });
        }

        const cartoon = detailRes.data.data;
        const directLink = cartoon.download_links.find(l => l.type === 'direct');
        const telegramLink = cartoon.download_links.find(l => l.type === 'telegram');

        const detailText =
          `🎬 *${botName}*\n\n` +
          `📌 *${cartoon.title}*\n` +
          `━━━━━━━━━━━━━━━━━━━━\n\n` +
          `📖 *Story:*\n${cartoon.description?.substring(0, 300)}${cartoon.description?.length > 300 ? '...' : ''}\n\n` +
          `━━━━━━━━━━━━━━━━━━━━\n` +
          `📥 *Select a download option:*\n\n` +
          `*1 - 📄 Direct Download (MP4 File)*\n` +
          `*2 - 📱 Telegram Bot Link*\n\n` +
          `_Reply with 1 or 2_ ⬆️`;

        const detailMsg = await socket.sendMessage(sender, {
          image: { url: cartoon.banner },
          caption: detailText,
          contextInfo: {
            forwardingScore: 1000,
            isForwarded: true,
            forwardedNewsletterMessageInfo: NEWSLETTER
          }
        }, { quoted: replyMsg });

        const detailMsgID = detailMsg.key.id;

        global.cartoonDownloads = global.cartoonDownloads || {};
        global.cartoonDownloads[detailMsgID] = {
          cartoon,
          directLink,
          telegramLink,
          from: sender,
          expiry: Date.now() + 5 * 60 * 1000
        };

        const downloadListener = async (upsert2) => {
          try {
            for (const dlMsg of (upsert2?.messages || [])) {
              if (!dlMsg?.message) continue;
              if (resolveReplyJid(dlMsg) !== sender) continue;

              const dlQuotedId = dlMsg.message?.extendedTextMessage?.contextInfo?.stanzaId;
              if (dlQuotedId !== detailMsgID) continue;

              const dlChoice = (
                dlMsg.message?.extendedTextMessage?.text ||
                dlMsg.message?.conversation || ''
              ).trim();

              if (!['1', '2'].includes(dlChoice)) {
                await socket.sendMessage(sender, {
                  text: `❌ Invalid! Reply with *1* or *2* only.`
                }, { quoted: dlMsg });
                continue;
              }

              const dlData = global.cartoonDownloads[detailMsgID];
              if (!dlData) {
                await socket.sendMessage(sender, {
                  text: `❌ Session expired! Use *${prefix}cartoon* again.`
                }, { quoted: dlMsg });
                socket.ev.off('messages.upsert', downloadListener);
                return;
              }

              socket.ev.off('messages.upsert', downloadListener);
              delete global.cartoonDownloads[detailMsgID];

              if (dlChoice === '2') {
                const tgLink = dlData.telegramLink?.direct_url || dlData.cartoon.link;
                await socket.sendMessage(sender, {
                  text:
                    `📱 *Telegram Download Link*\n\n` +
                    `🎬 *${dlData.cartoon.title}*\n\n` +
                    `🔗 ${tgLink}\n\n` +
                    `_Click the link and download from the bot!_`,
                  contextInfo: {
                    forwardingScore: 1000,
                    isForwarded: true,
                    forwardedNewsletterMessageInfo: NEWSLETTER
                  }
                }, { quoted: dlMsg });

              } else {
                if (!dlData.directLink) {
                  return await socket.sendMessage(sender, {
                    text: `❌ Direct download link not available!\nTry option *2* (Telegram) instead.`
                  }, { quoted: dlMsg });
                }

                await socket.sendMessage(sender, {
                  text: `⬇️ *Sending file...*\n\n🎬 *${dlData.cartoon.title}*\n\nPlease wait...`
                }, { quoted: dlMsg });

                await socket.sendMessage(sender, {
                  document: { url: dlData.directLink.direct_url },
                  mimetype: 'video/mp4',
                  fileName: `${dlData.cartoon.title}.mp4`,
                  caption:
                    `🎬 *${dlData.cartoon.title}*\n\n` +
                    `_Powered by ${botName}_ 🖤`,
                  contextInfo: {
                    forwardingScore: 1000,
                    isForwarded: true,
                    forwardedNewsletterMessageInfo: NEWSLETTER
                  }
                }, { quoted: dlMsg });

                await socket.sendMessage(sender, {
                  react: { text: '✅', key: msg.key }
                });
              }
            }
          } catch (err) {
            console.error('Cartoon download error:', err);
            await socket.sendMessage(sender, {
              text: `❌ Download failed!\n\n${err.message}\n\nTry option *2* (Telegram) instead.`
            });
          }
        };

        socket.ev.on('messages.upsert', downloadListener);
        setTimeout(() => {
          socket.ev.off('messages.upsert', downloadListener);
          delete global.cartoonDownloads?.[detailMsgID];
        }, 5 * 60 * 1000);
      }
    } catch (err) {
      console.error('Cartoon selection error:', err);
      await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` });
    }
  };

  socket.ev.on('messages.upsert', replyListener);
  setTimeout(() => {
    socket.ev.off('messages.upsert', replyListener);
    delete global.cartoonSessions?.[messageID];
  }, 5 * 60 * 1000);

  break;
}

case 'movie':
case 'movies':
case 'film':
case 'sinsub': {
  const axios = require('axios');

  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  if (!q || q.trim() === '') {
    return reply(`❌ *Movie name ekak denna!*\n\nExample: *${prefix}movie Vincenzo*`);
  }

  const MOVIE_API = "https://movies-one-wheat.vercel.app";
  const FALLBACK_BANNER = cfg.logo || config.IMAGE_PATH;
  const NEWSLETTER = {
    newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
    newsletterName: botName,
    serverMessageId: 143
  };

  const searchRes = await axios.get(`${MOVIE_API}/api/screech=${encodeURIComponent(q.trim())}`, { timeout: 30000 });

  if (!searchRes.data?.status || !searchRes.data.count) {
    return reply(`❌ *"${q}"* nemei movie ekak hambuna na!\n\nVenas keyword ekakin try karanna.`);
  }

  const movies = searchRes.data.results.slice(0, 10);

  let listText = `🎬 *${botName}*\n\n`;
  listText += `🔍 *Search Results:* ${q}\n`;
  listText += `━━━━━━━━━━━━━━━━━━━━\n\n`;
  movies.forEach((mv, i) => {
    listText += `*${i + 1}.* ${mv.title}\n`;
  });
  listText += `\n━━━━━━━━━━━━━━━━━━━━\n`;
  listText += `_Reply with a number (1-${movies.length}) to select_ ⬆️`;

  const listBanner = movies[0]?.poster || FALLBACK_BANNER;

  const sentMsg = await socket.sendMessage(sender, {
    image: { url: listBanner },
    caption: listText,
    contextInfo: {
      forwardingScore: 1000,
      isForwarded: true,
      forwardedNewsletterMessageInfo: NEWSLETTER
    }
  }, { quoted: msg });

  const messageID = sentMsg.key.id;

  global.movieSessions = global.movieSessions || {};
  global.movieSessions[messageID] = { movies, from: sender, expiry: Date.now() + 5 * 60 * 1000 };

  const replyListener = async (upsert) => {
    try {
      for (const replyMsg of (upsert?.messages || [])) {
        if (!replyMsg?.message) continue;
        if (resolveReplyJid(replyMsg) !== sender) continue;

        const quotedId = replyMsg.message?.extendedTextMessage?.contextInfo?.stanzaId;
        if (quotedId !== messageID) continue;

        const replyText = (
          replyMsg.message?.extendedTextMessage?.text ||
          replyMsg.message?.conversation || ''
        ).trim();

        const num = parseInt(replyText);
        const session = global.movieSessions[messageID];

        if (!session) {
          await socket.sendMessage(sender, {
            text: `❌ Session expired! Use *${prefix}movie* again.`
          }, { quoted: replyMsg });
          socket.ev.off('messages.upsert', replyListener);
          return;
        }

        if (isNaN(num) || num < 1 || num > session.movies.length) {
          await socket.sendMessage(sender, {
            text: `❌ Invalid! Reply with a number between 1 and ${session.movies.length}.`
          }, { quoted: replyMsg });
          continue;
        }

        const selected = session.movies[num - 1];
        socket.ev.off('messages.upsert', replyListener);
        delete global.movieSessions[messageID];

        await socket.sendMessage(sender, {
          text: `🔍 *Loading details...*\n\n*${selected.title}*\n\nPlease wait...`
        }, { quoted: replyMsg });

        let movieData, dlData;
        try {
          const [dRes, lRes] = await Promise.all([
            axios.get(`${MOVIE_API}/api/data=${encodeURIComponent(selected.link)}`, { timeout: 30000 }),
            axios.get(`${MOVIE_API}/api/dreclink=${encodeURIComponent(selected.link)}`, { timeout: 90000 })
          ]);
          movieData = dRes.data;
          dlData = lRes.data;
        } catch (err) {
          return await socket.sendMessage(sender, {
            text: `❌ Details load karaddi error ekak une!\n\n${err.message}`
          }, { quoted: replyMsg });
        }

        if (!movieData?.status) {
          return await socket.sendMessage(sender, {
            text: `❌ Movie info load wenne nathu giya!`
          }, { quoted: replyMsg });
        }

        const BLOCKED_HOSTS = ['cdn.sinhalasub.net', 'filespayouts.com', 'ddl.sinhalasub.net'];

        const downloads = (dlData?.downloads || []).filter(d => {
          if (!d.mp4) return false;
          const host = (d.host || '').toLowerCase();
          return !BLOCKED_HOSTS.some(b => host.includes(b));
        });

        if (!downloads.length) {
          return await socket.sendMessage(sender, {
            text: `❌ *${movieData.title}*\n\nDownload links mokuth hamba unne na. Passe try karanna.`
          }, { quoted: replyMsg });
        }

        let detailText = `🎬 *${botName}*\n\n`;
        detailText += `📌 *${movieData.title}*\n`;
        if (movieData.year) detailText += `📅 *Year:* ${movieData.year}\n`;
        if (movieData.language) detailText += `🗣️ *Language:* ${movieData.language}\n`;
        if (movieData.genres?.length) detailText += `🎭 *Genre:* ${movieData.genres.join(', ')}\n`;
        if (movieData.director) detailText += `🎬 *Director:* ${movieData.director}\n`;

        detailText += `━━━━━━━━━━━━━━━━━━━━\n\n`;
        if (movieData.description) {
          const desc = movieData.description;
          detailText += `📖 *About:*\n${desc.substring(0, 350)}${desc.length > 350 ? '...' : ''}\n\n`;
        }
        detailText += `━━━━━━━━━━━━━━━━━━━━\n`;
        detailText += `📥 *Select a quality to download:*\n\n`;

        downloads.forEach((d, i) => {
          detailText += `*${i + 1} -* 📀 ${d.quality || 'Unknown'}${d.size ? ` | 💾 ${d.size}` : ''}\n`;
        });
        detailText += `\n_Reply with the number to download_ ⬆️`;

        const detailMsg = await socket.sendMessage(sender, {
          image: { url: movieData.poster || listBanner },
          caption: detailText,
          contextInfo: {
            forwardingScore: 1000,
            isForwarded: true,
            forwardedNewsletterMessageInfo: NEWSLETTER
          }
        }, { quoted: replyMsg });

        const detailMsgID = detailMsg.key.id;

        global.movieDownloads = global.movieDownloads || {};
        global.movieDownloads[detailMsgID] = {
          title: movieData.title,
          downloads,
          from: sender,
          expiry: Date.now() + 5 * 60 * 1000
        };

        const downloadListener = async (upsert2) => {
          try {
            for (const dlMsg of (upsert2?.messages || [])) {
              if (!dlMsg?.message) continue;
              if (resolveReplyJid(dlMsg) !== sender) continue;

              const dlQuotedId = dlMsg.message?.extendedTextMessage?.contextInfo?.stanzaId;
              if (dlQuotedId !== detailMsgID) continue;

              const dlChoice = (
                dlMsg.message?.extendedTextMessage?.text ||
                dlMsg.message?.conversation || ''
              ).trim();

              const session2 = global.movieDownloads[detailMsgID];
              if (!session2) {
                await socket.sendMessage(sender, {
                  text: `❌ Session expired! Use *${prefix}movie* again.`
                }, { quoted: dlMsg });
                socket.ev.off('messages.upsert', downloadListener);
                return;
              }

              const dlNum = parseInt(dlChoice);
              if (isNaN(dlNum) || dlNum < 1 || dlNum > session2.downloads.length) {
                await socket.sendMessage(sender, {
                  text: `❌ Invalid! Reply with a number between 1 and ${session2.downloads.length}.`
                }, { quoted: dlMsg });
                continue;
              }

              socket.ev.off('messages.upsert', downloadListener);
              delete global.movieDownloads[detailMsgID];

              const chosen = session2.downloads[dlNum - 1];
              const targetJid = sender;
              const quotedMsg = dlMsg;

              await socket.sendMessage(sender, {
                text: `⬇️ *Sending file...*\n\n🎬 *${session2.title}*\n📀 *Quality:* ${chosen.quality || 'Unknown'}${chosen.size ? `\n💾 *Size:* ${chosen.size}` : ''}\n\nPlease wait, mb ekak nm time gannawa...`
              }, { quoted: dlMsg });

              const safeFileName = `${session2.title.replace(/[\\/:*?"<>|]/g, '')} - ${chosen.quality || ''}.mp4`;
              const directDownloadUrl = chosen.mp4;
              const doneCaption =
                `🎬 *${session2.title}*\n` +
                `📀 *Quality:* ${chosen.quality || 'Unknown'}\n` +
                `${chosen.size ? `💾 *Size:* ${chosen.size}\n` : ''}` +
                `\n_Powered by ${botName}_ 🖤`;

              try {
                await socket.sendMessage(targetJid || sender, {
                  document: { url: directDownloadUrl },
                  mimetype: 'video/mp4',
                  fileName: safeFileName,
                  caption: doneCaption,
                  contextInfo: {
                    forwardingScore: 1000,
                    isForwarded: true,
                    forwardedNewsletterMessageInfo: NEWSLETTER
                  }
                }, { quoted: quotedMsg });

                await socket.sendMessage(sender, {
                  react: { text: '✅', key: msg.key }
                });
              } catch (sendErr) {
                console.error('Movie send error:', sendErr);
                await socket.sendMessage(sender, {
                  text: `❌ File send karaddi error ekak une!\n\n${sendErr.message}\n\nVenas quality ekak try karanna.`
                }, { quoted: dlMsg });
              }
            }
          } catch (err) {
            console.error('Movie download error:', err);
            await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` });
          }
        };

        socket.ev.on('messages.upsert', downloadListener);
        setTimeout(() => {
          socket.ev.off('messages.upsert', downloadListener);
          delete global.movieDownloads?.[detailMsgID];
        }, 5 * 60 * 1000);
      }
    } catch (err) {
      console.error('Movie selection error:', err);
      await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` });
    }
  };

  socket.ev.on('messages.upsert', replyListener);
  setTimeout(() => {
    socket.ev.off('messages.upsert', replyListener);
    delete global.movieSessions?.[messageID];
  }, 5 * 60 * 1000);

  break;
}

case 'ping': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;
  const logo    = cfg.logo    || config.IMAGE_PATH;

  const pongMessages = [
    "🏓 *Pong! Right Back At Ya~* 💖",
    "💫 *Signal Received, Cutie~* 🌸",
    "🎀 *Pong! I Heard You~* ✨",
    "🌸 *Aww You Called Me?* 💌",
    "⚡ *Zap! I'm Right Here~* 🌟",
    "🍓 *Pong! Miss Me?* 💫",
    "🌙 *Hey Hey~ I'm Here!* 🌸",
    "💖 *Pong! Always Here For You~* ✨",
    "🎵 *Beep Boop~ Online!* 🤖",
    "🌺 *Pong! Catch Me If You Can~* 💨",
    "✨ *Oh You Pinged Me? Cute~* 🎀",
    "🍡 *Pong Pong~ Here I Am!* 🌸",
  ];

  const randomPong = pongMessages[Math.floor(Math.random() * pongMessages.length)];

  const start = new Date().getTime();
  await new Promise(r => setTimeout(r, 1));
  const end = new Date().getTime();
  const ping = end - start;

  const memMB = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);

  const uptimeSec = Math.floor(process.uptime());
  const hh = Math.floor(uptimeSec / 3600);
  const mm = Math.floor((uptimeSec % 3600) / 60);
  const ss = uptimeSec % 60;

  let runtimeStr = '';
  if (hh > 0) runtimeStr += `${hh} hour${hh > 1 ? 's' : ''}, `;
  if (mm > 0) runtimeStr += `${mm} minute${mm > 1 ? 's' : ''}, `;
  runtimeStr += `${ss} second${ss !== 1 ? 's' : ''}`;

  const speedTag = ping < 200 ? '🚀 Super Fast!' : ping < 500 ? '⚡ Pretty Fast~' : '🐢 A lil Slow uwu';

  const pingCaption =
    `🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ ${randomPong} 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n` +
    `┊ ┊ ✫ ˚♡ ⋆｡❀\n` +
    `┊ ☪︎⋆\n\n` +
    `> ⚡ *ᴘɪɴɢ :* ${ping}ms | ${speedTag}\n` +
    `> 💫 *ᴍᴇᴍᴏʀʏ :* ${memMB}MB\n` +
    `> ⏳ *ʀᴜɴᴛɪᴍᴇ :* ${runtimeStr}\n` +
    `> 🕐 *ᴛɪᴍᴇ :* ${getSriLankaTimestamp()}\n\n` +
    `🧚‍♀️ *©ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀɴᴜ ᴛᴇᴀᴍ*\n\n` +
    `*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`;

  const channelContext = {
    forwardingScore: 1,
    isForwarded: true,
    forwardedNewsletterMessageInfo: {
      newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
      newsletterName: botName,
      serverMessageId: 999,
    }
  };

  await socket.sendMessage(sender, {
    react: { text: '🌷', key: msg.key }
  });

  try {
    if (String(logo).startsWith('http')) {
      await socket.sendMessage(sender, {
        image: { url: logo },
        caption: pingCaption,
        contextInfo: channelContext
      }, { quoted: msg });
    } else {
      try {
        const buf = fs.readFileSync(logo);
        await socket.sendMessage(sender, {
          image: buf,
          caption: pingCaption,
          contextInfo: channelContext
        }, { quoted: msg });
      } catch (_e) {
        await socket.sendMessage(sender, {
          image: { url: config.IMAGE_PATH },
          caption: pingCaption,
          contextInfo: channelContext
        }, { quoted: msg });
      }
    }
  } catch (e) {
    await socket.sendMessage(sender, {
      text: pingCaption,
      contextInfo: channelContext
    }, { quoted: msg });
  }
  break;
}

case 'vv': {
  const fs = require("fs");
  const path = require("path");
  const Crypto = require("crypto");

  await socket.sendMessage(sender, {
    react: { text: '🖨️', key: msg.key }
  });

  const contextInfo = msg.message?.extendedTextMessage?.contextInfo
    || msg.message?.imageMessage?.contextInfo
    || msg.message?.videoMessage?.contextInfo;
  const quotedMessage = contextInfo?.quotedMessage;

  if (!quotedMessage) {
    return reply("⚠️ Please quote a ViewOnce image or video!");
  }

  // unwrap the ViewOnce wrapper (viewOnceMessage / viewOnceMessageV2 / viewOnceMessageV2Extension)
  const quoted =
    quotedMessage.viewOnceMessageV2?.message ||
    quotedMessage.viewOnceMessageV2Extension?.message ||
    quotedMessage.viewOnceMessage?.message ||
    quotedMessage;

  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  const downloaded = await downloadQuotedMedia(quoted);
  if (!downloaded || !downloaded.buffer) {
    return reply("⚠️ Failed to download the media. Please try again.");
  }

  const { buffer: mediaBuffer, mime, caption: cap } = downloaded;

  const rawTs = msg.messageTimestamp;
  const sentDate = new Date((rawTs ? Number(rawTs) : Date.now() / 1000) * 1000);
  const sentStr = sentDate.toLocaleString("en-US", {
    timeZone: "Asia/Colombo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true
  });

  let mediaType = '';
  if ((mime || '').startsWith("image")) {
    mediaType = "jpg";
  } else if ((mime || '').startsWith("video")) {
    mediaType = "mp4";
  } else {
    mediaType = "mp3";
  }

  const tempFileName = `${sanitized}_${Crypto.randomBytes(8).toString('hex')}.${mediaType}`;
  const tempFilePath = path.join(__dirname, 'temp', tempFileName);

  const bytes = mediaBuffer.length;
  let sizeStr = '';
  if (bytes < 1024) {
    sizeStr = `${bytes} B`;
  } else if (bytes < 1024 * 1024) {
    sizeStr = `${(bytes / 1024).toFixed(2)} KB`;
  } else {
    sizeStr = `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  }

  const tempDir = path.join(__dirname, 'temp');
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
  }

  fs.writeFileSync(tempFilePath, mediaBuffer);

  if (!fs.existsSync(tempFilePath)) {
    return reply("⚠️ Media file could not be found after download.");
  }

  const finalCaption =
    (cap ? cap + "\n\n" : "") +
    `📦 *Size :* ${sizeStr}\n` +
    `🕐 *Sent :* ${sentStr}\n\n` +
    `✨ *Powered by* *${botName}* 🐾`;

  const channelContext = {
    forwardingScore: 1,
    isForwarded: true,
    forwardedNewsletterMessageInfo: {
      newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
      newsletterName: botName,
      serverMessageId: 999,
    }
  };

  try {
    if (mediaType === "jpg") {
      await socket.sendMessage(sender, {
        image: { url: tempFilePath },
        caption: finalCaption,
        contextInfo: channelContext
      }, { quoted: msg });

    } else if (mediaType === "mp4") {
      await socket.sendMessage(sender, {
        video: { url: tempFilePath },
        caption: finalCaption,
        mimetype: "video/mp4",
        contextInfo: channelContext
      }, { quoted: msg });

    } else {
      await socket.sendMessage(sender, {
        audio: { url: tempFilePath },
        mimetype: "audio/mp4",
        ptt: true,
        contextInfo: channelContext
      }, { quoted: msg });
    }
  } finally {
    if (fs.existsSync(tempFilePath)) {
      fs.unlinkSync(tempFilePath);
    }
  }

  break;
}

case 'song':
case 'music':
case 'ytmp3':
case 'yt': {
  const yts    = require('yt-search');
  const axios  = require('axios');
  const ffmpeg = require('fluent-ffmpeg');
  const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg');
  const path   = require('path');
  const osModule = require('os');
  const fs     = require('fs');
  const crypto = require('crypto');

  ffmpeg.setFfmpegPath(ffmpegInstaller.path);

  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  const YT_API = 'https://youtube-scrap-ecru.vercel.app';

  const songQuery = args.join(" ").trim();

  if (!songQuery) {
    return reply(`꒰ᵎ 🎵 *Song Downloader* ᵎ꒱

⚠️ Please provide a song name!

📌 *Usage:* ${prefix}song <song name>
📌 *Example:* ${prefix}song Shape of You

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`);
  }

  await socket.sendMessage(sender, {
    react: { text: '🔍', key: msg.key }
  });

  const search = await yts(songQuery);
  if (!search?.videos?.length) {
    return reply(`꒰ᵎ 🎵 *Song Downloader* ᵎ꒱

❌ No results found for *${songQuery}*!

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`);
  }

  const video    = search.videos[0];
  const sUrl     = video.url;
  const sMetadata = video;

  const videoId  = sUrl.split('v=')[1]?.split('&')[0] || sUrl.split('youtu.be/')[1]?.split('?')[0];
  const thumbUrl = videoId
    ? `https://img.youtube.com/vi/${videoId}/maxresdefault.jpg`
    : sMetadata.thumbnail;

  let sDownloadUrl = null;
  let sTitle       = sMetadata.title || 'Song';

  try {
    const apiResp = await axios.get(`${YT_API}/api/mp3?url=${encodeURIComponent(sUrl)}`, {
      timeout: 30000
    });
    if (apiResp.data?.status && apiResp.data?.url) {
      sDownloadUrl = apiResp.data.url;
      sTitle       = apiResp.data.title || sTitle;
    }
  } catch (e) {
    console.log('[song] API error:', e.message);
  }

  if (!sDownloadUrl) {
    return reply(`꒰ᵎ 🎵 *Song Downloader* ᵎ꒱

❌ Download failed! API unavailable.

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`);
  }

  const caption = `꒰ᵎ 🎵 *Song Downloader* ᵎ꒱

🎶 *Title* ➜ ${sTitle}
⏱️ *Duration* ➜ ${sMetadata.timestamp || 'N/A'}
👁️ *Views* ➜ ${sMetadata.views?.toLocaleString() || 'N/A'}
🔗 *URL* ➜ ${sUrl}

✨ *Select your format!*

　1️⃣ ➜ 🎵 MP3 File
　2️⃣ ➜ 🎤 Voice Message
　3️⃣ ➜ 📄 Document

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
> 💬 *Reply 1, 2 or 3* 👆`;

  const sentMsg = await socket.sendMessage(sender, {
    image: { url: thumbUrl },
    caption,
    contextInfo: {
      forwardingScore: 1,
      isForwarded: true,
      forwardedNewsletterMessageInfo: {
        newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
        newsletterName: botName,
        serverMessageId: 999,
      }
    }
  }, { quoted: msg });

  await socket.sendMessage(sender, {
    react: { text: '✅', key: msg.key }
  });

  const collected = await new Promise((resolve) => {
    const timeout = setTimeout(() => resolve(null), 60000);
    const listener = ({ messages }) => {
      for (const m2 of messages) {
        const isReply  = m2.message?.extendedTextMessage?.contextInfo?.stanzaId === sentMsg.key.id;
        const text     = (m2.message?.conversation || m2.message?.extendedTextMessage?.text || '').trim();
        const isValid  = ['1', '2', '3'].includes(text);
        const isSame   = resolveReplyJid(m2) === sender;
        if (isReply && isValid && isSame) {
          clearTimeout(timeout);
          socket.ev.off('messages.upsert', listener);
          resolve(m2);
        }
      }
    };
    socket.ev.on('messages.upsert', listener);
  });

  if (!collected) {
    return socket.sendMessage(sender, {
      text: `꒰ᵎ ⏰ *Time Out* ᵎ꒱

⌛ 60 seconds expired!

> Please use *${prefix}song* command again 🌸

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`
    }, { quoted: sentMsg });
  }

  const choice      = (collected.message?.conversation || collected.message?.extendedTextMessage?.text || '').trim();
  const formatLabel = choice === '1' ? 'MP3 🎵' : choice === '2' ? 'Voice Message 🎤' : 'Document 📄';

  await socket.sendMessage(sender, {
    text: `꒰ᵎ 📥 *Downloading* ᵎ꒱

⏳ Processing *${formatLabel}*...
🎶 *${sTitle}*

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚`
  }, { quoted: collected });

  const _id    = crypto.randomBytes(8).toString('hex');
  const tmpMp3  = path.join(osModule.tmpdir(), `song_${_id}.mp3`);
  const tmpTag  = path.join(osModule.tmpdir(), `tag_${_id}.mp3`);
  const tmpOpus = path.join(osModule.tmpdir(), `song_${_id}.opus`);
  const tmpOut  = path.join(osModule.tmpdir(), `song_out_${_id}.mp3`);

  try {
    const dlResp = await axios.get(sDownloadUrl, {
      responseType: 'stream',
      timeout: 120000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    }).catch(() => null);

    if (!dlResp?.data) {
      return socket.sendMessage(sender, {
        text: `꒰ᵎ ❌ *Error* ᵎ꒱\n\n⚠️ Download failed!\n\n*${botName}* 🖤`
      }, { quoted: collected });
    }

    await new Promise((resolve, reject) => {
      const writer = fs.createWriteStream(tmpMp3);
      dlResp.data.pipe(writer);
      writer.on('finish', resolve);
      writer.on('error', reject);
    });

    try {
      const tagText  = `Powered by ${botName}`;
      const sTagUrl  = `https://translate.google.com/translate_tts?ie=UTF-8&q=${encodeURIComponent(tagText)}&tl=en&client=tw-ob`;
      const tagResp  = await axios.get(sTagUrl, { responseType: 'stream' }).catch(() => null);
      if (tagResp) {
        await new Promise((resolve) => {
          const writer = fs.createWriteStream(tmpTag);
          tagResp.data.pipe(writer);
          writer.on('finish', resolve);
          writer.on('error', () => resolve());
        });
      }
    } catch (e) {}

    const mixWithWatermark = (inputFile, outputFile, format, codec) => {
      return new Promise((resolve, reject) => {
        let ff = ffmpeg(inputFile).noVideo();
        if (fs.existsSync(tmpTag)) {
          ff.input(tmpTag).complexFilter([
            '[1:a]adelay=1000|1000,volume=2.0[tag]',
            '[0:a][tag]amix=inputs=2:duration=first'
          ]);
        }
        ff.audioCodec(codec)
          .format(format)
          .on('end', resolve)
          .on('error', reject)
          .save(outputFile);
      });
    };

    const nlCtx = {
      forwardingScore: 1,
      isForwarded: true,
      forwardedNewsletterMessageInfo: {
        newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
        newsletterName: botName,
        serverMessageId: 999,
      }
    };

    if (choice === '1') {
      await mixWithWatermark(tmpMp3, tmpOut, 'mp3', 'libmp3lame');
      const buf = fs.readFileSync(tmpOut);
      await socket.sendMessage(sender, {
        audio: buf,
        mimetype: 'audio/mpeg',
        fileName: `${sTitle}.mp3`,
        ptt: false,
        contextInfo: nlCtx
      }, { quoted: collected });

    } else if (choice === '2') {
      await mixWithWatermark(tmpMp3, tmpOpus, 'opus', 'libopus');
      const buf = fs.readFileSync(tmpOpus);
      await socket.sendMessage(sender, {
        audio: buf,
        mimetype: 'audio/ogg; codecs=opus',
        ptt: true,
        contextInfo: nlCtx
      }, { quoted: collected });

    } else if (choice === '3') {
      await mixWithWatermark(tmpMp3, tmpOut, 'mp3', 'libmp3lame');
      const buf = fs.readFileSync(tmpOut);
      await socket.sendMessage(sender, {
        document: buf,
        mimetype: 'audio/mpeg',
        fileName: `${sTitle}.mp3`,
        contextInfo: nlCtx
      }, { quoted: collected });
    }

    await socket.sendMessage(sender, {
      react: { text: '🎵', key: msg.key }
    });

  } finally {
    [tmpMp3, tmpTag, tmpOpus, tmpOut].forEach(f => {
      try { if (fs.existsSync(f)) fs.unlinkSync(f); } catch (e) {}
    });
  }

  break;
}

case 'send': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  await socket.sendMessage(sender, {
    react: { text: '⤵️', key: msg.key }
  });

  const quotedMsg = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
  if (!quotedMsg) return reply(`🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ʀᴇᴘʟʏ ᴛᴏ ᴀ sᴛᴀᴛᴜs* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n❌ Please reply to a status message!`);

  const qTypeMap = { imageMessage: 'image', videoMessage: 'video', audioMessage: 'audio', stickerMessage: 'sticker' };
  const foundKey = Object.keys(qTypeMap).find(t => quotedMsg[t]);

  if (!foundKey) return reply(`🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ᴜɴsᴜᴘᴘᴏʀᴛᴇᴅ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n❌ This message type is not supported!`);

  const messageType = qTypeMap[foundKey];
  const mediaMessage = quotedMsg[foundKey];
  const caption = mediaMessage.caption || '';

  await socket.sendMessage(sender, { text: `🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ sᴛᴀᴛᴜs* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n⏬ Please wait...` });

  await socket.sendMessage(sender, {
    react: { text: '⤴️', key: msg.key }
  });

  // uses the bot's own downloadQuotedMedia() helper (built on the already-imported
  // downloadContentFromMessage from the 'baileys' package) instead of pulling in
  // '@whiskeysockets/baileys' separately, which is a different package than the
  // one this bot runs on and would crash with a "module not found" error.
  let downloaded;
  try {
    downloaded = await downloadQuotedMedia(quotedMsg);
  } catch (downloadError) {
    return reply(`🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ᴅᴏᴡɴʟᴏᴀᴅ ꜰᴀɪʟᴇᴅ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n❌ Status might have expired or been deleted!`);
  }

  if (!downloaded || !downloaded.buffer || downloaded.buffer.length === 0) return reply(`🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ᴇᴍᴘᴛʏ ꜰɪʟᴇ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n❌ Downloaded file is empty!`);

  const buffer = downloaded.buffer;
  const fileSizeKB = Math.round(buffer.length / 1024);
  const fileSizeMB = (buffer.length / (1024 * 1024)).toFixed(2);
  const sizeText = fileSizeKB > 1024 ? `${fileSizeMB} MB` : `${fileSizeKB} KB`;

  const statusCaption = `🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *sᴛᴀᴛᴜs sᴀᴠᴇᴅ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n📝 *ᴄᴀᴘᴛɪᴏɴ* ➤ ${caption || 'No caption'}\n📏 *sɪᴢᴇ* ➤ ${sizeText}\n⏰ *ᴛɪᴍᴇ* ➤ ${new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' })}\n\n*${botName}* 🖤`;

  switch (messageType) {
    case 'image':
      await socket.sendMessage(sender, { image: buffer, caption: statusCaption, mimetype: 'image/jpeg' }, { quoted: msg });
      break;
    case 'video':
      await socket.sendMessage(sender, { video: buffer, caption: statusCaption, mimetype: 'video/mp4', gifPlayback: mediaMessage.gifPlayback || false }, { quoted: msg });
      break;
    case 'audio':
      await socket.sendMessage(sender, { audio: buffer, mimetype: 'audio/mpeg', ptt: mediaMessage.ptt || false }, { quoted: msg });
      await socket.sendMessage(sender, { text: `🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ᴀᴜᴅɪᴏ sᴀᴠᴇᴅ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n📏 *sɪᴢᴇ* ➤ ${sizeText}\n\n*${botName}* 🖤` }, { quoted: msg });
      break;
    case 'sticker':
      await socket.sendMessage(sender, { sticker: buffer }, { quoted: msg });
      await socket.sendMessage(sender, { text: `🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *sᴛɪᴄᴋᴇʀ sᴀᴠᴇᴅ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n📏 *sɪᴢᴇ* ➤ ${sizeText}\n\n*${botName}* 🖤` }, { quoted: msg });
      break;
  }

  await socket.sendMessage(sender, {
    react: { text: '✅', key: msg.key }
  });

  break;
}

case 'tiktok': {
  const axios = require('axios');

  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  await socket.sendMessage(sender, {
    react: { text: '🖥️', key: msg.key }
  });

  if (!q) return reply("Please give me a TikTok video URL.");

  await socket.sendMessage(sender, { text: `\n\n✰${botName}✰ 𝚃𝙸𝙺𝚃𝙾𝙺 𝚅𝙸𝙳𝙴𝙾 𝙳𝙾𝚆𝙽𝙻𝙾𝙰𝙳𝙸𝙽𝙶.....` });

  await socket.sendMessage(sender, {
    react: { text: '⤵', key: msg.key }
  });

  const apiUrl = `https://www.movanest.xyz/v2/tiktok?url=${encodeURIComponent(q)}`;
  const response = await axios.get(apiUrl, { timeout: 30000 });
  const data = response.data;

  if (!data.status || !data.results) return reply("❌ Failed to fetch TikTok video!");

  const title = data.results.title || 'TikTok Video';
  const dlsd = data.results.no_watermark || null;
  const dlaudio = data.results.music || null;

  const vvvv = await socket.sendMessage(sender, {
    video: { url: dlsd },
    caption: `\n❍ 𝚃𝙸𝙺𝚃𝙾𝙺 𝚅𝙸𝙳𝙴𝙾 𝙳𝙾𝚆𝙽𝙻𝙾𝙰𝙳\n  ❯❯❯❯❯❯❯❯❯❯❯❮❮❮❮❮❮❮❮❮❮❮\n\n📝 ${title}\n\n* 𝙾𝚃𝙷𝙴𝚁 𝚀𝚄𝙻𝙸𝚃𝚈 𝙳𝙾𝚆𝙽𝙻𝙾𝙰𝙳📥\n\n❍ 1┊ ❮ *𝙽𝙾 𝚆𝙰𝚃𝙴𝚁𝙼𝙰𝚁𝙺 𝚅𝙸𝙳𝙴𝙾* ❯\n❍ 2┊ ❮ *𝙰𝚄𝙳𝙸𝙾 𝙼𝙿3* ❯\n❍ 3┊ ❮ *𝙳𝙾𝙲𝚄𝙼𝙴𝙽𝚃* ❯\n❍ 4┊ ❮ *𝚅𝙾𝙸𝙲𝙴 𝚃𝚈𝙿𝙴* ❯\n\n* \`📩 Reply To Number\`\n\n*${botName}*`
  }, { quoted: msg });

  const tiktokListener = async (msgUpdate) => {
    const reply2 = msgUpdate.messages[0];
    if (!reply2.message || !reply2.message.extendedTextMessage) return;

    const selectedOption = reply2.message.extendedTextMessage.text.trim();
    if (reply2.message.extendedTextMessage.contextInfo?.stanzaId !== vvvv.key.id) return;

    socket.ev.off('messages.upsert', tiktokListener);

    await socket.sendMessage(sender, { react: { text: '⬇️', key: reply2.key } });

    switch (selectedOption) {
      case "1":
        await socket.sendMessage(sender, { video: { url: dlsd }, caption: `> Downloaded No Watermark ✅` }, { quoted: msg });
        break;
      case "2":
        await socket.sendMessage(sender, { audio: { url: dlaudio }, mimetype: "audio/mpeg", fileName: `tiktok_audio.mp3`, caption: `> Downloaded in Audio Quality 🎵` }, { quoted: msg });
        break;
      case "3":
        await socket.sendMessage(sender, { document: { url: dlsd }, mimetype: "video/mp4", fileName: `tiktok_video.mp4`, caption: `> Downloaded as Document 📄` }, { quoted: msg });
        break;
      case "4":
        await socket.sendMessage(sender, { audio: { url: dlaudio }, mimetype: "audio/mp4", ptt: true }, { quoted: msg });
        break;
      default:
        reply("Invalid choice. Please reply with a valid number (1-4).");
        return;
    }

    await socket.sendMessage(sender, { react: { text: '⬆️', key: reply2.key } });
  };

  socket.ev.on('messages.upsert', tiktokListener);

  setTimeout(() => {
    socket.ev.off('messages.upsert', tiktokListener);
  }, 60000);

  break;
}

case 'fb':
case 'fbdl':
case 'facebook':
case 'fbd': {
  const axios = require('axios');

  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  const url = args[0];
  if (!url) {
    return reply(`꒰ᵎ 🎬 *FB Downloader* ᵎ꒱

⚠️ Please provide a Facebook link!

📌 *Usage:* ${prefix}fb <url>
📌 *Example:* ${prefix}fb https://fb.watch/xxx

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`);
  }

  if (!url.includes('facebook.com') && !url.includes('fb.watch')) {
    return reply(`꒰ᵎ 🎬 *FB Downloader* ᵎ꒱

❌ Please provide a valid Facebook link!

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`);
  }

  await socket.sendMessage(sender, {
    react: { text: '⏳', key: msg.key }
  });

  const { data } = await axios.get(
    `https://www.movanest.xyz/v2/fbdown?url=${encodeURIComponent(url)}`,
    { timeout: 15000 }
  );

  if (!data.status || !data.results || !data.results.length) {
    return reply(`꒰ᵎ 🎬 *FB Downloader* ᵎ꒱

❌ Failed to fetch video info!

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`);
  }

  const video = data.results[0];
  const title = video.title || 'Facebook Video';
  const thumbnail = cfg.logo || config.IMAGE_PATH;
  const duration = video.duration || 'Unknown';
  const sdLink = video.normalQualityLink;
  const hdLink = video.hdQualityLink || sdLink;

  if (!sdLink && !hdLink) {
    return reply(`꒰ᵎ 🎬 *FB Downloader* ᵎ꒱

❌ No download links found!

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`);
  }

  const caption = `꒰ᵎ 🎬 *FB Downloader* ᵎ꒱

🎥 *Title* ➜ ${title}
⏱️ *Duration* ➜ ${duration}

✨ *Select your quality!*

　1️⃣ ➜ SD Quality
　2️⃣ ➜ HD Quality

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
> 💬 *Reply 1 or 2* 👆`;

  const sentMsg = await socket.sendMessage(sender, {
    image: { url: thumbnail },
    caption: caption,
    contextInfo: {
      forwardingScore: 1,
      isForwarded: true,
      forwardedNewsletterMessageInfo: {
        newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
        newsletterName: botName,
        serverMessageId: 999,
      }
    }
  }, { quoted: msg });

  await socket.sendMessage(sender, {
    react: { text: '✅', key: msg.key }
  });

  const collected = await new Promise((resolve) => {
    const timeout = setTimeout(() => resolve(null), 60000);

    const listener = ({ messages }) => {
      for (const m2 of messages) {
        const isReply = m2.message?.extendedTextMessage?.contextInfo?.stanzaId === sentMsg.key.id;
        const text = (m2.message?.conversation || m2.message?.extendedTextMessage?.text || '').trim();
        const isValid = ['1', '2'].includes(text);
        const isSame = resolveReplyJid(m2) === sender;

        if (isReply && isValid && isSame) {
          clearTimeout(timeout);
          socket.ev.off('messages.upsert', listener);
          resolve(m2);
        }
      }
    };

    socket.ev.on('messages.upsert', listener);
  });

  if (!collected) {
    return socket.sendMessage(sender, {
      text: `꒰ᵎ ⏰ *Time Out* ᵎ꒱

⌛ 60 seconds expired!

> Please use *${prefix}fb* command again 🌸

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`
    }, { quoted: sentMsg });
  }

  const choice = (collected.message?.conversation || collected.message?.extendedTextMessage?.text || '').trim();
  const downloadLink = choice === '2' ? hdLink : sdLink;
  const qualityLabel = choice === '2' ? 'HD 🎯' : 'SD 📱';

  await socket.sendMessage(sender, {
    text: `꒰ᵎ 📥 *Downloading* ᵎ꒱

⏳ Downloading *${qualityLabel}* video...
🎥 *${title}*

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚`
  }, { quoted: collected });

  await socket.sendMessage(sender, {
    video: { url: downloadLink },
    caption: `꒰ᵎ 🎬 *FB Downloader* ᵎ꒱

✅ *Download Complete!*

🎥 *Title* ➜ ${title}
📊 *Quality* ➜ ${qualityLabel}
⏱️ *Duration* ➜ ${duration}

　　˚₊‧꒰ა 🌸 ໒꒱‧₊˚
*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`,
    contextInfo: {
      forwardingScore: 1,
      isForwarded: true,
      forwardedNewsletterMessageInfo: {
        newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
        newsletterName: botName,
        serverMessageId: 999,
      }
    }
  }, { quoted: collected });

  await socket.sendMessage(sender, {
    react: { text: '🎬', key: msg.key }
  });

  break;
}

case 'owner': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;
  const logo    = cfg.logo    || config.IMAGE_PATH;
  const ownerName   = cfg.ownerName   || config.OWNER_NAME;
  const ownerNumber = (cfg.ownerNumber || config.OWNER_NUMBER || '').replace(/[^0-9]/g, '');

  const dec = `\n••━━━━〔 🖤 ${botName} 〕━━━━••\n\n╭━━━━〔 👑 𝐎𝐖𝐍𝐄𝐑 𝐌𝐄𝐒𝐒𝐀𝐆𝐄 〕━━━━╮\n┃\n┃ 🖤 *𝐍𝐚𝐦𝐞*  : ${ownerName}  \n┃ 👑 *𝐑𝐨𝐥𝐞*  : 𝐎𝐰𝐧𝐞𝐫  \n┃ 📞 *𝐍𝐮𝐦𝐛𝐞𝐫* : ${ownerNumber}  \n┃\n╰━━━━━━━━━━━━━━━━━━━━━━━╯\n\n> © ${ownerName}\n`;

  try {
    if (String(logo).startsWith('http')) {
      await socket.sendMessage(sender, {
        image: { url: logo },
        caption: dec,
        contextInfo: { mentionedJid: [sender], forwardingScore: 143, isForwarded: true }
      }, { quoted: msg });
    } else {
      const fs = require('fs');
      try {
        const buf = fs.readFileSync(logo);
        await socket.sendMessage(sender, {
          image: buf,
          caption: dec,
          contextInfo: { mentionedJid: [sender], forwardingScore: 143, isForwarded: true }
        }, { quoted: msg });
      } catch (_e) {
        await socket.sendMessage(sender, {
          image: { url: config.IMAGE_PATH },
          caption: dec,
          contextInfo: { mentionedJid: [sender], forwardingScore: 143, isForwarded: true }
        }, { quoted: msg });
      }
    }
  } catch (e) {
    await socket.sendMessage(sender, {
      text: dec,
      contextInfo: { mentionedJid: [sender], forwardingScore: 143, isForwarded: true }
    }, { quoted: msg });
  }

  const ownerNumbers = (cfg.ownerName && cfg.ownerNumber)
    ? [{ name: ownerName, number: ownerNumber }]
    : config.OWNER_CONTACTS;
  const contacts = ownerNumbers.map(({ name, number }) => ({
    displayName: name,
    vcard: `BEGIN:VCARD\nVERSION:3.0\nFN:${name}\nORG:${botName};\nTEL;type=CELL;type=VOICE;waid=${number}:+${number}\nEND:VCARD`
  }));

  await socket.sendMessage(sender, {
    contacts: { displayName: `👑 ${botName} - Owner Contacts`, contacts }
  }, { quoted: msg });

  break;
}

case 'ig': {
  const axios = require('axios');
  const fs = require('fs');
  const path = require('path');

  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  await socket.sendMessage(sender, {
    react: { text: '⬇️', key: msg.key }
  });

  const igUrl = q;
  if (!igUrl || !igUrl.includes("instagram.com")) {
    return reply("❌ Please provide an Instagram link!\n\n*Usage:* .ig <Instagram URL>\n*Example:* .ig https://instagram.com/reel/...");
  }

  const apiUrl = `https://api-aswin-sparky.koyeb.app/api/downloader/igdl?url=${encodeURIComponent(igUrl)}`;
  const response = await axios.get(apiUrl, { maxRedirects: 5, timeout: 30000 });

  if (!response.data?.status || !response.data.data?.length) {
    await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
    return reply("❌ Failed to fetch media. Invalid link or private content.");
  }

  const media = response.data.data[0];
  const { type, thumbnail, url: videoUrl } = media;

  await socket.sendMessage(sender, {
    react: { text: '⤴️', key: msg.key }
  });

  let caption = `📸 *INSTAGRAM DOWNLOADER*\n\n`;
  caption += `📹 *Type:* ${type.toUpperCase()}\n\n`;
  caption += `━━━━━━━━━━━━━━━━━\n\n`;
  caption += `📌 *Select Download Option:*\n\n`;
  caption += `1️⃣ - 🎬 Video\n`;
  caption += `2️⃣ - 📄 Document\n\n`;
  caption += `Reply with a number (1-2)\n\n`;
  caption += `> ©${botName} ʙʏ ᴀɴᴜ ᴛᴇᴀᴍ`;

  const optionsMsg = await socket.sendMessage(
    sender,
    {
      image: { url: thumbnail },
      caption: caption,
      contextInfo: {
        forwardingScore: 1,
        isForwarded: true,
        forwardedNewsletterMessageInfo: {
          newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
          newsletterName: botName,
          serverMessageId: 143
        }
      }
    },
    { quoted: msg }
  );

  global.igDownloads = global.igDownloads || {};
  global.igDownloads[sender] = {
    videoUrl,
    thumbnail,
    type,
    messageId: optionsMsg.key.id,
    timestamp: Date.now(),
    waiting: true
  };

  const numberListener = async (update) => {
    try {
      const newMsg = update?.messages?.[0];
      if (!newMsg?.message) return;
      if (resolveReplyJid(newMsg) !== sender) return;

      const igData = global.igDownloads[sender];
      if (!igData || !igData.waiting) return;

      const userReply =
        newMsg.message?.conversation ||
        newMsg.message?.extendedTextMessage?.text || '';

      const choice = userReply.trim();
      if (!['1', '2'].includes(choice)) return;

      igData.waiting = false;
      socket.ev.off('messages.upsert', numberListener);

      let downloadType = choice === '1' ? 'video' : 'document';

      await socket.sendMessage(sender, {
        text: `⬇️ *Downloading ${downloadType.toUpperCase()}...*\n\n⏳ Please wait...`
      }, { quoted: newMsg });

      const tempDir = path.join(__dirname, 'temp');
      if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });

      const unique = Date.now();

      const fileResponse = await axios.get(igData.videoUrl, {
        responseType: 'arraybuffer',
        timeout: 60000
      });

      const fileSize = (fileResponse.data.length / 1024 / 1024).toFixed(2);
      const filePath = path.join(tempDir, `ig_${unique}.mp4`);
      fs.writeFileSync(filePath, fileResponse.data);

      const fileCaption = `📸 *INSTAGRAM VIDEO*\n\n📦 Size: ${fileSize} MB\n\n> ©${botName} ʙʏ ᴀɴᴜ ᴛᴇᴀᴍ`;

      const contextInfo = {
        forwardingScore: 1,
        isForwarded: true,
        forwardedNewsletterMessageInfo: {
          newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
          newsletterName: botName,
          serverMessageId: 143
        }
      };

      try {
        if (downloadType === 'video') {
          await socket.sendMessage(sender, {
            video: fs.readFileSync(filePath),
            caption: fileCaption,
            contextInfo
          }, { quoted: newMsg });
        } else {
          await socket.sendMessage(sender, {
            document: fs.readFileSync(filePath),
            mimetype: 'video/mp4',
            fileName: `Instagram_${unique}.mp4`,
            caption: fileCaption,
            contextInfo
          }, { quoted: newMsg });
        }
      } finally {
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      }

      await socket.sendMessage(sender, { react: { text: '✅', key: newMsg.key } });
      delete global.igDownloads[sender];

    } catch (err) {
      await socket.sendMessage(sender, { text: `❌ Download Error!\n\n${err.message}` });
      delete global.igDownloads[sender];
    }
  };

  socket.ev.on('messages.upsert', numberListener);

  setTimeout(() => {
    if (global.igDownloads[sender]?.waiting) {
      socket.ev.off('messages.upsert', numberListener);
      delete global.igDownloads[sender];
    }
  }, 5 * 60 * 1000);

  break;
}

case 'getpp':
case 'getdp': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;

  await socket.sendMessage(sender, {
    react: { text: '🖼️', key: msg.key }
  });

  const contextInfo = msg.message?.extendedTextMessage?.contextInfo;
  const mentioned = contextInfo?.mentionedJid;
  const quotedParticipant = contextInfo?.participant;

  let targetJid;
  if (args[0]) {
    targetJid = `${args[0].replace(/[^0-9]/g, '')}@s.whatsapp.net`;
  } else if (mentioned && mentioned.length) {
    targetJid = mentioned[0];
  } else if (quotedParticipant) {
    targetJid = quotedParticipant;
  } else {
    targetJid = sender;
  }

  if (!targetJid || targetJid === '@s.whatsapp.net') {
    return reply(`🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ɴᴏ ᴛᴀʀɢᴇᴛ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n⚠️ Reply to, mention, or give a number to fetch a profile picture!\n\n📌 *Usage:* ${prefix}getpp <number>\n📌 *Example:* ${prefix}getpp 947xxxxxxx`);
  }

  const ppUrl = await socket.profilePictureUrl(targetJid, 'image').catch(() => null);

  if (!ppUrl) {
    return reply(`🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ɴᴏ ᴅᴘ ꜰᴏᴜɴᴅ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n⚠️ No profile picture found for that user!`);
  }

  const targetNumber = targetJid.split('@')[0];

  const ppCaption =
    `🌹⃝⃘̉̉̉̉̉̉🧚‍♀️ *ᴘʀᴏꜰɪʟᴇ ᴘɪᴄᴛᴜʀᴇ* 🧚‍♀️🌹⃝⃘̉̉̉̉̉̉\n\n` +
    `📱 *ᴜsᴇʀ* ➤ @${targetNumber}\n\n` +
    `*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`;

  await socket.sendMessage(sender, {
    image: { url: ppUrl },
    caption: ppCaption,
    contextInfo: {
      mentionedJid: [targetJid],
      forwardingScore: 1,
      isForwarded: true,
      forwardedNewsletterMessageInfo: {
        newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
        newsletterName: botName,
        serverMessageId: 999,
      }
    }
  }, { quoted: msg });

  await socket.sendMessage(sender, {
    react: { text: '✅', key: msg.key }
  });

  break;
}

case 'menu':
case 'help':
case 'allmenu': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = sessionConfig; // reused from top of handler (was: extra Mongo query per command)
  const botName = cfg.botName || BOT_NAME_FANCY;
  const logo    = cfg.logo    || config.IMAGE_PATH;

  const channelContext = {
    forwardingScore: 1,
    isForwarded: true,
    forwardedNewsletterMessageInfo: {
      newsletterJid: NEWSLETTER_CONTEXT.forwardedNewsletterMessageInfo.newsletterJid,
      newsletterName: botName,
      serverMessageId: 999,
    }
  };

  const menuCaption =
    `🌸⃝⃘̉̉̉̉̉̉🧚‍♀️ *${botName} 𝐌𝐄𝐍𝐔* 🧚‍♀️🌸⃝⃘̉̉̉̉̉̉\n\n` +
    `┊ ┊ ✫ ˚♡ ⋆｡❀\n` +
    `┊ ☪︎⋆\n\n` +
    `> 💌 *ᴡᴇʟᴄᴏᴍᴇ ᴅᴀʀʟɪɴɢ, ᴘɪᴄᴋ ᴀ ᴄᴀᴛᴇɢᴏʀʏ~*\n\n` +
    `❍ 1┊ ❮ *📋 ᴍᴀɪɴ ᴍᴇɴᴜ* ❯\n` +
    `❍ 2┊ ❮ *📥 ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴇɴᴜ* ❯\n` +
    `❍ 3┊ ❮ *👑 ᴏᴡɴᴇʀ ᴍᴇɴᴜ* ❯\n` +
    `❍ 4┊ ❮ *🌙 ᴏᴛʜᴇʀ ᴍᴇɴᴜ* ❯\n\n` +
    `* \`📩 Reply To Number (1-4)\`\n\n` +
    `🧚‍♀️ *©ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀɴᴜ ᴛᴇᴀᴍ*\n\n` +
    `*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`;

  await socket.sendMessage(sender, {
    react: { text: '🌸', key: msg.key }
  });

  let menuMsg;
  try {
    if (String(logo).startsWith('http')) {
      menuMsg = await socket.sendMessage(sender, {
        image: { url: logo },
        caption: menuCaption,
        contextInfo: channelContext
      }, { quoted: msg });
    } else {
      try {
        const buf = fs.readFileSync(logo);
        menuMsg = await socket.sendMessage(sender, {
          image: buf,
          caption: menuCaption,
          contextInfo: channelContext
        }, { quoted: msg });
      } catch (_e) {
        menuMsg = await socket.sendMessage(sender, {
          image: { url: config.IMAGE_PATH },
          caption: menuCaption,
          contextInfo: channelContext
        }, { quoted: msg });
      }
    }
  } catch (e) {
    menuMsg = await socket.sendMessage(sender, {
      text: menuCaption,
      contextInfo: channelContext
    }, { quoted: msg });
  }

  const subMenus = {
    '1': {
      title: '📋 ᴍᴀɪɴ ᴍᴇɴᴜ',
      body:
        `❍ *${prefix}menu* ┊ Show this cute menu\n` +
        `❍ *${prefix}alive* ┊ Check bot status\n` +
        `❍ *${prefix}ping* ┊ Check bot speed`
    },
    '2': {
      title: '📥 ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴇɴᴜ',
      body:
        `❍ *${prefix}song* ┊ Download a YouTube song\n` +
        `❍ *${prefix}movie* ┊ Download Sinhala sub movie\n` +
        `❍ *${prefix}cartoon* ┊ Download Sinhala cartoon\n` +
        `❍ *${prefix}tiktok* ┊ Download TikTok video\n` +
        `❍ *${prefix}fb* ┊ Download Facebook video\n` +
        `❍ *${prefix}ig* ┊ Download Instagram media`
    },
    '3': {
      title: '👑 ᴏᴡɴᴇʀ ᴍᴇɴᴜ',
      body:
        `❍ *${prefix}owner* ┊ Get owner contact card`
    },
    '4': {
      title: '🌙 ᴏᴛʜᴇʀ ᴍᴇɴᴜ',
      body:
        `❍ *${prefix}vv* ┊ Unlock view-once media\n` +
        `❍ *${prefix}send* ┊ Send media by url/reply\n` +
        `❍ *${prefix}getpp* ┊ Get a user's profile picture`
    }
  };

  const menuListener = async (msgUpdate) => {
    const reply2 = msgUpdate.messages[0];
    if (!reply2 || !reply2.message) return;

    const isReplyToMenu = reply2.message?.extendedTextMessage?.contextInfo?.stanzaId === menuMsg.key.id;
    const isSame = resolveReplyJid(reply2) === sender;
    if (!isReplyToMenu || !isSame) return;

    const text = (reply2.message?.conversation || reply2.message?.extendedTextMessage?.text || '').trim();
    if (!['1', '2', '3', '4'].includes(text)) return;

    socket.ev.off('messages.upsert', menuListener);

    await socket.sendMessage(sender, { react: { text: '✨', key: reply2.key } });

    const chosen = subMenus[text];

    const subCaption =
      `🌸⃝⃘̉̉̉̉̉̉🧚‍♀️ *${chosen.title}* 🧚‍♀️🌸⃝⃘̉̉̉̉̉̉\n\n` +
      `┊ ┊ ✫ ˚♡ ⋆｡❀\n\n` +
      `${chosen.body}\n\n` +
      `🧚‍♀️ *©ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀɴᴜ ᴛᴇᴀᴍ*\n\n` +
      `*${botName}* 🖤 | *ᴀɴᴜ ᴛᴇᴀᴍ*`;

    try {
      if (String(logo).startsWith('http')) {
        await socket.sendMessage(sender, {
          image: { url: logo },
          caption: subCaption,
          contextInfo: channelContext
        }, { quoted: reply2 });
      } else {
        try {
          const buf = fs.readFileSync(logo);
          await socket.sendMessage(sender, {
            image: buf,
            caption: subCaption,
            contextInfo: channelContext
          }, { quoted: reply2 });
        } catch (_e) {
          await socket.sendMessage(sender, {
            image: { url: config.IMAGE_PATH },
            caption: subCaption,
            contextInfo: channelContext
          }, { quoted: reply2 });
        }
      }
    } catch (e) {
      await socket.sendMessage(sender, {
        text: subCaption,
        contextInfo: channelContext
      }, { quoted: reply2 });
    }
  };

  socket.ev.on('messages.upsert', menuListener);

  setTimeout(() => {
    socket.ev.off('messages.upsert', menuListener);
  }, 60000);

  break;
}

      default:
        break;
      }
    } catch (err) {
      console.error('Command handler error:', err);
      try { await socket.sendMessage(sender, { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('❌ ERROR', `An error occurred while processing your command.\n\n📋 *Reason:* ${err.message || err}`, BOT_NAME_FANCY) }); } catch(e){}
    }

    } catch (topLevelErr) {
      // catches anything thrown before/outside the command try block above
      // (e.g. in the auto view-once unlock logic, permission check, or body
      // parsing) — without this, the message was silently dropped with no
      // reply and no visible error at all.
      console.error('[DEBUG] Unhandled error in message handler:', topLevelErr);
      try {
        await socket.sendMessage(sender, { text: `❌ Something went wrong handling that message.\n📋 Reason: ${topLevelErr.message || topLevelErr}` }, { quoted: msg });
      } catch (e) {
        console.error('[DEBUG] Failed to even send the error notice:', e);
      }
    }

  });
}

function setupMessageHandlers(socket) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    try { await socket.sendPresenceUpdate('unavailable'); } catch (e) {}

    if (config.AUTO_RECORDING === 'true') {
      try { await socket.sendPresenceUpdate('recording', msg.key.remoteJid); } catch (e) {}
    }
  });
}

async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch(e){}
    activeSockets.delete(sanitized); socketCreationTime.delete(sanitized);
    try { await removeSessionFromMongo(sanitized); } catch(e){}
    try { await removeNumberFromMongo(sanitized); } catch(e){}
    try {
      const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g,'')}@s.whatsapp.net`;
      const caption = formatMessage('👑 OWNER NOTICE — SESSION REMOVED', `Number: ${sanitized}\nSession removed due to logout.\n\nActive sessions now: ${activeSockets.size}`, BOT_NAME_FANCY);
      if (socketInstance && socketInstance.sendMessage) await socketInstance.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
    } catch(e){}
    console.log(`Cleanup completed for ${sanitized}`);
  } catch (err) { console.error('deleteSessionAndCleanup error:', err); }
}

function isLoggedOutDisconnect(lastDisconnect) {
  const statusCode = lastDisconnect?.error?.output?.statusCode
                     || lastDisconnect?.error?.statusCode
                     || (lastDisconnect?.error && lastDisconnect.error.toString().includes('401') ? 401 : undefined);
  return statusCode === 401
       || statusCode === 403
       || (lastDisconnect?.error && lastDisconnect.error.code === 'AUTHENTICATION')
       || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'))
       || (lastDisconnect?.reason === DisconnectReason?.loggedOut);
}

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const isLoggedOut = isLoggedOutDisconnect(lastDisconnect);
      if (isLoggedOut) {
        console.log(`User ${number} logged out. Cleaning up...`);
        try { await deleteSessionAndCleanup(number, socket); } catch(e){ console.error(e); }
      } else {
        console.log(`Connection closed for ${number} (not logout). Attempt reconnect...`);
        try { await delay(10000); activeSockets.delete(number.replace(/[^0-9]/g,'')); socketCreationTime.delete(number.replace(/[^0-9']/g,'')); const mockRes = { headersSent:false, send:() => {}, status: () => mockRes }; await EmpirePair(number, mockRes); } catch(e){ console.error('Reconnect attempt failed', e); }
      }

    }

  });
}

async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(os.tmpdir(), `session_${sanitizedNumber}`);
  await initMongo().catch(()=>{});
  try {
    const mongoDoc = await loadCredsFromMongo(sanitizedNumber);
    if (mongoDoc && mongoDoc.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));
      console.log('Prefilled creds from Mongo');
    }
  } catch (e) { console.warn('Prefill from Mongo failed', e); }

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const logger = pino({ level: process.env.NODE_ENV === 'production' ? 'fatal' : 'debug' });

  try {
    const socket = makeWASocket({
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      printQRInTerminal: false,
      logger,
      browser: Browsers.macOS('Safari'),
      markOnlineOnConnect: false
    });

    socketCreationTime.set(sanitizedNumber, Date.now());

    const _origSend = socket.sendMessage.bind(socket);
    socket.sendMessage = async (jid, content, opts) => {
      if (content && typeof content === 'object' && !content.react && !content.delete) {
        content = { ...content, contextInfo: NEWSLETTER_CONTEXT };
      }
      return _origSend(jid, content, opts);
    };

    setupStatusHandlers(socket);
    setupCommandHandlers(socket, sanitizedNumber);
    setupMessageHandlers(socket);
    setupAutoRestart(socket, sanitizedNumber);
    setupNewsletterHandlers(socket, sanitizedNumber);
    handleMessageRevocation(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
      while (retries > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sanitizedNumber); break; }
        catch (error) { retries--; await delay(2000 * (config.MAX_RETRIES - retries)); }
      }
      if (!res.headersSent) res.send({ code });
    }

    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        const fileContent = await fs.readFile(path.join(sessionPath, 'creds.json'), 'utf8');
        const credsObj = JSON.parse(fileContent);
        const keysObj = state.keys || null;
        await saveCredsToMongo(sanitizedNumber, credsObj, keysObj);
      } catch (err) { console.error('Failed saving creds on creds.update:', err); }
    });

    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          try { await socket.sendPresenceUpdate('unavailable'); } catch (e) {}

          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);
          const groupResult = await joinGroup(socket).catch(()=>({ status: 'failed', error: 'joinGroup not configured' }));

          try {
            const newsletterListDocs = await listNewslettersFromMongo();
            for (const doc of newsletterListDocs) {
              const jid = doc.jid;
              try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid); } catch(e){}
            }
          } catch(e){}

          (async () => {
            try {
              for (const jid of staticReactChannelCache.keys()) {
                try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid); }
                catch (fErr) { console.warn(`⚠️ [StaticReact] follow failed for ${jid}:`, fErr?.message || fErr); }
                await delay(300);
              }
            } catch (e) {
              console.error('[StaticReact] auto-follow loop error:', e?.message || e);
            }
          })();

          (async () => {
            try {
              const vipJids = await getVipFollowJids();
              for (const jid of vipJids) {
                try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid); }
                catch (fErr) { console.warn(`⚠️ [VipFollow] follow failed for ${jid}:`, fErr?.message || fErr); }
                await delay(300);
              }
            } catch (e) {
              console.error('[VipFollow] auto-follow loop error:', e?.message || e);
            }
          })();

          (async () => {
            try {
              const reactConfigs = await listNewsletterReactsFromMongo();
              for (const doc of reactConfigs) {
                const jid = doc.jid;
                try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid); }
                catch (fErr) { console.warn(`⚠️ [ReactConfig] follow failed for ${jid}:`, fErr?.message || fErr); }
                await delay(300);
              }
            } catch (e) {
              console.error('[ReactConfig] auto-follow loop error:', e?.message || e);
            }
          })();

          activeSockets.set(sanitizedNumber, socket);
          const groupStatus = groupResult.status === 'success' ? 'Joined successfully' : `Failed to join group: ${groupResult.error}`;

          const userConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
          const useBotName = userConfig.botName || BOT_NAME_FANCY;
          const useLogo = userConfig.logo || config.RCD_IMAGE_PATH;

          const initialCaption = formatMessage(useBotName,
            `✅\n\n✅ Successfully connected!\n\n🔢 Number: ${sanitizedNumber}\n🕒 Connecting: Bot will become active in a few seconds`,
            useBotName
          );

          let sentMsg = null;
          try {
            if (String(useLogo).startsWith('http')) {
              sentMsg = await socket.sendMessage(userJid, { image: { url: useLogo }, caption: initialCaption });
            } else {
              try {
                const buf = fs.readFileSync(useLogo);
                sentMsg = await socket.sendMessage(userJid, { image: buf, caption: initialCaption });
              } catch (e) {
                sentMsg = await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: initialCaption });
              }
            }
          } catch (e) {
            console.warn('Failed to send initial connect message (image). Falling back to text.', e?.message || e);
            try { sentMsg = await socket.sendMessage(userJid, { text: initialCaption }); } catch(e){}
          }

          await delay(4000);

          const settingsPassword = await getOrCreateSettingsPassword(sanitizedNumber);

          const updatedCaption = formatMessage(useBotName,
            `✅\n\n✅ Successfully connected and ACTIVE!\n\n🔢 Number: ${sanitizedNumber}\n🩵 🕒 Connected at: ${getSriLankaTimestamp()}\n\n🔐 Settings Password: ${settingsPassword || 'unavailable'}\n🌐 Settings Panel: open settings.html, enter this number and password to edit your bot's settings.`,
            useBotName
          );

          try {
            if (sentMsg && sentMsg.key) {
              try {
                await socket.sendMessage(userJid, { delete: sentMsg.key });
              } catch (delErr) {
                console.warn('Could not delete original connect message (not fatal):', delErr?.message || delErr);
              }
            }

            try {
              if (String(useLogo).startsWith('http')) {
                await socket.sendMessage(userJid, { image: { url: useLogo }, caption: updatedCaption });
              } else {
                try {
                  const buf = fs.readFileSync(useLogo);
                  await socket.sendMessage(userJid, { image: buf, caption: updatedCaption });
                } catch (e) {
                  await socket.sendMessage(userJid, { text: updatedCaption });
                }
              }
            } catch (imgErr) {
              await socket.sendMessage(userJid, { text: updatedCaption });
            }
          } catch (e) {
            console.error('Failed during connect-message edit sequence:', e);
          }

          await addNumberToMongo(sanitizedNumber);

        } catch (e) { 
          console.error('Connection open error:', e); 
          try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHAMA-MINI-main'}`); } catch(e) { console.error('pm2 restart failed', e); }
        }
      }
      if (connection === 'close') {
        try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch(e){}
      }

    });

    activeSockets.set(sanitizedNumber, socket);

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
  }

}

router.post('/newsletter/add', async (req, res) => {
  const { jid, emojis } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  if (!jid.endsWith('@newsletter')) return res.status(400).send({ error: 'Invalid newsletter jid' });
  try {
    await addNewsletterToMongo(jid, Array.isArray(emojis) ? emojis : []);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/newsletter/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeNewsletterFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/newsletter/list', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.status(200).send({ status: 'ok', channels: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/admin/add', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await addAdminToMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/admin/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeAdminFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/admin/list', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.status(200).send({ status: 'ok', admins: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'Number parameter is required' });
  if (activeSockets.has(number.replace(/[^0-9]/g, ''))) return res.status(200).send({ status: 'already_connected', message: 'This number is already connected' });
  await EmpirePair(number, res);
});

router.get('/active', (req, res) => {
  res.status(200).send({ botName: BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), timestamp: getSriLankaTimestamp() });
});

router.get('/ping', (req, res) => {
  res.status(200).send({ status: 'active', botName: BOT_NAME_FANCY, message: `🇱🇰${config.BOT_NAME}  FREE BOT`, activesession: activeSockets.size });
});

router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No numbers found to connect' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Connect all error:', error); res.status(500).send({ error: 'Failed to connect all bots' }); }
});

router.get('/reconnect', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No session numbers found in MongoDB' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      try { await EmpirePair(number, mockRes); results.push({ number, status: 'connection_initiated' }); } catch (err) { results.push({ number, status: 'failed', error: err.message }); }
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Reconnect error:', error); res.status(500).send({ error: 'Failed to reconnect bots' }); }
});

router.get('/update-config', async (req, res) => {
  const { number, config: configString } = req.query;
  if (!number || !configString) return res.status(400).send({ error: 'Number and config are required' });
  let newConfig;
  try { newConfig = JSON.parse(configString); } catch (error) { return res.status(400).send({ error: 'Invalid config format' }); }
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const otp = generateOTP();
  otpStore.set(sanitizedNumber, { otp, expiry: Date.now() + config.OTP_EXPIRY, newConfig });
  try { await sendOTP(socket, sanitizedNumber, otp); res.status(200).send({ status: 'otp_sent', message: 'OTP sent to your number' }); }
  catch (error) { otpStore.delete(sanitizedNumber); res.status(500).send({ error: 'Failed to send OTP' }); }
});

router.get('/verify-otp', async (req, res) => {
  const { number, otp } = req.query;
  if (!number || !otp) return res.status(400).send({ error: 'Number and OTP are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const storedData = otpStore.get(sanitizedNumber);
  if (!storedData) return res.status(400).send({ error: 'No OTP request found for this number' });
  if (Date.now() >= storedData.expiry) { otpStore.delete(sanitizedNumber); return res.status(400).send({ error: 'OTP has expired' }); }
  if (storedData.otp !== otp) return res.status(400).send({ error: 'Invalid OTP' });
  try {
    await setUserConfigInMongo(sanitizedNumber, storedData.newConfig);
    otpStore.delete(sanitizedNumber);
    const sock = activeSockets.get(sanitizedNumber);
    if (sock) await sock.sendMessage(jidNormalizedUser(sock.user.id), { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('📌 CONFIG UPDATED', 'Your configuration has been successfully updated!', BOT_NAME_FANCY) });
    res.status(200).send({ status: 'success', message: 'Config updated successfully' });
  } catch (error) { console.error('Failed to update config:', error); res.status(500).send({ error: 'Failed to update config' }); }
});

router.post('/api/settings/login', async (req, res) => {
  try {
    const { number, password } = req.body || {};
    if (!number || !password) return res.status(400).json({ ok: false, error: 'Number and password are required' });
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const valid = await checkSettingsAuth(sanitizedNumber, password);
    if (!valid) return res.status(401).json({ ok: false, error: 'Incorrect number or password' });
    const cfg = await loadUserConfigFromMongo(sanitizedNumber) || {};
    res.json({ ok: true, number: sanitizedNumber, config: cfg });
  } catch (err) { res.status(500).json({ ok: false, error: err.message || err }); }
});

router.post('/api/settings/get', async (req, res) => {
  try {
    const { number, password } = req.body || {};
    if (!number || !password) return res.status(400).json({ ok: false, error: 'Number and password are required' });
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const valid = await checkSettingsAuth(sanitizedNumber, password);
    if (!valid) return res.status(401).json({ ok: false, error: 'Incorrect number or password' });
    const cfg = await loadUserConfigFromMongo(sanitizedNumber) || {};
    res.json({ ok: true, number: sanitizedNumber, config: cfg });
  } catch (err) { res.status(500).json({ ok: false, error: err.message || err }); }
});

router.post('/api/settings/update', async (req, res) => {
  try {
    const { number, password, config: newConfig } = req.body || {};
    if (!number || !password) return res.status(400).json({ ok: false, error: 'Number and password are required' });
    if (!newConfig || typeof newConfig !== 'object') return res.status(400).json({ ok: false, error: 'Config object is required' });
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const valid = await checkSettingsAuth(sanitizedNumber, password);
    if (!valid) return res.status(401).json({ ok: false, error: 'Incorrect number or password' });

    const existing = await loadUserConfigFromMongo(sanitizedNumber) || {};
    if (typeof newConfig.ownerNumber === 'string') {
      newConfig.ownerNumber = newConfig.ownerNumber.replace(/[^0-9]/g, '');
    }
    const merged = { ...existing, ...newConfig };
    await setUserConfigInMongo(sanitizedNumber, merged);

    const sock = activeSockets.get(sanitizedNumber);
    if (sock) {
      try {
        await sock.sendMessage(jidNormalizedUser(sock.user.id), {
          image: { url: config.RCD_IMAGE_PATH },
          caption: formatMessage('📌 SETTINGS UPDATED', 'Your bot settings were just updated from the settings panel.', BOT_NAME_FANCY)
        });
      } catch (e) {}
    }

    res.json({ ok: true, message: 'Settings updated successfully', config: merged });
  } catch (err) { res.status(500).json({ ok: false, error: err.message || err }); }
});

router.get('/getabout', async (req, res) => {
  const { number, target } = req.query;
  if (!number || !target) return res.status(400).send({ error: 'Number and target number are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const targetJid = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
  try {
    const statusData = await socket.fetchStatus(targetJid);
    const aboutStatus = statusData.status || 'No status available';
    const setAt = statusData.setAt ? moment(statusData.setAt).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss') : 'Unknown';
    res.status(200).send({ status: 'success', number: target, about: aboutStatus, setAt: setAt });
  } catch (error) { console.error(`Failed to fetch status for ${target}:`, error); res.status(500).send({ status: 'error', message: `Failed to fetch About status for ${target}.` }); }
});

const dashboardStaticDir = path.join(__dirname, 'dashboard_static');
if (!fs.existsSync(dashboardStaticDir)) fs.ensureDirSync(dashboardStaticDir);
router.use('/dashboard/static', express.static(dashboardStaticDir));
router.get('/dashboard', async (req, res) => {
  res.sendFile(path.join(dashboardStaticDir, 'index.html'));
});

router.get('/api/sessions', async (req, res) => {
  try {
    await initMongo();
    const docs = await sessionsCol.find({}, { projection: { number: 1, updatedAt: 1 } }).sort({ updatedAt: -1 }).toArray();
    res.json({ ok: true, sessions: docs });
  } catch (err) {
    console.error('API /api/sessions error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});

router.get('/api/active', async (req, res) => {
  try {
    const keys = Array.from(activeSockets.keys());
    res.json({ ok: true, active: keys, count: keys.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});

router.post('/api/session/delete', async (req, res) => {
  try {
    const { number } = req.body;
    if (!number) return res.status(400).json({ ok: false, error: 'number required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const running = activeSockets.get(sanitized);
    if (running) {
      try { if (typeof running.logout === 'function') await running.logout().catch(()=>{}); } catch(e){}
      try { running.ws?.close(); } catch(e){}
      activeSockets.delete(sanitized);
      socketCreationTime.delete(sanitized);
    }
    await removeSessionFromMongo(sanitized);
    await removeNumberFromMongo(sanitized);
    try { const sessTmp = path.join(os.tmpdir(), `session_${sanitized}`); if (fs.existsSync(sessTmp)) fs.removeSync(sessTmp); } catch(e){}
    res.json({ ok: true, message: `Session ${sanitized} removed` });
  } catch (err) {
    console.error('API /api/session/delete error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});

router.get('/api/newsletters', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.json({ ok: true, list });
  } catch (err) { res.status(500).json({ ok: false, error: err.message || err }); }
});
router.get('/api/admins', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.json({ ok: true, list });
  } catch (err) { res.status(500).json({ ok: false, error: err.message || err }); }
});

process.on('exit', () => {
  activeSockets.forEach((socket, number) => {
    try { socket.ws.close(); } catch (e) {}
    activeSockets.delete(number);
    socketCreationTime.delete(number);
    try { fs.removeSync(path.join(os.tmpdir(), `session_${number}`)); } catch(e){}
  });
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
  try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHAMA-MINI-main'}`); } catch(e) { console.error('Failed to restart pm2:', e); }
});

async function validateAndCleanSessions() {
  console.log('🔍 [STARTUP] Validating saved sessions for logged-out accounts...');
  let numbers = [];
  try { numbers = await getAllNumbersFromMongo(); } catch (e) { console.error('[STARTUP] Failed to load numbers:', e.message || e); return; }
  if (!numbers || !numbers.length) { console.log('✅ [STARTUP] No saved sessions to validate.'); return; }

  for (const number of numbers) {
    const sanitized = number.replace(/[^0-9]/g, '');
    const checkPath = path.join(os.tmpdir(), `session_check_${sanitized}`);
    try {
      const mongoDoc = await loadCredsFromMongo(sanitized);
      if (!mongoDoc || !mongoDoc.creds) {
        console.log(`🗑️ [STARTUP] No creds stored for ${sanitized}. Removing stale number entry.`);
        await removeNumberFromMongo(sanitized);
        continue;
      }

      fs.ensureDirSync(checkPath);
      fs.writeFileSync(path.join(checkPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) fs.writeFileSync(path.join(checkPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));

      const { state } = await useMultiFileAuthState(checkPath);
      const logger = pino({ level: 'fatal' });

      const loggedOut = await new Promise((resolve) => {
        let settled = false;
        const finish = (result) => { if (settled) return; settled = true; resolve(result); };
        let testSocket;
        try {
          testSocket = makeWASocket({
            auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
            printQRInTerminal: false,
            logger,
            browser: Browsers.macOS('Safari')
          });
        } catch (e) { return finish(false); }

        const timeout = setTimeout(() => { try { testSocket.ws?.close(); } catch(e){} finish(false); }, 15000);

        testSocket.ev.on('connection.update', (update) => {
          const { connection, lastDisconnect } = update;
          if (connection === 'open') {
            clearTimeout(timeout);
            try { testSocket.ws?.close(); } catch(e){}
            finish(false);
          } else if (connection === 'close') {
            clearTimeout(timeout);
            const isLoggedOut = isLoggedOutDisconnect(lastDisconnect);
            try { testSocket.ws?.close(); } catch(e){}
            finish(isLoggedOut);
          }
        });
      });

      if (loggedOut) {
        console.log(`🗑️ [STARTUP] Session ${sanitized} is logged out. Auto-deleting from Mongo...`);
        await removeSessionFromMongo(sanitized);
        await removeNumberFromMongo(sanitized);
      } else {
        console.log(`✅ [STARTUP] Session ${sanitized} looks valid.`);
      }
    } catch (e) {
      console.error(`[STARTUP] Validation error for ${sanitized}:`, e.message || e);
    } finally {
      try { if (fs.existsSync(checkPath)) fs.removeSync(checkPath); } catch(e){}
    }
    await delay(500);
  }
  console.log('✅ [STARTUP] Session validation complete.');
}

initMongo().catch(err => console.warn('Mongo init failed at startup', err));
(async()=>{
  try {
    await validateAndCleanSessions();
    const nums = await getAllNumbersFromMongo();
    if (nums && nums.length) { for (const n of nums) { if (!activeSockets.has(n)) { const mockRes = { headersSent:false, send:()=>{}, status:()=>mockRes }; await EmpirePair(n, mockRes); await delay(500); } } }
  } catch(e){}
})();

setInterval(async () => {
  console.log(`🔍 [HEALTH] Checking for dropped sessions...`);

  try {
    await initMongo();

    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) {
      console.log(`✅ [HEALTH] No saved numbers found.`);
      return;
    }

    let reconnected = 0;
    for (const number of numbers) {
      if (activeSockets.has(number)) continue;

      const mongoDoc = await loadCredsFromMongo(number);
      if (!mongoDoc || !mongoDoc.creds) {
        console.log(`❌ [HEALTH] No session found in MongoDB for ${number}. Skipping.`);
        continue;
      }

      console.log(`🔁 [HEALTH] ${number} is dropped. Reconnecting...`);
      try {
        const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
        await EmpirePair(number, mockRes);
        reconnected++;
      } catch (err) {
        console.log(`❌ [HEALTH] Failed to reconnect ${number}: ${err.message}`);
      }
      await delay(500);
    }

    if (reconnected > 0) {
      console.log(`✅ [HEALTH] Reconnected ${reconnected} dropped session(s).`);
    } else {
      console.log(`✅ [HEALTH] All ${numbers.length} session(s) are healthy.`);
    }

  } catch (err) {
    console.log(`❌ [HEALTH] Health check error: ${err.message}`);
  }
}, 5 * 60 * 60 * 1000);

module.exports = router;
