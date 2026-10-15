// server.js
// Demo Express + WebSocket server with JWT auth for WS, demo trade endpoint, signal broadcasting,
// SQLite persistence (better-sqlite3) and a backend market-data aggregator using CoinGecko.

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');
const path = require('path');
const fetch = require('node-fetch');
const Database = require('better-sqlite3');
const { URL } = require('url');

const app = express();
app.use(cors());
app.use(bodyParser.json());

// Serve UI from ./public
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const JWT_SECRET = process.env.JWT_SECRET || 'change_this_secret_in_prod';
const PORT = process.env.PORT || 4000;

// ---------- SQLite DB (persistent) ----------
const db = new Database(path.join(__dirname, 'data.db'));
// Create tables
db.prepare(
  `CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    created_at TEXT
  )`
).run();

db.prepare(
  `CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    user TEXT,
    type TEXT,
    asset TEXT,
    amount REAL,
    price REAL,
    timestamp TEXT
  )`
).run();

db.prepare(
  `CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY,
    type TEXT,
    asset TEXT,
    price REAL,
    conf REAL,
    note TEXT,
    timestamp TEXT
  )`
).run();

// ---------- Helpers ----------
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN && client.isAuthenticated) {
      client.send(msg);
    }
  }
}

function insertSignalToDB(sig) {
  const stmt = db.prepare(`INSERT OR REPLACE INTO signals (id,type,asset,price,conf,note,timestamp) VALUES (?,?,?,?,?,?,?)`);
  stmt.run(sig.id, sig.type, sig.asset, sig.price || null, sig.conf || null, sig.note || null, sig.timestamp);
}

function insertTradeToDB(trade) {
  const stmt = db.prepare(`INSERT INTO trades (id,user,type,asset,amount,price,timestamp) VALUES (?,?,?,?,?,?,?)`);
  stmt.run(trade.id, trade.user, trade.type, trade.asset, trade.amount, trade.price || null, trade.timestamp);
}

// ---------- Market data aggregator (CoinGecko) ----------
let priceCache = {
  bitcoin: { usd: null, php: null, last_updated: null },
  ethereum: { usd: null, php: null, last_updated: null },
  updatedAt: null
};

async function fetchMarketData() {
  try {
    const url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd,php&include_24hr_change=false';
    const res = await fetch(url, { timeout: 10000 });
    if (!res.ok) throw new Error('CoinGecko error ' + res.status);
    const data = await res.json();
    if (data.bitcoin) {
      priceCache.bitcoin.usd = data.bitcoin.usd;
      priceCache.bitcoin.php = data.bitcoin.php;
    }
    if (data.ethereum) {
      priceCache.ethereum.usd = data.ethereum.usd;
      priceCache.ethereum.php = data.ethereum.php;
    }
    priceCache.updatedAt = new Date().toISOString();
  } catch (e) {
    console.warn('Market data fetch failed:', e.message || e);
  }
}
// warm initial
fetchMarketData();
setInterval(fetchMarketData, 10000);

// Expose /prices endpoint
app.get('/prices', (req, res) => {
  res.json(priceCache);
});

// ---------- Signal generation and persistence ----------
const SIGNAL_ASSETS = ['BTC', 'ETH', 'PHP', 'GOLD', 'MARKET'];
const SIGNAL_TYPES = ['BUY', 'SELL', 'HOLD', 'SCAN'];
function randRange(min, max) { return Math.random() * (max - min) + min; }
function makeRandomSignal() {
  const type = SIGNAL_TYPES[Math.floor(Math.random() * SIGNAL_TYPES.length)];
  const asset = SIGNAL_ASSETS[Math.floor(Math.random() * SIGNAL_ASSETS.length)];
  const price = Math.round(randRange(100, 70000) * 100) / 100;
  const conf = Math.round(randRange(50, 99));
  return {
    id: `sig_${Date.now()}_${Math.floor(Math.random()*10000)}`,
    type,
    asset,
    price,
    conf,
    note: type === 'SELL' ? `Quick profit taken +₱${(randRange(1,20)).toFixed(2)}` : null,
    timestamp: new Date().toISOString()
  };
}

let intervalId = null;
function startAutoBroadcast() {
  if (intervalId) return;
  intervalId = setInterval(() => {
    const sig = makeRandomSignal();
    insertSignalToDB(sig);
    broadcast({ event: 'signal', payload: sig });
    console.log('Broadcasted signal', sig.id);
  }, Math.floor(randRange(15000, 25000)));
}
startAutoBroadcast();

// ---------- Auth / users ----------
app.post('/login', (req, res) => {
  const { username } = req.body || {};
  if (!username) return res.status(400).json({ error: 'username required' });
  const payload = { username };
  // insert user if not exists
  const ustmt = db.prepare(`INSERT OR IGNORE INTO users (username, created_at) VALUES (?,?)`);
  ustmt.run(username, new Date().toISOString());
  const token = jwt.sign(payload, JWT_SECRET, { expiresIn: '6h' });
  res.json({ token, user: payload });
});

function verifyAuth(req, res, next) {
  const auth = req.headers.authorization || '';
  const m = auth.match(/^Bearer (.+)$/);
  if (!m) return res.status(401).json({ error: 'Missing Bearer token' });
  const token = m[1];
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    return next();
  } catch (e) {
    return res.status(401).json({ error: 'Invalid token' });
  }
}

// POST /trade -> persist trade and broadcast
app.post('/trade', verifyAuth, (req, res) => {
  const { type, asset, amount, price } = req.body || {};
  if (!type || !asset || !amount) return res.status(400).json({ error: 'type, asset, amount required' });
  const trade = {
    id: `trade_${Date.now()}_${Math.floor(Math.random()*10000)}`,
    user: req.user.username,
    type,
    asset,
    amount,
    price: price || null,
    timestamp: new Date().toISOString()
  };
  insertTradeToDB(trade);
  broadcast({ event: 'trade_executed', payload: trade });
  res.json({ status: 'ok', trade });
});

// GET /trades -> list trades
app.get('/trades', verifyAuth, (req, res) => {
  const rows = db.prepare('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 200').all();
  res.json({ trades: rows });
});

// POST /signal -> persist and broadcast
app.post('/signal', (req, res) => {
  const sig = req.body;
  if (!sig || !sig.type || !sig.asset) {
    return res.status(400).json({ error: 'Signal must include type and asset' });
  }
  sig.id = sig.id || `sig_${Date.now()}`;
  sig.timestamp = sig.timestamp || new Date().toISOString();
  insertSignalToDB(sig);
  broadcast({ event: 'signal', payload: sig });
  res.json({ status: 'sent', signal: sig });
});

// WebSocket with JWT token in query param
wss.on('connection', (ws, req) => {
  let token = null;
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    token = url.searchParams.get('token');
  } catch (e) { token = null; }

  if (!token) {
    ws.send(JSON.stringify({ event: 'error', payload: { msg: 'Missing token. Close.' } }));
    ws.isAuthenticated = false;
    ws.close(4001, 'Missing token');
    return;
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    ws.isAuthenticated = true;
    ws.user = decoded;
    ws.send(JSON.stringify({ event: 'welcome', payload: { msg: 'Connected to Signal Server', user: decoded.username, timestamp: new Date().toISOString() } }));
    console.log('WS authenticated for user:', decoded.username);
  } catch (e) {
    ws.isAuthenticated = false;
    ws.send(JSON.stringify({ event: 'error', payload: { msg: 'Invalid token' } }));
    ws.close(4002, 'Invalid token');
    return;
  }

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg && msg.action === 'echo') {
        ws.send(JSON.stringify({ event: 'echo', payload: msg.data }));
      }
    } catch (e) {}
  });

  ws.on('close', (code, reason) => {
    console.log('WS closed', code, reason);
  });
});

// Start server
server.listen(PORT, () => {
  console.log(`Server listening at http://localhost:${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT} (send token as ?token=...)`);
});
