// server.js
// Postgres-backed server with WebSocket signals, JWT auth, CoinGecko prices,
// automatic SQLite->Postgres migration and real auth (bcrypt + JWT + refresh tokens)

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');
const path = require('path');
const fetch = require('node-fetch');
const { Pool } = require('pg');
const fs = require('fs');
const { execFile } = require('child_process');
const { URL } = require('url');
const bcrypt = require('bcryptjs');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.use(bodyParser.json());

// Configurable app origin (used by frontend via /config.js)
const APP_ORIGIN = process.env.APP_ORIGIN || '';

// Serve a small config JS for the frontend before static files
app.get('/config.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.send(`window.APP_ORIGIN = ${JSON.stringify(APP_ORIGIN)};`);
});

// Serve UI from ./public
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const JWT_SECRET = process.env.JWT_SECRET || 'change_this_secret_in_prod';
const PORT = process.env.PORT || 4000;
const DATABASE_URL = process.env.DATABASE_URL || 'postgres://tm_user:tm_pass@db:5432/themarketboy';

// Admin bootstrap
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || null; // if set, will seed admin on startup

// ---------- Postgres pool ----------
const pool = new Pool({ connectionString: DATABASE_URL });

async function ensureSchema() {
  // Create users table (with password hash and role)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      username TEXT PRIMARY KEY,
      password_hash TEXT,
      role TEXT DEFAULT 'user',
      created_at TIMESTAMP WITH TIME ZONE
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS trades (
      id TEXT PRIMARY KEY,
      "user" TEXT,
      type TEXT,
      asset TEXT,
      amount DOUBLE PRECISION,
      price DOUBLE PRECISION,
      timestamp TIMESTAMP WITH TIME ZONE
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS signals (
      id TEXT PRIMARY KEY,
      type TEXT,
      asset TEXT,
      price DOUBLE PRECISION,
      conf DOUBLE PRECISION,
      note TEXT,
      timestamp TIMESTAMP WITH TIME ZONE
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS refresh_tokens (
      token TEXT PRIMARY KEY,
      username TEXT,
      expires_at TIMESTAMP WITH TIME ZONE
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS meta_kv (
      key TEXT PRIMARY KEY,
      value TEXT
    );
  `);
}

// Seed admin if ADMIN_PASSWORD provided
async function seedAdminIfNeeded() {
  if (!ADMIN_PASSWORD) return;
  const hash = await bcrypt.hash(ADMIN_PASSWORD, 10);
  await pool.query(`INSERT INTO users (username, password_hash, role, created_at) VALUES ($1,$2,$3,$4) ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash, role = EXCLUDED.role`, [ADMIN_USERNAME, hash, 'admin', new Date().toISOString()]);
  console.log(`Admin user '${ADMIN_USERNAME}' seeded (password from ADMIN_PASSWORD env).`);
}

// ---------- Market data aggregator (CoinGecko) ----------
let priceCache = { bitcoin: { usd: null, php: null }, ethereum: { usd: null, php: null }, updatedAt: null };
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
    console.warn('Market data fetch failed:', e && e.message ? e.message : e);
  }
}
fetchMarketData();
setInterval(fetchMarketData, 10000);
app.get('/prices', (req, res) => res.json(priceCache));

// ---------- Helpers ----------
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN && client.isAuthenticated) client.send(msg);
  }
}

async function insertSignal(sig) {
  await pool.query(
    `INSERT INTO signals (id,type,asset,price,conf,note,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING`,
    [sig.id, sig.type, sig.asset, sig.price || null, sig.conf || null, sig.note || null, sig.timestamp]
  );
}

async function insertTrade(trade) {
  await pool.query(
    `INSERT INTO trades (id,"user",type,asset,amount,price,timestamp) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING`,
    [trade.id, trade.user, trade.type, trade.asset, trade.amount, trade.price || null, trade.timestamp]
  );
}

// ---------- Auth helpers (JWT + refresh tokens) ----------
function makeAccessToken(payload) {
  // short lived access token (15m)
  return jwt.sign(payload, JWT_SECRET, { expiresIn: '15m' });
}

function makeRefreshToken() {
  return crypto.randomBytes(32).toString('hex');
}

async function saveRefreshToken(token, username, days = 7) {
  const expiresAt = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();
  await pool.query('INSERT INTO refresh_tokens (token, username, expires_at) VALUES ($1,$2,$3) ON CONFLICT (token) DO UPDATE SET username = EXCLUDED.username, expires_at = EXCLUDED.expires_at', [token, username, expiresAt]);
}

async function revokeRefreshToken(token) {
  await pool.query('DELETE FROM refresh_tokens WHERE token = $1', [token]);
}

async function verifyRefreshToken(token) {
  const r = await pool.query('SELECT username, expires_at FROM refresh_tokens WHERE token = $1', [token]);
  if (r.rows.length === 0) return null;
  const row = r.rows[0];
  if (new Date(row.expires_at) < new Date()) return null;
  return row.username;
}

// Middleware to verify access token
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

function requireAdmin(req, res, next) {
  if (!req.user || req.user.role !== 'admin') return res.status(403).json({ error: 'admin required' });
  return next();
}

// ---------- Automatic SQLite -> Postgres migration ----------
async function maybeMigrateFromSqlite() {
  const marker = 'sqlite_migrated';
  try {
    const r = await pool.query(`SELECT value FROM meta_kv WHERE key = $1`, [marker]);
    if (r.rows && r.rows.length) {
      console.log('Migration marker found, skipping migration');
      return;
    }
  } catch (e) {
    // table may not exist; ignore
  }

  const sqlitePath = path.join(__dirname, 'data.db');
  if (!fs.existsSync(sqlitePath)) {
    console.log('No data.db found, skipping sqlite migration');
    return;
  }

  console.log('data.db found; running migrate-sqlite-to-postgres.js');
  return new Promise((resolve, reject) => {
    execFile('node', [path.join(__dirname, 'migrate-sqlite-to-postgres.js')], { env: process.env }, (err, stdout, stderr) => {
      if (err) {
        console.error('Migration script error', err, stderr);
        return reject(err);
      }
      console.log('Migration output:', stdout);
      pool.query(`INSERT INTO meta_kv (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, [marker, new Date().toISOString()])
        .then(()=>resolve())
        .catch(reject);
    });
  });
}

// ---------- Signal generator (demo auto-broadcast) ----------
const SIGNAL_ASSETS = ['BTC','ETH','PHP','GOLD','MARKET'];
const SIGNAL_TYPES = ['BUY','SELL','HOLD','SCAN'];
function randRange(min,max){return Math.random()*(max-min)+min}
function makeRandomSignal(){
  const type = SIGNAL_TYPES[Math.floor(Math.random()*SIGNAL_TYPES.length)];
  const asset = SIGNAL_ASSETS[Math.floor(Math.random()*SIGNAL_ASSETS.length)];
  const price = Math.round(randRange(100,70000)*100)/100;
  const conf = Math.round(randRange(50,99));
  return { id:`sig_${Date.now()}_${Math.floor(Math.random()*10000)}`, type, asset, price, conf, note: type==='SELL'?`Quick profit taken +₱${(randRange(1,20)).toFixed(2)}`:null, timestamp: new Date().toISOString() };
}
let intervalId = null;
function startAutoBroadcast(){ if(intervalId) return; intervalId = setInterval(async ()=>{ const sig=makeRandomSignal(); await insertSignal(sig); broadcast({event:'signal', payload:sig}); console.log('Broadcasted signal', sig.id); }, Math.floor(randRange(15000,25000))); }

// ---------- Auth endpoints: register / login / token / logout ----------
app.post('/register', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'username and password required' });
  const hash = await bcrypt.hash(password, 10);
  try {
    await pool.query('INSERT INTO users (username, password_hash, role, created_at) VALUES ($1,$2,$3,$4)', [username, hash, 'user', new Date().toISOString()]);
    res.json({ status: 'ok' });
  } catch (e) {
    if (e.code === '23505') return res.status(409).json({ error: 'username exists' });
    console.error('Register error', e);
    res.status(500).json({ error: 'internal_error' });
  }
});

app.post('/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'username and password required' });
  const r = await pool.query('SELECT username, password_hash, role FROM users WHERE username = $1', [username]);
  if (r.rows.length === 0) return res.status(401).json({ error: 'invalid_credentials' });
  const user = r.rows[0];
  const ok = await bcrypt.compare(password, user.password_hash || '');
  if (!ok) return res.status(401).json({ error: 'invalid_credentials' });

  const payload = { username: user.username, role: user.role || 'user' };
  const accessToken = makeAccessToken(payload);
  const refreshToken = makeRefreshToken();
  await saveRefreshToken(refreshToken, user.username, 14); // 14 days
  res.json({ accessToken, refreshToken, user: { username: user.username, role: user.role } });
});

// Refresh endpoint
app.post('/token', async (req, res) => {
  const { refreshToken } = req.body || {};
  if (!refreshToken) return res.status(400).json({ error: 'refreshToken required' });
  const username = await verifyRefreshToken(refreshToken);
  if (!username) return res.status(401).json({ error: 'invalid_refresh' });
  const r = await pool.query('SELECT username, role FROM users WHERE username = $1', [username]);
  if (r.rows.length === 0) return res.status(401).json({ error: 'invalid_refresh' });
  const user = r.rows[0];
  const accessToken = makeAccessToken({ username: user.username, role: user.role || 'user' });
  res.json({ accessToken });
});

// Logout: revoke refresh token
app.post('/logout', async (req, res) => {
  const { refreshToken } = req.body || {};
  if (!refreshToken) return res.status(400).json({ error: 'refreshToken required' });
  await revokeRefreshToken(refreshToken);
  res.json({ status: 'ok' });
});

// Protected trade endpoint (uses access token)
app.post('/trade', verifyAuth, async (req, res) => {
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
  await insertTrade(trade);
  broadcast({ event: 'trade_executed', payload: trade });
  res.json({ status: 'ok', trade });
});

app.get('/trades', verifyAuth, async (req, res) => {
  const r = await pool.query('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 200');
  res.json({ trades: r.rows });
});

app.post('/signal', async (req, res) => {
  const sig = req.body;
  if (!sig || !sig.type || !sig.asset) return res.status(400).json({ error: 'Signal must include type and asset' });
  sig.id = sig.id || `sig_${Date.now()}`;
  sig.timestamp = sig.timestamp || new Date().toISOString();
  await insertSignal(sig);
  broadcast({ event: 'signal', payload: sig });
  res.json({ status: 'sent', signal: sig });
});

// ---------- Admin API ----------
app.get('/admin/data', verifyAuth, requireAdmin, async (req, res) => {
  const trades = (await pool.query('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 500')).rows;
  const signals = (await pool.query('SELECT * FROM signals ORDER BY timestamp DESC LIMIT 500')).rows;
  const users = (await pool.query('SELECT username, role, created_at FROM users ORDER BY created_at DESC LIMIT 500')).rows;
  res.json({ trades, signals, users });
});

// Admin: promote/demote user
app.post('/admin/user/role', verifyAuth, requireAdmin, async (req, res) => {
  const { username, role } = req.body || {};
  if (!username || !role) return res.status(400).json({ error: 'username and role required' });
  await pool.query('UPDATE users SET role = $1 WHERE username = $2', [role, username]);
  res.json({ status: 'ok' });
});

// ---------- WebSocket handling ----------
wss.on('connection', async (ws, req) => {
  let token = null;
  try { const url = new URL(req.url, `http://${req.headers.host}`); token = url.searchParams.get('token'); } catch(e){ token = null; }
  if (!token) { ws.send(JSON.stringify({ event:'error', payload:{ msg:'Missing token. Close.' } })); ws.isAuthenticated=false; ws.close(4001,'Missing token'); return; }
  try { const decoded = jwt.verify(token, JWT_SECRET); ws.isAuthenticated=true; ws.user=decoded; ws.send(JSON.stringify({ event:'welcome', payload:{ msg:'Connected to Signal Server', user: decoded.username, role: decoded.role, timestamp: new Date().toISOString() } })); console.log('WS authenticated for user:', decoded.username); } catch(e){ ws.isAuthenticated=false; ws.send(JSON.stringify({ event:'error', payload:{ msg:'Invalid token' } })); ws.close(4002,'Invalid token'); return; }
  ws.on('message', (data)=>{ try{ const msg = JSON.parse(data); if (msg && msg.action==='echo') ws.send(JSON.stringify({ event:'echo', payload: msg.data })); }catch(e){} });
  ws.on('close', (code, reason)=>{ console.log('WS closed', code, reason); });
});

// ---------- Startup ----------
(async function start(){
  try{
    await pool.connect();
    await ensureSchema();
    await seedAdminIfNeeded();
    const ct = (await pool.query('SELECT count(*)::int AS c FROM trades')).rows[0].c;
    if (ct === 0 && fs.existsSync(path.join(__dirname,'data.db'))) {
      console.log('No trades in Postgres and data.db exists — running migration');
      await maybeMigrateFromSqlite();
    }
    startAutoBroadcast();
    server.listen(PORT, ()=>{
      console.log(`Server listening at http://localhost:${PORT}`);
      console.log(`WebSocket endpoint: ws://localhost:${PORT} (send token as ?token=...)`);
    });
  }catch(err){ console.error('Startup error', err); process.exit(1); }
})();
