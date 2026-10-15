// server.js
// Postgres-backed server with WebSocket signals, JWT auth, CoinGecko prices,
// automatic SQLite->Postgres migration (calls migrate-sqlite-to-postgres.js if data.db exists)

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

const app = express();
app.use(cors());
app.use(bodyParser.json());

// Serve UI from ./public
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const JWT_SECRET = process.env.JWT_SECRET || 'change_this_secret_in_prod';
const DATABASE_URL = process.env.DATABASE_URL || 'postgres://tm_user:tm_pass@db:5432/themarketboy';
const PORT = process.env.PORT || 4000;

// ---------- Postgres pool ----------
const pool = new Pool({ connectionString: DATABASE_URL });

async function ensureSchema() {
  // Create tables if not exist
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      username TEXT PRIMARY KEY,
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
    CREATE TABLE IF NOT EXISTS meta_kv (
      key TEXT PRIMARY KEY,
      value TEXT
    );
  `);
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
  // Execute migration script (it will connect to the same DATABASE_URL)
  return new Promise((resolve, reject) => {
    execFile('node', [path.join(__dirname, 'migrate-sqlite-to-postgres.js')], { env: process.env }, (err, stdout, stderr) => {
      if (err) {
        console.error('Migration script error', err, stderr);
        return reject(err);
      }
      console.log('Migration output:', stdout);
      // set marker
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

// ---------- Auth / endpoints ----------
app.post('/login', async (req,res)=>{
  const { username } = req.body || {};
  if (!username) return res.status(400).json({ error: 'username required' });
  // create user if not exists
  await pool.query(`INSERT INTO users (username, created_at) VALUES ($1, $2) ON CONFLICT (username) DO NOTHING`, [username, new Date().toISOString()]);
  const token = jwt.sign({ username }, JWT_SECRET, { expiresIn: '6h' });
  res.json({ token, user: { username } });
});

function verifyAuth(req,res,next){
  const auth = req.headers.authorization || '';
  const m = auth.match(/^Bearer (.+)$/);
  if (!m) return res.status(401).json({ error: 'Missing Bearer token' });
  const token = m[1];
  try{
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded; return next();
  }catch(e){ return res.status(401).json({ error:'Invalid token' }); }
}

app.post('/trade', verifyAuth, async (req,res)=>{
  const { type, asset, amount, price } = req.body || {};
  if (!type || !asset || !amount) return res.status(400).json({ error: 'type, asset, amount required' });
  const trade = { id:`trade_${Date.now()}_${Math.floor(Math.random()*10000)}`, user: req.user.username, type, asset, amount, price: price||null, timestamp: new Date().toISOString() };
  await insertTrade(trade);
  broadcast({ event: 'trade_executed', payload: trade });
  res.json({ status:'ok', trade });
});

app.get('/trades', verifyAuth, async (req,res)=>{
  const r = await pool.query('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 200');
  res.json({ trades: r.rows });
});

app.post('/signal', async (req,res)=>{
  const sig = req.body;
  if (!sig || !sig.type || !sig.asset) return res.status(400).json({ error: 'Signal must include type and asset' });
  sig.id = sig.id || `sig_${Date.now()}`;
  sig.timestamp = sig.timestamp || new Date().toISOString();
  await insertSignal(sig);
  broadcast({ event: 'signal', payload: sig });
  res.json({ status:'sent', signal: sig });
});

// ---------- Admin UI endpoint (static) ----------
// public/admin.html exists; protect with JWT on client side. Also provide an API
app.get('/admin/data', verifyAuth, async (req,res)=>{
  // simple auth: only allow username 'admin' or any user for demo
  const { username } = req.user || {};
  if (!username) return res.status(403).json({ error: 'forbidden' });
  const trades = (await pool.query('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 500')).rows;
  const signals = (await pool.query('SELECT * FROM signals ORDER BY timestamp DESC LIMIT 500')).rows;
  res.json({ trades, signals });
});

// ---------- WebSocket handling ----------
wss.on('connection', async (ws, req) => {
  let token = null;
  try { const url = new URL(req.url, `http://${req.headers.host}`); token = url.searchParams.get('token'); } catch(e){ token = null; }
  if (!token) { ws.send(JSON.stringify({ event:'error', payload:{ msg:'Missing token. Close.' } })); ws.isAuthenticated=false; ws.close(4001,'Missing token'); return; }
  try { const decoded = jwt.verify(token, JWT_SECRET); ws.isAuthenticated=true; ws.user=decoded; ws.send(JSON.stringify({ event:'welcome', payload:{ msg:'Connected to Signal Server', user: decoded.username, timestamp: new Date().toISOString() } })); console.log('WS authenticated for user:', decoded.username); } catch(e){ ws.isAuthenticated=false; ws.send(JSON.stringify({ event:'error', payload:{ msg:'Invalid token' } })); ws.close(4002,'Invalid token'); return; }
  ws.on('message', (data)=>{ try{ const msg = JSON.parse(data); if (msg && msg.action==='echo') ws.send(JSON.stringify({ event:'echo', payload: msg.data })); }catch(e){} });
  ws.on('close', (code, reason)=>{ console.log('WS closed', code, reason); });
});

// ---------- Startup ----------
(async function start(){
  try{
    await pool.connect();
    await ensureSchema();
    // If Postgres empty and data.db exists, migrate
    const ct = (await pool.query('SELECT count(*)::int AS c FROM trades')).rows[0].c;
    if (ct === 0 && fs.existsSync(path.join(__dirname,'data.db'))) {
      console.log('No trades in Postgres and data.db exists — running migration');
      await maybeMigrateFromSqlite();
    }
    // start broadcasting signals
    startAutoBroadcast();
    server.listen(PORT, ()=>{
      console.log(`Server listening at http://localhost:${PORT}`);
      console.log(`WebSocket endpoint: ws://localhost:${PORT} (send token as ?token=...)`);
    });
  }catch(err){ console.error('Startup error', err); process.exit(1); }
})();
