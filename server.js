// server.js
// Demo Express + WebSocket server with JWT auth for WS, demo trade endpoint, and signal broadcasting.
//
// Run:
//   export JWT_SECRET="change_this"
//   npm install
//   node server.js
//
// Notes:
// - This is a demo only. Do NOT use the default secret in production.

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');
const path = require('path');
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

// In-memory stores (demo)
let trades = [];

// Helper: broadcast object to all authenticated clients
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN && client.isAuthenticated) {
      client.send(msg);
    }
  }
}

// Create random demo signal
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
    note: type === 'SELL' ? `Quick profit taken +₱${(randRange(1,20)).toFixed(2)}` : undefined,
    timestamp: new Date().toISOString()
  };
}

// Auto broadcast demo signals (every 15-25s)
let intervalId = null;
function startAutoBroadcast() {
  if (intervalId) return;
  intervalId = setInterval(() => {
    const sig = makeRandomSignal();
    console.log('Broadcasting signal', sig.id, sig.type, sig.asset);
    broadcast({ event: 'signal', payload: sig });
  }, Math.floor(randRange(15000, 25000)));
}
startAutoBroadcast();

// Simple login: accepts any username/password for demo and issues a JWT
app.post('/login', (req, res) => {
  const { username } = req.body || {};
  if (!username) return res.status(400).json({ error: 'username required' });
  // Demo: no password check. In prod, validate credentials.
  const payload = { username };
  const token = jwt.sign(payload, JWT_SECRET, { expiresIn: '6h' });
  res.json({ token, user: payload });
});

// Protected trade execution: requires Authorization Bearer token
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

// POST /trade to simulate executing a trade
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
  trades.push(trade);
  // Optionally also broadcast trade execution to clients
  broadcast({ event: 'trade_executed', payload: trade });
  res.json({ status: 'ok', trade });
});

// GET /trades to list demo trades (protected)
app.get('/trades', verifyAuth, (req, res) => {
  // For demo: return all trades
  res.json({ trades });
});

// Allow pushing custom signals via POST /signal (unprotected for demo)
// Body: { type, asset, price, conf, note }
app.post('/signal', (req, res) => {
  const sig = req.body;
  if (!sig || !sig.type || !sig.asset) {
    return res.status(400).json({ error: 'Signal must include type and asset' });
  }
  sig.id = sig.id || `sig_${Date.now()}`;
  sig.timestamp = sig.timestamp || new Date().toISOString();
  broadcast({ event: 'signal', payload: sig });
  res.json({ status: 'sent', signal: sig });
});

// WebSocket connection: validate token from query parameter ?token=...
wss.on('connection', (ws, req) => {
  // parse token from querystring
  let token = null;
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    token = url.searchParams.get('token');
  } catch (e) {
    token = null;
  }

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
    // Accept client messages for demo; we echo 'echo' actions
    try {
      const msg = JSON.parse(data);
      if (msg && msg.action === 'echo') {
        ws.send(JSON.stringify({ event: 'echo', payload: msg.data }));
      }
    } catch (e) {
      // ignore non-json
    }
  });

  ws.on('close', (code, reason) => {
    console.log('WS closed', code, reason);
  });
});

// Start the HTTP + WS server
server.listen(PORT, () => {
  console.log(`Server listening at http://localhost:${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT} (send token as ?token=...)`);
});
