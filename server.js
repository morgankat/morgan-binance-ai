
import express from 'express';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json({limit:'50kb'}));

const PORT = process.env.PORT || 8080;
const ROOT = process.cwd();
const stateFile = process.env.STATE_FILE || path.join(ROOT,'bot-state.json');

const defaults = {
  mode: process.env.BINANCE_MODE === 'testnet' ? 'testnet' : 'live',
  market: 'spot',
  symbols: ['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT'],
  interval: '5m',
  scanSeconds: 20,
  auto: false,
  riskPct: 0.5,
  maxOpen: 2,
  leverage: 2,
  lastScan: null,
  lastError: null,
  scans: [],
  prices: {}
};

let state = {...defaults};

try {
  const saved = JSON.parse(fs.readFileSync(stateFile,'utf8'));
  state = {...state,...saved,auto:false};
} catch {}

const credentials = () => ({
  key: process.env.BINANCE_API_KEY || '',
  secret: process.env.BINANCE_API_SECRET || ''
});

const dashKey = () => process.env.DASHBOARD_API_KEY || '';

function persist() {
  try {
    fs.writeFileSync(stateFile,JSON.stringify(state,null,2),{mode:0o600});
  } catch(e) {
    console.error('State save:',e.message);
  }
}

function safeEqual(a,b) {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x,y);
}

app.use('/api',(req,res,next) => {
  if(req.path === '/health') return next();

  const k = dashKey();

  if(!k || k.length < 11) {
    return res.status(503).json({
      error:'Set your DASHBOARD_API_KEY (at least 11 characters) in Railway Variables.'
    });
  }

  if(!safeEqual(req.get('X-Dashboard-Key') || '',k)) {
    return res.status(401).json({
      error:'Dashboard password required or incorrect.'
    });
  }

  next();
});

const bases = kind => {
  if(kind === 'futures') {
    return state.mode === 'testnet'
      ? 'https://demo-fapi.binance.com'
      : 'https://fapi.binance.com';
  }

  return state.mode === 'testnet'
    ? 'https://testnet.binance.vision'
    : 'https://data-api.binance.vision';
};

async function reqBinance(kind,endpoint,params={},signed=false) {
  const p = new URLSearchParams();

  for(const [k,v] of Object.entries(params)) {
    if(v !== undefined && v !== null) p.set(k,String(v));
  }

  const headers = {};

  if(signed) {
    const {key,secret} = credentials();

    if(!key || !secret) {
      throw Error('Binance API key and secret are missing in Railway Variables.');
    }

    p.set('timestamp',Date.now());
    p.set('recvWindow','8000');

    p.set(
      'signature',
      crypto.createHmac('sha256',secret)
        .update(p.toString())
        .digest('hex')
    );

    headers['X-MBX-APIKEY'] = key;
  }

  const base = signed
    ? (kind === 'futures'
        ? (state.mode === 'testnet' ? 'https://demo-fapi.binance.com' : 'https://fapi.binance.com')
        : (state.mode === 'testnet' ? 'https://testnet.binance.vision' : 'https://api.binance.com'))
    : bases(kind);

  const url = base + endpoint + (p.size ? '?' + p.toString() : '');

  const r = await fetch(url,{
    headers,
    signal:AbortSignal.timeout(12000)
  });

  let d;

  try {
    d = await r.json();
  } catch {
    d = {msg:'Non-JSON response'};
  }

  if(!r.ok) throw Error(d.msg || `Binance HTTP ${r.status}`);

  return d;
}

function ema(a,n) {
  const k = 2/(n+1);
  let v = a[0];

  for(let i=1;i<a.length;i++) {
    v = a[i]*k + v*(1-k);
  }

  return v;
}

function calcRsi(a,n=14) {
  let up=0,down=0;

  for(let i=a.length-n;i<a.length;i++) {
    const d = a[i]-a[i-1];

    if(d>0) up+=d;
    else down-=d;
  }

  if(!down) return 100;

  return 100-100/(1+up/down);
}

function analyze(rows) {
  const close = rows.slice(0,-1).map(x=>+x[4]);

  if(close.length<60) {
    return {
      side:'WAIT',
      reason:'Waiting for at least 60 completed candles'
    };
  }

  const recent = close.slice(-60);
  const e9 = ema(recent,9);
  const e21 = ema(recent,21);
  const e50 = ema(recent,50);
  const rsi = calcRsi(close);
  const last = close.at(-1);
  const prev = close.at(-2);

  if(e9>e21 && e21>e50 && last>e9 && last>prev && rsi>=52 && rsi<=72) {
    return {
      side:'BUY',
      reason:'Bullish EMA 9/21/50 alignment; price above EMA 9; RSI momentum confirmed',
      rsi:+rsi.toFixed(1)
    };
  }

  if(e9<e21 && e21<e50 && last<e9 && last<prev && rsi>=28 && rsi<=48) {
    return {
      side:'SELL',
      reason:'Bearish EMA 9/21/50 alignment; price below EMA 9; RSI momentum confirmed',
      rsi:+rsi.toFixed(1)
    };
  }

  return {
    side:'WAIT',
    reason:'No aligned EMA trend and RSI confirmation',
    rsi:+rsi.toFixed(1)
  };
}

let scanBusy = false;

async function monitorCycle() {
  if(scanBusy) return;

  scanBusy = true;

  try {
    const out = [];

    for(const symbol of state.symbols.slice(0,20)) {
      try {
        const [ticker,rows] = await Promise.all([
          reqBinance(
            state.market,
            state.market === 'futures' ? '/fapi/v1/ticker/price' : '/api/v3/ticker/price',
            {symbol}
          ),
          reqBinance(
            state.market,
            state.market === 'futures' ? '/fapi/v1/klines' : '/api/v3/klines',
            {symbol,interval:state.interval,limit:100}
          )
        ]);

        const a = analyze(rows);

        out.push({
          symbol,
          market:state.market,
          price:+ticker.price,
          side:a.side,
          reason:a.reason,
          rsi:a.rsi ?? null,
          at:new Date().toISOString()
        });

      } catch(e) {
        out.push({
          symbol,
          side:'ERROR',
          reason:e.message,
          at:new Date().toISOString()
        });
      }
    }

    state.scans = [...out,...state.scans].slice(0,100);
    state.lastScan = new Date().toISOString();
    state.lastError = out.find(x=>x.side==='ERROR')?.reason || null;

    persist();

  } catch(e) {
    state.lastError = e.message;
    persist();
  } finally {
    scanBusy = false;
  }
}

setInterval(
  monitorCycle,
  Math.max(10000,Math.min(120000,state.scanSeconds*1000))
);

setTimeout(monitorCycle,1500);

app.get('/api/health',(req,res) => {
  res.json({ok:true,service:'MORGAN AI'});
});

app.get('/api/status',(req,res) => {
  res.json({
    ok:true,
    configured:!!(credentials().key && credentials().secret),
    dashboardProtected:dashKey().length>=11,
    mode:state.mode,
    market:state.market,
    monitoring:true,
    auto:false,
    tradingEnabled:false,
    settings:state,
    lastScan:state.lastScan,
    lastError:state.lastError
  });
});

app.get('/api/market',async(req,res) => {
  try {
    const symbols = (req.query.symbols || state.symbols.join(','))
      .split(',').slice(0,30);

    const kind = req.query.market === 'futures' ? 'futures' : 'spot';

    const data = await Promise.all(symbols.map(async symbol => {
      const d = await reqBinance(
        kind,
        kind === 'futures' ? '/fapi/v1/ticker/24hr' : '/api/v3/ticker/24hr',
        {symbol}
      );

      return {
        symbol:d.symbol,
        price:+d.lastPrice,
        change:+d.priceChangePercent,
        volume:+d.quoteVolume
      };
    }));

    res.json({data});

  } catch(e) {
    res.status(502).json({error:e.message});
  }
});

app.get('/api/klines',async(req,res) => {
  try {
    const symbol = String(req.query.symbol || 'BTCUSDT').toUpperCase();
    const kind = req.query.market === 'futures' ? 'futures' : 'spot';
    const interval = String(req.query.interval || state.interval);

    if(!/^[A-Z0-9]{5,20}$/.test(symbol) ||
       !['1m','3m','5m','15m','30m','1h','4h','1d'].includes(interval)) {
      return res.status(400).json({error:'Invalid symbol or timeframe'});
    }

    const candles = await reqBinance(
      kind,
      kind === 'futures' ? '/fapi/v1/klines' : '/api/v3/klines',
      {symbol,interval,limit:500}
    );

    res.json({symbol,market:kind,interval,candles});

  } catch(e) {
    res.status(502).json({error:e.message});
  }
});

app.get('/api/scan',(req,res) => {
  res.json({
    monitoring:true,
    lastScan:state.lastScan,
    scans:state.scans.slice(0,50),
    lastError:state.lastError
  });
});

app.get('/api/account',async(req,res) => {
  try {
    const kind = req.query.market === 'futures' ? 'futures' : 'spot';

    const d = await reqBinance(
      kind,
      kind === 'futures' ? '/fapi/v2/account' : '/api/v3/account',
      {},
      true
    );

    if(kind === 'futures') {
      return res.json({
        market:kind,
        totalWalletBalance:+d.totalWalletBalance,
        totalUnrealizedProfit:+d.totalUnrealizedProfit,
        availableBalance:+d.availableBalance,
        canTrade:!!d.canTrade
      });
    }

    res.json({
      market:kind,
      canTrade:!!d.canTrade,
      canWithdraw:!!d.canWithdraw,
      balances:d.balances
        .filter(x=>+x.free>0 || +x.locked>0)
        .map(x=>({
          asset:x.asset,
          free:+x.free,
          locked:+x.locked
        }))
    });

  } catch(e) {
    res.status(502).json({error:e.message});
  }
});

app.post('/api/settings',(req,res) => {
  const b = req.body || {};

  if(b.symbols) {
    const syms = [...new Set(
      b.symbols.map(x=>String(x).trim().toUpperCase())
        .filter(x=>/^[A-Z0-9]{5,20}$/.test(x))
    )].slice(0,20);

    if(!syms.length) {
      return res.status(400).json({error:'Enter valid trading symbols'});
    }

    state.symbols = syms;
  }

  if(b.interval) {
    if(!['1m','3m','5m','15m','30m','1h','4h','1d'].includes(b.interval)) {
      return res.status(400).json({error:'Invalid timeframe'});
    }

    state.interval = b.interval;
  }

  if(b.market && ['spot','futures'].includes(b.market)) {
    state.market = b.market;
  }

  persist();
  monitorCycle();

  res.json({ok:true,settings:state});
});

app.post('/api/auto',(req,res) => {
  state.auto = false;
  persist();

  res.status(403).json({
    error:'Order execution is locked in this build. Monitoring and analysis run automatically; no real orders can be placed.'
  });
});

app.post('/api/stop',(req,res) => {
  state.auto = false;
  persist();
  res.json({ok:true,auto:false});
});

app.get('/',(req,res) => {
  res.sendFile(path.join(ROOT,'index.html'));
});

app.use(express.static(ROOT));

app.listen(PORT,() => {
  console.log(`MORGAN AI monitoring server listening on ${PORT}`);
});
