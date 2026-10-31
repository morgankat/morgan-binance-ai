
import express from 'express';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import dotenv from 'dotenv';

dotenv.config();

const app = express();

app.use(express.json({ limit: '100kb' }));

// Serve the dashboard from the repository root.
app.get('/', (req, res) => {
  res.sendFile(path.join(process.cwd(), 'index.html'));
});

// Serve additional static files from /public.
app.use(express.static(path.join(process.cwd(), 'public')));

const stateFile =
  process.env.STATE_FILE ||
  path.join(process.cwd(), 'bot-state.json');

const defaults = {
  mode: process.env.BINANCE_MODE === 'live' ? 'live' : 'testnet',
  auto: false,
  market: 'spot',
  symbols: ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'],
  riskPct: 0.5,
  maxOpen: 2,
  leverage: 2,
  scanSeconds: 15,
  strategy: 'ema_momentum',
  maxDailyLossPct: 5,
  maxPositions: 2,
  allowLive: false,
  lastScan: null,
  lastError: null,
  ledger: [],
  dayStartEquity: null,
  dayKey: null
};

let state = { ...defaults };

try {
  const saved = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
  state = { ...state, ...saved, auto: false };
} catch {}

function persist() {
  try {
    fs.writeFileSync(
      stateFile,
      JSON.stringify(state, null, 2),
      { mode: 0o600 }
    );
  } catch (e) {
    console.error('State persistence:', e.message);
  }
}

const spotBase = () =>
  state.mode === 'live'
    ? 'https://api.binance.com'
    : 'https://testnet.binance.vision';

const futBase = () =>
  state.mode === 'live'
    ? 'https://fapi.binance.com'
    : 'https://testnet.binancefuture.com';

const credentials = () => ({
  key: process.env.BINANCE_API_KEY || '',
  secret: process.env.BINANCE_API_SECRET || ''
});

async function request(
  base,
  pathname,
  params = {},
  signed = false,
  method = 'GET'
) {
  const p = new URLSearchParams();

  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) {
      p.set(k, String(v));
    }
  }

  const headers = {};

  if (signed) {
    const { key, secret } = credentials();

    if (!key || !secret) {
      throw Error(
        'Set BINANCE_API_KEY and BINANCE_API_SECRET in Railway Variables.'
      );
    }

    p.set('timestamp', Date.now());
    p.set('recvWindow', '7000');

    p.set(
      'signature',
      crypto
        .createHmac('sha256', secret)
        .update(p.toString())
        .digest('hex')
    );

    headers['X-MBX-APIKEY'] = key;
  }

  const url =
    base +
    pathname +
    (p.size ? '?' + p.toString() : '');

  const r = await fetch(url, { method, headers });

  let d;

  try {
    d = await r.json();
  } catch {
    d = { msg: 'Non-JSON response' };
  }

  if (!r.ok) {
    throw Error(d.msg || `Binance HTTP ${r.status}`);
  }

  return d;
}

const signed = (kind, p = {}, method = 'GET') =>
  request(
    kind === 'futures' ? futBase() : spotBase(),
    kind === 'futures'
      ? '/fapi/v1/' + p.endpoint
      : '/api/v3/' + p.endpoint,
    Object.fromEntries(
      Object.entries(p).filter(([k]) => k !== 'endpoint')
    ),
    true,
    method
  );

async function exchangeInfo(kind) {
  return request(
    kind === 'futures' ? futBase() : spotBase(),
    kind === 'futures'
      ? '/fapi/v1/exchangeInfo'
      : '/api/v3/exchangeInfo'
  );
}

async function klines(
  symbol,
  kind = 'spot',
  interval = '5m',
  limit = 120
) {
  return request(
    kind === 'futures' ? futBase() : spotBase(),
    kind === 'futures'
      ? '/fapi/v1/klines'
      : '/api/v3/klines',
    { symbol, interval, limit }
  );
}

function ema(vals, period) {
  const k = 2 / (period + 1);
  let e = vals[0];

  for (let i = 1; i < vals.length; i++) {
    e = vals[i] * k + e * (1 - k);
  }

  return e;
}

function rsi(vals, period = 14) {
  let g = 0;
  let l = 0;

  for (let i = vals.length - period; i < vals.length; i++) {
    const d = vals[i] - vals[i - 1];

    if (d > 0) {
      g += d;
    } else {
      l -= d;
    }
  }

  if (!l) return 100;

  return 100 - 100 / (1 + g / l);
}

function signal(candles) {
  const c = candles
    .slice(0, -1)
    .map(x => +x[4]);

  if (c.length < 60) {
    return {
      side: 'WAIT',
      why: 'Not enough candles'
    };
  }

  const e9 = ema(c.slice(-50), 9);
  const e21 = ema(c.slice(-80), 21);
  const e50 = ema(c.slice(-100), 50);
  const rv = rsi(c);

  const last = c.at(-1);
  const prev = c.at(-2);

  if (
    e9 > e21 &&
    e21 > e50 &&
    last > e9 &&
    last > prev &&
    rv >= 52 &&
    rv <= 72
  ) {
    return {
      side: 'BUY',
      why: 'EMA trend + RSI momentum',
      rsi: rv
    };
  }

  if (
    e9 < e21 &&
    e21 < e50 &&
    last < e9 &&
    last < prev &&
    rv <= 48 &&
    rv >= 28
  ) {
    return {
      side: 'SELL',
      why: 'EMA trend + RSI momentum',
      rsi: rv
    };
  }

  return {
    side: 'WAIT',
    why: 'Trend/momentum not aligned',
    rsi: rv
  };
}

async function getPrice(symbol, kind) {
  const d = await request(
    kind === 'futures' ? futBase() : spotBase(),
    kind === 'futures'
      ? '/fapi/v1/ticker/price'
      : '/api/v3/ticker/price',
    { symbol }
  );

  return +d.price;
}

async function account(kind = state.market) {
  if (kind === 'futures') {
    return signed('futures', { endpoint: 'account' });
  }

  return signed('spot', { endpoint: 'account' });
}

async function openPositions() {
  if (state.market === 'spot') {
    const a = await account('spot');

    return a.balances
      .filter(x => +x.free > 0 || +x.locked > 0)
      .filter(
        x =>
          ![
            'USDT',
            'USDC',
            'FDUSD',
            'BTC',
            'BNB',
            'ETH'
          ].includes(x.asset)
      )
      .map(x => ({
        asset: x.asset,
        amount: +x.free + +x.locked
      }));
  }

  const a = await signed('futures', {
    endpoint: 'positionRisk'
  });

  return a
    .filter(x => Math.abs(+x.positionAmt) > 0)
    .map(x => ({
      symbol: x.symbol,
      side: +x.positionAmt > 0 ? 'LONG' : 'SHORT',
      qty: +x.positionAmt,
      entry: +x.entryPrice,
      mark: +x.markPrice,
      unrealized: +x.unRealizedProfit,
      leverage: +x.leverage,
      liquidation: +x.liquidationPrice
    }));
}

async function setLeverage(symbol, leverage) {
  return signed(
    'futures',
    {
      endpoint: 'leverage',
      symbol,
      leverage
    },
    'POST'
  );
}

async function orderSpot(symbol, side, quote) {
  if (side === 'BUY') {
    return signed(
      'spot',
      {
        endpoint: 'order',
        symbol,
        side,
        type: 'MARKET',
        quoteOrderQty: quote
      },
      'POST'
    );
  }

  const asset = symbol.replace(/USDT$/, '');
  const a = await account('spot');
  const b = a.balances.find(x => x.asset === asset);
  const qty = +( +b?.free || 0 );

  if (qty <= 0) {
    throw Error(`No free ${asset} balance to sell`);
  }

  return signed(
    'spot',
    {
      endpoint: 'order',
      symbol,
      side,
      type: 'MARKET',
      quantity: qty
    },
    'POST'
  );
}

async function orderFutures(symbol, side, qty) {
  return signed(
    'futures',
    {
      endpoint: 'order',
      symbol,
      side,
      type: 'MARKET',
      quantity: qty
    },
    'POST'
  );
}

async function qtyRules(symbol, kind) {
  const info = await exchangeInfo(kind);
  const s = info.symbols.find(x => x.symbol === symbol);

  if (!s || s.status !== 'TRADING') {
    throw Error(`${symbol} is not tradable on ${kind}`);
  }

  const lot = s.filters.find(
    x => x.filterType === 'LOT_SIZE'
  );

  const notional = s.filters.find(
    x =>
      ['MIN_NOTIONAL', 'NOTIONAL'].includes(x.filterType)
  );

  return {
    step: +lot.stepSize,
    min: +lot.minQty,
    max: +lot.maxQty,
    minNotional: +(notional?.minNotional || 5),
    precision: s.quantityPrecision
  };
}

function roundQty(q, step) {
  return Math.floor(q / step) * step;
}

async function executeSignal(symbol, sig) {
  if (sig.side === 'WAIT') return null;

  const kind = state.market;
  const price = await getPrice(symbol, kind);
  const bal = await account(kind);
  let quote = 0;

  if (kind === 'spot') {
    const usdt = +(
      bal.balances.find(x => x.asset === 'USDT')?.free || 0
    );

    quote = Math.max(
      0,
      usdt * state.riskPct / 100
    );

    if (quote < 5) {
      throw Error(
        'Spot order below Binance minimum; available USDT or risk sizing too small.'
      );
    }

    return orderSpot(
      symbol,
      'BUY',
      quote.toFixed(2)
    );
  }

  const wallet = +bal.totalWalletBalance;
  const margin = wallet * state.riskPct / 100;
  const notional = margin * state.leverage;

  const rules = await qtyRules(symbol, 'futures');

  const qty = roundQty(
    notional / price,
    rules.step
  );

  if (
    qty < rules.min ||
    qty * price < rules.minNotional
  ) {
    throw Error(
      'Futures size below exchange minimum; increase risk/available margin.'
    );
  }

  await setLeverage(symbol, state.leverage);

  return orderFutures(
    symbol,
    sig.side === 'BUY' ? 'BUY' : 'SELL',
    qty.toFixed(rules.precision)
  );
}

async function scanCycle() {
  if (!state.auto) return;

  try {
    const kind = state.market;

    for (const symbol of state.symbols) {
      const k = await klines(
        symbol,
        kind,
        '5m',
        120
      );

      const sig = signal(k);

      state.lastScan = {
        at: new Date().toISOString(),
        symbol,
        ...sig
      };

      if (sig.side === 'WAIT') continue;

      // Spot trading is long-only in this version.
      if (kind === 'spot' && sig.side === 'SELL') {
        continue;
      }

      // Futures avoids duplicate exposure on the same symbol.
      if (kind === 'futures') {
        const pos = await openPositions();

        if (
          pos.length >= state.maxOpen ||
          pos.some(p => p.symbol === symbol)
        ) {
          continue;
        }
      }

      const order = await executeSignal(symbol, sig);

      if (order) {
        state.ledger.push({
          time: new Date().toISOString(),
          symbol,
          market: kind,
          side: sig.side,
          orderId: order.orderId,
          status: order.status,
          signal: sig.why
        });

        state.ledger = state.ledger.slice(-500);
        persist();
      }
    }
  } catch (e) {
    state.lastError = e.message;
    console.error('scan cycle:', e.message);
    persist();
  }
}

let timer = setInterval(
  scanCycle,
  Math.max(5000, state.scanSeconds * 1000)
);

app.get('/api/status', (req, res) => {
  res.json({
    ok: true,
    mode: state.mode,
    configured: !!(
      credentials().key &&
      credentials().secret
    ),
    auto: state.auto,
    settings: state,
    liveAvailable: true,
    lastScan: state.lastScan,
    lastError: state.lastError
  });
});

app.post('/api/mode', (req, res) => {
  const next = req.body?.mode;

  if (!['live', 'testnet'].includes(next)) {
    return res.status(400).json({
      error: 'Mode must be live or testnet'
    });
  }

  if (
    next === 'live' &&
    req.body?.confirm !== 'I UNDERSTAND LIVE TRADING'
  ) {
    return res.status(400).json({
      error: 'Live mode requires explicit confirmation'
    });
  }

  state.auto = false;
  state.mode = next;
  persist();

  res.json({
    ok: true,
    mode: state.mode,
    auto: false,
    message:
      'Mode changed. Automation stopped; restart explicitly.'
  });
});

app.get('/api/market', async (req, res) => {
  try {
    const symbols = (
      req.query.symbols || state.symbols.join(',')
    )
      .split(',')
      .slice(0, 30);

    const data = await Promise.all(
      symbols.map(async symbol => {
        const x = await request(
          'https://api.binance.com',
          '/api/v3/ticker/24hr',
          { symbol }
        );

        return {
          symbol: x.symbol,
          price: +x.lastPrice,
          change: +x.priceChangePercent,
          volume: +x.quoteVolume
        };
      })
    );

    res.json({
      mode: state.mode,
      data
    });
  } catch (e) {
    res.status(502).json({
      error: e.message
    });
  }
});

app.get('/api/account', async (req, res) => {
  try {
    res.json({
      mode: state.mode,
      account: await account()
    });
  } catch (e) {
    res.status(400).json({
      error: e.message
    });
  }
});

app.get('/api/positions', async (req, res) => {
  try {
    res.json({
      mode: state.mode,
      market: state.market,
      positions: await openPositions()
    });
  } catch (e) {
    res.status(400).json({
      error: e.message
    });
  }
});

app.get('/api/earnings', (req, res) => {
  res.json({
    ledger: state.ledger,
    mode: state.mode,
    note:
      'Exchange fills are authoritative; ledger records submitted orders only.'
  });
});

app.get('/api/orders', async (req, res) => {
  try {
    const symbol = (
      req.query.symbol || 'BTCUSDT'
    ).toUpperCase();

    res.json({
      orders: await signed(
        state.market === 'futures' ? 'futures' : 'spot',
        {
          endpoint: 'openOrders',
          symbol
        }
      ),
      mode: state.mode
    });
  } catch (e) {
    res.status(400).json({
      error: e.message
    });
  }
});

app.post('/api/settings', (req, res) => {
  const b = req.body || {};

  if (b.riskPct !== undefined) {
    const n = Number(b.riskPct);

    if (
      !Number.isFinite(n) ||
      n <= 0 ||
      n > 100
    ) {
      return res.status(400).json({
        error: 'Risk percent must be between 0 and 100.'
      });
    }

    state.riskPct = n;
  }

  if (b.maxOpen !== undefined) {
    state.maxOpen = Math.max(
      1,
      Math.min(30, Number(b.maxOpen) || 1)
    );
  }

  if (b.leverage !== undefined) {
    state.leverage = Math.max(
      1,
      Math.min(125, Number(b.leverage) || 1)
    );
  }

  if (b.market) {
    state.market = ['spot', 'futures'].includes(b.market)
      ? b.market
      : state.market;
  }

  if (b.symbols) {
    state.symbols = [
      ...new Set(
        b.symbols
          .map(x => String(x).toUpperCase())
          .filter(x => /^[A-Z0-9]{5,20}$/.test(x))
      )
    ].slice(0, 30);
  }

  if (b.scanSeconds !== undefined) {
    state.scanSeconds = Math.max(
      5,
      Math.min(300, Number(b.scanSeconds) || 15)
    );
  }

  persist();

  res.json({
    ok: true,
    settings: state
  });
});

app.post('/api/auto', (req, res) => {
  if (req.body?.enabled) {
    if (
      !credentials().key ||
      !credentials().secret
    ) {
      return res.status(400).json({
        error:
          'Configure API keys in backend environment first.'
      });
    }

    if (
      state.mode === 'live' &&
      req.body?.confirm !== 'I UNDERSTAND LIVE TRADING'
    ) {
      return res.status(400).json({
        error:
          'Confirm live trading before enabling.'
      });
    }
  }

  state.auto = !!req.body?.enabled;
  state.lastError = null;
  persist();

  res.json({
    ok: true,
    auto: state.auto,
    mode: state.mode
  });
});

app.post('/api/stop', (req, res) => {
  state.auto = false;
  persist();

  res.json({
    ok: true,
    auto: false,
    message:
      'Automation stopped. Existing exchange positions remain open; manage/close them separately.'
  });
});

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    service: 'Morgan Binance Autonomous Engine',
    time: new Date().toISOString()
  });
});

const port = process.env.PORT || 3000;

app.listen(port, () => {
  console.log(
    `Morgan Binance Bot running on ${port} (${state.mode})`
  );
});
