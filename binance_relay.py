#!/usr/bin/env python3
"""
Megan Futures relay for Termux (stdlib only, no pip installs).

It holds your Binance key, runs Megan's scan/entry/exit/risk engine on your phone and serves the
app on http://127.0.0.1:8787. Close the app, the relay keeps trading while Termux is alive.

  first time :  python binance_relay.py --setup
  every time :  nohup python binance_relay.py > relay.log 2>&1 &
  change key :  python binance_relay.py --setup --reset
Scope: USDT-M futures auto trading. Spot auto trading stays in the app (spot calls still go through the relay).
"""
import os, sys, re, json, time, hmac, math, hashlib, getpass, secrets, threading, http.client, urllib.parse, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor

DIR = os.path.expanduser('~/.megan_relay'); os.makedirs(DIR, exist_ok=True)
CF, SF = DIR + '/relay.json', DIR + '/state.json'
PORT = int(os.environ.get('PORT', '8787'))
now = lambda: int(time.time() * 1000)
cl = lambda x, a, b: max(a, min(b, x))
sign = lambda x: (x > 0) - (x < 0)

# ---------------------------------------------------------------- config / setup
def setup():
    c = {}
    if os.path.exists(CF) and '--reset' not in sys.argv:
        c = json.load(open(CF))
    if not c.get('k'):
        if ('--setup' in sys.argv or '--reset' in sys.argv) and sys.stdin.isatty():
            print('Binance key needs Reading + Futures. NEVER enable Withdrawals.')
            c['testnet'] = input('Use TESTNET (no real money)? [Y/n]: ').strip().lower() != 'n'
            if not c['testnet'] and input('Type LIVE to trade real money: ').strip() != 'LIVE':
                sys.exit('Cancelled.')
            c['k'] = getpass.getpass('API key: ').strip(); c['s'] = getpass.getpass('Secret key: ').strip()
        else:
            c.update(k='', s='', testnet=c.get('testnet', True))   # idle: log in from the app
        c['token'] = c.get('token') or secrets.token_hex(8)
        json.dump(c, open(CF, 'w')); os.chmod(CF, 0o600)
        print('Saved to', CF)
    c.setdefault('token', secrets.token_hex(8))
    return c

# ---------------------------------------------------------------- Binance REST
class Err(Exception):
    def __init__(s, c, m): super().__init__(m); s.code = c

CP, CPL = {}, threading.Lock()
def take_conn(host, fresh_only=False):
    # reuse a recently used connection only; Binance closes idle ones, so old ones are dropped instead of reused
    with CPL:
        l = CP.setdefault(host, [])
        while l and not fresh_only:
            cn, ts = l.pop()
            if time.time() - ts < 8: return cn, False
            try: cn.close()
            except Exception: pass
        if fresh_only:
            for cn, ts in l:
                try: cn.close()
                except Exception: pass
            l.clear()
    return http.client.HTTPSConnection(host, timeout=15), True
def give_conn(host, cn):
    with CPL:
        l = CP.setdefault(host, [])
        if len(l) < 8: l.append((cn, time.time())); return
    cn.close()

class BN:
    def __init__(s, c):
        s.k, s.s, s.off, s.until, s.w = c['k'], c['s'], 0, 0, 0
        s.base = {'f': 'https://demo-fapi.binance.com' if c['testnet'] else 'https://fapi.binance.com',
                  's': 'https://testnet.binance.vision' if c['testnet'] else 'https://api.binance.com'}
    def call(s, method, path, p=None, mode=0, mk='f'):
        if time.time() < s.until: raise Err(-1003, 'rate limited, pausing')
        q = [(k, v) for k, v in (p or {}).items() if v is not None]
        if mode == 1:
            q += [('recvWindow', 10000), ('timestamp', now() + s.off)]
            q.append(('signature', hmac.new(s.s.encode(), urllib.parse.urlencode(q).encode(), hashlib.sha256).hexdigest()))
        host = s.base[mk].split('//')[1]; target = path + ('?' + urllib.parse.urlencode(q) if q else '')
        hd = {'X-MBX-APIKEY': s.k, 'User-Agent': 'megan-relay'}
        for attempt in (0, 1):
            cn, fresh = take_conn(host, attempt == 1)
            try:
                cn.request(method, target, headers=hd); r = cn.getresponse(); body, st, h = r.read(), r.status, r.headers; give_conn(host, cn); break
            except Exception as e:
                try: cn.close()
                except Exception: pass
                if attempt == 0 and not fresh and method == 'GET': continue  # stale keep-alive connection, safe to repeat a read
                raise Err(0, 'network: %s' % e)
        try: j = json.loads(body)
        except Exception: j = None
        if h.get('x-mbx-used-weight-1m'): s.w = int(h.get('x-mbx-used-weight-1m'))
        if st in (429, 418):
            s.until = time.time() + int(h.get('retry-after') or 30); raise Err(st, 'rate limit hit')
        if st >= 400 or (isinstance(j, dict) and isinstance(j.get('code'), int) and j['code'] < 0):
            c = j.get('code', st) if isinstance(j, dict) else st
            if c == -1021: s.sync()
            raise Err(c, j.get('msg', 'HTTP %s' % st) if isinstance(j, dict) else 'HTTP %s' % st)
        return j
    def sync(s):
        try: t0 = now(); s.off = s.call('GET', '/fapi/v1/time')['serverTime'] - (t0 + now()) // 2
        except Exception: pass

C = setup()
if '--setup' in sys.argv:
    print('Token for the app:', C['token']); print('Now start it:  nohup python binance_relay.py > relay.log 2>&1 &'); sys.exit()
B = BN(C)

# ---------------------------------------------------------------- state
DEF = dict(riskPct=.25, lev=3, margin='ISOLATED', sizePct=8, maxOpen=1, minConf=68, slAtr=1.5, rr=2, lockR=1.0, trailGive=.25,
           runner=True, dailyLoss=3, dailyTarget=0, cooldownMin=10, minVol=30, universe=40, pairsMode='auto',
           whitelist=[], blacklist=[], longs=True, shorts=True, staleCandles=24, maxSpread=.08, minAtr=.12,
           futures=True, mode='custom', profile='aggressive',
           fast=True, aiOn=True, aiMin=55, aiUrl='https://megan-ai-relay.morgankaerega51.workers.dev')
PRE = {'safe': dict(riskPct=0.4, lev=3, sizePct=5, maxOpen=2, minConf=72, slAtr=1.6, rr=1.6, lockR=.7, trailGive=.3),
       'balanced': dict(riskPct=0.6, lev=5, sizePct=8, maxOpen=3, minConf=67, slAtr=1.5, rr=1.8, lockR=.8, trailGive=.33),
       'aggressive': dict(riskPct=0.25, lev=3, sizePct=8, maxOpen=1, minConf=68, slAtr=1.5, rr=2, lockR=1.0, trailGive=.25),
       'turbo': dict(riskPct=0.35, lev=4, sizePct=10, maxOpen=1, minConf=65, slAtr=1.4, rr=2.2, lockR=1.0, trailGive=.25)}
CFG, META, JOURNAL, COOL, LASTE = {}, {}, [], {}, {}
FI, T, KC, SIG, POS, BAL, LOGS = {}, {}, {}, {}, [], dict(wallet=0, avail=0), []
ST = dict(run=False, halt='', haltDay='', flat=False, flatUntil=0, dual=False, upd=0, statsAt=0, n=0, rbase=0, rbaseDay='')
DAYNET = dict(t=0, v=0); CLOSING, OPENING, EP, NOTE = {}, set(), {}, {}
LOCK = threading.Lock(); POOL = ThreadPoolExecutor(5)

def eff():
    E = dict(DEF); E.update(CFG)
    if E.get('mode') == 'full': E.update(PRE.get(E.get('profile'), PRE['aggressive']))
    return E

def log(kind, msg, alert=False):
    ST['n'] += 1; LOGS.insert(0, dict(n=ST['n'], t=now(), k=kind, m=msg, a=alert)); del LOGS[400:]
    print(time.strftime('%H:%M:%S'), kind.upper(), msg, flush=True)

def note(tag, e):
    if now() - NOTE.get(tag, 0) > 30000: NOTE[tag] = now(); log('warn', '%s: %s' % (tag, e))

def acct_id(): return hashlib.sha256((C.get('k') or '').encode()).hexdigest()[:10] + ('t' if C.get('testnet') else 'l')
def sf(): return DIR + '/state_%s.json' % acct_id()
mask = lambda: ('...' + C['k'][-4:]) if C.get('k') else ''

def save():
    if not C.get('k'): return
    with LOCK:
        try:
            json.dump(dict(cfg=CFG, meta=META, journal=JOURNAL[:600], cool=COOL, laste=LASTE, run=ST['run'], halt=ST['halt'], haltDay=ST['haltDay'], rbase=ST['rbase'], rbaseDay=ST['rbaseDay']), open(sf() + '.tmp', 'w'))
            os.replace(sf() + '.tmp', sf())
        except Exception: pass

def load():
    if not C.get('k'): return
    try: d = json.load(open(sf() if os.path.exists(sf()) else SF))   # SF = the old single state file, adopted once
    except Exception: return
    CFG.update(d.get('cfg', {})); META.update(d.get('meta', {})); JOURNAL[:] = d.get('journal', []); COOL.update(d.get('cool', {})); LASTE.update(d.get('laste', {}))
    ST.update(run=bool(d.get('run')) and bool(CFG), halt=d.get('halt', ''), haltDay=d.get('haltDay', ''), rbase=d.get('rbase', 0), rbaseDay=d.get('rbaseDay', ''))

# ---------------------------------------------------------------- helpers
def dec(x):
    s = ('%.10f' % x).rstrip('0'); return len(s.split('.')[1]) if '.' in s else 0
floor_step = lambda x, st: '%.*f' % (dec(st), math.floor(x / st + 1e-9) * st)
round_tick = lambda x, t: '%.*f' % (dec(t), round(x / t) * t)
mkey = lambda p: 'f:%s:%s' % (p['sym'], p['side'])
def safe(fn, *a):
    try: return fn(*a)
    except Exception as e: note(getattr(fn, '__name__', 'task'), e)

# ---------------------------------------------------------------- market data
def load_ex():
    for s in B.call('GET', '/fapi/v1/exchangeInfo')['symbols']:
        if s['quoteAsset'] != 'USDT' or s['status'] != 'TRADING' or s.get('contractType') != 'PERPETUAL': continue
        f = {x['filterType']: x for x in s['filters']}
        nf = f.get('MIN_NOTIONAL') or f.get('NOTIONAL') or {}
        FI[s['symbol']] = dict(tick=float(f['PRICE_FILTER']['tickSize']), step=float(f['LOT_SIZE']['stepSize']), minQty=float(f['LOT_SIZE']['minQty']),
                               mktMax=float((f.get('MARKET_LOT_SIZE') or {}).get('maxQty') or f['LOT_SIZE']['maxQty']), minNot=float(nf.get('notional') or nf.get('minNotional') or 5))

def load_stats():
    for x in B.call('GET', '/fapi/v1/ticker/24hr'):
        if x['symbol'] in FI:
            c, o = float(x['lastPrice']), float(x['openPrice']); prev = T.get(x['symbol'], {})
            T[x['symbol']] = dict(s=x['symbol'], c=prev.get('c', c) if now() - prev.get('live', 0) < 5000 else c, o=o, q=float(x['quoteVolume']), P=(c - o) / o * 100 if o else 0, live=prev.get('live', 0))
    ST['statsAt'] = now()

def ticks():
    for x in B.call('GET', '/fapi/v1/ticker/bookTicker'):
        t = T.get(x['symbol'])
        if t and float(x['bidPrice']) > 0 and float(x['askPrice']) > 0:
            t['bid'], t['ask'] = float(x['bidPrice']), float(x['askPrice']); t['c'] = (t['bid'] + t['ask']) / 2; t['live'] = now()

def klines(s, iv, lim):
    return [dict(t=k[0], o=float(k[1]), h=float(k[2]), l=float(k[3]), c=float(k[4]), v=float(k[5]), ct=k[6]) for k in B.call('GET', '/fapi/v1/klines', dict(symbol=s, interval=iv, limit=lim))]

closed = lambda bars: [b for b in bars if b['ct'] < now() + B.off]
px_of = lambda sym, fb=0: (T.get(sym) or {}).get('c') or fb

# ---------------------------------------------------------------- indicators + Megan's read (port of the app)
def ema(a, n):
    k = 2 / (n + 1); e = a[0]; o = [e]
    for x in a[1:]: e = x * k + e * (1 - k); o.append(e)
    return o

def rsi(c, n=14):
    if len(c) < n + 2: return 50
    g = l = 0
    for i in range(1, n + 1):
        d = c[i] - c[i - 1]
        if d >= 0: g += d
        else: l -= d
    g /= n; l /= n
    for i in range(n + 1, len(c)):
        d = c[i] - c[i - 1]; g = (g * (n - 1) + max(d, 0)) / n; l = (l * (n - 1) + max(-d, 0)) / n
    return 100 if l == 0 else 100 - 100 / (1 + g / l)

tr = lambda B_, i: max(B_[i]['h'] - B_[i]['l'], abs(B_[i]['h'] - B_[i - 1]['c']), abs(B_[i]['l'] - B_[i - 1]['c']))
def atr(B_, n=14):
    if len(B_) < n + 2: return 0
    a = sum(tr(B_, i) for i in range(1, n + 1)) / n
    for i in range(n + 1, len(B_)): a = (a * (n - 1) + tr(B_, i)) / n
    return a

def cd_detect(B_, e):
    if e < 16 or e >= len(B_): return None
    ids = range(max(1, e - 14), e + 1); at = sum(tr(B_, i) for i in ids) / len(ids)
    if not at > 0: return None
    bd = lambda c: abs(c['c'] - c['o']); rg = lambda c: max(1e-12, c['h'] - c['l'])
    bu = lambda c: c['c'] > c['o']; be = lambda c: c['c'] < c['o']; md = lambda c: (c['o'] + c['c']) / 2
    big = lambda c: bd(c) >= .7 * at and bd(c) >= .45 * rg(c)
    A, P, Q, R = B_[e], B_[e - 1], B_[e - 2], B_[e - 3]
    if big(R) and be(R) and bd(Q) <= .5 * bd(R) and bd(P) <= .5 * bd(R) and max(Q['h'], P['h']) <= R['c'] + .6 * bd(R) and big(A) and be(A) and A['c'] < R['c']: return (5, 'SELL')
    if big(R) and be(R) and bu(Q) and bd(Q) >= .3 * at and bu(P) and bd(P) >= .3 * at and P['c'] > Q['c'] and be(A) and bd(A) <= .6 * bd(P) and bd(A) <= .5 * at and A['c'] >= P['o']: return (6, 'BUY')
    if big(Q) and bu(Q) and bd(P) <= .6 * bd(Q) and P['c'] >= P['o'] - .05 * at and P['c'] >= Q['c'] - .3 * bd(Q) and big(A) and be(A) and bd(A) >= .6 * bd(Q) and A['c'] <= Q['c'] - .5 * bd(Q): return (1, 'SELL')
    if big(Q) and be(Q) and bd(P) <= .6 * bd(Q) and md(P) <= md(Q) and P['c'] <= Q['c'] + .3 * bd(Q) and big(A) and bu(A) and bd(A) >= .6 * bd(Q) and A['c'] >= Q['c'] + .5 * bd(Q): return (3, 'BUY')
    if big(Q) and bu(Q) and be(P) and bd(P) >= .35 * at and P['c'] <= md(Q) and bu(A) and bd(A) <= .6 * bd(P) and A['c'] < P['o']: return (4, 'SELL')
    if be(A) and rg(A) >= .8 * at and bd(A) <= .3 * rg(A) and (A['h'] - max(A['o'], A['c'])) >= .6 * rg(A) and (min(A['o'], A['c']) - A['l']) <= .12 * rg(A) and A['h'] >= max(P['h'], Q['h'], R['h']): return (2, 'SELL')

def struct(B_, at):
    n = len(B_) - 1; o = dict(dir=0, clear=False, shift=0, two=0)
    if n < 14: return o
    H, L = [], []
    for i in range(max(2, n - 70), n - 2):
        x = B_[i]
        if all(x['h'] > B_[j]['h'] for j in (i - 1, i - 2, i + 1, i + 2)): H.append(x['h'])
        if all(x['l'] < B_[j]['l'] for j in (i - 1, i - 2, i + 1, i + 2)): L.append(x['l'])
    c1, c2 = B_[n], B_[n - 1]
    o['two'] = 1 if (c1['c'] > c1['o'] and c2['c'] > c2['o']) else -1 if (c1['c'] < c1['o'] and c2['c'] < c2['o']) else 0
    if len(H) >= 2 and len(L) >= 2:
        H1, L1, H2, L2 = H[-2], L[-2], H[-1], L[-1]
        if H2 > H1 and L2 > L1: o['dir'], o['clear'] = 1, True
        elif H2 < H1 and L2 < L1: o['dir'], o['clear'] = -1, True
        up = c1['c'] > H2 + .1 * at and c1['c'] > c1['o']; dn = c1['c'] < L2 - .1 * at and c1['c'] < c1['o']
        if up and not dn and o['dir'] != 1: o.update(dir=1, clear=False, shift=1)
        elif dn and not up and o['dir'] != -1: o.update(dir=-1, clear=False, shift=-1)
    return o

def sd_zones(B, at):
    # same rules as the cTrader bot: a base of up to 3 candles before a strong impulse, still untouched by a closing break
    n = len(B) - 1; out = dict(d=None, s=None)
    for i in range(n - 3, max(3, n - 81), -1):
        if out['d'] and out['s']: break
        up = (B[i+1]['c'] - B[i]['c']) >= 1.4 * at and B[i+1]['c'] > B[i+1]['o']
        dn = (B[i]['c'] - B[i+1]['c']) >= 1.4 * at and B[i+1]['c'] < B[i+1]['o']
        if not up and not dn: continue
        base = B[max(0, i - 2):i + 1]; zl = min(x['l'] for x in base); zh = max(x['h'] for x in base)
        if zh - zl > 1.6 * at: continue
        if up and not out['d'] and not any(x['c'] < zl for x in B[i+2:]): out['d'] = dict(zl=zl, zh=zh, i=i)
        if dn and not out['s'] and not any(x['c'] > zh for x in B[i+2:]): out['s'] = dict(zl=zl, zh=zh, i=i)
    return out

ZST = {}
def zone_scan(B, at):
    out = []; n = len(B) - 1
    for i in range(n - 3, max(3, n - 121), -1):
        up = (B[i+1]['c'] - B[i]['c']) >= 1.4 * at and B[i+1]['c'] > B[i+1]['o']
        dn = (B[i]['c'] - B[i+1]['c']) >= 1.4 * at and B[i+1]['c'] < B[i+1]['o']
        if not up and not dn: continue
        base = B[max(0, i - 2):i + 1]; zl = min(x['l'] for x in base); zh = max(x['h'] for x in base)
        if zh - zl > 1.6 * at: continue
        out.append(dict(kind='d' if up else 's', zl=zl, zh=zh, t0=B[i]['t']))
    return out

def stable_zones(sym, tag, B, at):
    # Remembers every zone by its base candle. Boundaries never move. Dead = a candle closed through it. Fresh = touched at most once.
    st = ZST.setdefault((sym, tag), {})
    for z in zone_scan(B, at):
        zid = '%s%d' % (z['kind'], z['t0'])
        if zid not in st: st[zid] = dict(kind=z['kind'], zl=z['zl'], zh=z['zh'], t0=z['t0'], dead=False)
    px = B[-1]['c']; best = dict(d=None, s=None)
    idx = {b['t']: k for k, b in enumerate(B)}
    for zid, z in list(st.items()):
        if now() - z['t0'] > 6 * 3600000: st.pop(zid); continue
        j = idx.get(z['t0'], -1); after = B[j + 2:] if j >= 0 else B
        if not z['dead']:
            z['dead'] = any((b['c'] < z['zl']) if z['kind'] == 'd' else (b['c'] > z['zh']) for b in after)
        if z['dead']: continue
        vis, inz = 0, False
        for b in after:
            hit = (b['l'] <= z['zh']) if z['kind'] == 'd' else (b['h'] >= z['zl'])
            if hit and not inz: vis += 1
            inz = hit
        if vis > 1: continue
        if z['kind'] == 'd' and z['zl'] <= px and (best['d'] is None or z['zh'] > best['d']['zh']): best['d'] = dict(zl=z['zl'], zh=z['zh'], i=max(j, 0))
        if z['kind'] == 's' and z['zh'] >= px and (best['s'] is None or z['zl'] < best['s']['zl']): best['s'] = dict(zl=z['zl'], zh=z['zh'], i=max(j, 0))
    return best

def rejection(B, d):
    # a reversal needs proof: a candle that wicked into the zone and closed back out of it
    for b in B[-2:]:
        rg = b['h'] - b['l']
        if rg <= 0: continue
        if d > 0 and min(b['o'], b['c']) - b['l'] >= .4 * rg and b['c'] >= b['l'] + .5 * rg: return True
        if d < 0 and b['h'] - max(b['o'], b['c']) >= .4 * rg and b['c'] <= b['h'] - .5 * rg: return True
    return False

def analyze(sym, B5, B15, E):
    n = len(B5) - 1
    if n < 60: return None
    c = [b['c'] for b in B5]; e9, e21, e50 = ema(c, 9), ema(c, 21), ema(c, 50); at = atr(B5); px = c[n]
    if not (at > 0 and px > 0): return None
    ap = at / px * 100; last = B5[n]; body = abs(last['c'] - last['o'])
    trend = cl((e9[n] - e21[n]) / at * .8, -1, 1) * .6 + cl((e21[n] - e50[n]) / at * .5, -1, 1) * .4
    slope = cl((e21[n] - e21[n - 5]) / at, -1, 1); mom = cl((c[n] - c[n - 3]) / at / 2, -1, 1); r = rsi(c)
    va = sum(b['v'] for b in B5[n - 20:n]) / 20; vol = last['v'] / va if va > 0 else 1; st = struct(B5, at)
    t15 = 0; at2 = 0; Zh = dict(d=None, s=None)
    if B15 and len(B15) >= 55:
        c2 = [b['c'] for b in B15]; a2 = atr(B15); f, s_ = ema(c2, 9), ema(c2, 21); m = len(c2) - 1
        if a2 > 0: t15 = cl((f[m] - s_[m]) / a2 * .8, -1, 1); at2 = a2; Zh = stable_zones(sym, 'b', B15, a2)
    pat = cd_detect(B5, n); patS = 0 if not pat else (1 if pat[1] == 'BUY' else -1)
    hh = max(b['h'] for b in B5[n - 20:n]); ll = min(b['l'] for b in B5[n - 20:n])
    brk = 1 if (c[n] > hh and vol >= 1.5 and body >= .5 * at and last['c'] > last['o']) else -1 if (c[n] < ll and vol >= 1.5 and body >= .5 * at and last['c'] < last['o']) else 0
    L3 = B5[n - 2:n + 1]
    pbU = 1 if (trend > .25 and st['dir'] >= 0 and min(b['l'] for b in L3) <= e21[n] + .35 * at and last['c'] > last['o'] and body >= .3 * at and c[n] > e21[n]) else 0
    pbD = -1 if (trend < -.25 and st['dir'] <= 0 and max(b['h'] for b in L3) >= e21[n] - .35 * at and last['c'] < last['o'] and body >= .3 * at and c[n] < e21[n]) else 0
    pb = pbU or pbD
    sc = trend * .28 + mom * .14 + slope * .06 + t15 * .18 + st['dir'] * (.12 if st['clear'] else .05) + (sign(mom) * .07 if vol > 1.3 else 0) + patS * .22 + brk * .18 + pb * .14
    if sc > 0 and r > 80: sc *= .6
    if sc < 0 and r < 20: sc *= .6
    d = 1 if sc >= 0 else -1; conf = int(cl(50 + abs(sc) * 60, 50, 96) + .5)
    if pat and patS != d: pat = None
    Z = stable_zones(sym, 'a', B5, at); zd, zs, zb = Z['d'], Z['s'], ''
    if d > 0 and zd and zd['zl'] - .2 * at <= px <= zd['zh'] + .4 * at and rejection(B5, d): conf = min(96, conf + 6); zb = 'Demand Bounce'
    if d < 0 and zs and zs['zl'] - .4 * at <= px <= zs['zh'] + .2 * at and rejection(B5, d): conf = min(96, conf + 6); zb = 'Supply Rejection'
    if not zb and at2 > 0:   # zones on the slower chart count too: that is the chart you look at
        if d > 0 and Zh['d'] and Zh['d']['zl'] - .2 * at2 <= px <= Zh['d']['zh'] + .5 * at2 and rejection(B5, d): conf = min(96, conf + 6); zb = '5m Demand Bounce'
        if d < 0 and Zh['s'] and Zh['s']['zl'] - .5 * at2 <= px <= Zh['s']['zh'] + .2 * at2 and rejection(B5, d): conf = min(96, conf + 6); zb = '5m Supply Rejection'
    rh, rl = max(b['h'] for b in B5[n - 59:n + 1]), min(b['l'] for b in B5[n - 59:n + 1]); loc = (px - rl) / (rh - rl) if rh > rl else .5
    ok, block = True, ''
    w8 = B5[n - 7:n + 1]; run = (px - min(b['l'] for b in w8)) / at if d > 0 else (max(b['h'] for b in w8) - px) / at; stretch = (px - e21[n]) / at * d
    if st['dir'] == -d and not (brk == d or st['shift'] == d): ok, block = False, 'confirmed structure is against it'
    elif sign(trend) != d and not pat and brk != d and abs(trend) > .12: ok, block = False, 'trend does not agree'
    elif t15 * d < -.08 and brk != d: ok, block = False, 'the 5m trend is against it'
    elif trend * d < -.15 and brk != d: ok, block = False, 'the short-term trend is against it'
    elif ap < E['minAtr'] * (.5 if E.get('fast', True) else 1): ok, block = False, 'too quiet'
    elif ap > 6: ok, block = False, 'too wild'
    elif abs(sc) < .12: ok, block = False, 'no clear edge'
    elif (d > 0 and zs and zs['zl'] - .6 * at <= px <= zs['zh']) or (d < 0 and zd and zd['zl'] <= px <= zd['zh'] + .6 * at): ok, block = False, 'price is right at a %s zone' % ('supply' if d > 0 else 'demand')
    elif at2 > 0 and ((d > 0 and Zh['s'] and Zh['s']['zl'] - .8 * at2 <= px <= Zh['s']['zh']) or (d < 0 and Zh['d'] and Zh['d']['zl'] <= px <= Zh['d']['zh'] + .8 * at2)): ok, block = False, 'price is right at a 5m %s zone' % ('supply' if d > 0 else 'demand')
    elif not zb and brk != d and ((d < 0 and loc < .30) or (d > 0 and loc > .70)): ok, block = False, 'price is already at the %s of its recent range, too little room to move' % ('bottom' if d < 0 else 'top')
    elif run > (4.5 if brk == d else 3.2) or stretch > 1.7: ok, block = False, 'already moved far, entering now would be chasing'
    elif (r > 74 if d > 0 else r < 26): ok, block = False, 'overextended (RSI %d)' % r
    strat = 'Candle Pattern #%d' % pat[0] if pat else 'Volume Breakout' if brk == d else 'Pullback Resume' if pb == d else 'Trend Follow'
    strat = zb or strat
    return dict(sym=sym, dir=d, side='LONG' if d > 0 else 'SHORT', conf=conf, strat=strat, ok=ok, block=block, atr=at, atrPct=ap, px=px, trend=trend, mom=mom, vol=vol, st=st, zones=Z, zonesH=Zh, atrH=at2, loc=loc, candleT=last['t'], ct=last['ct'], ts=now())

flip_against = lambda sg, d: bool(sg) and sg['st']['dir'] == -d and sg['st']['two'] == -d and sg['trend'] * d < -.05 and sg['mom'] * d < -.02 and sg['conf'] >= 58
def plan_for(sg, E, lev=None):
    px = sg['px']; mn = px * .005; sl = max(E['slAtr'] * sg['atr'], .8 * sg.get('atrH', 0), mn); sl = min(sl, px * (.6 / max(1, lev or E['lev'])))
    sl = max(sl, mn * .8) if sl < mn * .8 else sl
    return sl, sl * E['rr']
def ladder(eq, E):
    # bigger balance = more positions (never above 5) and more expensive markets; a $1 account gets one small trade on a cheap coin
    if eq < 5: return dict(lev=20, cap=85, open=1, risk=6)
    if eq < 20: return dict(lev=10, cap=60, open=1, risk=4)
    if eq < 50: return dict(lev=8, cap=40, open=2, risk=3)
    if eq < 150: return dict(lev=6, cap=25, open=3, risk=2.5)
    if eq < 500: return dict(lev=5, cap=min(E['sizePct'], 15), open=4, risk=max(E['riskPct'] * 1.5, 1.5))
    return dict(lev=min(E['lev'], 5), cap=min(E['sizePct'], 8), open=5, risk=max(E['riskPct'] * 1.5, 1.5))

def min_cost(sym):
    fi = FI[sym]; px = px_of(sym) or 0
    return max(fi['minNot'], fi['minQty'] * px, fi['step'] * px)
def affordable(sym, E):
    L = ladder(max(BAL['wallet'], 0), E); cap = max(BAL['avail'], 0) * min(L['cap'], 90) / 100
    return min_cost(sym) * 1.06 <= cap * L['lev']

def size_plan(E, slp, min_not):
    eq, avail = max(BAL['wallet'], 0), BAL['avail']; L = ladder(eq, E)
    capm = min(avail * L['cap'] / 100, avail * .9); maxlev = max(1, min(L['lev'], safe_lev(slp, max(E['lev'], L['lev']))))
    want = eq * E['riskPct'] / 100 / max(slp, 1e-6) if E.get('riskPct', 0) > 0 else 1e18
    notional = max(min(want, capm * maxlev), min_not * 1.06)
    if capm <= 0: return None
    lev = max(1, math.ceil(notional / capm - 1e-9))
    if lev > maxlev or notional * slp > eq * L['risk'] / 100: return None  # does not fit, or the minimum order is too risky for this balance
    return dict(lev=lev, margin=notional / lev, notional=notional)
safe_lev = lambda slp, want: max(1, min(int(want), int(.6 / max(slp, .0005))))
STABLE = re.compile(r'^(USDC|FDUSD|TUSD|BUSD|USDP|DAI|EUR|AEUR|USD1|XUSD)')

def realized(sym, since):
    try:
        j = B.call('GET', '/fapi/v1/userTrades', dict(symbol=sym, startTime=int(since) - 60000, limit=100), 1)
        g = sum(float(x.get('realizedPnl', 0)) for x in j); fee = sum(abs(float(x.get('commission', 0))) for x in j if x.get('commissionAsset') == 'USDT')
        return g - fee
    except Exception: return None

def strat_stats(strat):
    xs = [j for j in JOURNAL if j.get('strat') == strat and 'net' in j][:20]
    return len(xs), sum(1 for j in xs if j['net'] > 0), sum(j['net'] for j in xs)

def strat_bad(strat):
    n, w, net = strat_stats(strat)
    return n >= 8 and net < 0 and w / n < .40

def pair_bad(sym):
    xs = [j for j in JOURNAL[:12] if j.get('sym') == sym and 'net' in j][:2]
    return len(xs) == 2 and all(j['net'] < 0 for j in xs) and now() - xs[0]['t'] < 3 * 3600000

def candidates(E):
    arr = [x for x in T.values() if x['s'] in FI and x['s'] not in E['blacklist'] and not STABLE.match(x['s']) and abs(x.get('P', 0)) < 30 and affordable(x['s'], E)]
    if E['pairsMode'] == 'manual': w = set(E['whitelist']); return [x['s'] for x in arr if x['s'] in w]
    byv = sorted([x for x in arr if x['q'] >= E['minVol'] * 1e6], key=lambda x: -x['q'])[:int(E['universe']) + (30 if BAL['wallet'] < 50 else 0)]
    mov = sorted([x for x in arr if x['q'] >= E['minVol'] * .3e6], key=lambda x: -abs(x['P']))[:8]
    return list(dict.fromkeys(x['s'] for x in byv + mov))

T15 = {}
def trend15(sym):
    v = T15.get(sym)
    if v and now() - v[0] < 240000: return v[1]
    k = closed(klines(sym, '15m', 60)); t = None
    if len(k) >= 30:
        c = [b['c'] for b in k]; a = atr(k)
        if a > 0: f, sl = ema(c, 9), ema(c, 21); t = cl((f[-1] - sl[-1]) / a * .8, -1, 1)
    T15[sym] = (now(), t); return t

def scan():
    E = eff()
    if now() - ST['statsAt'] > 55000: load_stats()
    cands = candidates(E); nt = now() + B.off
    need = [s for s in cands if s not in KC or nt > KC[s]['next'] or now() - KC[s]['ts'] > 120000][:16]
    def one(s):
        fast = E.get('fast', True); b5, b15 = closed(klines(s, '1m' if fast else '5m', 99)), closed(klines(s, '5m' if fast else '15m', 70))
        KC[s] = dict(ts=now(), next=(b5[-1]['ct'] if b5 else nt) + (61500 if fast else 301500))
        sg = analyze(s, b5, b15, E)
        if sg and fast and sg['ok'] and sg['conf'] >= E['minConf']:
            t4 = trend15(s)
            if t4 is not None:
                sg['t4'] = t4
                if t4 * sg['dir'] < -.2: sg['ok'] = False; sg['block'] = 'the 15m trend is against it'
        if sg: SIG[s] = sg
    list(POOL.map(lambda s: safe(one, s), need))
    for s in [s for s, v in SIG.items() if s not in cands and now() - v['ts'] > 600000]: SIG.pop(s, None)

# ---------------------------------------------------------------- account
def ftry(key, paths):
    for p in ([EP[key]] if key in EP else paths):
        try: r = B.call('GET', p, {}, 1); EP[key] = p; return r
        except Err as e:
            if e.code in (-2015, -1022, -1021, -2014, 0): raise
    raise Err(-1, 'no balance/position endpoint worked')

def refresh():
    b = ftry('bal', ['/fapi/v3/balance', '/fapi/v2/balance']); p = ftry('pos', ['/fapi/v2/positionRisk', '/fapi/v3/positionRisk'])
    u = next((x for x in b if x['asset'] == 'USDT'), {}); BAL.update(wallet=float(u.get('balance', 0)), avail=float(u.get('availableBalance', 0)))
    out = []
    for x in p:
        amt = float(x['positionAmt'])
        if amt == 0: continue
        side = 'SHORT' if x.get('positionSide') == 'SHORT' or (x.get('positionSide') != 'LONG' and amt < 0) else 'LONG'
        lev = int(float(x.get('leverage') or (META.get('f:%s:%s' % (x['symbol'], side)) or {}).get('lev') or 1))
        out.append(dict(sym=x['symbol'], side=side, qty=abs(amt), qtyStr=str(x['positionAmt']).lstrip('-'), entry=float(x['entryPrice']), mark=float(x['markPrice']), lev=lev))
    POS[:] = out; ST['upd'] = now()

def daynet():
    t0 = int(time.time() // 86400 * 86400 * 1000)
    try:
        j = B.call('GET', '/fapi/v1/income', dict(startTime=t0, limit=1000), 1)
        v = sum(float(r['income']) for r in j if r['incomeType'] in ('REALIZED_PNL', 'COMMISSION', 'FUNDING_FEE'))
    except Err as e:
        if e.code not in (-2015, -2014, -1022) or BAL['wallet'] <= 0: raise
        dk = time.strftime('%Y-%m-%d', time.gmtime())
        if ST.get('wbaseDay') != dk: ST['wbaseDay'] = dk; ST['wbase'] = BAL['wallet']
        v = BAL['wallet'] - ST['wbase']
        note('daynet', 'Binance refused the income history (%s). Using the wallet balance change for the daily loss limit instead.' % e)
    DAYNET.update(t=now(), v=v)

upnl = lambda p: (px_of(p['sym'], p['mark']) - p['entry']) * p['qty'] * (1 if p['side'] == 'LONG' else -1)

# ---------------------------------------------------------------- orders
def place_algo(sym, side, typ, trig, ps):
    base = dict(symbol=sym, side=side, type=typ, triggerPrice=trig, workingType='MARK_PRICE', closePosition='true')
    if ST['dual']: base['positionSide'] = ps
    try: return dict(id=B.call('POST', '/fapi/v1/algoOrder', dict(base, algoType='CONDITIONAL'), 1)['algoId'], kind='algo')
    except Err as e:
        if e.code in (-2015, -1022, -1021, -2019): raise
        o = dict(symbol=sym, side=side, type=typ, stopPrice=trig, workingType='MARK_PRICE', closePosition='true')
        if ST['dual']: o['positionSide'] = ps
        try: return dict(id=B.call('POST', '/fapi/v1/order', o, 1)['orderId'], kind='std')
        except Err: raise e

def cancel_prot(sym, ids):
    for x in ids or []:
        if not x: continue
        try:
            if x['kind'] == 'algo': B.call('DELETE', '/fapi/v1/algoOrder', dict(algoId=x['id']), 1)
            else: B.call('DELETE', '/fapi/v1/order', dict(symbol=sym, orderId=x['id']), 1)
        except Exception: pass

def cancel_all(sym):
    for path in ('/fapi/v1/algoOpenOrders', '/fapi/v1/allOpenOrders'):
        try: B.call('DELETE', path, dict(symbol=sym), 1)
        except Exception: pass

def close_fut(p, reason):
    k = mkey(p)
    if now() - CLOSING.get(k, 0) < 8000: return
    CLOSING[k] = now()
    try:
        par = dict(symbol=p['sym'], side='SELL' if p['side'] == 'LONG' else 'BUY', type='MARKET', quantity=p['qtyStr'], newOrderRespType='RESULT')
        if ST['dual']: par['positionSide'] = p['side']
        else: par['reduceOnly'] = 'true'
        B.call('POST', '/fapi/v1/order', par, 1); cancel_all(p['sym']); META.pop(k, None)
        pn = upnl(p); E = eff()
        JOURNAL.insert(0, dict(t=now(), venue='f', sym=p['sym'], side=p['side'], reason=reason, upnl=pn, net=pn, strat=(META.get(k) or {}).get('strat', ''), held=int((now() - (META.get(k) or {}).get('openedAt', now())) / 60000), auto=True, risk=bool(re.search('stop|lock|guard|flipped|follow', reason, re.I))))
        if pn > 0: COOL.pop(p['sym'], None)
        else: COOL[p['sym']] = now() + E['cooldownMin'] * 120000; ST['lossUntil'] = now() + 60000
        log('win' if pn > 0 else 'loss', 'CLOSED %s %s: %s. About %+.2f USDT before fees.' % (p['side'], p['sym'], reason, pn), True); save()
    except Exception: CLOSING.pop(k, None); raise

def open_fut(s, E, sl, tp):
    sym, fi = s['sym'], FI[s['sym']]; OPENING.add(sym); LASTE[sym] = s['candleT']  # one try per candle, so a skip never repeats or blocks other pairs
    try:
        sp0 = size_plan(E, sl / s['px'], fi['minNot']); avail = BAL['avail']
        if not sp0: return log('info', '%s: does not fit the balance right now, skipped.' % sym)
        lev0, margin = sp0['lev'], sp0['margin']
        try: bt = B.call('GET', '/fapi/v1/ticker/bookTicker', dict(symbol=sym)); sp = (float(bt['askPrice']) - float(bt['bidPrice'])) / float(bt['bidPrice']) * 100
        except Err: sp = 0
        if sp > E['maxSpread']: return log('info', '%s: spread %.3f%% too wide, skipped.' % (sym, sp))
        try: B.call('POST', '/fapi/v1/marginType', dict(symbol=sym, marginType='CROSSED' if E['margin'] == 'CROSSED' else 'ISOLATED'), 1)
        except Err as e:
            if e.code in (-2015, -1022): raise
        lev, tries = lev0, 0
        while True:
            try: lev = int(B.call('POST', '/fapi/v1/leverage', dict(symbol=sym, leverage=lev), 1).get('leverage', lev)); break
            except Err as e:
                tries += 1
                if tries < 6 and lev > 1 and (e.code == -4028 or 'leverage' in str(e).lower()): lev = max(1, lev // 2); continue
                raise
        price = px_of(sym); qty = float(floor_step(margin * lev / price, fi['step']))
        if qty * price < fi['minNot'] * 1.02:
            need = math.ceil(fi['minNot'] * 1.06 / price / fi['step']) * fi['step']
            if need * price / lev <= max(avail, 0) * .95 and need * price / lev <= margin * 1.35 + .3: qty = float(floor_step(need + fi['step'] * .001, fi['step']))
            else: return log('info', '%s: margin too small for the minimum order, skipped.' % sym)
        qty = min(max(qty, fi['minQty']), fi['mktMax'])
        p = dict(symbol=sym, side='BUY' if s['side'] == 'LONG' else 'SELL', type='MARKET', quantity=floor_step(qty, fi['step']), newOrderRespType='RESULT')
        if ST['dual']: p['positionSide'] = s['side']
        r = B.call('POST', '/fapi/v1/order', p, 1)
        entry = float(r.get('avgPrice') or 0)
        if not entry:
            refresh(); q = next((x for x in POS if x['sym'] == sym and x['side'] == s['side']), None); entry = q['entry'] if q else price
        d = 1 if s['side'] == 'LONG' else -1; opp = 'SELL' if d > 0 else 'BUY'
        m = dict(venue='f', sym=sym, side=s['side'], entry=entry, lev=lev, auto=True, openedAt=now(), strat=s['strat'], conf=s['conf'], why=s['strat'], peak=0, locked=False,
                 runner=bool(E['runner']), qty=float(r.get('executedQty') or 0), spr0=sp / 100, r=sl, sl=entry - d * sl, tp=entry + d * tp)
        key = 'f:%s:%s' % (sym, s['side']); META[key] = m
        try: m['slId'] = place_algo(sym, opp, 'STOP_MARKET', round_tick(m['sl'], fi['tick']), s['side'])
        except Exception as e:
            log('err', '%s: could not place the exchange stop (%s). Closing it, no unprotected trades.' % (sym, e), True)
            refresh(); q = next((x for x in POS if x['sym'] == sym and x['side'] == s['side']), None)
            if q: close_fut(q, 'No stop could be placed')
            return
        if not m['runner']:
            try: m['tpId'] = place_algo(sym, opp, 'TAKE_PROFIT_MARKET', round_tick(m['tp'], fi['tick']), s['side'])
            except Exception as e: log('warn', '%s: take-profit not placed: %s' % (sym, e))
        log('bet', '%s %s %dx @ %s | stop %s%s | %s %d%%' % (s['side'], sym, lev, entry, round_tick(m['sl'], fi['tick']), '' if m['runner'] else ' | target ' + round_tick(m['tp'], fi['tick']), s['strat'], s['conf']), True)
        save(); safe(refresh)
    finally: OPENING.discard(sym)

# ---------------------------------------------------------------- exits, risk, entries
def eval_exit(m, d, price, E, spr=0):
    R = m.get('r')
    if not R or not m.get('sl'): return None, False
    prof = (price - m['entry']) * d
    m['peak'] = max(m.get('peak', 0), prof)
    # The exchange-side protective stop is authoritative; do not exit on a tiny tick.
    if (price - m['sl']) * d <= 0:
        return ('Profit lock' if m.get('locked') and prof > 0 else 'Stop loss'), False
    if not m.get('runner') and m.get('tp') and (price - m['tp']) * d >= 0:
        return 'Take profit', False
    f = m['entry'] * (.0014 + max(spr, m.get('spr0', 0)))
    pk = m['peak']
    # Wait for meaningful movement before protecting profit; no half-close at tiny R.
    if not m.get('locked') and pk >= max(1.0 * R, 4 * f):
        be = m['entry'] + d * max(2 * f, .10 * R)
        if (be - m['sl']) * d > 0:
            m['sl'] = be; m['locked'] = True; m['tier'] = 1
            log('info', '%s: profit protection activated after a meaningful move' % m.get('sym', ''))
            changed = True
        else: changed = False
    else: changed = False
    if pk >= max(1.5 * R, 6 * f):
        keep = .45 if pk < 2.5 * R else .62 if pk < 4 * R else .78
        trl = m['entry'] + d * max(2 * f, pk * keep)
        if (trl - m['sl']) * d > 0:
            m['sl'] = trl; m['locked'] = True; changed = True
    # Time exit only after a long wait and only if trade is genuinely failing.
    if now() - m['openedAt'] > max(E['staleCandles'], 36) * 300000 and prof < -.25 * R:
        return 'Stale losing trade', False
    return None, changed

def sync_stop(p, m, fi):
    if time.time() - m.get('lastAmend', 0) < 4: return
    was = m.get('slSent', m['entry'] - (1 if p['side'] == 'LONG' else -1) * m['r'])
    if abs(m['sl'] - was) < min(.2 * m['r'], m['entry'] * .0027): return
    m['lastAmend'] = time.time()
    try:
        old = m.get('slId'); m['slId'] = place_algo(p['sym'], 'SELL' if p['side'] == 'LONG' else 'BUY', 'STOP_MARKET', round_tick(m['sl'], fi['tick']), p['side']); m['slSent'] = m['sl']; cancel_prot(p['sym'], [old])
    except Exception: pass

def partial_close(p, m):
    fi = FI[p['sym']]; half = float(floor_step(p['qty'] / 2, fi['step'])); m['wantScale'] = False; m['scaled'] = True
    if half <= 0 or half < fi['minQty'] or p['qty'] - half < fi['minQty']: return  # too small to split, the trailing stop handles it
    par = dict(symbol=p['sym'], side='SELL' if p['side'] == 'LONG' else 'BUY', type='MARKET', quantity=floor_step(half, fi['step']), newOrderRespType='RESULT')
    if ST['dual']: par['positionSide'] = p['side']
    else: par['reduceOnly'] = 'true'
    try: B.call('POST', '/fapi/v1/order', par, 1)
    except Err as e:
        if e.code == 0: m['scaled'] = False  # network problem, try again
        raise
    d = 1 if p['side'] == 'LONG' else -1; got = (px_of(p['sym'], p['mark']) - p['entry']) * half * d
    m['banked'] = m.get('banked', 0) + got; m['qty'] = p['qty'] - half
    f = m['entry'] * (.0014 + m.get('spr0', 0)); ns = m['entry'] + d * max(f, .45 * m['peak'])
    if (ns - m['sl']) * d > 0: m['sl'] = ns
    log('win', 'BANKED half of %s %s: about %+.2f USDT in cash, the other half keeps running with a locked stop.' % (p['side'], p['sym'], got), True)
    safe(refresh); save()

def has_stop(sym):
    # True/False only when Binance clearly answers, None when unsure (then nothing is changed)
    try: j = B.call('GET', '/fapi/v1/openAlgoOrders', dict(symbol=sym), 1)
    except Exception: return None
    if isinstance(j, dict): j = j.get('orders') or j.get('data') or j.get('rows')
    if not isinstance(j, list): return None
    return any(str(x.get('orderType') or x.get('type') or x.get('algoType') or '').upper().startswith('STOP') for x in j)

def guard_stops():
    # Every auto trade must keep its exchange stop, even when Megan is paused. If price already ran through the stop, close at market.
    if not C.get('k') or not ST.get('boot') or now() - ST['upd'] > 60000: return
    for p in list(POS):
        m = META.get(mkey(p))
        if not m or not m.get('auto') or not m.get('sl') or not m.get('r'): continue
        price = px_of(p['sym'], p['mark']); d = 1 if p['side'] == 'LONG' else -1
        if price and (price - m['sl']) * d < -.25 * m['r']:
            log('err', '%s: price is past the stop but the trade is still open. Closing it now at market.' % p['sym'], True); safe(close_fut, p, 'Backup stop: the exchange stop did not fire'); continue
        if now() - m.get('stopOk', 0) < 600000 or time.time() - m.get('guardAt', 0) < 30: continue
        m['guardAt'] = time.time(); hs = has_stop(p['sym'])
        if hs is None: continue
        if hs: m['stopOk'] = now(); continue
        try:
            place_algo(p['sym'], 'SELL' if p['side'] == 'LONG' else 'BUY', 'STOP_MARKET', round_tick(m['sl'], FI[p['sym']]['tick']), p['side']); m['stopOk'] = now()
            log('warn', '%s: the exchange stop was missing. I put it back.' % p['sym'], True)
        except Exception as e:
            msg = str(e)
            if 'existing' in msg or 'open positions' in msg: m['stopOk'] = now(); continue      # the stop is there, or the trade just closed
            if now() - m.get('guardErrAt', 0) > 600000: m['guardErrAt'] = now(); log('err', '%s: I could not check or place the exchange stop (%s).' % (p['sym'], msg[:90]), True)

def manage():
    if not ST['run'] or now() - ST['upd'] > 30000: return
    E = eff()
    for p in list(POS):
        m = META.get(mkey(p)); bk = T.get(p['sym']) or {}; price = (bk.get('bid') if p['side'] == 'LONG' else bk.get('ask')) or px_of(p['sym'], p['mark'])
        if not m or not m.get('auto') or not m.get('sl') or not price: continue
        spr = (bk['ask'] - bk['bid']) / bk['c'] if bk.get('bid') and bk.get('ask') and bk.get('c') else 0
        why, ch = eval_exit(m, 1 if p['side'] == 'LONG' else -1, price, E, spr)
        if why: safe(close_fut, p, why)
        elif ch: sync_stop(p, m, FI[p['sym']])
    for k in [k for k, m in META.items() if k[:2] == 'f:' and now() - m.get('openedAt', 0) > 20000 and not any(mkey(p) == k for p in POS) and ST['upd'] > now() - 15000]:
        m = META.pop(k)
        if m.get('locked'): COOL.pop(m['sym'], None)
        else: COOL[m['sym']] = now() + eff()['cooldownMin'] * 120000; ST['lossUntil'] = now() + 60000
        net = realized(m['sym'], m.get('openedAt', 0))
        if net is None: net = ((m.get('sl') or m['entry']) - m['entry']) * (1 if m['side'] == 'LONG' else -1) * (m.get('qty') or 0)   # from the stop price when Binance will not say
        JOURNAL.insert(0, dict(t=now(), venue='f', sym=m['sym'], side=m['side'], reason='Closed on exchange (stop/target)', upnl=net, net=net, strat=m.get('strat', ''), held=int((now() - m.get('openedAt', now())) / 60000), auto=bool(m.get('auto')), risk=True)); save()

def risk_guard():
    if not ST['run']: return
    E, dk = eff(), time.strftime('%Y-%m-%d', time.gmtime())
    if ST['halt'] and ST['haltDay'] != dk: ST.update(halt='', haltDay=''); log('info', 'New trading day: trading allowed again.', True)
    if ST['halt']:
        if ST['flat'] and time.time() < ST['flatUntil']:
            for p in list(POS): safe(close_fut, p, 'Daily loss guard')
        return
    if now() - DAYNET['t'] > 120000: return
    tot, sb = DAYNET['v'] + sum(upnl(p) for p in POS), max(1, BAL['wallet'] - DAYNET['v'])
    if ST['rbaseDay'] == dk: tot -= ST['rbase']  # Resume pressed today: the loss limit counts from that moment
    lim = E['dailyLoss']
    if lim > 0 and BAL['wallet'] < 5: lim = max(lim, 15)   # a $5 minimum order risks about 5% of a $1 account on every stop-out
    if lim > 0 and tot <= -lim / 100 * sb:
        ST.update(halt='Daily loss limit of %s%% reached. New trades paused until the next UTC day.' % lim, haltDay=dk, flat=True, flatUntil=time.time() + 60); save()
        log('err', 'CIRCUIT BREAKER: ' + ST['halt'], True)
        for p in list(POS): safe(close_fut, p, 'Daily loss guard')
    elif E['dailyTarget'] > 0 and DAYNET['v'] >= E['dailyTarget'] / 100 * sb:
        ST.update(halt='Daily profit target reached. No new trades today.', haltDay=dk, flat=False); save(); log('win', 'TARGET HIT', True)

AI_SYS = ("You are MEGAN, the AI trader who OWNS and runs this Binance USDT-futures bot. You get one setup the rules engine found and you make the final call. "
          "Think like a careful scalper: take trades that move with trend and momentum, bounce off fresh demand or reject fresh supply, and pass on chasing, thin or choppy moves, "
          "or entries right under supply (long) or right above demand (short). You may only answer with the engine side or HOLD. "
          "Reply with ONLY one JSON object, no markdown: {\"action\":\"LONG\"|\"SHORT\"|\"HOLD\",\"confidence\":0-100,\"reason\":\"one short spoken sentence\",\"stopLoss\":price or 0,\"takeProfit\":price or 0}.")
AIC = {}
def ai_call(url, user, timeout=9):
    u = urllib.parse.urlparse(url); cn = (http.client.HTTPSConnection if u.scheme == 'https' else http.client.HTTPConnection)(u.netloc, timeout=timeout)
    try:
        cn.request('POST', u.path or '/', json.dumps(dict(systemPrompt=AI_SYS, userPrompt=user, history=[])), {'Content-Type': 'application/json'})
        r = cn.getresponse(); raw = r.read().decode('utf-8', 'replace')
    finally: cn.close()
    if r.status != 200: raise Exception('AI relay HTTP %d %s' % (r.status, raw[:90]))
    txt = json.loads(raw)['choices'][0]['message']['content']; m = re.search(r'\{.*\}', txt, re.S)
    if not m: raise Exception('AI answer was not JSON')
    return json.loads(m.group(0))

def ai_check(s, E, sl, tp):
    # Megan's verdict on a setup. None = she could not answer, the engine then trades by its own rules.
    if not E.get('aiOn', True): return None
    key = (s['sym'], s['candleT'])
    if key in AIC: return AIC[key]
    d = 1 if s['side'] == 'LONG' else -1; Z = s.get('zones') or {}; zf = lambda z: [round(z['zl'], 8), round(z['zh'], 8)] if z else None
    pack = dict(symbol=s['sym'], engineSide=s['side'], price=s['px'], strategy=s['strat'], engineConfidence=s['conf'], atrPercent=round(s['atrPct'], 3), trend=round(s['trend'], 2),
                momentum=round(s['mom'], 2), volumeRatio=round(s['vol'], 2), demandZone=zf(Z.get('d')), supplyZone=zf(Z.get('s')), balanceUSDT=round(BAL['wallet'], 2),
                openPositions=[dict(sym=p['sym'], side=p['side']) for p in POS], engineStop=round(s['px'] - d * sl, 8), engineTarget=round(s['px'] + d * tp, 8))
    try:
        v = ai_call(E.get('aiUrl') or DEF['aiUrl'], json.dumps(pack)); act = str(v.get('action', 'HOLD')).upper()
        out = dict(action=act if act in ('LONG', 'SHORT') else 'HOLD', conf=int(float(v.get('confidence') or 0)), reason=str(v.get('reason') or '')[:160], stop=float(v.get('stopLoss') or 0), target=float(v.get('takeProfit') or 0))
    except Exception as e:
        if now() - ST.get('aiWarn', 0) > 90000: ST['aiWarn'] = now(); log('warn', 'Megan AI not reachable (%s). I trade by my own rules until she answers.' % str(e)[:140])
        return None
    AIC[key] = out
    for k in [k for k in AIC if k[1] < now() - 3600000]: AIC.pop(k, None)
    return out

def ai_levels(s, v, sl, tp):
    # use Megan's stop/target only when they are sensible next to the engine's (never tighter than 0.8x or wider than 2.5x)
    d = 1 if s['side'] == 'LONG' else -1
    if v.get('stop') and (s['px'] - v['stop']) * d > 0:
        x = (s['px'] - v['stop']) * d
        if .8 * sl <= x <= 2.5 * sl: sl = x
    if v.get('target') and (v['target'] - s['px']) * d > 0:
        x = (v['target'] - s['px']) * d
        if 1.2 * sl <= x <= 6 * sl: tp = x
    return sl, tp

def decide():
    if not ST['run'] or ST['halt'] or not ST.get('boot') or now() - ST['upd'] > 30000 or now() - DAYNET['t'] > 180000: return
    E, nt = eff(), now() + B.off
    if not E['futures']: return
    if now() < ST.get('streakUntil', 0): return
    rec = [j for j in JOURNAL if 'net' in j][:3]
    if len(rec) == 3 and all(j['net'] < 0 for j in rec) and now() - rec[0]['t'] < 90 * 60000 and ST.get('streakSeen') != rec[0]['t']:
        ST['streakSeen'] = rec[0]['t']; ST['streakUntil'] = now() + 90 * 60000
        log('warn', 'Three losing trades in a row. I pause new trades for 90 minutes so the market can change. Tap Resume to override.', True); return
    cap = min(5, ladder(max(BAL['wallet'], 0), E)['open'])
    if len(POS) + len(OPENING) >= cap or now() < ST.get('lossUntil', 0): return
    # Another position opens only when every open one is already in profit with its stop locked at break-even or better.
    for p in POS:
        m = META.get(mkey(p))
        if m and m.get('auto') and not (m.get('locked') and upnl(p) > 0): return
    held = {p['sym'] for p in POS}; stale = 66000 if E.get('fast', True) else 330000
    for s in sorted(SIG.values(), key=lambda s: -(s['conf'] + min(10, s['atrPct'] * 4) + (3 if s['vol'] > 1.5 else 0))):
        if not s['ok'] or s['conf'] < E['minConf'] or (s['dir'] > 0 and not E['longs']) or (s['dir'] < 0 and not E['shorts']): continue
        if s['sym'] in held or COOL.get(s['sym'], 0) > now() or nt > s['ct'] + stale or LASTE.get(s['sym']) == s['candleT'] or s['sym'] not in FI: continue
        if not affordable(s['sym'], E): continue
        if strat_bad(s['strat']):
            if now() - ST.get('sbLog', 0) > 600000: ST['sbLog'] = now(); n_, w_, net_ = strat_stats(s['strat']); log('info', 'I stopped using "%s": %d trades, %d wins, net %+.2f USDT. It has to earn its place back.' % (s['strat'], n_, w_, net_))
            continue
        if pair_bad(s['sym']): continue
        sl, tp = plan_for(s, E, E['lev'])
        if not size_plan(E, sl / s['px'], FI[s['sym']]['minNot']): continue
        px = px_of(s['sym'])
        if px and (abs(px - s['px']) > .9 * s['atr'] or (px - s['px']) * s['dir'] > .35 * s['atr']): continue
        v = ai_check(s, E, sl, tp)
        if v is not None:
            if v['action'] != s['side'] or v['conf'] < E.get('aiMin', 55):
                LASTE[s['sym']] = s['candleT']; log('ai', 'Megan passed on %s %s: %s' % (s['side'], s['sym'], v['reason'] or 'not convinced')); continue
            sl, tp = ai_levels(s, v, sl, tp); log('ai', 'Megan approves %s %s (%d%%): %s' % (s['side'], s['sym'], v['conf'], v['reason']))
        return open_fut(s, E, sl, tp)

# ---------------------------------------------------------------- HTTP API for the app
def my_ips():
    out = {}
    for k, h in (('v4', 'api.ipify.org'), ('v6', 'api64.ipify.org')):
        try:
            cn = http.client.HTTPSConnection(h, timeout=6); cn.request('GET', '/'); out[k] = cn.getresponse().read().decode().strip(); cn.close()
        except Exception: out[k] = ''
    return out

def bootstrap():
    ST['dual'] = bool(B.call('GET', '/fapi/v1/positionSide/dual', {}, 1)['dualSidePosition']); refresh(); daynet()

def reset_mem():
    for d in (CFG, META, COOL, LASTE, KC, SIG, AIC, CLOSING, EP, NOTE): d.clear()
    JOURNAL.clear(); POS[:] = []; OPENING.clear(); LOGS[:] = []
    BAL.update(wallet=0, avail=0); DAYNET.update(t=0, v=0)
    ST.update(boot=False, run=False, halt='', haltDay='', flat=False, flatUntil=0, dual=False, upd=0, statsAt=0, rbase=0, rbaseDay='')

def do_login(body):
    k, sec, tn = str(body.get('k') or '').strip(), str(body.get('s') or '').strip(), bool(body.get('testnet', True))
    if not k or not sec: return 400, dict(error='Paste both the API key and the secret key.')
    if C.get('k'): return 409, dict(error='Already logged in. Log out first.')
    tb = BN(dict(k=k, s=sec, testnet=tn))
    try: tb.sync(); tb.call('GET', '/fapi/v2/balance', {}, 1)
    except Err as e: return 400, dict(error='Binance refused the key: %s. Check the Futures permission, testnet or live, and the IP restriction.' % e)
    with LOCK:
        C.update(k=k, s=sec, testnet=tn); json.dump(C, open(CF, 'w')); os.chmod(CF, 0o600)
        B.__init__(C); reset_mem()
    B.sync(); load()
    try: bootstrap(); ST['boot'] = True
    except Exception as e: note('login', e)
    log('sys', 'Logged in from the app: %s account %s.' % ('TESTNET' if tn else 'LIVE', mask()), True)
    return 200, dict(ok=True, testnet=tn, acct=mask())

def do_logout(body):
    if not C.get('k'): return 200, dict(ok=True)
    if body.get('closeAll'):
        for p in list(POS): safe(close_fut, p, 'Logout: closing all trades')
        time.sleep(1.5)
    ST['run'] = False; save()
    with LOCK:
        who = mask(); C.update(k='', s=''); json.dump(C, open(CF, 'w')); os.chmod(CF, 0o600)
    time.sleep(1.0)                      # let loops that were mid-call finish; they skip work while logged out
    with LOCK: B.__init__(C); reset_mem()
    log('sys', 'Logged out %s. The relay is idle until you log in again from the app.' % who, True)
    return 200, dict(ok=True)

class H(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(s, *a): pass
    def out(s, code, obj):
        b = json.dumps(obj).encode(); s.send_response(code)
        for k, v in (('Content-Type', 'application/json'), ('Content-Length', str(len(b))), ('Access-Control-Allow-Origin', '*'), ('Access-Control-Allow-Headers', 'Content-Type, X-Relay-Token'),
                     ('Access-Control-Allow-Methods', 'GET, POST, OPTIONS'), ('Access-Control-Allow-Private-Network', 'true')): s.send_header(k, v)
        s.end_headers(); s.wfile.write(b)
    def do_OPTIONS(s): s.out(200, {})
    def auth(s):
        if hmac.compare_digest(s.headers.get('X-Relay-Token', ''), C['token']): return True
        s.out(401, dict(error='Wrong relay token')); return False
    def do_GET(s):
        if not s.auth(): return
        u = urllib.parse.urlparse(s.path)
        if u.path == '/ip': return s.out(200, my_ips())
        if u.path != '/s': return s.out(404, dict(error='not found'))
        since = int((urllib.parse.parse_qs(u.query).get('since') or ['-1'])[0])
        s.out(200, dict(login=bool(C.get('k')), acct=mask(), run=ST['run'], halt=ST['halt'], testnet=C['testnet'], dual=ST['dual'], n=ST['n'], meta=snap(META), journal=snap(JOURNAL[:300]), log=[x for x in LOGS if x['n'] > since][:80] if since >= 0 else [], weight=B.w, upd=ST['upd']))
    def do_POST(s):
        if not s.auth(): return
        try: body = json.loads(s.rfile.read(int(s.headers.get('Content-Length') or 0)) or '{}')
        except Exception: return s.out(400, dict(error='bad json'))
        p = s.path
        if p == '/login': code, o = do_login(body); return s.out(code, o)
        if p == '/logout': code, o = do_logout(body); return s.out(code, o)
        if not C.get('k'): return s.out(409, dict(error='Not logged in. Log in from the app first.'))
        if p == '/cfg': CFG.clear(); CFG.update(body); save(); return s.out(200, {})
        if p == '/meta':
            for k, v in body.items():
                if k in META: v = dict(v, peak=max(v.get('peak', 0), META[k].get('peak', 0)))
                META[k] = v
            save(); return s.out(200, {})
        if p == '/clear': JOURNAL.clear(); save(); return s.out(200, {})
        if p == '/resume':
            ST.update(streakUntil=0, halt='', haltDay='', flat=False, flatUntil=0, rbase=DAYNET['v'] + sum(upnl(q) for q in POS), rbaseDay=time.strftime('%Y-%m-%d', time.gmtime())); save()
            log('info', 'Resumed by you: the daily loss limit now counts from this moment.', True); return s.out(200, {})
        if p == '/run':
            if body.get('on') and not CFG: return s.out(400, dict(error='App has not sent its settings yet'))
            ST['run'] = bool(body.get('on')); save()
            log('sys', 'MEGAN %s on the Termux relay (%s).' % ('STARTED' if ST['run'] else 'STOPPED', 'TESTNET' if C['testnet'] else 'LIVE'), True); return s.out(200, {})
        if p == '/p':
            path = body.get('path', '')
            if 'withdraw' in path.lower(): return s.out(200, dict(ok=False, code=-1, msg='Blocked by relay'))
            try: return s.out(200, dict(ok=True, j=B.call(body.get('method', 'GET'), path, body.get('params') or {}, int(body.get('mode', 1)), body.get('mk', 'f'))))
            except Err as e: return s.out(200, dict(ok=False, code=e.code, msg=str(e)))
        s.out(404, dict(error='not found'))

def snap(o):
    for _ in range(5):
        try: return json.loads(json.dumps(o))
        except RuntimeError: time.sleep(.01)
    return {}

def boot_retry():
    if C.get('k') and not ST.get('boot'): bootstrap(); ST['boot'] = True; log('sys', 'Account setup finished: Binance accepted the key again.')

def loop(fn, every):
    def run():
        while True:
            t = time.time()
            if C.get('k') or fn == B.sync: safe(fn)
            time.sleep(max(.2, every - (time.time() - t)))
    threading.Thread(target=run, daemon=True).start()

def main():
    os.system('termux-wake-lock 2>/dev/null')
    load(); B.sync()
    while not FI:
        try: load_ex(); (load_stats() if C.get('k') else None)
        except Exception as e: print('waiting for Binance:', e); time.sleep(5)
    while C.get('k'):  # keep trying while the phone has no internet; a rejected key logs the relay out instead of stopping it
        try: bootstrap(); ST['boot'] = True; break
        except Err as e:
            if e.code in (0, -1003, -1021, 429, 418) or e.code >= 500: print('waiting for internet/Binance:', e, flush=True); time.sleep(5); B.sync(); continue
            print('Binance refused the saved key (%s). I keep it and retry every 20s (IP restriction or permission changed?). Log out in the app to forget it.' % e, flush=True); break
    for fn, ev in ((ticks, .5), (refresh, 5), (manage, .5), (risk_guard, 1), (scan, 4), (decide, 1.5), (daynet, 30), (boot_retry, 20), (guard_stops, 15), (B.sync, 45), (load_stats, 60), (save, 15)): loop(fn, ev)
    log('sys', 'Relay up on 127.0.0.1:%d | %s | Megan %s' % (PORT, 'TESTNET' if C['testnet'] else 'LIVE', 'RESUMED' if ST['run'] else ('idle (start her from the app)' if C.get('k') else 'waiting for you to log in from the app')))
    print('Relay token for the app:', C['token'], flush=True)
    ThreadingHTTPServer(('127.0.0.1', PORT), H).serve_forever()

if __name__ == '__main__':
    main()
