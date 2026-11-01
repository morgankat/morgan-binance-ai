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
import os, sys, re, json, time, hmac, math, hashlib, getpass, secrets, threading, http.client, urllib.parse
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
        if not sys.stdin.isatty():
            sys.exit('No key saved yet. Run once:  python binance_relay.py --setup')
        print('Binance key needs Reading + Futures. NEVER enable Withdrawals.')
        c['testnet'] = input('Use TESTNET (no real money)? [Y/n]: ').strip().lower() != 'n'
        if not c['testnet'] and input('Type LIVE to trade real money: ').strip() != 'LIVE':
            sys.exit('Cancelled.')
        c['k'] = getpass.getpass('API key: ').strip(); c['s'] = getpass.getpass('Secret key: ').strip()
        c['token'] = secrets.token_hex(8)
        json.dump(c, open(CF, 'w')); os.chmod(CF, 0o600)
        print('Saved to', CF)
    return c

# ---------------------------------------------------------------- Binance REST
class Err(Exception):
    def __init__(s, c, m): super().__init__(m); s.code = c

CP, CPL = {}, threading.Lock()
def take_conn(host):
    with CPL:
        l = CP.setdefault(host, [])
        if l: return l.pop(), False
    return http.client.HTTPSConnection(host, timeout=15), True
def give_conn(host, cn):
    with CPL:
        l = CP.setdefault(host, [])
        if len(l) < 8: l.append(cn); return
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
            cn, fresh = take_conn(host)
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
DEF = dict(riskPct=.8, lev=10, margin='ISOLATED', sizePct=12, maxOpen=4, minConf=62, slAtr=1.3, rr=2, lockR=.8, trailGive=.35,
           runner=True, dailyLoss=8, dailyTarget=0, cooldownMin=10, minVol=30, universe=40, pairsMode='auto',
           whitelist=[], blacklist=[], longs=True, shorts=True, staleCandles=24, maxSpread=.08, minAtr=.12,
           futures=True, mode='custom', profile='aggressive')
PRE = {'safe': dict(riskPct=0.4, lev=3, sizePct=5, maxOpen=2, minConf=72, slAtr=1.6, rr=1.6, lockR=.7, trailGive=.3),
       'balanced': dict(riskPct=0.6, lev=5, sizePct=8, maxOpen=3, minConf=67, slAtr=1.5, rr=1.8, lockR=.8, trailGive=.33),
       'aggressive': dict(riskPct=0.8, lev=10, sizePct=12, maxOpen=4, minConf=62, slAtr=1.3, rr=2, lockR=.8, trailGive=.35),
       'turbo': dict(riskPct=1.2, lev=20, sizePct=15, maxOpen=5, minConf=58, slAtr=1.15, rr=2.4, lockR=.9, trailGive=.4)}
CFG, META, JOURNAL, COOL, LASTE = {}, {}, [], {}, {}
FI, T, KC, SIG, POS, BAL, LOGS = {}, {}, {}, {}, [], dict(wallet=0, avail=0), []
ST = dict(run=False, halt='', haltDay='', flat=False, flatUntil=0, dual=False, upd=0, statsAt=0, n=0)
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

def save():
    with LOCK:
        try:
            json.dump(dict(cfg=CFG, meta=META, journal=JOURNAL[:600], cool=COOL, laste=LASTE, run=ST['run'], halt=ST['halt'], haltDay=ST['haltDay']), open(SF + '.tmp', 'w'))
            os.replace(SF + '.tmp', SF)
        except Exception: pass

def load():
    try: d = json.load(open(SF))
    except Exception: return
    CFG.update(d.get('cfg', {})); META.update(d.get('meta', {})); JOURNAL[:] = d.get('journal', []); COOL.update(d.get('cool', {})); LASTE.update(d.get('laste', {}))
    ST.update(run=bool(d.get('run')) and bool(CFG), halt=d.get('halt', ''), haltDay=d.get('haltDay', ''))

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

def analyze(sym, B5, B15, E):
    n = len(B5) - 1
    if n < 60: return None
    c = [b['c'] for b in B5]; e9, e21, e50 = ema(c, 9), ema(c, 21), ema(c, 50); at = atr(B5); px = c[n]
    if not (at > 0 and px > 0): return None
    ap = at / px * 100; last = B5[n]; body = abs(last['c'] - last['o'])
    trend = cl((e9[n] - e21[n]) / at * .8, -1, 1) * .6 + cl((e21[n] - e50[n]) / at * .5, -1, 1) * .4
    slope = cl((e21[n] - e21[n - 5]) / at, -1, 1); mom = cl((c[n] - c[n - 3]) / at / 2, -1, 1); r = rsi(c)
    va = sum(b['v'] for b in B5[n - 20:n]) / 20; vol = last['v'] / va if va > 0 else 1; st = struct(B5, at)
    t15 = 0
    if B15 and len(B15) >= 55:
        c2 = [b['c'] for b in B15]; a2 = atr(B15); f, s_ = ema(c2, 9), ema(c2, 21); m = len(c2) - 1
        if a2 > 0: t15 = cl((f[m] - s_[m]) / a2 * .8, -1, 1)
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
    ok, block = True, ''
    w8 = B5[n - 7:n + 1]; run = (px - min(b['l'] for b in w8)) / at if d > 0 else (max(b['h'] for b in w8) - px) / at; stretch = (px - e21[n]) / at * d
    if st['dir'] == -d and not (brk == d or st['shift'] == d): ok, block = False, 'confirmed structure is against it'
    elif sign(trend) != d and not pat and brk != d and abs(trend) > .12: ok, block = False, 'trend does not agree'
    elif t15 * d < -.4 and brk != d: ok, block = False, '15m trend is against it'
    elif ap < E['minAtr']: ok, block = False, 'too quiet'
    elif ap > 6: ok, block = False, 'too wild'
    elif abs(sc) < .12: ok, block = False, 'no clear edge'
    elif run > (4.5 if brk == d else 3.2) or stretch > 1.7: ok, block = False, 'already moved far, entering now would be chasing'
    elif (r > 74 if d > 0 else r < 26): ok, block = False, 'overextended (RSI %d)' % r
    strat = 'Candle Pattern #%d' % pat[0] if pat else 'Volume Breakout' if brk == d else 'Pullback Resume' if pb == d else 'Trend Follow'
    return dict(sym=sym, dir=d, side='LONG' if d > 0 else 'SHORT', conf=conf, strat=strat, ok=ok, block=block, atr=at, atrPct=ap, px=px, trend=trend, mom=mom, vol=vol, st=st, candleT=last['t'], ct=last['ct'], ts=now())

flip_against = lambda sg, d: bool(sg) and sg['st']['dir'] == -d and sg['st']['two'] == -d and sg['trend'] * d < -.05 and sg['mom'] * d < -.02 and sg['conf'] >= 58
def plan_for(sg, E, lev=None):
    px = sg['px']; mn = px * .0035; sl = max(E['slAtr'] * sg['atr'], mn); sl = min(sl, px * (.6 / max(1, lev or E['lev'])))
    sl = max(sl, mn * .8) if sl < mn * .8 else sl
    return sl, sl * E['rr']
def ladder(eq, E):
    # bigger balance, more positions and more margin per trade; smaller balance, a single small trade
    if eq < 20: return dict(lev=min(E['lev'], 10), cap=50, open=1)
    if eq < 100: return dict(lev=min(E['lev'], 8), cap=30, open=2)
    if eq < 500: return dict(lev=min(E['lev'], 6), cap=min(E['sizePct'], 15), open=min(E['maxOpen'], 3))
    if eq < 2000: return dict(lev=min(E['lev'], 5), cap=min(E['sizePct'], 8), open=E['maxOpen'])
    return dict(lev=min(E['lev'], 4), cap=min(E['sizePct'], 5), open=E['maxOpen'])

def size_plan(E, slp, min_not):
    eq, avail = max(BAL['wallet'], 0), BAL['avail']; L = ladder(eq, E)
    capm = min(avail * L['cap'] / 100, avail * .9); maxlev = max(1, min(L['lev'], safe_lev(slp, E['lev'])))
    want = eq * E['riskPct'] / 100 / max(slp, 1e-6) if E.get('riskPct', 0) > 0 else 1e18
    notional = max(min(want, capm * maxlev), min_not * 1.06)
    if capm <= 0: return None
    lev = max(1, math.ceil(notional / capm - 1e-9))
    if lev > maxlev or notional * slp > eq * max(E.get('riskPct', 1) * 2.5, 5) / 100: return None  # does not fit, or the minimum order is too risky for this balance
    return dict(lev=lev, margin=notional / lev, notional=notional)
safe_lev = lambda slp, want: max(1, min(int(want), int(.6 / max(slp, .0005))))
STABLE = re.compile(r'^(USDC|FDUSD|TUSD|BUSD|USDP|DAI|EUR|AEUR|USD1|XUSD)')

def candidates(E):
    arr = [x for x in T.values() if x['s'] in FI and x['s'] not in E['blacklist'] and not STABLE.match(x['s'])]
    if E['pairsMode'] == 'manual': w = set(E['whitelist']); return [x['s'] for x in arr if x['s'] in w]
    byv = sorted([x for x in arr if x['q'] >= E['minVol'] * 1e6], key=lambda x: -x['q'])[:int(E['universe'])]
    mov = sorted([x for x in arr if x['q'] >= E['minVol'] * .3e6], key=lambda x: -abs(x['P']))[:8]
    return list(dict.fromkeys(x['s'] for x in byv + mov))

def scan():
    E = eff()
    if now() - ST['statsAt'] > 55000: load_stats()
    cands = candidates(E); nt = now() + B.off
    need = [s for s in cands if s not in KC or nt > KC[s]['next'] or now() - KC[s]['ts'] > 120000][:16]
    def one(s):
        b5, b15 = closed(klines(s, '5m', 99)), closed(klines(s, '15m', 70))
        KC[s] = dict(ts=now(), next=(b5[-1]['ct'] if b5 else nt) + 301500)
        sg = analyze(s, b5, b15, E)
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
    j = B.call('GET', '/fapi/v1/income', dict(startTime=t0, limit=1000), 1)
    DAYNET.update(t=now(), v=sum(float(r['income']) for r in j if r['incomeType'] in ('REALIZED_PNL', 'COMMISSION', 'FUNDING_FEE')))

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
        JOURNAL.insert(0, dict(t=now(), venue='f', sym=p['sym'], side=p['side'], reason=reason, upnl=pn, auto=True, risk=bool(re.search('stop|lock|guard|flipped|follow', reason, re.I))))
        COOL[p['sym']] = now() + E['cooldownMin'] * 60000 * (1 if pn > 0 else 2)
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
                 runner=bool(E['runner']), qty=float(r.get('executedQty') or 0), r=sl, sl=entry - d * sl, tp=entry + d * tp)
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
def eval_exit(m, d, price, E):
    R = m.get('r')
    if not R or not m.get('sl'): return None, False
    prof = (price - m['entry']) * d; m['peak'] = max(m.get('peak', 0), prof)
    if (price - m['sl']) * d <= 0: return ('Profit lock' if m.get('locked') and prof > 0 else 'Break-even stop' if m.get('locked') else 'Stop loss'), False
    if not m.get('runner') and m.get('tp') and (price - m['tp']) * d >= 0: return 'Take profit', False
    ch = False; f = m['entry'] * .0018; pk = m['peak']  # f = round-trip fees + slippage in price terms
    if pk >= 1.6 * f:
        tier = 5 if pk >= 12 * f else 4 if pk >= 7 * f else 3 if pk >= 4 * f else 2 if pk >= 2.5 * f else 1
        trl = m['entry'] + d * (f if tier == 1 else max(f, pk * (0, 0, .6, .7, .78, .85)[tier])); m['locked'] = True
        if (trl - m['sl']) * d > 0:
            m['sl'] = trl; ch = True
            if tier != m.get('tier'):
                m['tier'] = tier; log('info', '%s: stop raised, now locks about %+.2f USDT (level %d of 5)' % (m.get('sym', ''), (trl - m['entry']) * d * m.get('qty', 0), tier))
    if now() - m['openedAt'] > E['staleCandles'] * 300000 and prof < .3 * R: return 'No follow-through', False
    return None, ch

def sync_stop(p, m, fi):
    if time.time() - m.get('lastAmend', 0) < 4: return
    was = m.get('slSent', m['entry'] - (1 if p['side'] == 'LONG' else -1) * m['r'])
    if abs(m['sl'] - was) < min(.2 * m['r'], m['entry'] * .0027): return
    m['lastAmend'] = time.time()
    try:
        old = m.get('slId'); m['slId'] = place_algo(p['sym'], 'SELL' if p['side'] == 'LONG' else 'BUY', 'STOP_MARKET', round_tick(m['sl'], fi['tick']), p['side']); m['slSent'] = m['sl']; cancel_prot(p['sym'], [old])
    except Exception: pass

def manage():
    if not ST['run'] or now() - ST['upd'] > 30000: return
    E = eff()
    for p in list(POS):
        m = META.get(mkey(p)); bk = T.get(p['sym']) or {}; price = (bk.get('bid') if p['side'] == 'LONG' else bk.get('ask')) or px_of(p['sym'], p['mark'])
        if not m or not m.get('auto') or not m.get('sl') or not price: continue
        why, ch = eval_exit(m, 1 if p['side'] == 'LONG' else -1, price, E)
        if why: safe(close_fut, p, why)
        elif ch: sync_stop(p, m, FI[p['sym']])
    for k in [k for k, m in META.items() if k[:2] == 'f:' and now() - m.get('openedAt', 0) > 20000 and not any(mkey(p) == k for p in POS) and ST['upd'] > now() - 15000]:
        m = META.pop(k); COOL[m['sym']] = now() + eff()['cooldownMin'] * 120000
        JOURNAL.insert(0, dict(t=now(), venue='f', sym=m['sym'], side=m['side'], reason='Closed on exchange (stop/target)', upnl=0, auto=bool(m.get('auto')), risk=True)); save()

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
    if E['dailyLoss'] > 0 and tot <= -E['dailyLoss'] / 100 * sb:
        ST.update(halt='Daily loss limit of %s%% reached. New trades paused until the next UTC day.' % E['dailyLoss'], haltDay=dk, flat=True, flatUntil=time.time() + 60); save()
        log('err', 'CIRCUIT BREAKER: ' + ST['halt'], True)
        for p in list(POS): safe(close_fut, p, 'Daily loss guard')
    elif E['dailyTarget'] > 0 and DAYNET['v'] >= E['dailyTarget'] / 100 * sb:
        ST.update(halt='Daily profit target reached. No new trades today.', haltDay=dk, flat=False); save(); log('win', 'TARGET HIT', True)

def decide():
    if not ST['run'] or ST['halt'] or now() - ST['upd'] > 30000: return
    E, nt = eff(), now() + B.off
    for p in list(POS):
        m, sg, d = META.get(mkey(p)), SIG.get(p['sym']), (1 if p['side'] == 'LONG' else -1)
        if m and m.get('auto') and sg and m.get('flipSeen') != sg['candleT'] and flip_against(sg, d): m['flipSeen'] = sg['candleT']; safe(close_fut, p, 'Structure flipped against the trade')
    if len(POS) + len(OPENING) >= min(E['maxOpen'], ladder(max(BAL['wallet'], 0), E)['open']) or not E['futures']: return
    held = {p['sym'] for p in POS}
    for s in sorted(SIG.values(), key=lambda s: -(s['conf'] + min(10, s['atrPct'] * 4) + (3 if s['vol'] > 1.5 else 0))):
        if not s['ok'] or s['conf'] < E['minConf'] or (s['dir'] > 0 and not E['longs']) or (s['dir'] < 0 and not E['shorts']): continue
        if s['sym'] in held or COOL.get(s['sym'], 0) > now() or nt > s['ct'] + 330000 or LASTE.get(s['sym']) == s['candleT'] or s['sym'] not in FI: continue
        sl, tp = plan_for(s, E, E['lev'])
        if not size_plan(E, sl / s['px'], FI[s['sym']]['minNot']): continue
        px = px_of(s['sym'])
        if px and (abs(px - s['px']) > .9 * s['atr'] or (px - s['px']) * s['dir'] > .35 * s['atr']): continue
        return open_fut(s, E, sl, tp)

# ---------------------------------------------------------------- HTTP API for the app
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
        if u.path != '/s': return s.out(404, dict(error='not found'))
        since = int((urllib.parse.parse_qs(u.query).get('since') or ['-1'])[0])
        s.out(200, dict(run=ST['run'], halt=ST['halt'], testnet=C['testnet'], dual=ST['dual'], n=ST['n'], meta=snap(META), journal=snap(JOURNAL[:300]), log=[x for x in LOGS if x['n'] > since][:80] if since >= 0 else [], weight=B.w, upd=ST['upd']))
    def do_POST(s):
        if not s.auth(): return
        try: body = json.loads(s.rfile.read(int(s.headers.get('Content-Length') or 0)) or '{}')
        except Exception: return s.out(400, dict(error='bad json'))
        p = s.path
        if p == '/cfg': CFG.clear(); CFG.update(body); save(); return s.out(200, {})
        if p == '/meta':
            for k, v in body.items():
                if k in META: v = dict(v, peak=max(v.get('peak', 0), META[k].get('peak', 0)))
                META[k] = v
            save(); return s.out(200, {})
        if p == '/resume': ST.update(halt='', haltDay=''); save(); return s.out(200, {})
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

def loop(fn, every):
    def run():
        while True:
            t = time.time(); safe(fn); time.sleep(max(.2, every - (time.time() - t)))
    threading.Thread(target=run, daemon=True).start()

def main():
    os.system('termux-wake-lock 2>/dev/null')
    load(); B.sync()
    while not FI:
        try: load_ex(); load_stats()
        except Exception as e: print('waiting for Binance:', e); time.sleep(5)
    while True:  # keep trying while the phone has no internet; only a rejected key stops the relay
        try: ST['dual'] = bool(B.call('GET', '/fapi/v1/positionSide/dual', {}, 1)['dualSidePosition']); refresh(); daynet(); break
        except Err as e:
            if e.code in (0, -1003, -1021, 429, 418) or e.code >= 500: print('waiting for internet/Binance:', e, flush=True); time.sleep(5); B.sync(); continue
            sys.exit('Binance refused the key: %s\nCheck Futures permission, testnet/live choice and IP whitelist (run --setup --reset).' % e)
    for fn, ev in ((ticks, .5), (refresh, 5), (manage, .5), (risk_guard, 1), (scan, 12), (decide, 3), (daynet, 30), (load_stats, 60), (save, 15)): loop(fn, ev)
    log('sys', 'Relay up on 127.0.0.1:%d | %s | Megan %s' % (PORT, 'TESTNET' if C['testnet'] else 'LIVE', 'RESUMED' if ST['run'] else 'idle (start her from the app)'))
    print('Relay token for the app:', C['token'], flush=True)
    ThreadingHTTPServer(('127.0.0.1', PORT), H).serve_forever()

if __name__ == '__main__':
    main()
