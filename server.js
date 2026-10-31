import express from 'express';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import dotenv from 'dotenv';
dotenv.config();

const app=express();
app.use(express.json({limit:'50kb'}));
const PORT=Number(process.env.PORT||8080);
const ROOT=process.cwd();
const stateFile=process.env.STATE_FILE||path.join(ROOT,'bot-state.json');
const INTERVALS=['1m','3m','5m','15m','30m','1h','4h','1d'];
const defaults={mode:process.env.BINANCE_MODE==='testnet'?'testnet':'live',market:'spot',symbols:['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT'],interval:'1m',scanSeconds:20,auto:false,scans:[],lastScan:null,lastError:null};
let state={...defaults};
try{const old=JSON.parse(fs.readFileSync(stateFile,'utf8'));state={...state,...old,auto:false};}catch{}
state.market=['spot','futures'].includes(state.market)?state.market:'spot';
state.symbols=Array.isArray(state.symbols)?state.symbols:defaults.symbols;
state.interval=INTERVALS.includes(state.interval)?state.interval:'1m';
const creds=()=>({key:process.env.BINANCE_API_KEY||'',secret:process.env.BINANCE_API_SECRET||''});
const dashboardKey=()=>process.env.DASHBOARD_API_KEY||'';
function save(){try{fs.writeFileSync(stateFile,JSON.stringify(state,null,2),{mode:0o600});}catch(e){console.error('State save failed:',e.message);}}
function equalSafe(a,b){const x=Buffer.from(String(a)),y=Buffer.from(String(b));return x.length===y.length&&crypto.timingSafeEqual(x,y);}
app.use('/api',(req,res,next)=>{if(req.path==='/health')return next();const key=dashboardKey();if(key.length<11)return res.status(503).json({error:'Set DASHBOARD_API_KEY to your existing password (minimum 11 characters) in Railway Variables.'});if(!equalSafe(req.get('X-Dashboard-Key')||'',key))return res.status(401).json({error:'Dashboard password is missing or incorrect.'});next();});
function base(kind,signed=false){if(kind==='futures')return state.mode==='testnet'?'https://demo-fapi.binance.com':'https://fapi.binance.com';if(state.mode==='testnet')return 'https://testnet.binance.vision';return signed?'https://api.binance.com':'https://data-api.binance.vision';}
async function binance(kind,endpoint,params={},signed=false){const p=new URLSearchParams();for(const [k,v] of Object.entries(params))if(v!==undefined&&v!==null)p.set(k,String(v));const headers={};if(signed){const {key,secret}=creds();if(!key||!secret)throw Error('Binance API key or secret is missing from Railway Variables.');p.set('timestamp',Date.now());p.set('recvWindow','10000');p.set('signature',crypto.createHmac('sha256',secret).update(p.toString()).digest('hex'));headers['X-MBX-APIKEY']=key;}const response=await fetch(base(kind,signed)+endpoint+(p.size?'?'+p.toString():''),{headers,signal:AbortSignal.timeout(15000)});let data;try{data=await response.json();}catch{data={msg:'Binance returned an unreadable response.'};}if(!response.ok){const err=new Error(data.msg||`Binance HTTP ${response.status}`);err.status=response.status;err.binanceCode=data.code;throw err;}return data;}
function ema(values,n){const k=2/(n+1);let v=values[0]||0;for(let i=1;i<values.length;i++)v=values[i]*k+v*(1-k);return v;}
function rsi(values,n=14){if(values.length<n+1)return 50;let gains=0,losses=0;for(let i=values.length-n;i<values.length;i++){const d=values[i]-values[i-1];if(d>0)gains+=d;else losses-=d;}if(losses===0)return 100;return 100-100/(1+gains/losses);}
function analyze(klines){const c=klines.slice(0,-1).map(k=>({open:+k[1],high:+k[2],low:+k[3],close:+k[4]}));if(c.length<60)return {side:'WAIT',strategy:'WARMUP',reason:'Waiting for 60 completed candles'};const closes=c.map(x=>x.close),last=c.at(-1),prev=c.at(-2),r=rsi(closes),e9=ema(closes.slice(-60),9),e21=ema(closes.slice(-60),21),e50=ema(closes.slice(-60),50);
 // Basic swing-origin zones: recent local pivot candles followed by displacement.
 const look=c.slice(-42,-2);let demand=null,supply=null;
 for(let i=look.length-3;i>=2;i--){const x=look[i],before=look[i-1],after=look[i+1],next=look[i+2];if(!demand&&x.low<=before.low&&x.low<=after.low&&next.close>x.high&&next.close>after.close)demand={low:x.low,high:Math.max(x.open,x.close)};if(!supply&&x.high>=before.high&&x.high>=after.high&&next.close<x.low&&next.close<after.close)supply={low:Math.min(x.open,x.close),high:x.high};}
 const bullishCandle=last.close>last.open,bearishCandle=last.close<last.open;
 const demandTouch=!!demand&&last.low<=demand.high*1.002&&last.close>=demand.low;
 const supplyTouch=!!supply&&last.high>=supply.low*.998&&last.close<=supply.high;
 const bullishTrend=e9>e21,bearishTrend=e9<e21;
 const buyPullback=bullishTrend&&last.close>e9&&last.close>prev.close&&bullishCandle;
 const sellPullback=bearishTrend&&last.close<e9&&last.close<prev.close&&bearishCandle;
 // RSI is a soft momentum context, not an extreme overbought/oversold veto.
 if(demandTouch&&bullishCandle&&r>35&&r<92)return {side:'BUY',strategy:'SUPPLY_DEMAND',reason:'Bullish rejection from a recent demand zone',zone:{type:'DEMAND',low:demand.low,high:demand.high},rsi:+r.toFixed(1),ema9:e9,ema21:e21};
 if(supplyTouch&&bearishCandle&&r>8&&r<65)return {side:'SELL',strategy:'SUPPLY_DEMAND',reason:'Bearish rejection from a recent supply zone',zone:{type:'SUPPLY',low:supply.low,high:supply.high},rsi:+r.toFixed(1),ema9:e9,ema21:e21};
 if(buyPullback&&r>38&&r<93)return {side:'BUY',strategy:'EMA_PULLBACK',reason:'Bullish EMA 9/21 trend with confirmed pullback continuation',rsi:+r.toFixed(1),ema9:e9,ema21:e21};
 if(sellPullback&&r>7&&r<62)return {side:'SELL',strategy:'EMA_PULLBACK',reason:'Bearish EMA 9/21 trend with confirmed pullback continuation',rsi:+r.toFixed(1),ema9:e9,ema21:e21};
 return {side:'WAIT',strategy:'MONITOR',reason:'Waiting for a confirmed zone rejection or trend pullback',rsi:+r.toFixed(1),zone:demand?{type:'DEMAND',...demand}:supply?{type:'SUPPLY',...supply}:null,ema9:e9,ema21:e21};}
let busy=false;
async function scanOnce(){if(busy)return;busy=true;try{const results=[];for(const symbol of state.symbols.slice(0,20)){try{const endpoint=state.market==='futures'?'/fapi/v1/klines':'/api/v3/klines';const [ticker,klines]=await Promise.all([binance(state.market,state.market==='futures'?'/fapi/v1/ticker/price':'/api/v3/ticker/price',{symbol}),binance(state.market,endpoint,{symbol,interval:state.interval,limit:120})]);const a=analyze(klines);results.push({symbol,market:state.market,price:+ticker.price,...a,at:new Date().toISOString()});}catch(e){results.push({symbol,side:'ERROR',strategy:'DATA_ERROR',reason:e.message,at:new Date().toISOString()});}}state.scans=[...results,...state.scans].slice(0,150);state.lastScan=new Date().toISOString();state.lastError=results.find(x=>x.side==='ERROR')?.reason||null;save();}catch(e){state.lastError=e.message;save();}finally{busy=false;}}
setTimeout(scanOnce,1200);setInterval(scanOnce,Math.max(10000,Math.min(120000,Number(state.scanSeconds)||20)*1000));
app.get('/api/health',(req,res)=>res.json({ok:true,service:'MORGAN AI'}));
app.get('/api/status',(req,res)=>res.json({ok:true,configured:!!(creds().key&&creds().secret),dashboardProtected:dashboardKey().length>=11,mode:state.mode,market:state.market,monitoring:true,auto:false,tradingEnabled:false,settings:state,lastScan:state.lastScan,lastError:state.lastError}));
app.get('/api/klines',async(req,res)=>{try{const symbol=String(req.query.symbol||'BTCUSDT').toUpperCase(),kind=req.query.market==='futures'?'futures':'spot',interval=String(req.query.interval||state.interval);if(!/^[A-Z0-9]{5,20}$/.test(symbol)||!INTERVALS.includes(interval))return res.status(400).json({error:'Invalid symbol or timeframe'});const candles=await binance(kind,kind==='futures'?'/fapi/v1/klines':'/api/v3/klines',{symbol,interval,limit:500});res.json({symbol,market:kind,interval,candles});}catch(e){res.status(502).json({error:e.message,code:e.binanceCode||null});}});
app.get('/api/market',async(req,res)=>{try{const kind=req.query.market==='futures'?'futures':'spot',symbols=String(req.query.symbols||state.symbols.join(',')).split(',').slice(0,30);const data=await Promise.all(symbols.map(async symbol=>{const d=await binance(kind,kind==='futures'?'/fapi/v1/ticker/24hr':'/api/v3/ticker/24hr',{symbol});return {symbol:d.symbol,price:+d.lastPrice,change:+d.priceChangePercent,volume:+d.quoteVolume};}));res.json({data});}catch(e){res.status(502).json({error:e.message,code:e.binanceCode||null});}});
app.get('/api/scan',(req,res)=>res.json({monitoring:true,lastScan:state.lastScan,scans:state.scans.slice(0,60),lastError:state.lastError}));
app.get('/api/account',async(req,res)=>{try{const kind=req.query.market==='futures'?'futures':'spot';const d=await binance(kind,kind==='futures'?'/fapi/v2/account':'/api/v3/account',{},true);if(kind==='futures')return res.json({market:kind,totalWalletBalance:+d.totalWalletBalance,totalUnrealizedProfit:+d.totalUnrealizedProfit,availableBalance:+d.availableBalance,canTrade:!!d.canTrade});res.json({market:kind,canTrade:!!d.canTrade,canWithdraw:!!d.canWithdraw,balances:(d.balances||[]).filter(x=>+x.free>0||+x.locked>0).map(x=>({asset:x.asset,free:+x.free,locked:+x.locked}))});}catch(e){res.status(502).json({error:e.message,code:e.binanceCode||null});}});
app.post('/api/settings',(req,res)=>{const b=req.body||{};if(b.market&&['spot','futures'].includes(b.market))state.market=b.market;if(b.interval){if(!INTERVALS.includes(b.interval))return res.status(400).json({error:'Invalid timeframe'});state.interval=b.interval;}if(b.symbols){const s=[...new Set(b.symbols.map(x=>String(x).trim().toUpperCase()).filter(x=>/^[A-Z0-9]{5,20}$/.test(x)))].slice(0,20);if(!s.length)return res.status(400).json({error:'No valid symbols provided'});state.symbols=s;}save();scanOnce();res.json({ok:true,settings:state});});
app.post('/api/auto',(req,res)=>res.status(403).json({error:'Live order execution is disabled in this release. Monitoring and strategy analysis are active; no orders will be sent.'}));
app.post('/api/stop',(req,res)=>res.json({ok:true,auto:false}));
app.get('/',(req,res)=>res.sendFile(path.join(ROOT,'index.html')));
app.use(express.static(ROOT));
app.listen(PORT,()=>console.log(`MORGAN AI listening on ${PORT}`));
