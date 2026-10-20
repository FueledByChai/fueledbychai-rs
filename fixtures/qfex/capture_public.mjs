import fs from 'node:fs';
import { performance } from 'node:perf_hooks';

const symbol = process.argv[2] ?? 'AAPL-USD';
const durationMs = Number(process.argv[3] ?? 30000);
const out = process.argv[4] ?? '/tmp/qfex-e3jy-live.ndjson';
const rawOut = out.replace(/\.ndjson$/, '.raw.ndjson');
const ws = new WebSocket('wss://mds.qfex.com/');
const file = fs.createWriteStream(out, { flags: 'w' });
const rawFile = fs.createWriteStream(rawOut, { flags: 'w' });
const start = performance.now();
const counts = { bbo: 0, level2: 0, trade: 0, errors: 0, other: 0 };
let finished = false;

const publicFields = new Set([
  'type', 'symbol', 'sequence', 'time', 'bid', 'ask', 'trade_id',
  'price', 'size', 'side', 'execution_type', 'sig_figs', 'contents',
]);

function publicItems(item) {
  if (Array.isArray(item)) return item.flatMap(publicItems);
  if (!item || typeof item !== 'object') return [];
  if (Array.isArray(item.contents))
    return item.contents.flatMap(x => publicItems({ type: item.type, ...x }));
  if (['bbo', 'level2', 'trade'].includes(item.type) && item.symbol === symbol)
    return [item];
  return [];
}

function scrubbedPublicFrame(item) {
  if (Array.isArray(item)) return item.length > 0 && item.every(scrubbedPublicFrame);
  if (!item || typeof item !== 'object') return false;
  if (!Object.keys(item).every(key => publicFields.has(key))) return false;
  if (Array.isArray(item.contents))
    return item.contents.length > 0 && item.contents.every(x =>
      scrubbedPublicFrame({ type: item.type, ...x })
    );
  return ['bbo', 'level2', 'trade'].includes(item.type) && item.symbol === symbol;
}

function record(item, recvMs) {
  if (!item || typeof item !== 'object') return;
  if (Array.isArray(item)) { for (const x of item) record(x, recvMs); return; }
  if (Array.isArray(item.contents)) {
    for (const x of item.contents) record({ type: item.type, ...x }, recvMs);
    return;
  }
  const type = item.type;
  if (item.err) { counts.errors++; return; }
  if (!['bbo', 'level2', 'trade'].includes(type) || item.symbol !== symbol) {
    counts.other++;
    return;
  }
  const keys = type === 'trade'
    ? ['type', 'symbol', 'sequence', 'time', 'trade_id', 'price', 'size', 'side', 'execution_type']
    : ['type', 'symbol', 'sequence', 'time', 'bid', 'ask', 'sig_figs'];
  const clean = { recv_ms: Math.round(recvMs * 1000) / 1000 };
  for (const key of keys) if (Object.hasOwn(item, key)) clean[key] = item[key];
  file.write(JSON.stringify(clean) + '\n');
  counts[type]++;
}

function finish() {
  if (finished) return;
  finished = true;
  ws.close();
  file.end(() => rawFile.end(() => {
    process.stdout.write(JSON.stringify({ symbol, durationMs, counts, out, rawOut }) + '\n');
  }));
}

ws.addEventListener('open', () => {
  for (const channel of ['bbo', 'level2', 'trade'])
    ws.send(JSON.stringify({ type: 'subscribe', channels: [channel], symbols: [symbol] }));
});
ws.addEventListener('message', (event) => {
  try {
    const item = JSON.parse(event.data);
    const recvMs = performance.now() - start;
    if (publicItems(item).length === 0) { counts.other++; return; }
    if (!scrubbedPublicFrame(item)) { counts.errors++; return; }
    rawFile.write(JSON.stringify({ recv_ms: Math.round(recvMs * 1000) / 1000, raw: event.data }) + '\n');
    record(item, recvMs);
  }
  catch { counts.errors++; }
});
ws.addEventListener('error', () => { counts.errors++; });
setTimeout(finish, durationMs);
