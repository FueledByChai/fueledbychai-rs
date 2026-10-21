import fs from 'node:fs';
import { resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';

const fields = {
  bbo: ['type', 'symbol', 'sequence', 'time', 'bid', 'ask', 'sig_figs'],
  level2: ['type', 'symbol', 'sequence', 'time', 'bid', 'ask', 'sig_figs'],
  trade: ['type', 'symbol', 'sequence', 'time', 'trade_id', 'price', 'size', 'side', 'execution_type'],
};

export function outputPaths(out) {
  if (!out.endsWith('.ndjson')) throw new Error('capture output must end in .ndjson');
  return { out, rawOut: out.replace(/\.ndjson$/, '.raw.ndjson') };
}

// Only these whitelisted public fields may reach either file. Each member of a
// QFEX array or contents envelope gets its own raw/projection pair.
export function projectFrame(raw, symbol, recvMs) {
  const records = [];
  let errors = 0;
  let other = 0;
  const received = Math.round(recvMs * 1000) / 1000;

  function visit(item, rawItem) {
    if (Array.isArray(item)) {
      for (const member of item) visit(member, JSON.stringify(member));
      return;
    }
    if (!item || typeof item !== 'object') { other++; return; }
    if (Object.hasOwn(item, 'err')) { errors++; return; }
    if (Array.isArray(item.contents)) {
      if (Object.keys(item).some(key => !['type', 'contents'].includes(key))) {
        errors++;
        return;
      }
      for (const member of item.contents) {
        const flattened = { type: item.type, ...member };
        visit(flattened, JSON.stringify(flattened));
      }
      return;
    }
    const keys = fields[item.type];
    if (!keys || item.symbol !== symbol) { other++; return; }
    if (!keys.every(key => Object.hasOwn(item, key)) ||
        Object.keys(item).some(key => !keys.includes(key))) {
      errors++;
      return;
    }
    const projection = { recv_ms: received };
    for (const key of keys) projection[key] = item[key];
    records.push({
      raw: { recv_ms: received, raw: rawItem },
      projection,
      type: item.type,
    });
  }

  visit(JSON.parse(raw), raw);
  return { records, errors, other };
}

export function startCapture({
  symbol = 'AAPL-USD',
  durationMs = 30000,
  out = '/tmp/qfex-e3jy-live.ndjson',
  WebSocketImpl = WebSocket,
  onSummary = summary => process.stdout.write(JSON.stringify(summary) + '\n'),
} = {}) {
  const { rawOut } = outputPaths(out);
  const ws = new WebSocketImpl('wss://mds.qfex.com/');
  const file = fs.createWriteStream(out, { flags: 'w' });
  const rawFile = fs.createWriteStream(rawOut, { flags: 'w' });
  const start = performance.now();
  const counts = { bbo: 0, level2: 0, trade: 0, errors: 0, other: 0 };
  let finished = false;

  function onMessage(event) {
    if (finished) return;
    try {
      const { records, errors, other } = projectFrame(event.data, symbol, performance.now() - start);
      counts.errors += errors;
      counts.other += other;
      for (const record of records) {
        rawFile.write(JSON.stringify(record.raw) + '\n');
        file.write(JSON.stringify(record.projection) + '\n');
        counts[record.type]++;
      }
    } catch {
      counts.errors++;
    }
  }

  ws.addEventListener('open', () => {
    if (finished) return;
    for (const channel of ['bbo', 'level2', 'trade'])
      ws.send(JSON.stringify({ type: 'subscribe', channels: [channel], symbols: [symbol] }));
  });
  ws.addEventListener('message', onMessage);
  ws.addEventListener('error', () => { if (!finished) counts.errors++; });
  const timeout = setTimeout(finish, durationMs);

  function finish() {
    if (finished) return;
    finished = true;
    clearTimeout(timeout);
    ws.removeEventListener('message', onMessage);
    ws.close();
    file.end(() => rawFile.end(() => onSummary({ symbol, durationMs, counts, out, rawOut })));
  }

  return { finish };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  startCapture({
    symbol: process.argv[2] ?? 'AAPL-USD',
    durationMs: Number(process.argv[3] ?? 30000),
    out: process.argv[4] ?? '/tmp/qfex-e3jy-live.ndjson',
  });
}
