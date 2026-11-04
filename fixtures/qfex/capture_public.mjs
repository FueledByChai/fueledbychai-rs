import fs from 'node:fs';
import { resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';

// Each public channel's whitelisted fields, and those among them a frame may omit.
const fields = {
  // QFEX BBO omits sig_figs in both documented and recorded live frames.
  bbo: { keys: ['type', 'symbol', 'sequence', 'time', 'bid', 'ask', 'sig_figs'], optional: ['sig_figs'] },
  level2: { keys: ['type', 'symbol', 'sequence', 'time', 'bid', 'ask', 'sig_figs'], optional: [] },
  trade: {
    keys: ['type', 'symbol', 'sequence', 'time', 'trade_id', 'price', 'size', 'side', 'execution_type'],
    optional: [],
  },
  // The mark_price page's example has no sequence; live frames carry one.
  mark_price: { keys: ['type', 'symbol', 'sequence', 'time', 'price'], optional: ['sequence'] },
  underlier: { keys: ['type', 'symbol', 'sequence', 'time', 'price', 'source'], optional: [] },
  // The funding_rate page's example has no annualised_funding_rate; live frames carry one.
  funding: {
    keys: ['type', 'symbol', 'sequence', 'time', 'time_remaining', 'funding_rate', 'annualised_funding_rate'],
    optional: ['annualised_funding_rate'],
  },
  market_stats: {
    keys: ['type', 'symbol', 'sequence', 'time', 'bbos', 'mark_prices', 'open_interests',
      'funding_rates', 'daily_volumes', 'daily_volumes_notional'],
    optional: [],
  },
};

// Each market_stats component array's entry fields, all required, as the market_stats page lists
// them. Every entry must name the captured symbol.
const statsEntries = {
  bbos: ['time', 'symbol', 'bid', 'ask'],
  mark_prices: ['time', 'symbol', 'price'],
  open_interests: ['time', 'symbol', 'open_interest'],
  funding_rates: ['time', 'symbol', 'funding_rate', 'annualised_funding_rate', 'time_remaining'],
  daily_volumes: ['symbol', 'volume'],
  daily_volumes_notional: ['symbol', 'volume'],
};

export const defaultChannels = ['bbo', 'level2', 'trade'];

export function outputPaths(out) {
  if (!out.endsWith('.ndjson')) throw new Error('capture output must end in .ndjson');
  return { out, rawOut: out.replace(/\.ndjson$/, '.raw.ndjson') };
}

function exactKeys(item, keys, optional) {
  return keys.every(key => optional.includes(key) || Object.hasOwn(item, key)) &&
    Object.keys(item).every(key => keys.includes(key));
}

function statsWhitelisted(item, symbol) {
  return Object.entries(statsEntries).every(([component, keys]) =>
    Array.isArray(item[component]) && item[component].every(entry =>
      entry && typeof entry === 'object' && !Array.isArray(entry) &&
      exactKeys(entry, keys, []) && entry.symbol === symbol));
}

// Only these whitelisted public fields may reach either file. Each member of a
// QFEX array or contents envelope gets its own raw/projection pair. A public type
// outside `channels` is counted as other and not saved.
export function projectFrame(raw, symbol, recvMs, channels = Object.keys(fields)) {
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
    const spec = channels.includes(item.type) ? fields[item.type] : undefined;
    if (!spec || item.symbol !== symbol) { other++; return; }
    if (!exactKeys(item, spec.keys, spec.optional) ||
        (item.type === 'market_stats' && !statsWhitelisted(item, symbol))) {
      errors++;
      return;
    }
    const projection = { recv_ms: received };
    for (const key of spec.keys) if (Object.hasOwn(item, key)) projection[key] = item[key];
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
  channels = defaultChannels,
  WebSocketImpl = WebSocket,
  onSummary = summary => process.stdout.write(JSON.stringify(summary) + '\n'),
} = {}) {
  const { rawOut } = outputPaths(out);
  for (const channel of channels)
    if (!Object.hasOwn(fields, channel)) throw new Error(`unknown channel ${channel}`);
  const ws = new WebSocketImpl('wss://mds.qfex.com/');
  const file = fs.createWriteStream(out, { flags: 'w' });
  const rawFile = fs.createWriteStream(rawOut, { flags: 'w' });
  // The wall-clock instant recv_ms counts from, for the README; neither file carries it.
  const startedAt = new Date().toISOString();
  const start = performance.now();
  const counts = { ...Object.fromEntries(channels.map(channel => [channel, 0])), errors: 0, other: 0 };
  let finished = false;

  function onMessage(event) {
    if (finished) return;
    try {
      const { records, errors, other } =
        projectFrame(event.data, symbol, performance.now() - start, channels);
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
    for (const channel of channels)
      ws.send(JSON.stringify({ type: 'subscribe', channels: [channel], symbols: [symbol] }));
  });
  ws.addEventListener('message', onMessage);
  ws.addEventListener('error', () => { if (!finished) counts.errors++; });
  ws.addEventListener('close', () => {
    if (finished) return;
    counts.errors++;
    finish();
  });
  const timeout = setTimeout(finish, durationMs);

  function finish() {
    if (finished) return;
    finished = true;
    clearTimeout(timeout);
    ws.removeEventListener('message', onMessage);
    ws.close();
    file.end(() => rawFile.end(() =>
      onSummary({ symbol, durationMs, channels, startedAt, counts, out, rawOut })));
  }

  return { finish };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  startCapture({
    symbol: process.argv[2] ?? 'AAPL-USD',
    durationMs: Number(process.argv[3] ?? 30000),
    out: process.argv[4] ?? '/tmp/qfex-e3jy-live.ndjson',
    channels: process.argv[5]?.split(',') ?? defaultChannels,
  });
}
