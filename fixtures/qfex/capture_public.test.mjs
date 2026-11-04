import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { defaultChannels, outputPaths, projectFrame, startCapture } from './capture_public.mjs';

const bbo = {
  type: 'bbo', symbol: 'NVDA-USD', sequence: 1, time: '2026-10-20T20:54:38Z',
  bid: [['100', '1']], ask: [['101', '2']],
};
const depth = { ...bbo, type: 'level2', sequence: 2, sig_figs: 5 };
const trade = {
  type: 'trade', symbol: 'NVDA-USD', sequence: 3, time: bbo.time,
  trade_id: '12345678-1234-1234-1234-123456789abc', price: '100',
  size: '1', side: 'BUY', execution_type: 'NEW',
};

// Shaped as QFEX's mark_price, underlier, funding_rate and market_stats pages show them, and
// as the 2026-11-04 capture recorded them (the funding frame's annualised_funding_rate is live).
const mark = {
  type: 'mark_price', symbol: 'NVDA-USD', price: '437.45', time: '2025-05-08T20:52:48.000Z',
};
const underlier = {
  type: 'underlier', sequence: 50, time: '2014-11-07T08:19:27.028459Z', symbol: 'NVDA-USD',
  price: '400.23', source: 'external',
};
const funding = {
  type: 'funding', sequence: 50, time: '2014-11-07T08:19:27.028459Z', symbol: 'NVDA-USD',
  time_remaining: 30, funding_rate: '0.021',
};
const stats = {
  type: 'market_stats', sequence: 4, time: '2026-11-04T17:59:25.117061621Z', symbol: 'NVDA-USD',
  bbos: [{ time: bbo.time, symbol: 'NVDA-USD', bid: [['100', '1']], ask: [['101', '2']] }],
  mark_prices: [{ time: bbo.time, symbol: 'NVDA-USD', price: '100.5' }],
  open_interests: [{ time: bbo.time, symbol: 'NVDA-USD', open_interest: '10' }],
  funding_rates: [{
    time: bbo.time, symbol: 'NVDA-USD', funding_rate: '0.00001',
    annualised_funding_rate: '0.0876', time_remaining: 200,
  }],
  daily_volumes: [{ symbol: 'NVDA-USD', volume: '5' }],
  daily_volumes_notional: [{ symbol: 'NVDA-USD', volume: '500' }],
};

class FakeWebSocket extends EventTarget {
  static last;
  constructor(url) {
    super();
    assert.equal(url, 'wss://mds.qfex.com/');
    this.sent = [];
    this.closed = false;
    FakeWebSocket.last = this;
  }
  send(frame) { this.sent.push(frame); }
  close() {
    this.closed = true;
    this.dispatchEvent(new Event('close'));
  }
  receive(value) {
    this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(value) }));
  }
}

test('error envelopes are counted before filtering and never saved', () => {
  const result = projectFrame(JSON.stringify([
    { err: { error_code: 'BadSubscription' } },
    bbo,
  ]), 'NVDA-USD', 1.25);
  assert.equal(result.errors, 1);
  assert.equal(result.other, 0);
  assert.equal(result.records.length, 1);
  assert.deepEqual(JSON.parse(result.records[0].raw.raw), bbo);
  assert.equal(result.records[0].projection.recv_ms, 1.25);
});

test('all saved live public frames remain capturable, including BBO without sig_figs', () => {
  const lines = fs.readFileSync(new URL('./live/nvda-2026-10-20.raw.ndjson', import.meta.url),
    'utf8').trim().split('\n');
  const counts = { bbo: 0, level2: 0, trade: 0 };
  for (const line of lines) {
    const saved = JSON.parse(line);
    const result = projectFrame(saved.raw, 'NVDA-USD', saved.recv_ms);
    assert.equal(result.errors, 0);
    assert.equal(result.records.length, 1);
    const record = result.records[0];
    assert.equal(record.raw.raw, saved.raw);
    counts[record.type]++;
    if (record.type === 'bbo') assert.equal(Object.hasOwn(record.projection, 'sig_figs'), false);
  }
  assert.deepEqual(counts, { bbo: 89, level2: 89, trade: 3 });
});

test('mark_price, underlier, funding and market_stats project exactly as received', () => {
  const live = { ...funding, annualised_funding_rate: '0.0013', sequence: 3 };
  const sequenced = { ...mark, sequence: 1 };
  for (const item of [mark, sequenced, underlier, { ...underlier, source: 'internal' }, funding,
    live, stats]) {
    const result = projectFrame(JSON.stringify(item), 'NVDA-USD', 3);
    assert.equal(result.errors, 0, item.type);
    assert.equal(result.other, 0, item.type);
    assert.equal(result.records.length, 1, item.type);
    const projection = { ...result.records[0].projection };
    delete projection.recv_ms;
    assert.deepEqual(projection, item);
    assert.equal(result.records[0].raw.raw, JSON.stringify(item));
  }
  // Every documented field but the optional ones is required.
  for (const [item, key] of [[underlier, 'source'], [funding, 'time_remaining'],
    [funding, 'funding_rate'], [mark, 'price'], [stats, 'funding_rates']]) {
    const short = { ...item };
    delete short[key];
    const result = projectFrame(JSON.stringify(short), 'NVDA-USD', 3);
    assert.equal(result.errors, 1, key);
    assert.equal(result.records.length, 0, key);
  }
});

test('a market_stats entry off the whitelist or for another symbol is never saved', () => {
  const cases = [
    { ...stats, funding_rates: [{ ...stats.funding_rates[0], account: 'x' }] },
    { ...stats, mark_prices: [{ time: bbo.time, symbol: 'NVDA-USD' }] },
    { ...stats, open_interests: [{ ...stats.open_interests[0], symbol: 'AAPL-USD' }] },
    { ...stats, daily_volumes: {} },
    { ...stats, bbos: [null] },
    { ...stats, extra: [] },
  ];
  for (const item of cases) {
    const result = projectFrame(JSON.stringify(item), 'NVDA-USD', 3);
    assert.equal(result.errors, 1, JSON.stringify(item));
    assert.equal(result.records.length, 0);
  }
  const empty = { ...stats, bbos: [], daily_volumes: [] };
  assert.equal(projectFrame(JSON.stringify(empty), 'NVDA-USD', 3).records.length, 1);
});

test('a public type outside the capture channels is counted as other, not saved', () => {
  const result = projectFrame(JSON.stringify([bbo, mark]), 'NVDA-USD', 3, ['mark_price']);
  assert.equal(result.other, 1);
  assert.deepEqual(result.records.map(row => row.type), ['mark_price']);
});

test('all saved frames of the 2026-11-04 capture remain capturable', () => {
  const channels = ['mark_price', 'underlier', 'funding', 'market_stats'];
  const lines = fs.readFileSync(new URL('./live/suig-2026-11-04.raw.ndjson', import.meta.url),
    'utf8').trim().split('\n');
  const counts = Object.fromEntries(channels.map(channel => [channel, 0]));
  for (const line of lines) {
    const saved = JSON.parse(line);
    const result = projectFrame(saved.raw, 'SUIG-USD', saved.recv_ms, channels);
    assert.equal(result.errors, 0);
    assert.equal(result.records.length, 1);
    assert.equal(result.records[0].raw.raw, saved.raw);
    counts[result.records[0].type]++;
  }
  assert.ok(channels.every(channel => counts[channel] > 0), JSON.stringify(counts));
});

test('array and contents batches make one paired raw/projection row per public item', () => {
  const frames = [
    JSON.stringify([bbo, depth]),
    JSON.stringify({ type: 'trade', contents: [{ ...trade, type: undefined }] }),
  ];
  const rows = frames.flatMap(frame => projectFrame(frame, 'NVDA-USD', 2).records);
  assert.equal(rows.length, 3);
  assert.deepEqual(rows.map(row => row.type), ['bbo', 'level2', 'trade']);
  for (const row of rows) {
    const raw = JSON.parse(row.raw.raw);
    const projection = { ...row.projection };
    delete projection.recv_ms;
    assert.deepEqual(raw, projection);
    assert.equal(row.raw.recv_ms, row.projection.recv_ms);
  }
});

test('a capture subscribes exactly its channels, one frame each, and refuses an unknown one', async () => {
  assert.deepEqual(defaultChannels, ['bbo', 'level2', 'trade']);
  assert.throws(() => startCapture({
    out: '/tmp/capture.ndjson', channels: ['funding_rate'], WebSocketImpl: FakeWebSocket,
  }), /unknown channel funding_rate/);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'qfex-capture-channels-test-'));
  try {
    let complete;
    const summary = new Promise(resolve => { complete = resolve; });
    const channels = ['mark_price', 'underlier', 'funding', 'market_stats'];
    const capture = startCapture({
      symbol: 'NVDA-USD', durationMs: 60000, out: path.join(dir, 'capture.ndjson'), channels,
      WebSocketImpl: FakeWebSocket, onSummary: complete,
    });
    const ws = FakeWebSocket.last;
    ws.dispatchEvent(new Event('open'));
    assert.deepEqual(ws.sent.map(JSON.parse), channels.map(channel => (
      { type: 'subscribe', channels: [channel], symbols: ['NVDA-USD'] })));
    ws.receive([mark, underlier, funding, stats, bbo]);
    capture.finish();
    const result = await summary;
    assert.deepEqual(result.channels, channels);
    assert.ok(!Number.isNaN(Date.parse(result.startedAt)));
    assert.deepEqual(result.counts, {
      mark_price: 1, underlier: 1, funding: 1, market_stats: 1, errors: 0, other: 1,
    });
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('output paths must have distinct ndjson names', () => {
  assert.deepEqual(outputPaths('/tmp/capture.ndjson'), {
    out: '/tmp/capture.ndjson', rawOut: '/tmp/capture.raw.ndjson',
  });
  assert.throws(() => outputPaths('/tmp/capture'), /must end in \.ndjson/);
  assert.throws(() => startCapture({ out: '/tmp/capture', WebSocketImpl: FakeWebSocket }),
    /must end in \.ndjson/);
});

test('finish detaches message handling before both streams close', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'qfex-capture-test-'));
  try {
    const out = path.join(dir, 'capture.ndjson');
    let complete;
    const summary = new Promise(resolve => { complete = resolve; });
    const capture = startCapture({
      symbol: 'NVDA-USD', durationMs: 60000, out,
      WebSocketImpl: FakeWebSocket, onSummary: complete,
    });
    const ws = FakeWebSocket.last;
    ws.dispatchEvent(new Event('open'));
    assert.equal(ws.sent.length, 3);
    ws.receive(bbo);
    ws.receive([depth, trade]);
    ws.receive({ err: { error_code: 'Rejected' } });
    capture.finish();
    ws.receive(bbo); // A frame queued during the close handshake must be ignored.
    const result = await summary;
    ws.receive(bbo); // Nor may a late frame write after the streams have ended.
    assert.equal(ws.closed, true);
    assert.deepEqual(result.counts, { bbo: 1, level2: 1, trade: 1, errors: 1, other: 0 });
    const projected = fs.readFileSync(out, 'utf8').trim().split('\n').map(JSON.parse);
    const raw = fs.readFileSync(result.rawOut, 'utf8').trim().split('\n').map(JSON.parse);
    assert.equal(projected.length, 3);
    assert.equal(raw.length, projected.length);
    for (let i = 0; i < raw.length; i++) {
      const expected = { ...projected[i] };
      delete expected.recv_ms;
      assert.deepEqual(JSON.parse(raw[i].raw), expected);
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('unexpected early WebSocket close finishes with an error', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'qfex-capture-close-test-'));
  try {
    let complete;
    const summary = new Promise(resolve => { complete = resolve; });
    startCapture({
      symbol: 'NVDA-USD', durationMs: 60000, out: path.join(dir, 'capture.ndjson'),
      WebSocketImpl: FakeWebSocket, onSummary: complete,
    });
    const ws = FakeWebSocket.last;
    ws.receive(bbo);
    ws.dispatchEvent(new Event('close'));
    const result = await summary;
    assert.equal(result.counts.bbo, 1);
    assert.equal(result.counts.errors, 1);
    assert.equal(ws.closed, true);
    assert.equal(fs.readFileSync(result.out, 'utf8').trim().split('\n').length, 1);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
