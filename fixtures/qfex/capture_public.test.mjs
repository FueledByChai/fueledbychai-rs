import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { outputPaths, projectFrame, startCapture } from './capture_public.mjs';

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
