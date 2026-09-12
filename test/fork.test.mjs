// fork 专属行为的回归护栏。这些改动不在上游，若被上游同步覆盖会静默丢失。
// 每条都对应一个明确的行为契约，不是实现细节。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import net from 'node:net';
import { setup, startProxy, startMockUpstream, allocPort } from './helpers.mjs';

const AUTH = { Authorization: 'Bearer user_test' };
const UUID = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const CHAT = { model: 'm', stream: true, messages: [{ role: 'user', content: 'hi' }] };

// ── 逆向对齐：上游 User-Agent 与 threadId ──────────────────
// CLI 侧 vy = "cli"（源码常量），threadId 与 x-session-id 同值
// （toWireThreadId 对合法 UUID 直接透传）。
test('fork: 上游请求 User-Agent 为 cli（对齐官方 CLI 常量）', async () => {
  const s = await setup();
  try {
    const r = await s.proxy.post('/v1/chat/completions', CHAT, { ...AUTH, 'x-session-id': UUID });
    await r.text();
    const g = s.mock.lastGenerate();
    assert.equal(g.headers['user-agent'], 'cli',
      '官方 CLI 发送 User-Agent: cli；发送 Node 默认 UA 是明显破绽');
  } finally { await s.close(); }
});

test('fork: 预请求（fingerprint/lifecycle）同样带 User-Agent: cli', async () => {
  const s = await setup();
  try {
    const r = await s.proxy.post('/v1/chat/completions', CHAT, { ...AUTH, 'x-session-id': UUID });
    await r.text();
    for (const path of ['/alpha/fingerprint/record', '/alpha/lifecycle-events']) {
      const req = s.mock.seen.find(x => x.url === path);
      assert.ok(req, path + ' 应被发出');
      assert.equal(req.headers['user-agent'], 'cli', path + ' 的 UA 也必须是 cli');
    }
  } finally { await s.close(); }
});

test('fork: threadId 与 x-session-id 同值', async () => {
  const s = await setup();
  try {
    const r = await s.proxy.post('/v1/chat/completions', CHAT, { ...AUTH, 'x-session-id': UUID });
    await r.text();
    const g = s.mock.lastGenerate();
    assert.equal(g.headers['x-session-id'], UUID, 'session 头应透传');
    assert.equal(g.body.threadId, UUID,
      'threadId 必须与 x-session-id 同值（CLI 的 toWireThreadId 行为）');
  } finally { await s.close(); }
});

test('fork: sessionId 非 UUID 时省略 threadId（而非填非法值）', async () => {
  const s = await setup();
  try {
    // CLI 的 toWireThreadId 对非 UUID 返回 undefined —— 代理必须同样省略该字段
    const r = await s.proxy.post('/v1/chat/completions', CHAT,
      { ...AUTH, 'x-session-id': 'not-a-uuid' });
    await r.text();
    const g = s.mock.lastGenerate();
    // 非 UUID 的 session 头会被忽略，回落到 per-key 生成的合法 UUID
    const tid = g.body.threadId;
    assert.ok(tid === undefined || /^[0-9a-f-]{36}$/.test(tid),
      'threadId 要么省略，要么是合法 UUID，不能是任意字符串。实际: ' + JSON.stringify(tid));
    assert.equal(tid, g.headers['x-session-id'], '若存在则必须与 x-session-id 同值');
  } finally { await s.close(); }
});

// ── issue #18：上游 HTTP(S) 代理（零依赖 CONNECT 隧道）──
/** 录制型 CONNECT 代理：记录每次 CONNECT 的 target，并做裸字节转发。 */
async function startRecordingProxy() {
  const port = await allocPort();
  const connects = [];
  const server = http.createServer((req, res) => { res.writeHead(405); res.end(); });
  server.on('connect', (req, clientSocket, head) => {
    connects.push(req.url);
    const [h, p] = req.url.split(':');
    const up = net.connect(Number(p || 443), h, () => {
      clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
      if (head?.length) up.write(head);
      up.pipe(clientSocket); clientSocket.pipe(up);
    });
    up.on('error', () => clientSocket.destroy());
    clientSocket.on('error', () => up.destroy());
  });
  await new Promise(r => server.listen(port, '127.0.0.1', r));
  return { port, connects, close: () => new Promise(r => server.close(r)) };
}

test('#18: 配置 CC_UPSTREAM_PROXY 后上游请求经 CONNECT 隧道', async () => {
  const rec = await startRecordingProxy();
  const mock = startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port,
    env: { CC_UPSTREAM_PROXY: 'http://127.0.0.1:' + rec.port } });
  try {
    const r = await proxy.post('/v1/chat/completions', CHAT, AUTH);
    await r.text();
    assert.ok(rec.connects.length >= 1, '应建立 CONNECT 隧道，实际 ' + JSON.stringify(rec.connects));
    // generate + fingerprint + lifecycle 三条都要走代理
    assert.equal(mock.generateCount(), 1, 'generate 应经隧道到达上游');
    assert.equal(mock.lastGenerate().headers['user-agent'], 'cli', '经代理时 UA 仍为 cli');
  } finally {
    await proxy.kill(); await mock.close(); await rec.close();
  }
});

test('#18: 预请求也走代理（避免同一账号从两个 IP 注册）', async () => {
  const rec = await startRecordingProxy();
  const mock = startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port,
    env: { CC_UPSTREAM_PROXY: 'http://127.0.0.1:' + rec.port } });
  try {
    const r = await proxy.post('/v1/chat/completions', CHAT, AUTH);
    await r.text();
    // 三条预请求 + generate 共 3 次 CONNECT（generate/fingerprint/lifecycle）
    assert.ok(rec.connects.length >= 3,
      'fingerprint 与 lifecycle 也必须走代理，否则账号会从两个 IP 注册。实际 CONNECT 数: ' + rec.connects.length);
  } finally {
    await proxy.kill(); await mock.close(); await rec.close();
  }
});

test('#18: 未配置代理时不建立任何 CONNECT', async () => {
  const rec = await startRecordingProxy();
  const mock = startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port });   // 不设 CC_UPSTREAM_PROXY
  try {
    const r = await proxy.post('/v1/chat/completions', CHAT, AUTH);
    await r.text();
    assert.equal(rec.connects.length, 0, '默认应直连，不经任何代理');
  } finally {
    await proxy.kill(); await mock.close(); await rec.close();
  }
});

test('#18: 探活端点不经过上游代理', async () => {
  const rec = await startRecordingProxy();
  const mock = startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port,
    env: { CC_UPSTREAM_PROXY: 'http://127.0.0.1:' + rec.port } });
  try {
    const before = rec.connects.length;
    const r = await proxy.get('/health');
    assert.equal(r.status, 200);
    assert.equal(rec.connects.length, before, '/health 是本地端点，不应触发上游 CONNECT');
  } finally {
    await proxy.kill(); await mock.close(); await rec.close();
  }
});

// ── 设备指纹派生 ─────────────────────────────────────────
test('fork: 同一 API key 在同一盐下得到稳定 thumbmark（重启后不变）', async () => {
  const mock = startMockUpstream();
  const env = { CC_FP_SALT: 'ci-salt', CC_FP_MODE: 'derived' };
  const p1 = await startProxy({ upstreamPort: mock.port, env });
  let first;
  try {
    const r = await p1.post('/v1/chat/completions', CHAT, AUTH);
    await r.text();
    first = JSON.parse(mock.seen.find(x => x.url === '/alpha/fingerprint/record').raw);
  } finally { await p1.kill(); }

  const p2 = await startProxy({ upstreamPort: mock.port, env });
  try {
    const r = await p2.post('/v1/chat/completions', CHAT, AUTH);
    await r.text();
    const fps = mock.seen.filter(x => x.url === '/alpha/fingerprint/record');
    const second = JSON.parse(fps[fps.length - 1].raw);
    assert.equal(second.thumbmark, first.thumbmark,
      'derived 模式下同一 key 重启后必须得到同一设备（否则每次重启都像换了台机器）');
  } finally { await p2.kill(); await mock.close(); }
});

test('fork: 不同 API key 得到不同 thumbmark', async () => {
  const mock = startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port, env: { CC_FP_SALT: 'ci-salt' } });
  try {
    for (const k of ['user_a', 'user_b', 'user_c']) {
      const r = await proxy.post('/v1/chat/completions', CHAT, { Authorization: 'Bearer ' + k });
      await r.text();
    }
    const tms = mock.seen.filter(x => x.url === '/alpha/fingerprint/record')
      .map(x => JSON.parse(x.raw).thumbmark);
    assert.equal(tms.length, 3, '三个 key 应各上报一次');
    assert.equal(new Set(tms).size, 3, '不同 key 必须映射到不同设备指纹（不能退化成哈希桶）');
  } finally { await proxy.kill(); await mock.close(); }
});

test('fork: 不同盐得到不同部署指纹', async () => {
  const mock = startMockUpstream();
  const grab = async (salt) => {
    const p = await startProxy({ upstreamPort: mock.port, env: { CC_FP_SALT: salt } });
    try {
      const r = await p.post('/v1/chat/completions', CHAT, AUTH);
      await r.text();
      const fps = mock.seen.filter(x => x.url === '/alpha/fingerprint/record');
      return JSON.parse(fps[fps.length - 1].raw).thumbmark;
    } finally { await p.kill(); }
  };
  try {
    const a = await grab('salt-one');
    const b = await grab('salt-two');
    assert.notEqual(a, b, '不同部署（不同盐）不应共享设备指纹');
  } finally { await mock.close(); }
});

test('fork: CC_FP_MODE=random 回退到原行为（重启换设备）', async () => {
  const mock = startMockUpstream();
  const env = { CC_FP_MODE: 'random' };
  const grab = async () => {
    const p = await startProxy({ upstreamPort: mock.port, env });
    try {
      const r = await p.post('/v1/chat/completions', CHAT, AUTH);
      await r.text();
      const fps = mock.seen.filter(x => x.url === '/alpha/fingerprint/record');
      return JSON.parse(fps[fps.length - 1].raw).thumbmark;
    } finally { await p.kill(); }
  };
  try {
    const a = await grab();
    const b = await grab();
    assert.notEqual(a, b, 'random 模式应保留「每进程随机」的原始行为');
  } finally { await mock.close(); }
});
