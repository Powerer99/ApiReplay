// Self-contained real-binary acceptance proof. All credentials are synthetic.
const assert = require('node:assert/strict');
const http = require('node:http');
const net = require('node:net');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');

const root = path.resolve(__dirname, '..');
process.chdir(root);
const artifacts = path.join(root, 'target', 'acceptance', new Date().toISOString().replace(/[:.]/g, '-'));
fs.mkdirSync(artifacts, { recursive: true });
const binary = path.join(root, 'target', 'debug', process.platform === 'win32' ? 'apireplay.exe' : 'apireplay');
const results = [], commands = [], children = [], servers = [], handles = [];
const limit = 1024 * 1024;
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const artifact = name => path.join(artifacts, name);
let releaseStream;

async function until(fn, description, timeout = 10000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    try { const result = await fn(); if (result) return result; } catch (error) { last = error.message; }
    await delay(30);
  }
  throw new Error(`${description} timed out${last ? ': ' + last : ''}`);
}

async function check(name, fn) {
  try { await fn(); results.push({ name, pass: true }); console.log(`PASS ${name}`); }
  catch (error) { results.push({ name, pass: false, error: error.message }); throw error; }
}

function request(port, method, url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path: url, headers }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.setTimeout(10000, () => req.destroy(new Error('HTTP request timeout')));
    req.on('error', reject); req.end(body);
  });
}

async function listen(server) {
  servers.push(server); server.listen(0, '127.0.0.1');
  await once(server, 'listening'); return server.address().port;
}

async function command(args, expected) {
  const child = spawn(binary, args, { windowsHide: true }); children.push(child);
  let stdout = '', stderr = '', timedOut = false;
  child.stdout.on('data', chunk => stdout += chunk); child.stderr.on('data', chunk => stderr += chunk);
  const timer = setTimeout(() => { timedOut = true; child.kill(); }, 20000);
  const [code] = await once(child, 'exit'); clearTimeout(timer);
  commands.push({ args, code, stdout, stderr, timedOut });
  assert.equal(timedOut, false, 'CLI exceeded 20 seconds');
  assert.equal(code, expected, `CLI exit code: ${args[0]}\n${stdout}\n${stderr}`);
  return { code, stdout, stderr };
}

async function proxy(target, filename) {
  const output = artifact(filename);
  const log = fs.openSync(artifact(filename + '.log'), 'w'); handles.push(log);
  const child = spawn(binary, ['record', '--listen', '127.0.0.1:0', '--target', target, '--output', output], { windowsHide: true, env: { ...process.env, NO_COLOR: '1', RUST_LOG: 'info' }, stdio: ['ignore', log, log] });
  children.push(child);
  const port = await until(() => {
    if (child.exitCode !== null) throw new Error('proxy exited during startup');
    const match = fs.readFileSync(artifact(filename + '.log'), 'utf8').match(/listen=127\.0\.0\.1:(\d+)/);
    return match && Number(match[1]);
  }, 'proxy startup');
  return { port, output, child };
}

function records(file) {
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}
async function recorded(file, count) {
  return until(() => { const rows = records(file); return rows.length >= count && rows; }, 'completed JSONL records');
}

function api(seen, version) {
  return http.createServer(async (req, res) => {
    try {
      const chunks = []; for await (const chunk of req) chunks.push(chunk);
      const body = Buffer.concat(chunks).toString();
      seen.push({ method: req.method, path: req.url, body, authorization: req.headers.authorization });
      const route = req.url.split('?')[0];
      if (route === '/binary') { res.writeHead(200, { 'content-type': 'application/octet-stream' }); res.end(Buffer.from([0, 255, 128])); return; }
      if (route === '/large' || route === '/exact') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('x'.repeat(limit + (route === '/large' ? 1 : 0))); return; }
      if (route === '/empty' || route === '/204' || route === '/304') { res.writeHead(route === '/empty' ? 200 : Number(route.slice(1))); res.end(); return; }
      if (route === '/stream') {
        res.writeHead(200, { 'content-type': 'text/plain' }); res.write('first');
        await new Promise(resolve => { releaseStream = resolve; }); res.end('last'); return;
      }
      if (route === '/security') {
        res.writeHead(200, { 'content-type': 'application/json', 'set-cookie': 'session=synthetic-response-secret' });
        res.end(JSON.stringify({ echo: body, token: 'synthetic-response-secret' })); return;
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ method: req.method, path: req.url, body, version: version() }));
    } catch { res.destroy(); }
  });
}

async function main() {
  execFileSync('cargo', ['build', '--locked'], { stdio: 'inherit', windowsHide: true });
  const seenA = [], seenB = []; let changed = false;
  const a = await listen(api(seenA, () => 1)), b = await listen(api(seenB, () => changed ? 2 : 1));
  const targetA = `http://127.0.0.1:${a}`, targetB = `http://127.0.0.1:${b}`;
  const baseline = await proxy(targetA, 'baseline.jsonl');
  await check('record preserves original methods, queries and bodies', async () => {
    for (const method of ['GET', 'POST', 'PUT', 'HEAD']) {
      const response = await request(baseline.port, method, '/items?active=true', method === 'GET' || method === 'HEAD' ? undefined : 'hello', { 'content-type': 'text/plain' });
      assert.equal(response.status, 200);
      if (method !== 'HEAD') assert.equal(JSON.parse(response.body).method, method);
      else assert.equal(response.body.length, 0);
    }
    const rows = await recorded(baseline.output, 4);
    assert.equal(rows.length, 4); assert(rows.every(row => row.schema_version === 1 && row.recorded_at_unix_ms > 0 && typeof row.duration_ms === 'number'));
  });
  const actual = artifact('actual.jsonl');
  await check('replay against a second service preserves ordered requests', async () => {
    await command(['replay', '--input', baseline.output, '--target', targetB, '--output', actual], 0);
    assert.deepEqual(seenB, seenA);
    assert(records(actual).every((row, index) => row.source_index === index + 1 && row.outcome === 'completed'));
  });
  await check('unchanged responses compare as four matches', async () => {
    await command(['compare', '--baseline', baseline.output, '--actual', actual, '--output', artifact('match.json')], 0);
    assert.equal(JSON.parse(fs.readFileSync(artifact('match.json'))).matched, 4);
  });
  await check('intentional response change produces nonzero mismatch and field locations', async () => {
    changed = true;
    await command(['replay', '--input', baseline.output, '--target', targetB, '--output', artifact('changed.jsonl')], 0);
    await command(['compare', '--baseline', baseline.output, '--actual', artifact('changed.jsonl'), '--output', artifact('mismatch.json')], 1);
    const report = JSON.parse(fs.readFileSync(artifact('mismatch.json')));
    assert.equal(report.mismatched, 3); // HEAD has no body to compare.
    assert(report.exchanges.some(row => row.differences.some(diff => diff.field === 'body/version')));
  });
  const probes = await proxy(targetA, 'probes.jsonl'); let count = 0;
  await check('synthetic key, numeric, nested and header credentials never reach recording', async () => {
    const body = JSON.stringify({ password: 'synthetic-key-secret', 'synthetic-key-secret': 'visible', token: 917364285, echo: 917364285, secret: { nested: ['synthetic-nested-secret'] } });
    const response = await request(probes.port, 'POST', '/security', body, { 'content-type': 'application/json', authorization: 'Bearer synthetic-auth-secret' });
    assert.equal(JSON.parse(response.body).echo, body);
    assert.equal(seenA.at(-1).body, body); assert.equal(seenA.at(-1).authorization, 'Bearer synthetic-auth-secret');
    await recorded(probes.output, ++count);
    const text = fs.readFileSync(probes.output, 'utf8');
    for (const secret of ['synthetic-key-secret', '917364285', 'synthetic-nested-secret', 'synthetic-auth-secret', 'synthetic-response-secret']) assert(!text.includes(secret), `leaked ${secret}`);
  });
  await check('concurrent HEAD and bodyless exchanges are each recorded once', async () => {
    await Promise.all(Array.from({ length: 12 }, (_, i) => request(probes.port, 'HEAD', `/head/${i}`)));
    for (const route of ['/empty', '/204', '/304']) assert.equal((await request(probes.port, 'GET', route)).body.length, 0);
    count += 15; const rows = await recorded(probes.output, count); assert.equal(rows.length, count);
    for (let i = 0; i < 12; i++) assert.equal(rows.filter(row => row.request.path_and_query === `/head/${i}` && row.response.body.kind === 'empty').length, 1);
  });
  await check('capture limits preserve traffic and omit whole binary/oversized bodies', async () => {
    assert.deepEqual((await request(probes.port, 'GET', '/binary')).body, Buffer.from([0, 255, 128]));
    assert.equal((await request(probes.port, 'GET', '/large')).body.length, limit + 1);
    assert.equal((await request(probes.port, 'GET', '/exact')).body.length, limit);
    assert.equal((await request(probes.port, 'POST', '/request-too-large', 'x'.repeat(10 * limit + 1))).status, 413);
    count += 3; const rows = await recorded(probes.output, count);
    const binary = rows.find(row => row.request.path_and_query === '/binary').response.body;
    const large = rows.find(row => row.request.path_and_query === '/large').response.body;
    assert.equal(binary.reason, 'binary'); assert.equal(large.reason, 'too_large');
    assert(!('content' in binary)); assert(!('content' in large));
    assert.equal(rows.find(row => row.request.path_and_query === '/exact').response.body.content.length, limit);
  });
  await check('first streamed chunk arrives before upstream completion', async () => {
    const response = new Promise((resolve, reject) => {
      const req = http.get({ host: '127.0.0.1', port: probes.port, path: '/stream' }, res => {
        let text = ''; res.on('data', chunk => {
          text += chunk;
          if (text === 'first') {
            try { assert.equal(records(probes.output).length, count); releaseStream(); } catch (error) { reject(error); }
          }
        });
        res.on('end', () => resolve(text)); res.on('error', reject);
      });
      req.setTimeout(10000, () => req.destroy(new Error('stream timeout'))); req.on('error', reject);
    });
    assert.equal(await response, 'firstlast'); await recorded(probes.output, ++count);
  });
  await check('upstream connection failure produces a recorded 502', async () => {
    const unavailable = await listen(net.createServer(socket => socket.destroy()));
    const failed = await proxy(`http://127.0.0.1:${unavailable}`, '502.jsonl');
    assert.equal((await request(failed.port, 'GET', '/unavailable')).status, 502);
    assert.equal((await recorded(failed.output, 1))[0].response.status, 502);
  });
  await check('invalid output, malformed input and unknown versions fail explicitly', async () => {
    await command(['record', '--target', targetA, '--output', artifact('missing/recording.jsonl')], 2);
    fs.writeFileSync(artifact('malformed.jsonl'), '{invalid}\n');
    await command(['replay', '--input', artifact('malformed.jsonl'), '--target', targetB, '--output', artifact('should-not-exist.jsonl')], 2);
    assert(!fs.existsSync(artifact('should-not-exist.jsonl')));
    const unknown = records(baseline.output)[0]; unknown.schema_version = 999;
    fs.writeFileSync(artifact('unknown.jsonl'), JSON.stringify(unknown) + '\n');
    await command(['replay', '--input', artifact('unknown.jsonl'), '--target', targetB, '--output', artifact('should-not-exist.jsonl')], 2);
    await command(['compare', '--baseline', baseline.output, '--actual', artifact('malformed.jsonl')], 2);
  });
  await check('redacted requests are skipped and incomplete comparisons never pass', async () => {
    await command(['replay', '--input', probes.output, '--target', targetB, '--output', artifact('incomplete.jsonl'), '--timeout-ms', '1000'], 2);
    assert.equal(records(artifact('incomplete.jsonl'))[0].outcome, 'skipped');
    await command(['compare', '--baseline', probes.output, '--actual', artifact('incomplete.jsonl'), '--output', artifact('incomplete-report.json')], 2);
    assert(JSON.parse(fs.readFileSync(artifact('incomplete-report.json'))).incomplete > 0);
  });
  fs.writeFileSync(artifact('upstream-observed.json'), JSON.stringify({ seenA, seenB }, null, 2));
}

(async () => {
  try { await main(); }
  catch (error) { console.error(error); results.push({ name: 'acceptance run', pass: false, error: error.message }); process.exitCode = 1; }
  finally {
    if (releaseStream) releaseStream();
    for (const child of children) {
      if (child.exitCode === null && child.signalCode === null) { const exited = once(child, 'exit'); child.kill(); await Promise.race([exited, delay(5000)]); }
    }
    for (const handle of handles) fs.closeSync(handle);
    for (const server of servers) { server.closeAllConnections?.(); await new Promise(resolve => server.close(resolve)); }
    const report = {
      commit: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8', windowsHide: true }).trim(),
      workingTree: execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8', windowsHide: true }),
      binarySha256: fs.existsSync(binary) ? crypto.createHash('sha256').update(fs.readFileSync(binary)).digest('hex') : null,
      syntheticCredentialsOnly: true, results, commands, artifacts,
    };
    fs.writeFileSync(artifact('results.json'), JSON.stringify(report, null, 2));
    console.log(`Evidence: ${artifact('results.json')}`);
  }
})();
