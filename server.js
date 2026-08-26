// Local LLM Gateway — merges multiple OpenAI-compatible upstreams into one
// endpoint with a single key. "auto" model = try every model until one answers.
// Zero dependencies. Run: node server.js
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 4567;
const HOST = process.env.HOST || '0.0.0.0';
const CONFIG_FILE = path.join(__dirname, 'providers.json');
const CONNECT_TIMEOUT_MS = +(process.env.CONNECT_TIMEOUT_MS || 60000); // per attempt, until headers
const IDLE_TIMEOUT_MS = +(process.env.IDLE_TIMEOUT_MS || 120000);      // max silence mid-stream

// ---------- config ----------
let config = { gatewayKey: 'sk-gw-' + crypto.randomBytes(16).toString('hex'), providers: [] };
try {
  if (fs.existsSync(CONFIG_FILE)) {
    const loaded = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
    config = { ...config, ...loaded };
    if (!config.gatewayKey) config.gatewayKey = 'sk-gw-' + crypto.randomBytes(16).toString('hex');
  }
} catch (e) { console.error('[config] load failed:', e.message); }
function save() { fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2)); }

function normalizeBase(url) {
  return String(url || '').trim().replace(/\/+$/, '').replace(/\/v1$/i, '');
}

// ---------- helpers ----------
function json(res, code, obj) {
  const body = JSON.stringify(sanitizeIds(obj));
  res.writeHead(code, { 'Content-Type': 'application/json', ...cors() });
  res.end(body);
}
function cors() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': '*',
    'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
  };
}

// Deep-walk a value and coerce every `id` property to a string.
// Strict clients (Zod-based SDKs etc.) reject numeric ids with
// "Expected 'id' to be a string" — this makes that error class impossible
// regardless of what an upstream returns. null/undefined/'' ids are
// replaced with generated ones (null is NOT a string either).
function sanitizeIds(v, depth = 0) {
  if (v == null || typeof v !== 'object' || depth > 12) return v;
  if (Array.isArray(v)) { for (let i = 0; i < v.length; i++) v[i] = sanitizeIds(v[i], depth + 1); return v; }
  for (const k of Object.keys(v)) {
    if ((k === 'id' || k === 'tool_call_id') && (v[k] == null || typeof v[k] !== 'string' || v[k] === '')) {
      v[k] = (typeof v[k] === 'string' && v[k] !== '') ? v[k]
        : (typeof v[k] === 'number' || typeof v[k] === 'boolean') ? String(v[k])
        : genId('id');
    } else {
      v[k] = sanitizeIds(v[k], depth + 1);
    }
  }
  return v;
}

// Append-only debug log for /v1 traffic — the trail of exactly what the
// gateway emitted, for diagnosing strict-client validation failures.
function debugLog(line) {
  const ts = new Date().toISOString();
  const entry = `[${ts}] ${line}\n`;
  process.stdout.write(entry);
  fs.appendFile(path.join(__dirname, 'gateway.log'), entry, () => {});
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', c => { data += c; if (data.length > 50 * 1024 * 1024) reject(new Error('body too large')); });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}
async function readJSON(req) {
  const raw = await readBody(req);
  if (!raw) return {};
  try { return JSON.parse(raw); } catch { throw new Error('invalid JSON body'); }
}
const genId = p => p + '_' + crypto.randomBytes(6).toString('hex');

// ---------- upstream calls ----------
function authHeaders(apiKey) {
  return apiKey ? { 'Authorization': `Bearer ${apiKey}`, 'x-api-key': apiKey } : {};
}

async function fetchModels(provider) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 15000);
  try {
    const r = await fetch(normalizeBase(provider.baseUrl) + '/v1/models', {
      headers: authHeaders(provider.apiKey), signal: ctrl.signal,
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const j = await r.json();
    const list = Array.isArray(j.data) ? j.data : (Array.isArray(j.models) ? j.models : []);
    return list.map(m => ({ id: m.id || m.name, owned_by: provider.name })).filter(m => m.id);
  } finally { clearTimeout(t); }
}

async function refreshProviderModels(provider) {
  try {
    provider.models = await fetchModels(provider);
    provider.lastError = null;
  } catch (e) {
    provider.models = provider.models || [];
    provider.lastError = e.name === 'AbortError' ? 'timeout' : e.message;
  }
  save();
  return provider;
}

// Build ordered candidate chain [{provider, model}]
let rrCounter = 0;
function candidatesFor(model) {
  const provs = config.providers.filter(p => (p.models || []).length > 0);
  if (!provs.length) return [];
  let chain;
  if (!model || model === 'auto') {
    chain = provs.flatMap(p => p.models.map(m => ({ provider: p, model: m.id })));
  } else {
    chain = provs.flatMap(p => p.models.filter(m => m.id === model).map(() => ({ provider: p, model })));
    if (!chain.length) chain = provs.map(p => ({ provider: p, model })); // raw name everywhere
  }
  const off = rrCounter++ % Math.max(chain.length, 1); // rotate start to spread load
  return chain.slice(off).concat(chain.slice(0, off));
}

// POST one chat completion. Returns {ok, response|error}
async function callUpstream(candidate, openaiPayload) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), CONNECT_TIMEOUT_MS);
  try {
    const payload = { ...openaiPayload, model: candidate.model };
    const r = await fetch(normalizeBase(candidate.provider.baseUrl) + '/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...authHeaders(candidate.provider.apiKey) },
      body: JSON.stringify(payload),
      signal: ctrl.signal,
    });
    clearTimeout(t);
    if (!r.ok) {
      const text = await r.text().catch(() => '');
      return { ok: false, error: `HTTP ${r.status} ${text.slice(0, 300)}` };
    }
    return { ok: true, response: r };
  } catch (e) {
    clearTimeout(t);
    return { ok: false, error: e.name === 'AbortError' ? `timeout after ${CONNECT_TIMEOUT_MS}ms` : e.message };
  }
}

// ---------- protocol translation ----------
function claudeToOpenAI(c) {
  const msgs = [];
  if (c.system) {
    const s = typeof c.system === 'string' ? c.system : (c.system || []).map(b => b.text || '').join('\n');
    if (s) msgs.push({ role: 'system', content: s });
  }
  for (const m of c.messages || []) {
    if (typeof m.content === 'string') { msgs.push({ role: m.role, content: m.content }); continue; }
    // tool results come back as user/tool_result blocks
    if (m.role === 'user' && Array.isArray(m.content) && m.content.some(b => b.type === 'tool_result')) {
      for (const b of m.content.filter(b => b.type === 'tool_result')) {
        msgs.push({ role: 'tool', tool_call_id: b.tool_use_id, content: typeof b.content === 'string' ? b.content : (b.content || []).map(x => x.text || '').join('\n') });
      }
      continue;
    }
    const content = m.content.map(b => {
      if (b.type === 'text') return { type: 'text', text: b.text };
      if (b.type === 'image') {
        const src = b.source || {};
        return { type: 'image_url', image_url: { url: `data:${src.media_type};base64,${src.data}` } };
      }
      return null;
    }).filter(Boolean);
    msgs.push({ role: m.role, content });
  }
  const o = { model: c.model, messages: msgs, max_tokens: c.max_tokens ?? 4096 };
  if (c.temperature != null) o.temperature = c.temperature;
  if (c.top_p != null) o.top_p = c.top_p;
  if (c.stop_sequences?.length) o.stop = c.stop_sequences;
  if (c.stream) o.stream = true;
  if (Array.isArray(c.tools) && c.tools.length) {
    o.tools = c.tools.map(t => ({ type: 'function', function: { name: t.name, description: t.description, parameters: t.input_schema } }));
    if (c.tool_choice) {
      o.tool_choice = c.tool_choice.type === 'auto' ? 'auto'
        : c.tool_choice.type === 'any' ? 'required'
        : { type: 'function', function: { name: c.tool_choice.name } };
    }
  }
  return o;
}

function safeParseArgs(s) { try { return JSON.parse(s || '{}'); } catch { return {}; } }

function normalizeContentText(c) {
  if (typeof c === 'string') return c;
  if (Array.isArray(c)) return c.map(b => (b && typeof b === 'object' && b.text) || '').join('');
  return '';
}

// Validate an upstream "success" response. Free/aggregating gateways often
// return 200 with empty completions, embedded error objects, or numeric ids —
// all of which must count as FAILURES so the chain moves to the next model,
// and ids must be coerced to strings so strict clients don't choke.
function validateCompletion(data) {
  if (!data || typeof data !== 'object') return { ok: false, reason: 'non-object response' };
  if (data.error) return { ok: false, reason: 'embedded error: ' + JSON.stringify(data.error).slice(0, 200) };
  const ch = data.choices;
  if (!Array.isArray(ch) || ch.length === 0) return { ok: false, reason: 'no choices in response' };
  const m = ch[0].message || {};
  const text = normalizeContentText(m.content).trim();
  const reasoning = String(m.reasoning_content ?? m.reasoning ?? '').trim();
  const tools = Array.isArray(m.tool_calls)
    ? m.tool_calls.filter(t => t && t.function && t.function.name)
    : [];
  if (!text && !reasoning && !tools.length) return { ok: false, reason: 'empty completion' };

  data.id = (data.id == null || data.id === '') ? genId('chatcmpl') : String(data.id);
  data.created = data.created ?? Math.floor(Date.now() / 1000);
  ch[0].message = {
    role: m.role || 'assistant',
    content: text || null,
    ...(reasoning ? { reasoning_content: reasoning } : {}),
    ...(tools.length ? {
      tool_calls: tools.map(t => ({
        id: String(t.id || genId('call')),
        type: 'function',
        function: { name: String(t.function.name), arguments: t.function.arguments ?? '{}' },
      })),
    } : {}),
  };
  return { ok: true, data };
}

function openAIToClaude(o, reqModel) {
  const choice = (o.choices && o.choices[0]) || {};
  const msg = choice.message || {};
  const content = [];
  // some upstreams (deepseek-style) put output in reasoning_content
  const reasoning = String(msg.reasoning_content ?? '').trim();
  if (reasoning) content.push({ type: 'thinking', thinking: reasoning });
  const txt = normalizeContentText(msg.content);
  if (txt) content.push({ type: 'text', text: txt });
  for (const tc of msg.tool_calls || []) {
    if (!tc?.function?.name) continue;
    content.push({ type: 'tool_use', id: String(tc.id || genId('toolu')), name: String(tc.function.name), input: safeParseArgs(tc.function.arguments) });
  }
  if (!content.length) content.push({ type: 'text', text: '' });
  return {
    id: o.id || genId('msg'), type: 'message', role: 'assistant', model: reqModel,
    content,
    stop_reason: choice.finish_reason === 'length' ? 'max_tokens' : choice.finish_reason === 'tool_calls' ? 'tool_use' : 'end_turn',
    stop_sequence: null,
    usage: { input_tokens: o.usage?.prompt_tokens ?? 0, output_tokens: o.usage?.completion_tokens ?? 0 },
  };
}

// Peek into an upstream SSE stream until we know it's actually delivering
// content. 200 + immediate error events / instant [DONE] / garbage must count
// as failures so the chain can still switch models — nothing has been sent to
// the client yet at probe time.
async function probeUpstreamStream(response) {
  const reader = response.body.getReader();
  const dec = new TextDecoder();
  const rawChunks = [];   // exact bytes, replayed to client on commit (OpenAI passthrough)
  let parseBuf = '';      // decoded text for event parsing

  while (true) {
    let r;
    try {
      r = await Promise.race([
        reader.read(),
        new Promise((_, rej) => setTimeout(() => rej(new Error('probe idle timeout')), IDLE_TIMEOUT_MS)),
      ]);
    } catch (e) {
      reader.cancel().catch(() => {});
      return { ok: false, error: e.message };
    }
    if (r.done) return { ok: false, error: 'stream ended before any content' };
    rawChunks.push(r.value);
    parseBuf += dec.decode(r.value, { stream: true });

    const lines = parseBuf.split('\n');
    parseBuf = lines.pop();
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data:')) continue;
      const ds = trimmed.slice(5).trim();
      if (ds === '[DONE]') return { ok: false, error: 'stream completed with no usable content' };
      let j; try { j = JSON.parse(ds); } catch { continue; }
      if (j.error) return { ok: false, error: 'upstream stream error: ' + JSON.stringify(j.error).slice(0, 200) };
      const d = j.choices?.[0]?.delta || {};
      if (
        (d.content && String(d.content).length) ||
        (d.reasoning_content && String(d.reasoning_content).length) ||
        (Array.isArray(d.tool_calls) && d.tool_calls.length)
      ) {
        return { ok: true, reader, chunks: rawChunks, backlog: lines.join('\n') + '\n' + parseBuf };
      }
    }
  }
}

// Translate an upstream OpenAI SSE stream into an Anthropic SSE stream
async function pipeOpenAISToAnthropic(source, res, reqModel) {
  const enc = new TextEncoder();
  res.writeHead(200, {
    'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache',
    'Connection': 'keep-alive', ...cors(),
  });
  const send = (event, data) => res.write(enc.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));

  send('message_start', {
    type: 'message_start',
    message: { id: genId('msg'), type: 'message', role: 'assistant', model: reqModel, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 0, output_tokens: 0 } },
  });

  const decoder = new TextDecoder();
  let outTokens = 0, stopReason = 'end_turn', nextIndex = 0;
  let textIdx = -1, thinkIdx = -1;
  const openToolBlocks = new Map(); // index -> {id, name, out}

  function closeBlock(idx) {
    if (idx >= 0) send('content_block_stop', { type: 'content_block_stop', index: idx });
    return -1;
  }
  function ensureTextBlock() {
    if (thinkIdx >= 0) { thinkIdx = closeBlock(thinkIdx); } // thinking ends when real text starts
    if (textIdx < 0) {
      textIdx = nextIndex++;
      send('content_block_start', { type: 'content_block_start', index: textIdx, content_block: { type: 'text', text: '' } });
    }
  }
  function ensureThinkBlock() {
    if (thinkIdx < 0) {
      thinkIdx = nextIndex++;
      send('content_block_start', { type: 'content_block_start', index: thinkIdx, content_block: { type: 'thinking', thinking: '' } });
    }
  }

  const reader = source.reader;
  let buf = source.backlog || '';
  let idleTimer = setTimeout(() => reader.cancel().catch(() => {}), IDLE_TIMEOUT_MS);
  const resetIdle = () => { clearTimeout(idleTimer); idleTimer = setTimeout(() => reader.cancel().catch(() => {}), IDLE_TIMEOUT_MS); };
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      resetIdle();
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) continue;
        const dataStr = trimmed.slice(5).trim();
        if (dataStr === '[DONE]') continue;
        let chunk; try { chunk = JSON.parse(dataStr); } catch { continue; }
        const delta = chunk.choices?.[0]?.delta || {};
        if (delta.reasoning_content) {
          ensureThinkBlock();
          send('content_block_delta', { type: 'content_block_delta', index: thinkIdx, delta: { type: 'thinking_delta', thinking: delta.reasoning_content } });
        }
        if (delta.content) {
          ensureTextBlock();
          outTokens += 1;
          send('content_block_delta', { type: 'content_block_delta', index: textIdx, delta: { type: 'text_delta', text: delta.content } });
        }
        for (const tc of delta.tool_calls || []) {
          const idx = tc.index ?? 0;
          if (!openToolBlocks.has(idx)) {
            thinkIdx = closeBlock(thinkIdx);
            textIdx = closeBlock(textIdx);
            const blkIndex = nextIndex++;
            openToolBlocks.set(idx, { id: (tc.id != null && tc.id !== '') ? String(tc.id) : genId('toolu'), name: tc.function?.name || '', out: blkIndex, declared: false });
          }
          const tb = openToolBlocks.get(idx);
          if (tc.function?.name) tb.name = tc.function.name;
          if (!tb.declared) {
            tb.declared = true;
            send('content_block_start', { type: 'content_block_start', index: tb.out, content_block: { type: 'tool_use', id: tb.id, name: tb.name, input: {} } });
          }
          if (tc.function?.arguments) {
            send('content_block_delta', { type: 'content_block_delta', index: tb.out, delta: { type: 'input_json_delta', partial_json: tc.function.arguments } });
          }
        }
        if (chunk.choices?.[0]?.finish_reason === 'length') stopReason = 'max_tokens';
        else if (chunk.choices?.[0]?.finish_reason === 'tool_calls') stopReason = 'tool_use';
      }
    }
  } catch (_) { /* client disconnect or upstream drop — just end */ }
  clearTimeout(idleTimer);
  closeBlock(thinkIdx);
  closeBlock(textIdx);
  for (const tb of openToolBlocks.values()) send('content_block_stop', { type: 'content_block_stop', index: tb.out });
  send('message_delta', { type: 'message_delta', delta: { stop_reason: stopReason, stop_sequence: null }, usage: { output_tokens: outTokens } });
  send('message_stop', { type: 'message_stop' });
  res.end();
}

// ---------- request handlers ----------
function authorized(req) {
  const h = req.headers['authorization'] || '';
  const bearer = h.startsWith('Bearer ') ? h.slice(7) : '';
  return bearer === config.gatewayKey || req.headers['x-api-key'] === config.gatewayKey;
}

async function handleChat(req, res, protocol) {
  if (!authorized(req)) return json(res, 401, { error: { message: 'invalid gateway key' } });
  const body = await readJSON(req).catch(e => { json(res, 400, { error: { message: e.message } }); });
  if (!body || !body.messages) return;

  const openaiPayload = protocol === 'claude' ? claudeToOpenAI(body) : body;
  const chain = candidatesFor(openaiPayload.model === undefined ? body.model : openaiPayload.model);
  if (!chain.length) return json(res, 503, { error: { message: 'no providers/models configured' } });

  const wantsStream = !!openaiPayload.stream;
  const errors = [];

  for (const cand of chain) {
    const t0 = Date.now();
    const result = await callUpstream(cand, { ...openaiPayload, stream: wantsStream });
    if (!result.ok) {
      errors.push(`[${cand.provider.name}/${cand.model}] ${result.error}`);
      debugLog(`[fail] ${cand.provider.name}/${cand.model}: ${result.error}`);
      continue;
    }
    debugLog(`[ok] ${cand.provider.name}/${cand.model} (${Date.now() - t0}ms)`);

    if (!wantsStream) {
      let data;
      try { data = await result.response.json(); } catch (e) {
        errors.push(`[${cand.provider.name}/${cand.model}] bad json: ${e.message}`);
        debugLog(`[fail] ${cand.provider.name}/${cand.model}: bad json`);
        continue;
      }
      const v = validateCompletion(data);
      if (!v.ok) {
        errors.push(`[${cand.provider.name}/${cand.model}] ${v.reason}`);
        debugLog(`[fail] ${cand.provider.name}/${cand.model}: ${v.reason}`);
        continue;
      }
      if (protocol === 'claude') return json(res, 200, openAIToClaude(v.data, body.model));
      return json(res, 200, v.data);
    }

    // streaming — probe first so a dead upstream still allows failover
    const probe = await probeUpstreamStream(result.response);
    if (!probe.ok) {
      errors.push(`[${cand.provider.name}/${cand.model}] stream: ${probe.error}`);
      debugLog(`[fail] ${cand.provider.name}/${cand.model}: stream ${probe.error}`);
      continue;
    }

    if (protocol === 'claude') {
      try { await pipeOpenAISToAnthropic(probe, res, body.model); } catch (_) {}
      return;
    }
    // OpenAI passthrough — replay probed bytes then pipe the rest,
    // rewriting every SSE chunk so ids are always strings
    res.writeHead(200, {
      'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache',
      'Connection': 'keep-alive', ...cors(),
    });
    const reader = probe.reader;
    const dec = new TextDecoder();
    let carry = '';
    let idleTimer = setTimeout(() => reader.cancel().catch(() => {}), IDLE_TIMEOUT_MS);
    const kick = () => { clearTimeout(idleTimer); idleTimer = setTimeout(() => reader.cancel().catch(() => {}), IDLE_TIMEOUT_MS); };
    const writeSanitized = text => {
      // process line-wise, preserving the exact event boundaries
      const lines = text.split('\n');
      carry = lines.pop();
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('data:') && trimmed !== 'data: [DONE]' && trimmed !== 'data:[DONE]') {
          const ds = trimmed.slice(5).trim();
          try { res.write(`data: ${JSON.stringify(sanitizeIds(JSON.parse(ds)))}\n\n`); continue; }
          catch { /* not JSON — forward untouched */ }
        }
        res.write(line + '\n');
      }
    };
    try {
      for (const c of probe.chunks) writeSanitized(dec.decode(c, { stream: true }));
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        kick();
        writeSanitized(dec.decode(value, { stream: true }));
      }
      if (carry) res.write(carry + '\n');
    } catch (_) {}
    clearTimeout(idleTimer);
    res.end();
    return;
  }

  json(res, 502, {
    error: { message: 'all models failed', details: errors.slice(-10), attempts: chain.length },
  });
}

function handleModels(req, res) {
  if (!authorized(req)) return json(res, 401, { error: { message: 'invalid gateway key' } });
  const data = config.providers.flatMap(p => (p.models || []).map(m => ({
    id: m.id, object: 'model', owned_by: m.owned_by || p.name,
    type: 'model', display_name: m.id, created_at: '2024-01-01T00:00:00Z',
  })));
  // superset of OpenAI + Anthropic list shapes
  json(res, 200, { object: 'list', data, has_more: false, first_id: data[0]?.id, last_id: data[data.length - 1]?.id });
}

// ---------- management API ----------
async function handleApi(req, res, url) {
  const route = `${req.method} ${url.pathname}`;

  if (route === 'GET /api/config') {
    return json(res, 200, {
      port: PORT, gatewayKey: config.gatewayKey,
      baseUrl: `http://localhost:${PORT}/v1`,
      timeout: { connectMs: CONNECT_TIMEOUT_MS, idleMs: IDLE_TIMEOUT_MS },
    });
  }
  if (route === 'GET /api/providers/list') {
    return json(res, 200, config.providers);
  }
  if (route === 'POST /api/providers') {
    const b = await readJSON(req);
    if (!b.baseUrl) return json(res, 400, { error: 'baseUrl required' });
    if (!/^https?:\/\//i.test(b.baseUrl)) return json(res, 400, { error: 'baseUrl must start with http(s)://' });
    const provider = {
      id: genId('prov'), name: (b.name || new URL(b.baseUrl).host).trim(),
      baseUrl: normalizeBase(b.baseUrl), apiKey: (b.apiKey || '').trim(), models: [],
    };
    config.providers.push(provider);
    await refreshProviderModels(provider); // fetch models immediately so UI shows status
    return json(res, 200, provider);
  }
  const pm = url.pathname.match(/^\/api\/providers\/([^/]+)(\/refresh)?$/);
  if (pm) {
    const p = config.providers.find(x => x.id === pm[1]);
    if (!p) return json(res, 404, { error: 'not found' });
    if (req.method === 'DELETE') {
      config.providers = config.providers.filter(x => x.id !== p.id);
      save();
      return json(res, 200, { ok: true });
    }
    if (req.method === 'POST') { await refreshProviderModels(p); return json(res, 200, p); }
  }
  json(res, 404, { error: 'unknown api route' });
}

// ---------- static + server ----------
const INDEX = path.join(__dirname, 'index.html');
function serveIndex(res) {
  fs.readFile(INDEX, (err, buf) => {
    if (err) return json(res, 500, { error: 'index.html missing' });
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(buf);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (req.method === 'OPTIONS') { res.writeHead(204, cors()); return res.end(); }

  try {
    if (url.pathname === '/' || url.pathname === '/index.html') return serveIndex(res);

    if (url.pathname.startsWith('/api/')) return await handleApi(req, res, url);

    if (url.pathname === '/v1/models' && req.method === 'GET') return handleModels(req, res);
    const singleModel = url.pathname.match(/^\/v1\/models\/(.+)$/);
    if (singleModel && req.method === 'GET') {
      if (!authorized(req)) return json(res, 401, { error: { message: 'invalid gateway key' } });
      const m = config.providers.flatMap(p => p.models || []).find(x => x.id === decodeURIComponent(singleModel[1]));
      if (!m) return json(res, 404, { error: { message: 'model not found' } });
      return json(res, 200, { id: m.id, object: 'model', owned_by: m.owned_by, type: 'model', display_name: m.id });
    }
    // Anthropic SDKs call this before every turn — must not 404
    if (url.pathname === '/v1/messages/count_tokens' && req.method === 'POST') {
      if (!authorized(req)) return json(res, 401, { error: { message: 'invalid gateway key' } });
      const b = await readJSON(req).catch(() => ({}));
      const text = JSON.stringify(b.system || '') + JSON.stringify(b.messages || []) + JSON.stringify(b.tools || []);
      return json(res, 200, { input_tokens: Math.max(1, Math.ceil(text.length / 4)) });
    }
    if (url.pathname === '/v1/chat/completions' && req.method === 'POST') { debugLog(`${req.socket.remoteAddress} POST /v1/chat/completions`); return await handleChat(req, res, 'openai'); }
    if (url.pathname === '/v1/messages' && req.method === 'POST') { debugLog(`${req.socket.remoteAddress} POST /v1/messages`); return await handleChat(req, res, 'claude'); }

    debugLog(`404 ${req.method} ${url.pathname}`);
    json(res, 404, { error: { message: `no route: ${req.method} ${url.pathname}` } });
  } catch (e) {
    console.error('[server]', e);
    if (!res.headersSent) json(res, 500, { error: { message: e.message } });
    else res.end();
  }
});

server.listen(PORT, HOST, () => {
  console.log(`LLM Gateway  ->  http://localhost:${PORT}`);
  console.log(`Gateway key  ->  ${config.gatewayKey}`);
  console.log(`OpenAI proto ->  /v1/chat/completions   Claude proto -> /v1/messages`);
  console.log(`Model "auto" tries every model across all providers until success.`);
});
