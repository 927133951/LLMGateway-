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
  const body = JSON.stringify(obj);
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

// Idle watchdog: abort if no bytes for IDLE_TIMEOUT_MS during body consumption
function makeIdleWatch(ctrl) {
  let t = setTimeout(() => ctrl.abort(), IDLE_TIMEOUT_MS);
  return {
    kick() { clearTimeout(t); t = setTimeout(() => ctrl.abort(), IDLE_TIMEOUT_MS); },
    stop() { clearTimeout(t); },
  };
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

function openAIToClaude(o, reqModel) {
  const choice = (o.choices && o.choices[0]) || {};
  const msg = choice.message || {};
  const content = [];
  // some upstreams (deepseek-style) put output in reasoning_content
  if (msg.reasoning_content) content.push({ type: 'thinking', thinking: msg.reasoning_content });
  if (msg.content) content.push({ type: 'text', text: msg.content });
  for (const tc of msg.tool_calls || []) {
    content.push({ type: 'tool_use', id: tc.id || genId('toolu'), name: tc.function?.name, input: safeParseArgs(tc.function?.arguments) });
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

// Translate an upstream OpenAI SSE stream into an Anthropic SSE stream
async function pipeOpenAISToAnthropic(upstream, res, reqModel) {
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
  let buf = '', outTokens = 0, stopReason = 'end_turn', nextIndex = 0;
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

  const reader = upstream.body.getReader();
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
            openToolBlocks.set(idx, { id: tc.id || genId('toolu'), name: tc.function?.name || '', out: blkIndex, declared: false });
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
      console.log(`[fail] ${cand.provider.name}/${cand.model}: ${result.error}`);
      continue;
    }
    console.log(`[ok] ${cand.provider.name}/${cand.model} (${Date.now() - t0}ms)`);

    if (!wantsStream) {
      let data;
      try { data = await result.response.json(); } catch (e) {
        errors.push(`[${cand.provider.name}/${cand.model}] bad json: ${e.message}`);
        continue;
      }
      if (protocol === 'claude') return json(res, 200, openAIToClaude(data, body.model));
      return json(res, 200, data);
    }

    // streaming
    if (protocol === 'claude') {
      try { await pipeOpenAISToAnthropic(result.response, res, body.model); } catch (_) {}
      return;
    }
    // OpenAI passthrough
    res.writeHead(200, {
      'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache',
      'Connection': 'keep-alive', ...cors(),
    });
    const reader = result.response.body.getReader();
    const idle = makeIdleWatch(new AbortController());
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        idle.kick();
        res.write(value);
      }
    } catch (_) {}
    idle.stop();
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
    if (url.pathname === '/v1/chat/completions' && req.method === 'POST') return await handleChat(req, res, 'openai');
    if (url.pathname === '/v1/messages' && req.method === 'POST') return await handleChat(req, res, 'claude');

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
