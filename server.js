// Local LLM Gateway — merges multiple OpenAI-compatible upstreams into one
// endpoint with a single key. "auto" model = try every model until one answers.
// Zero dependencies. Run: node server.js
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Last-resort guards: one bad socket must never kill the whole gateway.
// Writing to an aborted client connection raises otherwise-unhandled
// errors that terminate the process (every active conversation dies at once).
process.on('uncaughtException', e => {
  try { debugLog('[crash-guard] uncaughtException: ' + (e.stack || e)); } catch (_) {}
});
process.on('unhandledRejection', e => {
  try { debugLog('[rejection-guard] ' + ((e && e.stack) || String(e))); } catch (_) {}
});

const PORT = process.env.PORT || 4567;
const HOST = process.env.HOST || '0.0.0.0';
const CONFIG_FILE = path.join(__dirname, 'providers.json');
const CONNECT_TIMEOUT_MS = +(process.env.CONNECT_TIMEOUT_MS || 60000); // per attempt, until headers
const IDLE_TIMEOUT_MS = +(process.env.IDLE_TIMEOUT_MS || 240000);      // max silence mid-stream

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

// ---------- model health (in-memory circuit breaker) ----------
// Without this, a pool of 700 models means client retries re-walk the same
// first few dead entries forever. Failed models get benched (cooldown),
// proven-good models float to the front, and the auto cursor resumes where
// the previous request gave up instead of advancing one slot.
const HEALTH = new Map();
const FAIL_COOLDOWN_MS = +(process.env.FAIL_COOLDOWN_MS || 300000);   // bench 5min after a failure
const RATE_COOLDOWN_MS = +(process.env.RATE_COOLDOWN_MS || 600000);   // bench 10min after rate-limit
const hkey = c => c.provider.id + '|' + c.model;
function hget(c) {
  let h = HEALTH.get(hkey(c));
  if (!h) { h = { oks: 0, fails: 0, lastOkAt: 0, coolUntil: 0 }; HEALTH.set(hkey(c), h); }
  return h;
}
function markOk(c) {
  const h = hget(c);
  h.oks++; h.lastOkAt = Date.now(); h.coolUntil = 0;
}
function markFail(c, err) {
  const h = hget(c);
  h.fails++;
  const rate = /429|rate.?limit|too many/i.test(String(err || ''));
  const ms = rate ? RATE_COOLDOWN_MS : FAIL_COOLDOWN_MS;
  h.coolUntil = Date.now() + ms;
  debugLog(`[cool] ${c.provider.name}/${c.model} benched ${Math.round(ms / 1000)}s (${String(err || 'error').slice(0, 90)})`);
}

// Build ordered candidate chain [{provider, model}]
let autoCursor = 0;
let lastOffset = 0;
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
  const now = Date.now();
  const scored = chain.map((c, i) => ({ c, i, h: hget(c) }));
  const hot = scored.filter(x => x.h.coolUntil <= now);     // not benched
  const cold = scored.filter(x => x.h.coolUntil > now);      // recently failed — sink to the end
  // recently proven-good first, then untried (original order), then benched by soonest recovery
  hot.sort((a, b) => (b.h.lastOkAt - a.h.lastOkAt) || (a.i - b.i));
  cold.sort((a, b) => a.h.coolUntil - b.h.coolUntil);
  const ordered = hot.concat(cold).map(x => x.c);
  const isAuto = !model || model === 'auto';
  const off = isAuto ? (autoCursor % ordered.length) : 0;
  lastOffset = off;
  return ordered.slice(off).concat(ordered.slice(0, off));
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
async function pipeOpenAISToAnthropic(source, res, reqModel, label = '?', onInterrupted = null) {
  const enc = new TextEncoder();
  res.writeHead(200, {
    'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache',
    'Connection': 'keep-alive', ...cors(),
  });
  const send = (event, data) => {
    if (res.destroyed) return false;
    try { res.write(enc.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)); return true; }
    catch { return false; }
  };
  // if the client hangs up mid-stream, stop reading the upstream
  const onClose = () => source.reader.cancel().catch(() => {});
  res.on('close', onClose);

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
  let interrupted = null;
  let clientGone = false;
  let sawNaturalEnd = false;
  res.on('close', () => { clientGone = true; });
  let idleTimer = null;
  let idleFired = false;
  const armIdle = () => {
    idleTimer = setTimeout(() => { idleFired = true; reader.cancel().catch(() => {}); }, IDLE_TIMEOUT_MS);
  };
  armIdle();
  try {
    while (true) {
      let r;
      try {
        r = await reader.read();
      } catch (e) {
        interrupted = `upstream read failed: ${e.message}`;
        break;
      }
      if (r.done) { sawNaturalEnd = true; break; }
      resetIdle();
      buf += decoder.decode(r.value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) continue;
        const dataStr = trimmed.slice(5).trim();
        if (dataStr === '[DONE]') continue;
        let chunk; try { chunk = JSON.parse(dataStr); } catch { continue; }
        if (chunk.error && !chunk.choices) {
          interrupted = 'upstream error mid-stream: ' + String(chunk.error.message || JSON.stringify(chunk.error)).slice(0, 120);
          break;
        }
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
      if (interrupted) break;
    }
  } catch (_) { interrupted = interrupted || 'pipe write failed'; }
  clearTimeout(idleTimer);
  if (!interrupted && !sawNaturalEnd && !clientGone) interrupted = 'upstream ended without finish signal';
  if (idleFired && !sawNaturalEnd) interrupted = `no data for ${IDLE_TIMEOUT_MS}ms (idle timeout)`;
  res.off('close', onClose);
  closeBlock(thinkIdx);
  closeBlock(textIdx);
  for (const tb of openToolBlocks.values()) send('content_block_stop', { type: 'content_block_stop', index: tb.out });
  if (interrupted && !clientGone) {
    if (onInterrupted) { try { onInterrupted(interrupted); } catch (_) {} }
    debugLog(`[cut] ${label}: ${interrupted} — sent error event to client`);
    send('error', { type: 'error', error: { type: 'api_error', message: `upstream interrupted mid-response (transient — safe to retry, another model will be selected): ${interrupted}`, code: 'upstream_interrupted', retryable: true } });
  }
  if (!interrupted && stopReason === 'max_tokens') {
    debugLog(`[trunc] ${label}: model hit max_tokens — answer cut short`);
  }
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

  // Many tools omit max_tokens; backends then apply tiny internal defaults
  // and truncate mid-answer (looks like "conversation just died").
  // Give reasoning models room to think unless the caller chose a value.
  const MAX_TOKENS_DEFAULT = +(process.env.MAX_TOKENS_DEFAULT || 8192);
  if (openaiPayload.max_tokens == null) openaiPayload.max_tokens = MAX_TOKENS_DEFAULT;

  debugLog(`${req.socket.remoteAddress} ${protocol} proto | model=${openaiPayload.model} | stream=${!!openaiPayload.stream} | msgs=${(openaiPayload.messages || []).length}`);

  // global system prompt steering (e.g. force Chinese replies)
  const sys = String(config.systemPrompt || '').trim();
  if (sys && Array.isArray(openaiPayload.messages)) {
    const idx = openaiPayload.messages.findIndex(m => m.role === 'system');
    if (idx >= 0) {
      const prev = typeof openaiPayload.messages[idx].content === 'string' ? openaiPayload.messages[idx].content : JSON.stringify(openaiPayload.messages[idx].content);
      openaiPayload.messages[idx] = { role: 'system', content: sys + '\n\n' + prev };
    } else {
      openaiPayload.messages.unshift({ role: 'system', content: sys });
    }
    // recency reinforcement: small models ignore opening instructions when
    // drowning in long foreign-language context, so pin the requirement to
    // the very end of the visible context as well
    const last = openaiPayload.messages[openaiPayload.messages.length - 1];
    if (last && last.role === 'user' && typeof last.content === 'string') {
      last.content += '\n\n[System] ' + sys;
    }
  }

  const chain = candidatesFor(openaiPayload.model === undefined ? body.model : openaiPayload.model);
  if (!chain.length) return json(res, 503, { error: { message: 'no providers/models configured' } });

  const wantsStream = !!openaiPayload.stream;
  const errors = [];
  let tried = 0;

  for (const cand of chain) {
    tried++;
    const t0 = Date.now();
    const result = await callUpstream(cand, { ...openaiPayload, stream: wantsStream });
    if (!result.ok) {
      markFail(cand, result.error);
      errors.push(`[${cand.provider.name}/${cand.model}] ${result.error}`);
      debugLog(`[fail] ${cand.provider.name}/${cand.model}: ${result.error}`);
      continue;
    }
    debugLog(`[ok] ${cand.provider.name}/${cand.model} (${Date.now() - t0}ms)`);

    if (!wantsStream) {
      let data;
      try { data = await result.response.json(); } catch (e) {
        markFail(cand, 'bad json: ' + e.message);
        errors.push(`[${cand.provider.name}/${cand.model}] bad json: ${e.message}`);
        debugLog(`[fail] ${cand.provider.name}/${cand.model}: bad json`);
        continue;
      }
      const v = validateCompletion(data);
      if (!v.ok) {
        markFail(cand, v.reason);
        errors.push(`[${cand.provider.name}/${cand.model}] ${v.reason}`);
        debugLog(`[fail] ${cand.provider.name}/${cand.model}: ${v.reason}`);
        continue;
      }
      markOk(cand);
      if (protocol === 'claude') return json(res, 200, openAIToClaude(v.data, body.model));
      return json(res, 200, v.data);
    }

    // streaming — probe first so a dead upstream still allows failover
    const probe = await probeUpstreamStream(result.response);
    if (!probe.ok) {
      markFail(cand, 'stream: ' + probe.error);
      errors.push(`[${cand.provider.name}/${cand.model}] stream: ${probe.error}`);
      debugLog(`[fail] ${cand.provider.name}/${cand.model}: stream ${probe.error}`);
      continue;
    }
    markOk(cand);

    if (protocol === 'claude') {
      try {
        await pipeOpenAISToAnthropic(probe, res, body.model, `${cand.provider.name}/${cand.model}`,
          reason => markFail(cand, 'mid-stream: ' + reason));
      } catch (_) {}
      return;
    }
    // OpenAI passthrough — replay probed bytes then pipe the rest,
    // rewriting every SSE chunk so ids are always strings
    res.writeHead(200, {
      'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache',
      'Connection': 'keep-alive', ...cors(),
    });
    const label = `${cand.provider.name}/${cand.model}`;
    const reader = probe.reader;
    const dec = new TextDecoder();
    let carry = '';
    let sawDone = false, interrupted = null, clientGone = false, idleFired = false, finishSeen = null;
    res.on('close', () => { clientGone = true; });
    const onClientClose = () => reader.cancel().catch(() => {});
    res.on('close', onClientClose);
    let idleTimer = null;
    const armIdle = () => { idleTimer = setTimeout(() => { idleFired = true; reader.cancel().catch(() => {}); }, IDLE_TIMEOUT_MS); };
    armIdle();
    const kick = () => { clearTimeout(idleTimer); armIdle(); };
    const writeSanitized = text => {
      // process line-wise, preserving the exact event boundaries
      const lines = text.split('\n');
      carry = lines.pop();
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('data:') && trimmed !== 'data: [DONE]' && trimmed !== 'data:[DONE]') {
          const ds = trimmed.slice(5).trim();
          try {
            const obj = JSON.parse(ds);
            if (obj.error && !obj.choices) {
              interrupted = 'upstream error mid-stream: ' + String(obj.error.message || JSON.stringify(obj.error)).slice(0, 120);
              reader.cancel().catch(() => {});
              return;
            }
            const fr = obj.choices?.[0]?.finish_reason;
            if (fr) finishSeen = fr;
            res.write(`data: ${JSON.stringify(sanitizeIds(obj))}\n\n`);
            continue;
          } catch { /* not JSON — forward untouched */ }
        } else if (trimmed === 'data: [DONE]' || trimmed === 'data:[DONE]') {
          sawDone = true;
        }
        res.write(line + '\n');
      }
    };
    try {
      for (const c of probe.chunks) writeSanitized(dec.decode(c, { stream: true }));
      while (true) {
        let r;
        try { r = await reader.read(); } catch (e) { interrupted = `upstream read failed: ${e.message}`; break; }
        if (r.done) break;
        kick();
        writeSanitized(dec.decode(r.value, { stream: true }));
      }
      if (!interrupted && carry) res.write(carry + '\n');
    } catch (_) { interrupted = interrupted || 'pipe write failed'; }
    clearTimeout(idleTimer);
    res.off('close', onClientClose);
    if (!interrupted && !sawDone && !clientGone) interrupted = 'upstream ended without [DONE]';
    if (idleFired && !sawDone) interrupted = `no data for ${IDLE_TIMEOUT_MS}ms (idle timeout)`;
    if (interrupted && !clientGone) {
      markFail(cand, 'mid-stream: ' + interrupted);
      debugLog(`[cut] ${label}: ${interrupted} — sent error event to client`);
      res.write(`data: ${JSON.stringify({ error: { message: `upstream interrupted mid-response (transient — safe to retry, another model will be selected): ${interrupted}`, type: 'server_error', code: 'upstream_interrupted', retryable: true }, })}\n\n`);
    }
    if (!interrupted && finishSeen === 'length') {
      debugLog(`[trunc] ${label}: upstream ended with finish_reason=length — answer cut short by token cap`);
    }
    res.end();
    return;
  }

  // whole chain exhausted — remember how deep we got so the next request
  // (e.g. a client retry) resumes AFTER these instead of re-walking them
  if (!model || model === 'auto') {
    autoCursor = (lastOffset + tried) % Math.max(chain.length, 1);
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
      systemPrompt: config.systemPrompt || '',
      timeout: { connectMs: CONNECT_TIMEOUT_MS, idleMs: IDLE_TIMEOUT_MS },
    });
  }
  if (route === 'POST /api/config') {
    const b = await readJSON(req);
    if ('systemPrompt' in b) config.systemPrompt = String(b.systemPrompt || '');
    save();
    return json(res, 200, { ok: true, systemPrompt: config.systemPrompt || '' });
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
  // swallow socket-level errors (client disconnects mid-write etc.)
  res.on('error', () => {});
  req.on('error', () => {});
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
