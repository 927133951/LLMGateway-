// mock bad upstream: returns 200s that are structurally broken
const http = require('http');
http.createServer((req, res) => {
  if (req.url === '/v1/models') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ data: [{ id: 'broken-empty' }, { id: 'gpt-oss-20b' }] }));
  }
  let body = '';
  req.on('data', c => body += c);
  req.on('end', () => {
    const wantsStream = body.includes('"stream":true');
    const isBrokenEmpty = body.includes('broken-empty');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    if (wantsStream) {
      if (isBrokenEmpty) {
        // 200 SSE that instantly errors
        res.write('data: {"error": {"message": "boom"}}\n\n');
        return res.end('data: [DONE]\n\n');
      }
      // valid-looking stream but with NUMERIC ids everywhere
      const chunk = (delta, finish) =>
        `data: ${JSON.stringify({ id: 12345, choices: [{ index: 0, delta, finish_reason: finish ?? null }] })}\n\n`;
      res.write(chunk({ role: 'assistant', content: '' }));
      res.write(chunk({ content: 'OK' }));
      res.write(chunk({}, 'stop'));
      return res.end('data: [DONE]\n\n');
    }
    // empty completion with a NUMERIC id
    res.end(JSON.stringify({ id: 12345, choices: [{ message: { role: 'assistant', content: '' } }] }));
  });
}).listen(9999, () => console.log('mock bad upstream on :9999'));
