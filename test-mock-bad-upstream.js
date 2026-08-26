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
    res.writeHead(200, { 'Content-Type': 'application/json' });
    if (wantsStream) {
      // 200 SSE that instantly errors
      res.write('data: {"error": {"message": "boom"}}\n\n');
      res.end('data: [DONE]\n\n');
    } else {
      // empty completion with a NUMERIC id (the exact bug class reported)
      res.end(JSON.stringify({ id: 12345, choices: [{ message: { role: 'assistant', content: '' } }] }));
    }
  });
}).listen(9999, () => console.log('mock bad upstream on :9999'));
