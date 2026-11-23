const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const WS_PATH = process.env.WS_PATH || '/ws';
const UUID_STR = (process.env.UUID || '').trim();

function uuidToBytes(u) {
  const hex = u.replace(/-/g, '');
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) return null;
  return Buffer.from(hex, 'hex');
}
const UUID_BYTES = uuidToBytes(UUID_STR);
if (!UUID_BYTES) {
  console.error('UUID env var missing or invalid');
  process.exit(1);
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('ok\n');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== WS_PATH) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws) => {
  let buffer = Buffer.alloc(0);
  let tcp = null;
  let headerDone = false;

  ws.on('message', (data) => {
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (headerDone) {
      if (tcp && !tcp.destroyed) tcp.write(chunk);
      return;
    }
    buffer = Buffer.concat([buffer, chunk]);
    // minimum: 1 ver + 16 uuid + 1 addonlen
    if (buffer.length < 18) return;
    const version = buffer[0];
    const uuid = buffer.subarray(1, 17);
    if (!uuid.equals(UUID_BYTES)) { ws.close(); return; }
    const addonLen = buffer[17];
    let offset = 18 + addonLen;
    if (buffer.length < offset + 1) return;
    const command = buffer[offset]; offset += 1;
    if (command !== 1) { ws.close(); return; } // TCP only
    if (buffer.length < offset + 2) return;
    const port = buffer.readUInt16BE(offset); offset += 2;
    if (buffer.length < offset + 1) return;
    const addrType = buffer[offset]; offset += 1;
    let host;
    if (addrType === 1) {
      if (buffer.length < offset + 4) return;
      host = Array.from(buffer.subarray(offset, offset + 4)).join('.');
      offset += 4;
    } else if (addrType === 2) {
      if (buffer.length < offset + 1) return;
      const len = buffer[offset]; offset += 1;
      if (buffer.length < offset + len) return;
      host = buffer.subarray(offset, offset + len).toString('utf8');
      offset += len;
    } else if (addrType === 3) {
      if (buffer.length < offset + 16) return;
      const parts = [];
      for (let i = 0; i < 16; i += 2) parts.push(buffer.readUInt16BE(offset + i).toString(16));
      host = parts.join(':');
      offset += 16;
    } else { ws.close(); return; }
    const rest = buffer.subarray(offset);
    headerDone = true;
    tcp = net.connect(port, host, () => {
      if (rest.length && !tcp.destroyed) tcp.write(rest);
    });
    let responseSent = false;
    tcp.on('data', (d) => { if (ws.readyState === ws.OPEN) { if (!responseSent) { responseSent = true; ws.send(Buffer.concat([Buffer.from([version, 0]), d])); } else ws.send(d); } });
    tcp.on('close', () => { try { ws.close(); } catch (e) {} });
    tcp.on('error', (e) => { try { ws.close(); } catch (e2) {} });
  });

  ws.on('close', () => { if (tcp && !tcp.destroyed) tcp.destroy(); });
  ws.on('error', () => { if (tcp && !tcp.destroyed) tcp.destroy(); });
});

server.listen(PORT, () => console.log('listening on ' + PORT));
