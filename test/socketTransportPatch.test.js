const assert = require('assert');
const fs = require('fs');
const path = require('path');

describe('legacy Socket.IO credential transport patch', () => {
    const socketSource = fs.readFileSync(
        path.join(__dirname, '..', 'node_modules', 'socket.io-client', 'lib', 'socket.js'),
        'utf8',
    );
    const websocketSource = fs.readFileSync(
        path.join(
            __dirname,
            '..',
            'node_modules',
            'socket.io-client',
            'lib',
            'transports',
            'websocket.js',
        ),
        'utf8',
    );

    it('sends authenticated headers during the HTTP handshake', () => {
        assert(socketSource.includes("this.options['extraHeaders']"));
        assert(socketSource.includes('xhr.setRequestHeader(key, value)'));
    });

    it('forwards authenticated headers to the WebSocket upgrade', () => {
        assert(socketSource.includes("self.transport.open(self.options['extraHeaders'])"));
        assert(websocketSource.includes('headers: extraHeaders || {}'));
    });
});
