const assert = require('assert');
const http = require('http');
const {BaseAPI} = require('../out/api/base');

describe('multipart upload transport', () => {
    it('sends the filename field and binary bytes as multipart form data', async () => {
        const fixture = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x00, 0xff]);
        let request;
        const server = http.createServer((incoming, response) => {
            const chunks = [];
            incoming.on('data', chunk => chunks.push(chunk));
            incoming.on('end', () => {
                request = {
                    headers: incoming.headers,
                    url: incoming.url,
                    body: Buffer.concat(chunks),
                };
                response.setHeader('Content-Type', 'application/json');
                response.end(JSON.stringify({
                    success: true,
                    entity_id: 'binary-id',
                    entity_type: 'file',
                }));
            });
        });

        await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
        try {
            const {port} = server.address();
            const api = new BaseAPI(`http://127.0.0.1:${port}/`);
            const result = await api.uploadFile(
                {csrfToken: 'csrf', cookies: 'session=value'},
                'project-id',
                'folder-id',
                'fixture.png',
                fixture,
            );

            assert.strictEqual(result.type, 'success');
            assert.strictEqual(result.entity._id, 'binary-id');
            assert.strictEqual(request.url, '/project/project-id/upload?folder_id=folder-id');
            assert.match(request.headers['content-type'], /^multipart\/form-data; boundary=/);
            assert(request.body.includes(Buffer.from('name="name"\r\n\r\nfixture.png')));
            assert(request.body.includes(Buffer.from('name="qqfile"; filename="fixture.png"')));
            assert(request.body.includes(fixture));
        } finally {
            await new Promise(resolve => server.close(resolve));
        }
    });
});
