const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const {createHash} = require('node:crypto');
const {isEditableTextCandidate, shouldIgnore} = require('../out/sync/syncPolicy');

const directory = process.argv[2];
if (!directory) throw new Error('Usage: node scripts/verify-real-latex-files.cjs <elsarticle-source-directory>');
const files = ['elsarticle-harv.bst', 'elsarticle-num-names.bst', 'elsarticle-num.bst', 'elsarticle.dtx', 'elsarticle.ins', 'doc/makefile'];
const results = files.map(file => {
    const content = fs.readFileSync(path.join(directory, file));
    assert.ok(content.length > 0, file);
    assert.equal(isEditableTextCandidate(file, content), true, file);
    assert.equal(shouldIgnore(file), false, file);
    assert.equal(shouldIgnore(`.latex-build/${file}`), true, file);
    assert.equal(shouldIgnore(`paper/.latex-build/${file}`), true, file);
    return {file, bytes: content.length, sha256: createHash('sha256').update(content).digest('hex')};
});
console.log(JSON.stringify(results, null, 2));
