const assert = require('assert');
const fs = require('fs');
const path = require('path');
const {
    ROOT_NAME,
    URI_SCHEME,
    VIEW_CONTAINER_ID,
} = require('../out/extensionIdentity');

describe('independent extension namespace', () => {
    const manifest = JSON.parse(
        fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'),
    );

    it('preserves existing project URIs without reusing upstream contribution ids', () => {
        assert.strictEqual(URI_SCHEME, 'overleaf-workshop');
        assert.strictEqual(ROOT_NAME, 'hershel.overleaf-workshop-safe-sync');

        const activityBar = manifest.contributes.viewsContainers.activitybar;
        assert(activityBar.every(item => item.id !== 'overleaf-workshop'));
        assert.strictEqual(activityBar[0].id, VIEW_CONTAINER_ID);
        assert(/^[a-zA-Z0-9_-]+$/.test(VIEW_CONTAINER_ID));
        assert(manifest.contributes.views[VIEW_CONTAINER_ID]);
        assert.strictEqual(
            manifest.contributes.customEditors[0].viewType,
            `${ROOT_NAME}.pdfViewer`,
        );
        assert(manifest.activationEvents.includes('onStartupFinished'));
    });

    it('namespaces every contributed command and configuration key', () => {
        for (const command of manifest.contributes.commands) {
            assert(
                command.command.startsWith(`${ROOT_NAME}.`),
                `unscoped command: ${command.command}`,
            );
        }
        for (const key of Object.keys(manifest.contributes.configuration.properties)) {
            assert(key.startsWith(`${ROOT_NAME}.`), `unscoped configuration: ${key}`);
        }
    });
});
