#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

npx --no-install patch-package
npm run test:unit
npm run lint
npm run compile

readarray -t identity < <(node - <<'NODE'
const manifest = require('./package.json');
if (manifest.publisher !== 'hershel' || manifest.name !== 'overleaf-workshop-safe-sync') {
    throw new Error(`Unexpected extension identity: ${manifest.publisher}.${manifest.name}`);
}
if (!/^1\.0\.0-hershel\.\d+$/.test(manifest.version)) {
    throw new Error(`Unexpected release version: ${manifest.version}`);
}
if (typeof manifest.hershelBuildId !== 'string' || manifest.hershelBuildId.length < 8) {
    throw new Error('package.json must contain a non-empty hershelBuildId');
}
console.log(manifest.version);
console.log(manifest.hershelBuildId);
NODE
)

version="${identity[0]}"
build_id="${identity[1]}"
output="dist/hershel.overleaf-workshop-safe-sync-${version}.vsix"
mkdir -p dist
npx --no-install @vscode/vsce package --out "$output"

archive_hash="$(sha256sum "$output" | cut -d' ' -f1)"
printf 'release_version=%s\n' "$version"
printf 'build_id=%s\n' "$build_id"
printf 'vsix=%s\n' "$output"
printf 'sha256=%s\n' "$archive_hash"
