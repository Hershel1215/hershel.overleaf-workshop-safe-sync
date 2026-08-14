#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

release_inputs_digest() {
  find . \
    \( -path './.git' \
       -o -path './node_modules' \
       -o -path './out' \
       -o -path './dist' \
       -o -path './views/chat-view/node_modules' \
       -o -path './views/chat-view/dist' \) -prune \
    -o -type f ! -name '*.vsix' -print0 \
    | sort -z \
    | xargs -0 sha256sum \
    | sha256sum \
    | cut -d' ' -f1
}

input_digest="$(release_inputs_digest)"
rm -rf out views/chat-view/dist

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
const lock = require('./package-lock.json');
if (lock.version !== manifest.version || lock.packages?.['']?.version !== manifest.version) {
    throw new Error('package-lock.json release versions do not match package.json');
}
const releaseNumber = manifest.version.match(/\.([0-9]+)$/)?.[1];
if (!releaseNumber || !manifest.hershelBuildId.endsWith(`-rc${releaseNumber}`)) {
    throw new Error('hershelBuildId rc suffix does not match the release version');
}
const source = require('fs').readFileSync('./src/sync/e2eRunner.ts', 'utf8');
if (!source.includes(`HANDOFF_TARGET_VERSION = '${manifest.version}'`)
    || !source.includes(`HANDOFF_TARGET_BUILD_ID = '${manifest.hershelBuildId}'`)) {
    throw new Error('E2E reload handoff identity does not match package.json');
}
console.log(manifest.version);
console.log(manifest.hershelBuildId);
NODE
)

version="${identity[0]}"
build_id="${identity[1]}"
output="dist/hershel.overleaf-workshop-safe-sync-${version}.vsix"
mkdir -p dist
if [[ "$(release_inputs_digest)" != "$input_digest" ]]; then
  echo 'Release inputs changed while verification was running' >&2
  exit 1
fi
npx --no-install @vscode/vsce package --out "$output"

if [[ "$(release_inputs_digest)" != "$input_digest" ]]; then
  echo 'Release inputs changed while packaging was running' >&2
  exit 1
fi

unpacked="$(mktemp -d)"
trap 'rm -rf "$unpacked"' EXIT
unzip -qq "$output" -d "$unpacked"
extension_root="$unpacked/extension"

node - "$extension_root/package.json" "$version" "$build_id" <<'NODE'
const [manifestPath, version, buildId] = process.argv.slice(2);
const manifest = require(manifestPath);
if (manifest.publisher !== 'hershel'
    || manifest.name !== 'overleaf-workshop-safe-sync'
    || manifest.version !== version
    || manifest.hershelBuildId !== buildId) {
    throw new Error('Packaged extension manifest does not match the verified identity');
}
NODE

compare_tree_without_maps() {
  local local_root="$1"
  local packaged_root="$2"
  local local_list="$unpacked/local-files.txt"
  local packaged_list="$unpacked/packaged-files.txt"
  (cd "$local_root" && find . -type f ! -name '*.map' -print | sort) > "$local_list"
  (cd "$packaged_root" && find . -type f ! -name '*.map' -print | sort) > "$packaged_list"
  diff -u "$local_list" "$packaged_list"
  while IFS= read -r relative; do
    cmp "$local_root/$relative" "$packaged_root/$relative"
  done < "$local_list"
}

compare_tree_without_maps out "$extension_root/out"
compare_tree_without_maps views/chat-view/dist "$extension_root/views/chat-view/dist"

for required in out/extension.js out/sync/e2eRunner.js out/sync/durableFile.js; do
  test -f "$extension_root/$required"
  cmp "$required" "$extension_root/$required"
done

if find "$extension_root" \
  \( -path "$extension_root/src" -o -path "$extension_root/src/*" \
     -o -path "$extension_root/test" -o -path "$extension_root/test/*" \
     -o -path "$extension_root/scripts" -o -path "$extension_root/scripts/*" \
     -o -name '*.ts' -o -name '*.map' \) -print -quit | grep -q .; then
  echo 'Packaged VSIX contains excluded source, tests, scripts, TypeScript, or maps' >&2
  exit 1
fi

archive_hash="$(sha256sum "$output" | cut -d' ' -f1)"
runner_hash="$(sha256sum out/sync/e2eRunner.js | cut -d' ' -f1)"
printf 'release_version=%s\n' "$version"
printf 'build_id=%s\n' "$build_id"
printf 'vsix=%s\n' "$output"
printf 'sha256=%s\n' "$archive_hash"
printf 'runner_sha256=%s\n' "$runner_hash"
