/* Platform-aware native module loader for @sqd-pipes/delta-db */

let nativeBinding = null
let loadError = null

// 1. Try platform-specific native package (Node.js only)
if (typeof process !== 'undefined' && process.platform && process.arch) {
  const platformPackages = {
    'darwin-arm64': '@sqd-pipes/delta-db-darwin-arm64',
    'darwin-x64': '@sqd-pipes/delta-db-darwin-x64',
    'linux-x64': '@sqd-pipes/delta-db-linux-x64-gnu',
    'linux-arm64': '@sqd-pipes/delta-db-linux-arm64-gnu',
  }

  const key = `${process.platform}-${process.arch}`
  const pkg = platformPackages[key]

  if (pkg) {
    try {
      nativeBinding = require(pkg)
    } catch (e) {
      loadError = e
    }
  }

  // Fallback: try local .node file (dev build)
  if (!nativeBinding) {
    const { existsSync } = require('node:fs')
    const { join } = require('node:path')

    const suffixes = {
      'darwin-x64': 'darwin-x64',
      'darwin-arm64': 'darwin-arm64',
      'linux-x64': 'linux-x64-gnu',
      'linux-arm64': 'linux-arm64-gnu',
    }
    const suffix = suffixes[key]
    if (suffix) {
      const platformPath = join(__dirname, `delta-db.${suffix}.node`)
      if (existsSync(platformPath)) {
        try {
          nativeBinding = require(platformPath)
          loadError = null
        } catch (e) {
          loadError = e
        }
      }
    }

    // Fallback: unqualified .node file
    if (!nativeBinding) {
      const localFile = join(__dirname, 'delta-db.node')
      if (existsSync(localFile)) {
        try {
          nativeBinding = require(localFile)
          loadError = null
        } catch (e) {
          loadError = e
        }
      }
    }
  }
}

// 2. TODO: Try wasm fallback for browser/edge
// if (!nativeBinding) {
//   try {
//     nativeBinding = require('@sqd-pipes/delta-db-wasm')
//   } catch (e) {}
// }

if (!nativeBinding) {
  const platform = typeof process !== 'undefined' ? `${process.platform}-${process.arch}` : 'browser'
  const help = [
    `Failed to load delta-db binding for ${platform}.`,
    '',
    loadError ? `Error: ${loadError.message}` : '',
    '',
    'Install the platform package: npm install @sqd-pipes/delta-db-<platform>',
    'Or build from source: npm run build',
  ].join('\n')
  throw new Error(help)
}

module.exports = nativeBinding
