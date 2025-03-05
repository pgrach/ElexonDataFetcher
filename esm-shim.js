// ESM compatibility shim for esbuild
// This provides compatibility for modules that expect CommonJS in an ESM environment

import { createRequire } from 'module';
import * as url from 'url';

// Create a require function that can be used in ESM
globalThis.require = createRequire(import.meta.url);

// Polyfill __dirname and __filename which aren't available in ESM
globalThis.__filename = url.fileURLToPath(import.meta.url);
globalThis.__dirname = url.fileURLToPath(new URL('.', import.meta.url));

// Export createRequire to be used in bundled code
export { createRequire };
