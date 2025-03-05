#!/usr/bin/env node

/**
 * Server Build Fix Script
 * 
 * This script focuses solely on correctly bundling the server code
 * for production deployment.
 */

import { build } from 'esbuild';
import { existsSync, mkdirSync, writeFileSync } from 'fs';

console.log('Starting server build fix process...');

// Make sure the dist directory exists
if (!existsSync('./dist')) {
  mkdirSync('./dist');
}

if (!existsSync('./dist/server')) {
  mkdirSync('./dist/server');
}

// Create a redirection index.js file for compatibility
const indexRedirect = `
// This file redirects to the compiled ESM output
import './server/index.js';
`;

writeFileSync('./dist/index.js', indexRedirect);

// Main server file compilation
async function bundleServer() {
  try {
    console.log('Building server files...');
    
    // Build the main server index.ts
    await build({
      entryPoints: ['./server/index.ts'],
      outdir: './dist/server',
      bundle: true,
      platform: 'node',
      format: 'esm',
      packages: 'external',
      sourcemap: true,
      minify: false,
      external: [
        '@aws-sdk/*',
        'pg',
        'drizzle-orm',
        'express',
        'zod',
        '@db/*'
      ],
      inject: ['./esm-shim.js'],
      banner: {
        js: `
          // ESM interop for imported modules
          import { createRequire } from 'module';
          const require = createRequire(import.meta.url);
        `,
      },
    });
    
    console.log('Server build completed successfully');
  } catch (error) {
    console.error('Build failed:', error);
    process.exit(1);
  }
}

bundleServer();