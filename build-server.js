#!/usr/bin/env node

/**
 * Server Build Script
 * 
 * This script builds all server-side TypeScript files needed for production
 * using esbuild, ensuring that we correctly compile all necessary server files.
 */

import { build } from 'esbuild';
import { readdirSync, existsSync, mkdirSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import process from 'process';

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
async function buildServer() {
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
      minify: process.env.NODE_ENV === 'production',
      external: [
        '@aws-sdk/*',
        'pg',
        'drizzle-orm',
        'express',
        'zod',
        '@db/*'
      ],
      alias: {
        '@db': './db/index.js',
        '@db/schema': './db/schema.js'
      },
      inject: ['./esm-shim.js'],
      banner: {
        js: `
          // ESM interop for imported modules
          import { createRequire } from 'module';
          const require = createRequire(import.meta.url);
        `,
      },
    });
    
    // Additional utility scripts that might be needed in production
    const utilityScripts = [
      './unified_reconciliation.ts',
      './daily_reconciliation_check.ts',
      './run_migration.ts',
      './run_index_optimization.js'
    ];
    
    for (const script of utilityScripts) {
      if (existsSync(script)) {
        console.log(`Building utility script: ${script}`);
        
        // Get the output directory
        const outputDir = join('./dist', dirname(script));
        
        // Ensure the output directory exists
        if (!existsSync(outputDir)) {
          mkdirSync(outputDir, { recursive: true });
        }
        
        await build({
          entryPoints: [script],
          outdir: outputDir,
          bundle: true,
          platform: 'node',
          format: 'esm',
          packages: 'external',
          sourcemap: true,
          minify: process.env.NODE_ENV === 'production',
          external: [
            '@aws-sdk/*',
            'pg',
            'drizzle-orm',
            'express',
            'zod',
            '@db/*'
          ],
          alias: {
            '@db': './db/index.js',
            '@db/schema': './db/schema.js'
          },
          inject: ['./esm-shim.js'],
          banner: {
            js: `
              // ESM interop for imported modules
              import { createRequire } from 'module';
              const require = createRequire(import.meta.url);
            `,
          },
        });
      }
    }
    
    console.log('Server build completed successfully');
  } catch (error) {
    console.error('Build failed:', error);
    process.exit(1);
  }
}

buildServer();