#!/usr/bin/env node

/**
 * Backend Build Script
 * 
 * This script builds the backend using esbuild with optimized settings.
 */

const { execSync } = require('child_process');
const path = require('path');

// Output directory from command line or default
const outDir = process.argv[2] || 'dist';

// Build command with optimized settings
const buildCommand = `npx esbuild server/index.ts --platform=node --packages=external --bundle --format=esm --minify --outdir=${outDir}`;

console.log(`📦 Building backend: ${buildCommand}`);
execSync(buildCommand, { stdio: 'inherit' });