#!/usr/bin/env node

/**
 * Frontend Build Script
 * 
 * This script builds the frontend using Vite with optimized settings
 * without modifying the core vite.config.ts file.
 */

const { execSync } = require('child_process');
const path = require('path');

// Output directory from command line or default
const outDir = process.argv[2] || path.join('dist', 'public');

// Build command with optimized settings
const buildCommand = `npx vite build --outDir ${outDir} --minify esbuild --emptyOutDir`;

console.log(`📦 Building frontend: ${buildCommand}`);
execSync(buildCommand, { stdio: 'inherit' });