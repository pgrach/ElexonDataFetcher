#!/usr/bin/env node

/**
 * Optimized Build Script
 * 
 * This script implements the optimized build process suggested:
 * 1. Parallelize frontend and backend builds
 * 2. Add build validation
 * 
 * Usage:
 *   node optimized-build.js
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

console.log('🚀 Starting optimized build process...');

// Create a temporary directory for build output organization
const tempClientDir = path.join(__dirname, 'dist', '_client_temp');
if (!fs.existsSync(path.dirname(tempClientDir))) {
  fs.mkdirSync(path.dirname(tempClientDir), { recursive: true });
}
if (!fs.existsSync(tempClientDir)) {
  fs.mkdirSync(tempClientDir);
}

// Function to run a command and return stdout
function run(command, options = {}) {
  console.log(`> ${command}`);
  return execSync(command, { 
    stdio: 'inherit', 
    encoding: 'utf-8',
    ...options 
  });
}

// Start time measurement
const startTime = Date.now();

// Run both builds in parallel
try {
  console.log('📦 Building frontend and backend in parallel...');
  
  // Build both in parallel using multiple processes
  run('npx concurrently -n "frontend,backend" -c "blue,green" "npm run build:frontend" "npm run build:backend"', {
    env: {
      ...process.env,
      PATH_frontend: `npx vite build --outDir ${tempClientDir}`,
      PATH_backend: 'npx esbuild server/index.ts --platform=node --packages=external --bundle --format=esm --minify --outdir=dist'
    }
  });
  
  // Move client files to the correct location
  console.log('🔄 Organizing build output...');
  if (fs.existsSync(tempClientDir) && fs.existsSync(path.join(tempClientDir, 'index.html'))) {
    // Create public directory if it doesn't exist
    const publicDir = path.join(__dirname, 'dist', 'public');
    if (!fs.existsSync(publicDir)) {
      fs.mkdirSync(publicDir, { recursive: true });
    }
    
    // Copy files from temp directory to public
    run(`cp -R ${tempClientDir}/* dist/public/`);
    
    // Clean up temp directory
    run(`rm -rf ${tempClientDir}`);
  }
  
  // Validate build
  console.log('✅ Validating build output...');
  const hasIndexJs = fs.existsSync(path.join(__dirname, 'dist', 'index.js'));
  const hasIndexHtml = fs.existsSync(path.join(__dirname, 'dist', 'public', 'index.html'));
  
  if (!hasIndexJs) {
    throw new Error('Backend build failed: dist/index.js not found');
  }
  if (!hasIndexHtml) {
    throw new Error('Frontend build failed: dist/public/index.html not found');
  }
  
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`✨ Build completed successfully in ${duration}s`);
  console.log('Run `NODE_ENV=production node dist/index.js` to start the application');
  
} catch (error) {
  console.error('\n❌ Build failed:', error.message);
  process.exit(1);
}