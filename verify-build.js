#!/usr/bin/env node

/**
 * Build Verification Script
 * 
 * This script performs basic checks on the build output to ensure it's valid.
 */

const fs = require('fs');
const path = require('path');

console.log('🔍 Verifying build output...');

// Check backend build
const backendPath = path.join(__dirname, 'dist', 'server', 'index.js');
const backendExists = fs.existsSync(backendPath);
console.log(`Backend build: ${backendExists ? '✅ Found' : '❌ Missing'}`);

// Check frontend build
const frontendPath = path.join(__dirname, 'dist', 'client', 'index.html');
const frontendExists = fs.existsSync(frontendPath);
console.log(`Frontend build: ${frontendExists ? '✅ Found' : '❌ Missing'}`);

// Check compatibility directory
const compatPath = path.join(__dirname, 'dist', 'public', 'index.html');
const compatExists = fs.existsSync(compatPath);
console.log(`Compatibility directory: ${compatExists ? '✅ Found' : '❌ Missing'}`);

// Check if assets were built
const assetsPath = path.join(__dirname, 'dist', 'client', 'assets');
const assetsExist = fs.existsSync(assetsPath) && fs.readdirSync(assetsPath).length > 0;
console.log(`Frontend assets: ${assetsExist ? '✅ Found' : '❌ Missing'}`);

// Provide summary
if (backendExists && frontendExists && compatExists && assetsExist) {
  console.log('\n✨ Build verification passed! The build is ready for deployment.');
  console.log('\nTo start the application:');
  console.log('NODE_ENV=production node dist/server/index.js');
} else {
  console.log('\n❌ Build verification failed. Please check the errors above.');
  process.exit(1);
}