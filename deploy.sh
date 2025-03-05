#!/bin/bash

# Deploy script for production
echo "Starting deployment process..."

# Clean any previous build artifacts
rm -rf dist

# Create necessary directories
mkdir -p dist
mkdir -p dist/server
mkdir -p dist/db

# Copy package.json to dist for proper node_modules resolution
echo "Copying package files..."
cp package.json dist/

# Build the client with Vite
echo "Building client with Vite..."
npx vite build

# Create the expected directory structure
echo "Creating correct directory structure..."
mkdir -p dist/server/public
cp -r dist/public/* dist/server/public/

# Ensure we have the ESM shim
echo "Ensuring ESM compatibility..."

# Bundle the server
echo "Bundling server with esbuild..."
node fix-server-build.js

# Copy necessary JSON files that might be imported
echo "Copying data files..."
find server -name "*.json" -exec cp --parents {} dist/ \;

# Add type: module to package.json in dist for ESM support
echo "Configuring ESM in package.json..."
jq '. + {"type":"module"}' package.json > dist/package.json

echo "Deployment build completed."
echo "You can now start the application in production mode with:"
echo "NODE_ENV=production node dist/index.js"