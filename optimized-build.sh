#!/bin/bash

# Optimized Build Script
# This script implements the parallel build optimization without modifying package.json

echo "🚀 Starting optimized build process..."
start_time=$SECONDS

# Create build directories
mkdir -p dist/client dist/server

# Run both builds in parallel
echo "📦 Building frontend and backend in parallel..."
npm install concurrently --no-save &> /dev/null

# Use concurrently to run both build processes in parallel
npx concurrently \
  -n "frontend,backend" \
  -c "blue,green" \
  "node build-frontend.js dist/client" \
  "node build-backend.js dist/server"

# Check if build was successful
if [ ! -f dist/server/index.js ] || [ ! -f dist/client/index.html ]; then
  echo "❌ Build failed: Output files not found"
  exit 1
fi

# If the regular build process expects files in different locations, copy them there
echo "🔄 Organizing build output..."
mkdir -p dist/public
cp -R dist/client/* dist/public/

# Calculate build duration
duration=$((SECONDS - start_time))
echo "✨ Build completed successfully in ${duration}s"
echo ""
echo "To start the application:"
echo "NODE_ENV=production node dist/server/index.js"
echo ""
echo "Note: Since we can't modify package.json, you'll need to run this script directly:"
echo "./optimized-build.sh"