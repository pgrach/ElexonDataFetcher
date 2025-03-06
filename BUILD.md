# Optimized Build Process

This document outlines the optimized build process for this project.

## Overview

The standard build process uses:
- Vite for the frontend
- esbuild for the backend

Our optimized build process:
1. Runs frontend and backend builds in parallel using concurrently
2. Organizes output into separate directories
3. Preserves compatibility with the existing application
4. Validates build output to ensure everything is working

## How to Use

### Quick Start

To run the optimized build process:

```bash
./optimized-build.sh
```

This script:
- Installs concurrently if needed
- Builds the frontend and backend in parallel
- Organizes the output to match the expected directory structure
- Performs validation checks

### Output Structure

The build outputs to these directories:
- Frontend: `dist/client` (copied to `dist/public` for compatibility)
- Backend: `dist/server`

### Running the Application

After building, start the application with:

```bash
NODE_ENV=production node dist/server/index.js
```

## Advanced Usage

### Building Frontend Only

```bash
node build-frontend.js [output-dir]
```

### Building Backend Only

```bash
node build-backend.js [output-dir]
```

## Performance Considerations

This build process improves performance by:
1. **Parallelization**: Building frontend and backend simultaneously
2. **Minification**: Using esbuild for efficient minification
3. **Optimized output structure**: Placing files in appropriate directories

## Compatibility Notes

This build process maintains compatibility with the existing application by:
- Preserving the expected directory structure
- Ensuring the server can locate static assets
- Maintaining the same output file names and formats