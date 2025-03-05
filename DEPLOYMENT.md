# Deployment Guide

This document provides step-by-step instructions for deploying the Bitcoin Mining & Curtailment Analytics application to production environments.

## Prerequisites

- Node.js v20 or later
- PostgreSQL database
- AWS DynamoDB access configured
- Git
- jq (for build script processing)

## Deployment Process

### 1. Prepare Environment

Ensure all required environment variables are set:

```bash
# Database
export DATABASE_URL=postgresql://username:password@hostname:5432/database

# AWS DynamoDB
export AWS_REGION=eu-north-1
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Application settings
export NODE_ENV=production
export PORT=3000
```

### 2. Clone & Setup Repository

```bash
# Clone repository
git clone https://github.com/your-org/your-repo.git
cd your-repo

# Install dependencies
npm install
```

### 3. Build the Application

The most reliable way to build the application is using our automated build script:

```bash
# Make the scripts executable
chmod +x fix-build.sh
chmod +x deploy.sh

# Run the optimized build process
./fix-build.sh
```

This script:
- Updates TypeScript configuration for proper ESM module handling
- Creates necessary compatibility shims
- Builds both client and server components
- Prepares the deployment package

### 4. Manual Build (Alternative)

If you need more control over the build process, you can follow these steps:

```bash
# 1. Update tsconfig.json for proper ESM module configuration
cat > tsconfig.json << 'EOL'
{
  "include": ["client/src/**/*", "db/**/*", "server/**/*"],
  "exclude": ["node_modules", "build", "dist", "**/*.test.ts"],
  "compilerOptions": {
    "incremental": true,
    "tsBuildInfoFile": "./node_modules/typescript/tsbuildinfo",
    "noEmit": false,
    "target": "es2020",
    "module": "NodeNext",
    "strict": true,
    "downlevelIteration": true,
    "lib": ["esnext", "dom", "dom.iterable"],
    "jsx": "react-jsx",
    "esModuleInterop": true,
    "skipLibCheck": true,
    "moduleResolution": "NodeNext",
    "outDir": "./dist",
    "baseUrl": ".",
    "types": ["node", "vite/client"],
    "paths": {
      "@db": ["./db/index.ts"],
      "@db/*": ["./db/*"],
      "@/*": ["./client/src/*"]
    },
    "allowJs": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "allowSyntheticDefaultImports": true,
    "verbatimModuleSyntax": false
  }
}
EOL

# 2. Build client
npm run build

# 3. Build server with ESM module fixes
node fix-server-build.js

# 4. Copy package.json to dist folder and add ESM module type
cp package.json dist/
sed -i 's/"type": "commonjs"/"type": "module"/g' dist/package.json
```

### 5. Start the Application

Once built, the application can be started in production mode:

```bash
# Navigate to the dist directory
cd dist

# Start the application
NODE_ENV=production node index.js
```

Or using a process manager like PM2:

```bash
pm2 start index.js --name bitcoin-analytics
```

## Troubleshooting

### ESM Module Issues

If you encounter ESM module resolution issues:

1. Ensure your `tsconfig.json` has the correct settings:
   ```json
   {
     "compilerOptions": {
       "module": "NodeNext",
       "moduleResolution": "NodeNext"
     }
   }
   ```

2. Make sure `package.json` in the dist directory includes:
   ```json
   {
     "type": "module"
   }
   ```

3. Run the `fix-server-build.js` script to rebuild with proper ESM handling.

### File Path Resolution

If you encounter path resolution issues:

1. Check that file extensions are included in import statements
2. Verify the relative paths are correct for the ESM environment
3. Use the global `__dirname` and `__filename` polyfills from esm-shim.js

### Database Connection Issues

1. Ensure the DATABASE_URL environment variable is correctly set
2. Check PostgreSQL server is running and accessible
3. Verify database user has proper permissions

### AWS DynamoDB Issues

1. Verify AWS credentials are correctly configured
2. Ensure the DynamoDB table name matches the expected value
3. Check AWS region settings to match table region

## Deployment Checklist

Before going live:

- [ ] Run data reconciliation: `NODE_ENV=production node dist/unified_reconciliation.js status`
- [ ] Verify Bitcoin difficulty data: `NODE_ENV=production node dist/server/scripts/test-dynamo.js`
- [ ] Check database indexes: `NODE_ENV=production node dist/run_index_optimization.js`
- [ ] Ensure all daily calculations are up-to-date: `NODE_ENV=production node dist/daily_reconciliation_check.js`
- [ ] Test frontend/backend connectivity in production mode