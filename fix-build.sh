#!/bin/bash

# Check, fix, and rebuild the application with proper TypeScript configuration and ESM support
echo "== Starting build fix process =="

# 1. Backup the original tsconfig.json
echo "Creating backup of tsconfig.json"
cp tsconfig.json tsconfig.json.bak

# 2. Create a fixed TypeScript configuration
echo "Creating fixed TypeScript configuration"
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

# 3. Ensure ESM shim exists
echo "Creating ESM compatibility shim"
cat > esm-shim.js << 'EOL'
// ESM compatibility shim for esbuild
// This provides compatibility for modules that expect CommonJS in an ESM environment

import { createRequire } from 'module';
import * as url from 'url';

// Create a require function that can be used in ESM
globalThis.require = createRequire(import.meta.url);

// Polyfill __dirname and __filename which aren't available in ESM
globalThis.__filename = url.fileURLToPath(import.meta.url);
globalThis.__dirname = url.fileURLToPath(new URL('.', import.meta.url));

// Export createRequire to be used in bundled code
export { createRequire };
EOL

# 4. Run the deployment script
echo "Running deployment script"
./deploy.sh

# 5. Verify the build
if [ -f "./dist/index.js" ] && [ -f "./dist/server/index.js" ]; then
  echo "Build completed successfully!"
  echo "The application can now be started with: NODE_ENV=production node dist/index.js"
else
  echo "Build failed! Check for errors above."
  exit 1
fi

echo "== Build fix process completed =="