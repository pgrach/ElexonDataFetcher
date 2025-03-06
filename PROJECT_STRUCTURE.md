# Bitcoin Mining Analytics Platform - Project Structure

This document outlines the structure of the Bitcoin Mining Analytics platform to help developers understand how the codebase is organized.

## Core Directories

- `client/` - React frontend application
  - `src/` - Source code for the frontend
    - `components/` - Reusable React components
    - `pages/` - Page components
    - `hooks/` - Custom React hooks
    - `lib/` - Utility libraries and functions

- `server/` - Node.js/Express backend
  - `controllers/` - Controller functions for API endpoints
  - `middleware/` - Express middleware
  - `routes/` - API route definitions
  - `services/` - Business logic and service modules
  - `types/` - TypeScript type definitions
  - `utils/` - Utility functions

- `db/` - Database related code
  - `schema.ts` - Drizzle ORM schema definitions

- `scripts/` - Utility scripts for various tasks
  - `reconciliation/` - Data reconciliation scripts
  - `data-processing/` - Data processing scripts
  - `migrations/` - Database migration scripts
  - `utilities/` - General utility scripts

- `config/` - Configuration files
  - `drizzle.config.ts` - Drizzle ORM configuration
  - `tailwind.config.ts` - Tailwind CSS configuration
  - `postcss.config.js` - PostCSS configuration
  - `tsconfig.json` - TypeScript configuration
  - `theme.json` - UI theme configuration

- `data/` - Data files and checkpoints
  - `checkpoints/` - Reconciliation checkpoint files

## Key Files

- `server/index.ts` - Main entry point for the Express server
- `server/routes.ts` - Registration of all API routes
- `client/src/App.tsx` - Main React component and routing
- `client/src/main.tsx` - React application entry point

## Configuration Files

- `vite.config.ts` - Vite bundler configuration
- `.replit` - Replit configuration
- `replit.nix` - Replit Nix environment

## Legacy/Deprecated Files

Some files are maintained for backward compatibility but are considered deprecated:

- Files in the `backup/` directory
- Materialized view tables defined in `db/schema.ts` (now replaced with direct query optimizations)

## Running the Application

The main application is run using the "Start application" workflow, which executes `npm run dev` to start both the frontend and backend servers.

## Utility Scripts

See the `scripts/README.md` file for details on how to run various utility scripts for data processing, reconciliation, and other tasks.