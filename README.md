# Bitcoin Mining Analytics Platform

An advanced Bitcoin mining analytics platform that provides comprehensive insights into mining potential through sophisticated data reconciliation and multi-dimensional performance tracking.

## Technical Architecture

- TypeScript/Node.js backend with robust calculation verification
- PostgreSQL and AWS DynamoDB for advanced data management
- React frontend with real-time data visualization
- Comprehensive reconciliation tools for detecting and resolving mining data discrepancies
- Advanced API integrations for precise cryptocurrency insights

## Reconciliation Status

Currently, the platform shows a 65.02% reconciliation rate (984,547 calculations out of 1,514,223 expected) across three miner models:
- S19J_PRO: 328,189 calculations
- S9: 328,179 calculations
- M20S: 328,179 calculations

The primary gaps in reconciliation are in December 2023.

## Reconciliation Process

### 1. Daily Reconciliation
- Automated daily process to reconcile the previous day's curtailment records with Bitcoin calculations
- Uses `historicalReconciliation.reconcileDay()` function

### 2. Monthly Reconciliation
- Scheduled process on the 1st of each month
- Uses `historicalReconciliation.reconcilePreviousMonth()` function

### 3. Manual Reconciliation Tools
- `npx tsx reconciliation.ts status` - Check current reconciliation status
- `npx tsx reconciliation.ts find` - Find dates with missing calculations
- `npx tsx reconciliation.ts reconcile` - Fix all missing calculations
- `npx tsx reconciliation.ts date YYYY-MM-DD` - Fix a specific date

### 4. Special Tools
- `npx tsx check_reconciliation_status.ts` - Quick status check tool
- `npx tsx run_reconciliation.ts` - Focused tool for December 2023 reconciliation
- `npx tsx test_reconcile_date.ts` - Test tool for a specific date

## Data Model

The reconciliation process focuses on these key entities:

1. **Curtailment Records**
   - Contains curtailment data from Elexon API
   - Each record represents a unique period-farm combination

2. **Bitcoin Calculations**
   - Contains mining calculations for each curtailment record
   - Each unique period-farm combination should have calculations for all miner models

## Development

### Core Files
- `reconciliation.ts` - Main reconciliation tool
- `reconciliation.sql` - Consolidated SQL queries
- `server/services/historicalReconciliation.ts` - Core service for reconciliation logic
- `server/services/bitcoinService.ts` - Bitcoin calculation logic

### Running the Project
The app runs with the "Start application" workflow, which executes `npm run dev`.

### API Documentation
The platform exposes several API endpoints to access curtailment and mining data:

- `/api/curtailment/mining-potential` - Get Bitcoin mining potential
- `/api/curtailment/monthly-mining-potential` - Get monthly mining data
- `/api/summary/daily` - Get daily curtailment summaries
- `/api/summary/monthly` - Get monthly curtailment summaries
- `/api/summary/yearly` - Get yearly curtailment summaries