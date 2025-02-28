# Bitcoin Mining Analytics Platform

An advanced Bitcoin mining analytics platform that provides comprehensive insights into mining potential through sophisticated data reconciliation and multi-dimensional performance tracking.

## Technical Architecture

- TypeScript/Node.js backend with robust calculation verification
- PostgreSQL and AWS DynamoDB for advanced data management
- React frontend with real-time data visualization
- Comprehensive reconciliation tools for detecting and resolving mining data discrepancies
- Advanced API integrations for precise cryptocurrency insights

## Reconciliation Status

Currently, the platform shows a 68.46% reconciliation rate (1,035,073 calculations out of 1,511,934 expected) across three miner models:
- S19J_PRO: 345,031 calculations
- S9: 345,021 calculations
- M20S: 345,021 calculations

December 2023 reconciliation is now 100% complete. The current focus is on November 2023.

## Reconciliation Process

### 1. Daily Reconciliation
- Automated daily process to reconcile the previous day's curtailment records with Bitcoin calculations
- Uses `daily_reconciliation_check.ts` script

### 2. Core Reconciliation Tools
- `npx tsx simple_reconcile.ts status` - Check current reconciliation status
- `npx tsx simple_reconcile.ts find` - Find dates with missing calculations
- `npx tsx simple_reconcile.ts date YYYY-MM-DD` - Fix a specific date
- `npx tsx simple_reconcile.ts december` - Fix December 2023 specifically
- `npx tsx simple_reconcile.ts all` - Fix all missing dates (use with caution)

### 3. Advanced Reconciliation Options
- `npx tsx reconciliation.ts` - Comprehensive reconciliation system with additional options
- `npx tsx check_reconciliation_status.ts` - Quick status check tool
- `npx tsx reconciliation_progress_report.ts` - Generate detailed progress reports

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
- `simple_reconcile.ts` - Streamlined reconciliation tool
- `reconciliation.ts` - Comprehensive reconciliation system
- `daily_reconciliation_check.ts` - Automated daily reconciliation
- `reconciliation_progress_report.ts` - Status reporting tool
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