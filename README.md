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

### 1. Unified Reconciliation System
- `npx tsx unified_reconciliation.ts status` - Check current reconciliation status
- `npx tsx unified_reconciliation.ts analyze` - Analyze missing calculations and detect issues
- `npx tsx unified_reconciliation.ts reconcile [batchSize]` - Process all missing calculations
- `npx tsx unified_reconciliation.ts date YYYY-MM-DD` - Process a specific date
- `npx tsx unified_reconciliation.ts range YYYY-MM-DD YYYY-MM-DD [batchSize]` - Process a date range
- `npx tsx unified_reconciliation.ts critical YYYY-MM-DD` - Process a problematic date with extra safeguards
- `npx tsx unified_reconciliation.ts spot-fix YYYY-MM-DD PERIOD FARM_ID` - Fix specific records

### 2. Daily Automation
- Automated daily process to reconcile the previous day's curtailment records with Bitcoin calculations
- Uses `daily_reconciliation_check.ts` script which integrates with the unified system

### 3. Advanced Reconciliation
- `npx tsx comprehensive_reconciliation.ts` - High-performance reconciliation system
- `npx tsx comprehensive_reconciliation.ts report` - Generate detailed reconciliation reports

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
- `unified_reconciliation.ts` - Unified system for all reconciliation operations
- `comprehensive_reconciliation.ts` - High-performance reconciliation with reporting capabilities
- `daily_reconciliation_check.ts` - Automated daily reconciliation
- `batch_reconcile.ts` - Batch processing for historical dates
- `simple_reconcile.ts` - Simple tool for quick fixes of specific dates
- `server/services/historicalReconciliation.ts` - Core service for reconciliation logic
- `server/services/bitcoinService.ts` - Bitcoin calculation logic

### Running the Project
The app runs with the "Start application" workflow, which executes `npm run dev`.

### Documentation

#### Comprehensive Guide
- `RECONCILIATION.md` - Complete documentation for the reconciliation system, including user guide, status tracking, enhancement roadmap, and technical details

#### API Documentation
The platform exposes several API endpoints to access curtailment and mining data:

- `/api/curtailment/mining-potential` - Get Bitcoin mining potential
- `/api/curtailment/monthly-mining-potential` - Get monthly mining data
- `/api/summary/daily` - Get daily curtailment summaries
- `/api/summary/monthly` - Get monthly curtailment summaries
- `/api/summary/yearly` - Get yearly curtailment summaries