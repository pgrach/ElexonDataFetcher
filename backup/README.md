# Backup Directory

This directory contains files that have been moved from the active codebase as part of a decluttering effort. These files are preserved for historical reference but are no longer actively used in the main application.

## Contents

### Deprecated Services

- **miningPotentialService.ts**: Superseded by the optimized mining service (`server/services/optimizedMiningService.ts`).
- **miningPotentialRoutes.ts**: Legacy routes that used materialized views, replaced by direct query optimization in `server/routes/optimizedMiningRoutes.ts`.

### One-time Utility Scripts

- **scripts/run_index_optimization.js**: One-time database optimization script.
- **server_scripts/backfillMonthlySummaries.ts**: One-time script for historical data population.
- **server_scripts/processSingleDate.ts**: Single-purpose utility superseded by the unified reconciliation system.
- **server_scripts/reprocessDay.ts**: Functionality integrated into the reconciliation systems.
- **server_scripts/auditCurtailmentData.ts**: One-time audit script for data verification.
- **server_scripts/auditDecember2022.ts**: Special audit for a specific month.
- **server_scripts/auditDifficulty.ts**: One-time validation script for difficulty data.
- **server_scripts/auditHistoricalData.ts**: Historical data audit script.
- **server_scripts/test-dynamo.ts**: Test script for DynamoDB functionality.
- **updateLeadPartyNames.ts**: Script with broken dependency - attempts to import a non-existent function from curtailment service.
- **updateDifficulty.ts**: One-time script to update Bitcoin calculations for a specific date (2025-02-10) with corrected difficulty.
- **reprocessMonthlySummaries.ts**: One-time utility for bulk recalculation of monthly summaries for a specific date range.

## Remaining Active Scripts

The following scripts remain in the main codebase as they are used for regular maintenance tasks:

- **updateBmuMapping.ts**: Updates BMU mapping data file from the Elexon API. This is a maintenance script that may need to be run periodically.
- **ingestMonthlyData.ts**: Processes monthly data ingestion for settlement periods.
- **updateHistoricalCalculations.ts**: Updates historical bitcoin calculations with proper batching and validation.
- **processDifficultyMismatch.ts**: Detects and corrects difficulty mismatches in historical records.

## Rationale

These files were moved to:

1. Declutter the main codebase and reduce cognitive load
2. Maintain clear separation between active code and one-time utilities
3. Preserve historical scripts for reference without affecting active development
4. Simplify onboarding by focusing on essential active components

## Usage

If any of these scripts need to be used again, they should be manually copied back to their respective directories and tested thoroughly before use, as they may require updates to work with the current system.