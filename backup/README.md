# Consolidated Backup Directory

This directory contains files that have been moved from the active codebase as part of decluttering and standardization efforts. These files are preserved for historical reference but are no longer actively used in the main application.

## Directory Structure

- **scripts/**: Contains development and utility scripts that were previously in the main directory
- **server_scripts/**: Contains server-side scripts that were previously in the server directory
- **deprecated_files/**: Contains flat files, one-time utilities, and completely removed components

## Contents

### Deprecated Services

- **miningPotentialService.ts**: Superseded by the optimized mining service (`server/services/optimizedMiningService.ts`).
- **miningPotentialRoutes.ts**: Legacy routes that used materialized views, replaced by direct query optimization in `server/routes/optimizedMiningRoutes.ts`).

### One-time Utility Scripts

#### Previously Organized Scripts

- **scripts/run_index_optimization.js**: One-time database optimization script.
- **server_scripts/backfillMonthlySummaries.ts**: One-time script for historical data population.
- **server_scripts/processSingleDate.ts**: Single-purpose utility superseded by the unified reconciliation system.
- **server_scripts/reprocessDay.ts**: Functionality integrated into the reconciliation systems.

#### Deprecated Files

- **deprecated_files/auditCurtailmentData.ts**: One-time audit script for curtailment data verification.
- **deprecated_files/auditDecember2022.ts**: Special audit for a specific month (December 2022).
- **deprecated_files/auditDifficulty.ts**: One-time validation script for difficulty data.
- **deprecated_files/auditHistoricalData.ts**: Historical data audit script.
- **deprecated_files/backfillMonthlySummaries.ts**: One-time script for historical data population.
- **deprecated_files/processSingleDate.ts**: Single-purpose utility superseded by the unified reconciliation system.
- **deprecated_files/reprocessDay.ts**: Day reprocessing utility integrated into reconciliation systems.
- **deprecated_files/test-dynamo.ts**: Test script for DynamoDB functionality.
- **deprecated_files/updateLeadPartyNames.ts**: Script with broken dependency.
- **deprecated_files/updateDifficulty.ts**: One-time script to update Bitcoin calculations for a specific date.
- **deprecated_files/run_index_optimization.js**: One-time database optimization script.

### Deprecated Data Files

- **deprecated_files/bmu_mapping.json**: Duplicate BMU mapping file (server/data/bmuMapping.json is the canonical version).
- **deprecated_files/bmuMapping.json.backup**: Backup of BMU mapping file.

## Remaining Active Scripts

The following scripts remain in the main codebase as they are used for regular maintenance tasks:

- **server/scripts/maintenance/updateBmuMapping.ts**: Updates BMU mapping data file from the Elexon API.
- **server/scripts/ingestMonthlyData.ts**: Processes monthly data ingestion for settlement periods.
- **server/scripts/updateHistoricalCalculations.ts**: Updates historical bitcoin calculations.
- **server/scripts/processDifficultyMismatch.ts**: Detects and corrects difficulty mismatches in historical records.

## Rationale for Consolidation

These files were consolidated to:

1. Declutter the main codebase and reduce cognitive load
2. Maintain clear separation between active code and one-time utilities
3. Preserve historical scripts for reference without affecting active development
4. Eliminate confusion from having multiple backup directories with overlapping purposes
5. Simplify onboarding by focusing on essential active components

## Usage

If any of these scripts need to be used again, they should be manually copied back to their respective directories and tested thoroughly before use, as they may require updates to work with the current system.