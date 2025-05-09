# Backup Removed Files

This directory contains files that have been removed from the main codebase but preserved for reference. These files were part of the original system but are no longer actively used.

## One-time Scripts and Utilities

- `auditCurtailmentData.ts` - One-time audit script for curtailment data
- `auditDecember2022.ts` - Special audit for December 2022 data
- `auditDifficulty.ts` - One-time validation script for Bitcoin mining difficulty data
- `auditHistoricalData.ts` - Historical data audit script
- `test-dynamo.ts` - Test script for DynamoDB connectivity
- `updateLeadPartyNames.ts` - Script to update lead party names (had broken dependencies)
- `updateDifficulty.ts` - One-time script to update Bitcoin difficulty values
- `processSingleDate.ts` - Single-date processing utility (superseded by unified reconciliation)
- `reprocessDay.ts` - Day reprocessing utility (functionality integrated into reconciliation)
- `reprocessMonthlySummaries.ts` - One-time utility for reprocessing monthly summaries
- `bmu_mapping.json` - Duplicate BMU mapping file (server/data/bmuMapping.json is the canonical version)
- `bmuMapping.json.backup` - Backup of BMU mapping file

## Rationale for Removal

These files were removed from the active codebase for the following reasons:

1. One-time utilities that served a specific purpose and are no longer needed
2. Scripts that have been superseded by more comprehensive implementations
3. Audit scripts used for specific historical data validation
4. Test and development utilities
5. Duplicate data files that are now standardized

## Note

If any of these scripts need to be reactivated for specific maintenance or audit purposes, they can be moved back to the appropriate directories. However, they may require updates to match current schemas and code patterns.