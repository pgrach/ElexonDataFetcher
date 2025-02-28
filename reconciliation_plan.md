# Bitcoin Reconciliation Plan

## Current Status
- **Reconciliation Rate**: 65.02% (984,547 calculations out of 1,514,223 expected)
- **Complete Calculations**: 
  - S19J_PRO: 328,189 calculations
  - S9: 328,179 calculations
  - M20S: 328,179 calculations

## Identified Issues
1. **Missing December 2023 calculations**: The primary gap in reconciliation
2. **Period-Farm combinations**: Some unique combinations missing calculations for specific miner models
3. **Difficulty data retrieval**: Potential issues with historical difficulty retrieval for some dates

## Reconciliation Approach

### 1. Centralized System
- Consolidated all reconciliation code in `/server/services/historicalReconciliation.ts`
- Created a single entry point through `reconciliation.ts` for all reconciliation tasks
- Simplified SQL queries in `reconciliation.sql` for better maintainability

### 2. Dedicated Tools
- `check_reconciliation_status.ts`: Quick tool to check current reconciliation status
- `reconciliation.ts`: Complete tool with commands for status, finding issues, and fixing
- `comprehensive_reconciliation_plan.md`: Documentation of the overall approach

### 3. Execution Plan for December 2023
1. Identify all missing period-farm combinations for December 2023
2. Process each date sequentially, ensuring proper difficulty data is available
3. Verify results after each date is processed
4. Generate final reconciliation report

### 4. Ongoing Maintenance
- Daily automated reconciliation for new curtailment records
- Monthly verification of complete reconciliation
- Alerting system for any reconciliation failures

## Success Criteria
- 100% reconciliation rate across all dates
- All three miner models (S19J_PRO, S9, M20S) calculated for each unique period-farm combination
- Consistent calculation methodology across all dates