# Reconciliation Progress Report

## Current Status (Feb 28, 2025)

### Overall Reconciliation
- **Total Progress**: 47.73% overall reconciliation
- **Total Records**: 15,982,427 curtailment records
- **Expected Calculations**: 47,947,281 total Bitcoin calculations required
- **Actual Calculations**: 22,886,379 total Bitcoin calculations completed
- **Missing Calculations**: 25,060,902 calculations still needed

### Reconciliation by Miner Model
- **S19J_PRO**: 8,012,231 calculations (50.13% complete)
- **S9**: 7,462,103 calculations (46.69% complete)
- **M20S**: 7,412,045 calculations (46.37% complete)

### December 2023 Status
- **Completion**: 42.30% (24,050/56,856 calculations)
- **Priority Dates**: 
  - 2023-12-21: 26.42% (482/1,825 calculations)
  - 2023-12-22: 31.17% (569/1,825 calculations)
  - 2023-12-15: 39.78% (726/1,825 calculations)
  - 2023-12-18: 41.26% (753/1,825 calculations)
  - 2023-12-14: 42.85% (782/1,825 calculations)

## Recent Progress

### Tools Consolidation
- Consolidated 15+ overlapping reconciliation scripts into a single, centralized `reconciliation.ts` file
- Improved batch processing capabilities with specific date, period, and farm targeting
- Enhanced error handling and reporting for difficult reconciliation tasks

### Technical Improvements
- Fixed database queries to correctly handle column names across tables
- Implemented fallback to default difficulty (108105433845147) when DynamoDB lookup fails
- Added batch-limited approach (5-10 records per run) to prevent timeouts
- Created comprehensive progress reporting for monitoring reconciliation status

## Next Steps

1. **Focus on December 2023**: Continue targeted reconciliation of December 2023 data, which has been identified as having the largest number of missing calculations.

2. **Batch Processing**: Process smaller batches of records (5-10 at a time) to avoid timeouts and ensure stable progress.

3. **Period-Specific Reconciliation**: Target specific periods with the most missing calculations:
   - Period 7 (heavily used for curtailment)
   - Period 3 (substantial missing data)

4. **Regular Status Checks**: Run daily reconciliation checks to monitor progress and identify any new issues.

## How to Use the Reconciliation Tools

```bash
# Check overall status
npx tsx reconciliation.ts status

# Fix all missing calculations (limited to 30 dates at a time)
npx tsx reconciliation.ts reconcile

# Fix a specific date
npx tsx reconciliation.ts date 2023-12-21

# Fix a specific period on a date
npx tsx reconciliation.ts period 2023-12-21 7

# Fix a specific combination
npx tsx reconciliation.ts combo 2023-12-21 7 E_BABAW-1 M20S

# Process a batch of combinations for a date (default 10)
npx tsx reconciliation.ts batch 2023-12-21 5

# Focus on December 2023 data
npx tsx reconciliation.ts december

# Fix a date range
npx tsx reconciliation.ts range 2023-12-01 2023-12-31
```