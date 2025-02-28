# Comprehensive Bitcoin Calculation Reconciliation Plan

## Current Status
As of February 28, 2025, after initial reconciliation runs, we have the following completion percentages:
- 2024: 81.28% complete (156,351 missing records)
- 2025: 49.54% complete (122,118 missing records)
- 2022: 16.77% complete (514,097 missing records)
- 2023: 19.61% complete (315,210 missing records)

### Recent Progress
- Successfully processed 10 critical dates from 2023 with highest missing calculations
- Added approximately 75,645 Bitcoin calculations total
- Increased 2023 completion percentage from 0.32% to 19.61%

## Goal
Achieve 100% reconciliation between curtailment_records and historical_bitcoin_calculations tables, ensuring every curtailment record has corresponding calculations for all three miner models (S19J_PRO, M20S, S9).

## Reconciliation Approach

### 1. Batch Processing by Priority
Process dates in batches of 5-10, prioritized by the highest number of missing calculations. This approach avoids timeouts while making steady progress on the most critical dates.

### 2. Year-Specific Strategies
- **2023**: Focus on this year first due to lowest completion percentage
  - Process top 20 dates with most missing records
  - Run monthly batch jobs for remaining dates
  
- **2022**: Second priority
  - Process in monthly batches starting with most recent months
  
- **2025**: Third priority
  - Focus on completing February data first
  - Implement daily reconciliation process for real-time data
  
- **2024**: Final priority (highest completion rate)
  - Process remaining gaps in monthly batches

### 3. Verification Process
After each batch processing:
1. Verify the completion percentage has increased
2. Check for any errors or anomalies in the newly created records
3. Update status metrics to track progress

## Implementation Plan

### Phase 1: Critical Date Processing (2023) - [50% COMPLETE]
1. ✅ Identify and process top 10 dates from 2023 with highest missing calculations
2. ✅ Create optimized batch jobs to handle 5 dates per run
3. ✅ Achieved improvement: Increased 2023 completion from 0.32% to 19.61%
4. 🔄 Process next 10 dates (targeting 30%+ completion)

### Phase 2: Monthly Batch Processing (2023-2022)
1. Create month-based batch jobs to process remaining 2023 data
2. Begin processing 2022 data in monthly batches
3. Expected improvement: Increase 2023 completion to 60%, 2022 to 40%

### Phase 3: Recent Data Reconciliation (2025)
1. Complete reconciliation of all 2025 data
2. Implement automated daily reconciliation for new data
3. Expected improvement: Increase 2025 completion to 100%

### Phase 4: Gap Filling (2024)
1. Identify and process all remaining gaps in 2024 data
2. Expected improvement: Increase 2024 completion to 95%+

### Phase 5: Final Reconciliation
1. Comprehensive verification of all historical records
2. Final targeted processing of any remaining gaps
3. Expected outcome: All years at 100% completion

## Script Types and Functions

1. **Priority Date Processing**
   - Uses SQL function to process specific dates
   - Each date handled individually to ensure accurate calculations

2. **Monthly Batch Processing**
   - Identifies and processes all dates in a given month
   - Uses temporary tables to optimize calculation performance

3. **Verification Scripts**
   - Generates reports on completion percentages
   - Identifies remaining gaps for targeted processing

4. **Automated Reconciliation**
   - Daily jobs for real-time data reconciliation
   - Monitoring and alerting for calculation failures

## Success Metrics
- Increase in completion percentage after each batch
- Reduction in total missing records across all years
- Consistent 3:1 ratio between Bitcoin calculations and curtailment records
- Complete validation of calculation parameters (difficulty, pricing) for each date