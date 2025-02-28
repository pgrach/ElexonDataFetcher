# Comprehensive Reconciliation Plan

## Current Status
As of our analysis, we have identified the following gaps in Bitcoin calculation data:

| Year | Curtailment Records | Bitcoin Calculations | Expected Calculations | Completion % | Status |
|------|--------------------|--------------------|----------------------|------------|--------|
| 2022 | 205,883 | 103,387 | 617,649 | 16.74% | Incomplete |
| 2023 | 130,699 | 234 | 392,097 | 0.06% | Incomplete |
| 2024 | 278,434 | 678,951 | 835,302 | 81.28% | Incomplete |
| 2025 | 80,677 | 119,913 | 242,031 | 49.54% | Incomplete |

Total missing Bitcoin calculations: **1,184,594**

## Implementation Strategy

### 1. Prioritization
We've prioritized the reconciliation in the following order:
1. **2023 (Highest Priority)**: Only 0.06% complete with 391,863 missing records
2. **2022 (Second Priority)**: 16.74% complete with 514,262 missing records
3. **2025 (Third Priority)**: 49.54% complete with 122,118 missing records
4. **2024 (Final Priority)**: 81.28% complete with 156,351 missing records

### 2. Approach
For each year, we will:
1. Process months in order of priority:
   - Missing months before incomplete months
   - Within each category, process months with higher curtailment counts first
2. For each month, process all dates with curtailment records
3. For each date, process all farm_id and settlement_period combinations
4. For each record, generate Bitcoin calculations for all three miner models:
   - S19J_PRO
   - S9
   - M20S

### 3. Implementation
We have created several SQL scripts for different aspects of the reconciliation:

1. **full_reconciliation_implementation.sql**: Core functions for reconciliation
2. **execute_full_reconciliation.sql**: Master script to run the full process
3. **reconcile_2023_special.sql**: Specialized script for 2023 (highest priority)
4. **reconcile_missing_months.sql**: Script to process specific missing months
5. **reconcile_critical_dates.sql**: Script to handle specific test dates

### 4. Testing
We tested our approach with specific dates:
- `2023-01-15`: Added 180 Bitcoin calculation records (60 curtailment records x 3 models)
- `2023-06-20`: Added 21 Bitcoin calculation records (7 curtailment records x 3 models)
- `2025-02-28`: Added 12 Bitcoin calculation records (4 curtailment records x 3 models)

### 5. Execution Plan

#### Phase 1: 2023 Data Reconciliation
- Create monthly batches prioritizing October, September, and July (highest curtailment volumes)
- Process in batches with appropriate difficulty value: 37,935,772,752,142
- Expected to process ~130,699 curtailment records, generating 392,097 Bitcoin calculations

#### Phase 2: 2022 Data Reconciliation
- Create monthly batches, again prioritizing by missing status and curtailment volume
- Process with difficulty value: 25,000,000,000,000
- Expected to process ~205,883 curtailment records, generating 617,649 Bitcoin calculations

#### Phase 3: 2025 Data Reconciliation
- Process with difficulty value: 108,105,433,845,147
- Expected to process ~80,677 curtailment records, generating 242,031 Bitcoin calculations

#### Phase 4: 2024 Data Reconciliation
- Process with difficulty value: 68,980,189,436,404
- Expected to process ~278,434 curtailment records, generating 835,302 Bitcoin calculations

### 6. Monitoring & Verification
- Each phase includes verification steps to confirm successful completion
- A final verification will validate 100% reconciliation has been achieved
- Progress tracking tables record all operations for audit and reporting

## Performance Considerations
- Added DB indexes to optimize reconciliation queries
- Implemented batch processing to avoid DB performance issues
- Added transaction management to ensure data consistency
- Progress tracking to enable restart in case of interruption

## Risk Mitigation
- Transaction isolation to prevent data corruption
- Error handling to capture and log issues
- Record-level validation to ensure data quality
- ON CONFLICT clauses to handle potential duplicate scenarios