# Comprehensive Bitcoin Calculation Reconciliation Plan

## Current Status (as of February 28, 2025)

| Year | Curtailment Records | Bitcoin Calculations | Expected Calculations | Completion % |
|------|---------------------|----------------------|-----------------------|--------------|
| 2022 | 205,883             | 103,387              | 617,649               | 16.74%       |
| 2023 | 130,699             | 204                  | 392,097               | 0.05%        |
| 2024 | 278,434             | 678,951              | 835,302               | 81.28%       |
| 2025 | 80,677              | 119,913              | 242,031               | 49.54%       |
| **Total** | **695,693**    | **902,455**          | **2,087,079**         | **43.24%**   |

We need to add **1,184,624 missing Bitcoin calculation records** to achieve 100% reconciliation.

## Reconciliation Strategy

### 1. Create Year-Specific Reconciliation Scripts

We'll create four specialized scripts to process each year separately, allowing for year-specific optimizations:

1. `reconcile_2023.sql` - Highest priority (nearly all records missing)
2. `reconcile_2022.sql` - Second priority (large volume of missing records)
3. `reconcile_2025.sql` - Third priority (half complete)
4. `reconcile_2024.sql` - Final priority (mostly complete)

### 2. Optimized Processing Methodology

For each reconciliation script:

- Process in monthly batches for better tracking and error handling
- Use explicit difficulty values appropriate for each year
- Implement transaction blocks to ensure consistency
- Add verbose logging to track progress
- Utilize parallel processing where possible for efficiency

Example structure for 2023 (highest priority):

```sql
-- Optimized reconciliation for 2023
BEGIN;

-- Use appropriate difficulty value for 2023
SET LOCAL reconciliation.difficulty_2023 = 37935772752142;

-- Process each month in order
SELECT reconcile_month_2023('2023-01');
SELECT reconcile_month_2023('2023-02');
-- Continue for all months...

COMMIT;
```

### 3. Verification and Validation

After each batch processing:

1. **Validate count completeness**: Ensure the expected number of records exist
2. **Validate data integrity**: Check for correct calculations
3. **Generate reconciliation reports**: Document progress and identify any remaining issues

Example verification query:

```sql
-- Verification for specific month
WITH month_data AS (
  SELECT
    (SELECT COUNT(*) FROM curtailment_records 
     WHERE to_char(settlement_date, 'YYYY-MM') = '2023-01') AS curtailment_count,
    (SELECT COUNT(*) FROM historical_bitcoin_calculations 
     WHERE to_char(settlement_date, 'YYYY-MM') = '2023-01' AND miner_model = 'S19J_PRO') AS s19_count,
    (SELECT COUNT(*) FROM historical_bitcoin_calculations 
     WHERE to_char(settlement_date, 'YYYY-MM') = '2023-01' AND miner_model = 'S9') AS s9_count,
    (SELECT COUNT(*) FROM historical_bitcoin_calculations 
     WHERE to_char(settlement_date, 'YYYY-MM') = '2023-01' AND miner_model = 'M20S') AS m20s_count
)
SELECT
  curtailment_count,
  s19_count,
  s9_count,
  m20s_count,
  CASE
    WHEN s19_count = curtailment_count AND 
         s9_count = curtailment_count AND 
         m20s_count = curtailment_count THEN 'COMPLETE'
    ELSE 'INCOMPLETE'
  END AS status,
  ROUND((s19_count + s9_count + m20s_count) * 100.0 / (curtailment_count * 3), 2) AS completion_percentage
FROM month_data;
```

### 4. Implementation Timeline

| Phase | Task | Timeframe | Expected Records Added | Notes |
|-------|------|-----------|------------------------|-------|
| 1     | Reconcile 2023 | Day 1-2 | ~392,000 | Highest priority |
| 2     | Reconcile 2022 | Day 3-5 | ~514,000 | Second priority |
| 3     | Reconcile 2025 | Day 6   | ~122,000 | Third priority |
| 4     | Reconcile 2024 | Day 7   | ~156,000 | Least priority |
| 5     | Final verification | Day 8 | - | Ensure 100% reconciliation |

### 5. Script for Implementing Full Reconciliation

We'll create a master script that can be run to perform the full reconciliation in proper order:

```sql
-- Full reconciliation process
\echo 'Starting comprehensive Bitcoin calculation reconciliation'

\echo 'Phase 1: Reconciling 2023 (Highest priority)'
\i reconcile_2023.sql

\echo 'Phase 2: Reconciling 2022 (Second priority)'
\i reconcile_2022.sql

\echo 'Phase 3: Reconciling 2025 (Third priority)'
\i reconcile_2025.sql

\echo 'Phase 4: Reconciling 2024 (Final priority)'
\i reconcile_2024.sql

\echo 'Phase 5: Final verification'
\i verify_reconciliation.sql

\echo 'Reconciliation complete!'
```

### 6. Recovery and Restart Mechanisms

To handle potential failures during the reconciliation process:

1. Implement checkpoint system to track progress
2. Store completion status in a temporary table
3. Allow for resuming from the last successful month
4. Add error handling and retry logic

### 7. Post-Reconciliation Maintenance

After achieving 100% reconciliation:

1. Create automated daily validation checks
2. Implement database triggers to maintain reconciliation
3. Add monitoring alerts for any future discrepancies

## Expected Outcome

Upon completion of this plan, we will have:

1. **100% reconciliation** between curtailment_records and historical_bitcoin_calculations
2. **Complete audit trail** documenting the reconciliation process
3. **Automated safeguards** to maintain reconciliation going forward