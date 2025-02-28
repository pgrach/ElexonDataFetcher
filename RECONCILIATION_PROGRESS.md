# Bitcoin Mining Calculations Reconciliation

## Current Status

As of the latest check, the reconciliation status is:

- **Overall Completion**: 47.73% (1,003,413 out of 2,102,466 expected calculations)
- **December 2023 Completion**: 42.30% (24,050 out of 56,856 expected calculations)
- **Key Dates Progress**:
  - 2023-12-16: 54.41% (4,686 out of 8,613 calculations)
  - 2023-12-21: 34.34% (2,969 out of 8,646 calculations)
  - 2023-12-22: 51.22% (4,671 out of 9,120 calculations)

## Reconciliation Tools

We've developed several specialized tools to address the reconciliation requirements, each designed for specific use cases:

### 1. `reconcile_specific_combo.ts`

A precision tool for reconciling individual farm/model/period combinations:

```bash
npx tsx reconcile_specific_combo.ts <date> <period> <farm_id> <model>
```

Example:
```bash
npx tsx reconcile_specific_combo.ts 2023-12-21 7 E_BABAW-1 M20S
```

### 2. `reconcile_batch_limit.ts`

For efficient batch processing with execution time constraints:

```bash
npx tsx reconcile_batch_limit.ts <date> [batch_size]
```

Example:
```bash
npx tsx reconcile_batch_limit.ts 2023-12-21 5
```

### 3. `reconcile_date_period_combination.ts`

For processing multiple periods for a specific date:

```bash
npx tsx reconcile_date_period_combination.ts <date> [period1,period2,...]
```

Example:
```bash
npx tsx reconcile_date_period_combination.ts 2023-12-21 7,8,9
```

### 4. `reconciliation_progress_check.ts`

For monitoring reconciliation progress:

```bash
# Overall status and top missing dates
npx tsx reconciliation_progress_check.ts

# December status
npx tsx reconciliation_progress_check.ts december

# Specific date status
npx tsx reconciliation_progress_check.ts date 2023-12-21

# Top N missing dates
npx tsx reconciliation_progress_check.ts top 20
```

## Reconciliation Strategy

Our reconciliation approach follows these principles:

1. **Targeted Processing**: Focus on specific dates and periods with missing calculations
2. **Batch Processing**: Process data in manageable batches to avoid execution timeouts
3. **Progress Tracking**: Regular monitoring of completion percentages
4. **Prioritization**: Address dates with the highest number of missing calculations first

## Challenges and Solutions

| Challenge | Solution |
|-----------|----------|
| Execution timeouts | Implemented batch processing with limits |
| Database schema differences | Identified correct column names and adjusted scripts |
| Duplicate records | Added existence checks before inserting new records |
| Large data volumes | Created specialized tools for different reconciliation scenarios |

## Next Steps

1. Continue processing December 2023 data, focusing on:
   - 2023-12-17 (0% complete, 5,880 missing)
   - 2023-12-24 (0% complete, 4,485 missing)
   - 2023-12-23 (0% complete, 3,258 missing)

2. Address high-priority dates from 2022:
   - 2022-10-06 (0% complete, 17,691 missing)
   - 2022-06-11 (0% complete, 16,983 missing)
   - 2022-11-10 (0% complete, 15,486 missing)

3. Implement automated reconciliation for future data ingestion