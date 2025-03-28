# Data Reconciliation Report: 2025-03-27

## Summary
This report documents the successful data reconciliation process for settlement date **2025-03-27**. We have successfully populated **all 48 settlement periods** with curtailment records, filling the data gap that previously existed for periods 35-48.

## Initial State
- Periods 1-34: Already populated (1001 records)
- Periods 35-48: Missing data (0 records)

## Reconciliation Actions
We implemented a series of scripts to methodically retrieve and process data from the Elexon API:
1. Created a `process_single_period.cjs` script to handle one period at a time
2. Created batch scripts (`process_batch1.cjs`, `process_batch2.cjs`, `process_batch3.cjs`) for larger batches
3. Implemented a data validation script (`verify_2025_03_27.cjs`) to confirm data integrity

### Processing Statistics
- **1,393 new records** added for periods 35-48
- **30,875.71 MWh** total curtailment volume processed
- **£903,574.13** total curtailment payments processed
- **Average of 99.5 records per period** for the newly populated periods

### Period-by-Period Data
| Period | Records | Volume (MWh) | Payment (£) |
|--------|---------|--------------|-------------|
| 35     | 81      | 1730.45      | 34382.19    |
| 36     | 96      | 2020.69      | 52205.92    |
| 37     | 104     | 2154.73      | 55722.30    |
| 38     | 104     | 2145.77      | 55206.35    |
| 39     | 99      | 2184.08      | 54278.32    |
| 40     | 96      | 2142.51      | 50169.36    |
| 41     | 97      | 2154.87      | 60498.90    |
| 42     | 94      | 2119.24      | 59371.75    |
| 43     | 95      | 2080.80      | 57916.62    |
| 44     | 97      | 2194.70      | 62569.09    |
| 45     | 98      | 2362.75      | 79201.39    |
| 46     | 102     | 2487.21      | 87074.86    |
| 47     | 113     | 2510.38      | 94124.45    |
| 48     | 117     | 2587.54      | 100852.64   |

## Final State
- All 48 periods populated (2,394 total records)
- Total volume: 55,802.13 MWh
- Total payment: £628,670.56
- 69 distinct wind farms represented

## Top Active Wind Farms
The 5 most active wind farms in the newly processed periods:
1. T_MOWWO-4 (Moray Offshore Wind West Ltd): 1,953.52 MWh, £53,313.60
2. T_KLGLW-1 (SP Renewables UK Limited): 1,841.67 MWh, £53,520.22
3. T_MOWEO-1 (Moray Offshore Wind East Ltd): 2,662.70 MWh, £21,595.35
4. T_SOKYW-1 (South Kyle Wind Farm Limited): 1,947.13 MWh, £14,201.25
5. T_MOWEO-2 (Moray Offshore Wind East Ltd): 2,536.22 MWh, £12,289.70

## Technical Approach
1. **Data Source**: Used the Elexon API balancing/settlement/stack endpoints for bid and offer data
2. **Processing Strategy**: Implemented single-period processing with individual transactions for reliability
3. **Validation Criteria**: Filtered for negative volumes, SO flag = true, and valid wind farm BMU units
4. **Execution Method**: Used CommonJS scripts with PostgreSQL direct connections for optimal performance
5. **Error Handling**: Implemented comprehensive error handling with transaction rollbacks

## Recommendations
1. Consider implementing an automated daily check for missing periods using the pattern established in the `daily_reconciliation_check.ts` script
2. Enhance the monitoring system to alert when data gaps of more than 3 consecutive periods are detected
3. Run the verification script as part of the daily data pipeline to proactively identify any data inconsistencies

## Verification
The full verification report is available by running the `verify_2025_03_27.cjs` script, which provides detailed statistics on all 48 settlement periods for the date.