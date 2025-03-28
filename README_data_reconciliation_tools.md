# Data Reconciliation Toolkit

This toolkit contains a set of specialized scripts developed to verify and repair data completeness in the `curtailment_records` database for specific settlement dates. The primary focus is on retrieving missing data from the Elexon API and ensuring that all 48 settlement periods for a given date are properly populated.

## Available Scripts

### Single-Period Processing

- `process_single_period.cjs`: Processes a single settlement period for a specific date
  - Configuration variables at the top allow easy modification of date and period
  - Includes comprehensive error handling and transaction management
  - Reports detailed statistics after processing

### Batch Processing

- `process_batch1.cjs`: Processes periods 35-39 for 2025-03-27
- `process_batch2.cjs`: Processes periods 40-44 for 2025-03-27
- `process_batch3.cjs`: Processes periods 45-48 for 2025-03-27

### Verification Tools

- `verify_2025_03_27.cjs`: Generates comprehensive statistics and verification for 2025-03-27
  - Checks for any missing periods
  - Calculates volume and payment totals
  - Shows period-by-period breakdown of records
  - Identifies most active wind farms

### Documentation

- `final_report_2025_03_27.md`: Detailed reconciliation report for 2025-03-27
  - Documents the initial state, actions taken, and final state
  - Includes statistics and recommendations

## Usage Examples

### Process a Single Period
```bash
# Process period 45 for settlement date 2025-03-27
node process_single_period.cjs
```

### Verify a Settlement Date
```bash
# Verify all periods for settlement date 2025-03-27
node verify_2025_03_27.cjs
```

## Technical Notes

1. **Data Source**: The scripts use the Elexon API for balancing/settlement/stack data, specifically the bid and offer endpoints
2. **BMU Mapping**: Wind farm BMU units are identified using the mapping file at `server/data/bmuMapping.json`
3. **Database Connection**: Direct PostgreSQL connection via the `Pool` class from the pg package
4. **Error Handling**: Each script includes transaction management with proper commit/rollback semantics
5. **Record Validation**: Records are filtered based on volume (negative), SO flag (true), and valid BMU ID

## Developer Reference

### Key Functions
- `loadBmuMappings()`: Loads the BMU mapping file with wind farm identifiers
- `fetchElexonData()`: Retrieves data from the Elexon API for a specific period and date
- `processPeriod()`: Processes and inserts records for a specific period
- `processOnePeriod()`: Wraps a single period processing in a transaction

### Database Schema
The scripts work with the `curtailment_records` table, which has the following key fields:
- `settlement_date`: The settlement date (YYYY-MM-DD)
- `settlement_period`: The settlement period (1-48)
- `farm_id`: The BMU ID for the wind farm
- `lead_party_name`: The lead party name for the BMU
- `volume`: The curtailment volume (negative values)
- `payment`: The payment for the curtailment
- `original_price`: The original bid/offer price
- `final_price`: The final accepted price
- `so_flag`: Boolean flag indicating System Operator acceptance
- `cadl_flag`: Boolean flag for CADL (Continuous Acceptance Duration Limit)