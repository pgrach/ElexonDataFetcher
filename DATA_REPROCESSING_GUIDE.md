# Wind Farm & Bitcoin Data Reprocessing Guide

This guide provides step-by-step instructions for reprocessing wind farm curtailment data and Bitcoin mining potential calculations for any date. It explains the complete data pipeline, potential pitfalls, and provides robust scripts for performing these tasks.

## Data Pipeline Overview

```
Elexon API → curtailment_records → daily/monthly/yearly summaries → historical_bitcoin_calculations → bitcoin summary tables
```

The data pipeline consists of several interconnected components:

1. **Data Ingestion**: Raw curtailment data is fetched from the Elexon API and filtered using BMU mappings
2. **Curtailment Processing**: Valid curtailment records are stored in the `curtailment_records` table
3. **Summary Creation**: Daily, monthly, and yearly summaries are generated from the curtailment records
4. **Bitcoin Calculations**: For each curtailment record, potential Bitcoin mining is calculated for different miner models (S19J_PRO, S9, M20S)
5. **Bitcoin Summaries**: Daily, monthly, and yearly Bitcoin summaries are aggregated from the calculations

## Database Tables

Key tables involved in the reprocessing:

1. **curtailment_records**: Primary table holding all valid curtailment events
2. **daily_summaries / monthly_summaries / yearly_summaries**: Aggregated energy and payment data
3. **historical_bitcoin_calculations**: Individual Bitcoin calculations for each curtailment record
4. **bitcoin_daily_summaries / bitcoin_monthly_summaries / bitcoin_yearly_summaries**: Aggregated Bitcoin mining potentials

## Common Pitfalls and Solutions

When reprocessing data, be aware of these potential issues:

1. **Database Constraints**: Use `onConflictDoUpdate` to handle unique constraints when inserting data
2. **Calculation Discrepancies**: Always verify database totals against expected values
3. **Batch Processing**: Process large datasets in batches to avoid memory issues
4. **Data Verification**: Always verify the results with database queries after reprocessing
5. **SQL Syntax**: Use appropriate SQL functions (e.g., `TO_CHAR`, `SUBSTRING`) for date formatting

## Available Scripts

### 1. Comprehensive Data Reprocessing

For reprocessing both curtailment records and Bitcoin calculations:

```bash
./reprocess_date.sh YYYY-MM-DD
```

### 2. Bitcoin-Only Reprocessing

For reprocessing only Bitcoin calculations based on existing curtailment records:

```bash
./process_bitcoin.sh YYYY-MM-DD [difficulty]
```

## Reprocessing Steps

### Step 1: Clear Existing Data

Always clear existing data for the target date to avoid duplicates or inconsistencies:

```typescript
// Clear curtailment records
await db.delete(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, targetDate));

// Clear historical Bitcoin calculations
for (const minerModel of MINER_MODELS) {
  await db.delete(historicalBitcoinCalculations)
    .where(and(
      eq(historicalBitcoinCalculations.settlementDate, targetDate),
      eq(historicalBitcoinCalculations.minerModel, minerModel)
    ));
}
```

### Step 2: Fetch and Process Raw Data

Fetch the raw data from the Elexon API, apply filters, and store valid records:

```typescript
// Fetch data for each settlement period
for (let period = 1; period <= 48; period++) {
  const data = await elexonService.fetchData(targetDate, period);
  const validRecords = filterValidRecords(data, bmuMappings);
  await storeCurtailmentRecords(validRecords);
}
```

### Step 3: Process Bitcoin Calculations

Calculate Bitcoin mining potential for each curtailment record:

```typescript
// Process each miner model
for (const minerModel of MINER_MODELS) {
  // Get all curtailment records for this date
  const records = await getCurtailmentRecords(targetDate);
  
  // Process records and calculate Bitcoin
  await processCalculations(records, minerModel);
  
  // Update summaries
  await updateSummaries(targetDate, minerModel);
}
```

### Step 4: Verify Results

Always verify the results after reprocessing to ensure data integrity:

```typescript
// Check curtailment records
const recordCount = await db
  .select({ count: sql<number>`COUNT(*)::int` })
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, targetDate));

// Check Bitcoin calculations
for (const minerModel of MINER_MODELS) {
  const bitcoinCount = await db
    .select({ count: sql<number>`COUNT(*)::int` })
    .from(historicalBitcoinCalculations)
    .where(and(
      eq(historicalBitcoinCalculations.settlementDate, targetDate),
      eq(historicalBitcoinCalculations.minerModel, minerModel)
    ));
  
  console.log(`${minerModel}: ${bitcoinCount[0]?.count || 0} records`);
}
```

## Best Practices

1. **Clear Before Processing**: Always clear existing data for the target date first
2. **Process in Batches**: Break large operations into smaller batches
3. **Handle Constraints**: Use `onConflictDoUpdate` to handle unique constraints
4. **Verify Results**: Always verify the results with database queries after processing
5. **Log Progress**: Add detailed logging to track progress and identify issues

## Running the Reprocessing Scripts

For comprehensive reprocessing of a specific date:

```bash
# Reprocess curtailment data and Bitcoin calculations for 2025-04-02
./reprocess_date.sh 2025-04-02
```

For Bitcoin-only reprocessing with optional difficulty parameter:

```bash
# Reprocess Bitcoin calculations with default difficulty
./process_bitcoin.sh 2025-04-02

# Reprocess Bitcoin calculations with specific difficulty
./process_bitcoin.sh 2025-04-02 113757508810853
```

## Monitoring and Logging

The reprocessing scripts create detailed logs to help monitor progress and identify issues. Always check these logs if you encounter problems.

Example log output:
```
==== Bitcoin Calculation Processing for 2025-04-02 ====
Using difficulty: 113757508810853

Found 833 curtailment records for 2025-04-02
Found 833 valid curtailment records for 2025-04-02 with non-zero energy
Batch 1/17: Processed 50 records
...
Successfully processed 833 Bitcoin calculations for 2025-04-02 and S19J_PRO
Total Bitcoin calculated: 24.15836053
Database total for S19J_PRO: 12.66715512712768 BTC
```

## Troubleshooting

If you encounter issues during reprocessing:

1. **Missing Data**: Verify the Elexon API is returning data for the requested date
2. **Database Errors**: Check for unique constraint violations or other database errors
3. **Calculation Discrepancies**: Verify the Bitcoin calculation function is working correctly
4. **Performance Issues**: Consider breaking operations into smaller batches or increasing batch size based on available memory