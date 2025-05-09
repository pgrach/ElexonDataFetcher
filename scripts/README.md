# Data Reprocessing Scripts

This directory contains scripts for reprocessing data in the Bitcoin Mining Analytics platform.

## Available Scripts

### 1. `reprocess-complete.ts`

A comprehensive reprocessing script with many options for customization.

**Usage:**
```bash
# Process a single date
npx tsx scripts/reprocess-complete.ts --date 2025-05-08

# Process a date range
npx tsx scripts/reprocess-complete.ts --start 2025-05-01 --end 2025-05-08

# Force reprocessing even if data exists
npx tsx scripts/reprocess-complete.ts --date 2025-05-08 --force

# Skip wind data processing
npx tsx scripts/reprocess-complete.ts --date 2025-05-08 --skip-wind

# Skip Bitcoin calculations
npx tsx scripts/reprocess-complete.ts --date 2025-05-08 --skip-bitcoin

# Specify miner models to process
npx tsx scripts/reprocess-complete.ts --date 2025-05-08 --miners S19J_PRO,S9
```

### 2. `reprocess-may-8th.ts`

A specialized script for reprocessing only May 8th, 2025.

**Usage:**
```bash
npx tsx scripts/reprocess-may-8th.ts
```

### 3. `reprocess-date-range.ts`

A simpler script for processing a range of dates with fewer options.

**Usage:**
```bash
# Process a date range with default settings
npx tsx scripts/reprocess-date-range.ts --start 2025-05-01 --end 2025-05-08

# Skip wind data processing
npx tsx scripts/reprocess-date-range.ts --start 2025-05-01 --end 2025-05-08 --skip-wind

# Specify miner models to process
npx tsx scripts/reprocess-date-range.ts --start 2025-05-01 --end 2025-05-08 --miners S19J_PRO
```

## Data Processing Steps

All scripts follow a similar pattern for data processing:

1. **Clear existing data** for the target date(s)
2. **Process curtailment data** from Elexon API
3. **Process wind generation data** (if not skipped)
4. **Calculate Bitcoin mining potential** for each miner model
5. **Verify daily summaries** have been updated

## Performance Considerations

- The scripts use concurrency control to avoid overwhelming the Elexon API
- For multi-day processing, each date is processed with proper rate limiting
- Default concurrency is 3 dates at a time
- For large date ranges, expect several minutes of processing time

## Error Handling

- Failed processing for one date won't stop other dates from being processed
- Bitcoin calculation errors for one miner model won't stop other models
- Detailed logs are provided for debugging purposes