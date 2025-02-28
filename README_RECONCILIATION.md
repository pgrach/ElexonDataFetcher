# Bitcoin Mining Reconciliation System

## Overview

This reconciliation system ensures data integrity between curtailment records and Bitcoin mining calculations. It verifies that for every curtailment record (defined by date, period, and farm), there are corresponding Bitcoin calculations for each supported miner model.

## Directory Structure

The reconciliation system is now organized into the following structure:

```
.
├── docs/                              # Documentation
│   ├── RECONCILIATION.md              # Main documentation
│   ├── reconciliation_plan.md         # Initial planning document
│   └── comprehensive_reconciliation_plan.md  # Detailed plan
│
├── scripts/                           # All scripts
│   └── reconciliation/                # Reconciliation scripts
│       ├── ts/                        # TypeScript reconciliation scripts
│       │   ├── check_reconciliation_status.ts  # Check current status
│       │   ├── reconcile.ts           # Full reconciliation
│       │   ├── run_reconciliation.ts  # Simple wrapper
│       │   └── test_reconcile_date.ts # Test specific date
│       │
│       └── sql/                       # SQL scripts (historical)
│           ├── batch_reconciliation.sql
│           ├── complete_bitcoin_reconciliation.sql
│           └── ...
│
└── reconcile.js                       # Easy launcher script
```

## Using the Reconciliation Tool

The simplified launcher script (`reconcile.js`) provides an easy interface for running reconciliation tools:

```bash
node reconcile.js
```

This interactive tool allows you to:

1. Check the current reconciliation status
2. Run a full reconciliation process
3. Test reconciliation for a specific date

## Reconciliation Status

The system tracks the following key metrics:

- Total curtailment records
- Unique date-period-farm combinations
- Bitcoin calculations by miner model
- Overall reconciliation percentage

## Maintenance

Regular maintenance tasks:

1. Monitor reconciliation percentage regularly
2. Run full reconciliation after data imports
3. Test specific dates if anomalies are found

## Troubleshooting

If reconciliation is not reaching 100%:

1. Check for specific dates with missing calculations
2. Run a targeted test for those dates
3. Check for any errors in processing those dates
4. Verify DynamoDB difficulty data is available