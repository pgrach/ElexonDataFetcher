# Reconciliation System User Guide

## Overview
This guide provides instructions for using the Bitcoin mining calculation reconciliation system. The system ensures 100% alignment between curtailment records and historical Bitcoin calculations, which is critical for accurate mining potential reporting.

## Quick Reference

### Check Status
```bash
npx tsx reconciliation_manager.ts status
```

### Fix Missing Calculations
```bash
npx tsx reconciliation_manager.ts fix [batch-size]
```

### Analyze Reconciliation Issues
```bash
npx tsx reconciliation_manager.ts analyze
```

### Process a Specific Date
```bash
npx tsx reconciliation_manager.ts date YYYY-MM-DD
```

### Automated Daily Reconciliation
```bash
./auto_reconcile.sh
```

## System Components

### 1. Reconciliation Manager (`reconciliation_manager.ts`)
The central control point for all reconciliation operations. It orchestrates other components and provides a unified interface.

### 2. Efficient Reconciliation Tool (`efficient_reconciliation.ts`)
Optimized for processing large batches of missing calculations with checkpointing capability to resume interrupted processes.

### 3. Minimal Reconciliation Tool (`minimal_reconciliation.ts`)
Ultra-conservative processing for problematic dates that may cause timeouts with other methods.

### 4. Connection Analyzer (`connection_timeout_analyzer.ts`)
Diagnoses database connectivity issues and helps optimize reconciliation parameters.

### 5. Progress Reporting Tools
- `reconciliation_progress_check.ts`: Quick overview of current reconciliation status
- `reconciliation_progress_report.ts`: Detailed report on reconciliation completion
- `reconciliation_visualization.ts`: Visual representation of reconciliation progress

## Common Workflows

### Daily Reconciliation Process
1. Check current status: `npx tsx reconciliation_manager.ts status`
2. Analyze any issues: `npx tsx reconciliation_manager.ts analyze`
3. Fix missing calculations: `npx tsx reconciliation_manager.ts fix 5`
4. Verify results: `npx tsx reconciliation_manager.ts status`

### Handling Problematic Dates
If standard reconciliation timeouts for specific dates:
1. Identify the problematic date from analysis
2. Use minimal reconciliation: `npx tsx minimal_reconciliation.ts critical-date YYYY-MM-DD`
3. Alternatively, process in small sequential batches: `npx tsx minimal_reconciliation.ts sequence YYYY-MM-DD 1`

### Scheduled Reconciliation
Set up a daily cron job to run `./auto_reconcile.sh` which will:
- Check current reconciliation status
- Process missing calculations with appropriate batch size
- Fallback to minimal reconciliation for problematic dates
- Log all actions and results

## Performance Optimization

### Batch Size Selection
- Larger batch sizes (5-10) are faster but may cause timeouts
- Smaller batch sizes (1-3) are more reliable but slower
- Start with batch size 5 and reduce if timeouts occur

### Resource Considerations
- Reconciliation process is most efficient during off-peak hours
- Long-running reconciliation processes might impact API response times
- Consider using `nohup` or screen sessions for extensive reconciliation tasks

## Troubleshooting

### Timeouts
If experiencing timeouts:
1. Reduce batch size: `npx tsx reconciliation_manager.ts fix 2`
2. Use minimal reconciliation for specific dates
3. Check database connection: `npx tsx connection_timeout_analyzer.ts test`

### Corrupted Checkpoint
If checkpoint becomes corrupted:
1. Reset checkpoint: `npx tsx efficient_reconciliation.ts reset`
2. Restart with smaller batch size

### Database Connectivity Issues
If database connections are failing:
1. Run diagnostics: `npx tsx connection_timeout_analyzer.ts analyze`
2. Follow recommended actions from analysis

## Monitoring and Reporting

### Daily Status Check
Run `npx tsx daily_reconciliation_check.ts` to get a summary of reconciliation status for the current and previous day.

### Monthly Analysis
Generate a monthly reconciliation report with:
```bash
npx tsx reconciliation_progress_report.ts
```

### Visualization
Create visual heatmaps of reconciliation progress:
```bash
npx tsx reconciliation_visualization.ts
```

## Best Practices

1. **Regular Checks**: Run daily reconciliation checks to catch and fix issues early
2. **Batch Processing**: Process large reconciliation tasks in manageable batches
3. **Logging**: Keep detailed logs of all reconciliation activities
4. **Progressive Approach**: Start with efficient tools and fall back to minimal reconciliation for problematic dates
5. **Backup**: Always have a backup of critical checkpoint files

By following this guide, you can maintain 100% reconciliation between curtailment records and Bitcoin calculations, ensuring accurate and reliable mining potential reporting.