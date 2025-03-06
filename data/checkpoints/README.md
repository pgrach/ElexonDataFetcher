# Bitcoin Mining Analytics Platform - Checkpoints Directory

This directory contains checkpoint files used by various reconciliation and data processing scripts to track progress and enable resuming interrupted operations.

## Checkpoint Files

### Reconciliation Checkpoints

- `reconciliation_checkpoint.json` - Used by the unified reconciliation system to track progress of missing calculation processing
- `daily_reconciliation_checkpoint.json` - Used by the daily reconciliation check script to track which dates have been processed

### Structure Examples

#### Unified Reconciliation Checkpoint

```json
{
  "lastProcessedDate": "2025-03-05",
  "pendingDates": ["2025-03-06", "2025-03-07"],
  "completedDates": ["2025-03-01", "2025-03-02", "2025-03-03", "2025-03-04"],
  "startTime": 1709742856123,
  "lastUpdateTime": 1709743156789,
  "stats": {
    "totalRecords": 8564,
    "processedRecords": 7231,
    "successfulRecords": 7189,
    "failedRecords": 42,
    "timeouts": 3
  }
}
```

#### Daily Reconciliation Checkpoint

```json
{
  "lastRun": "2025-03-06T10:15:23.456Z",
  "dates": ["2025-03-04", "2025-03-05"],
  "processedDates": ["2025-03-04"],
  "lastProcessedDate": "2025-03-04",
  "status": "running",
  "startTime": "2025-03-06T10:15:23.456Z",
  "endTime": null
}
```

## Checkpoint Management

Checkpoints are managed by the `CheckpointManager` class in `server/utils/checkpoints.ts`, which provides the following features:

- Loading and saving checkpoint data
- Automatic progress tracking
- Safe updates with file locking
- Completion status tracking

## Manual Intervention

In case of issues, checkpoints can be manually reset or adjusted:

1. To restart a process from scratch, simply delete the checkpoint file
2. To skip problematic dates, edit the checkpoint file to move dates from `pendingDates` to `completedDates`
3. To force reprocessing of completed dates, move them from `completedDates` back to `pendingDates`

**Caution:** Manual edits should be performed with care to maintain data integrity.

## Checkpoint Utilities

The `scripts/utilities/` directory contains utilities for working with checkpoint files:

- `reset_checkpoints.ts` - Reset all checkpoints to start fresh
- `checkpoint_status.ts` - Display the current status of all checkpoints
- `validate_checkpoints.ts` - Validate checkpoint files for integrity

## Creating New Checkpoints

When creating a new long-running process that may need to be resumed, consider using the `CheckpointManager` class to manage progress. Example:

```typescript
import { CheckpointManager } from '../../server/utils/checkpoints';

// Create a checkpoint manager for your process
const checkpoint = new CheckpointManager('my-process', {
  id: 'my-process-1',
  created: new Date().toISOString(),
  lastUpdated: new Date().toISOString(),
  status: 'pending',
  progress: 0,
  // Add your process-specific data here
  processedItems: [],
  pendingItems: ['item1', 'item2', 'item3'],
}, 10); // Auto-save every 10 seconds

// Initialize (load existing or create new)
const data = checkpoint.init();

// Update progress
checkpoint.update(current => ({
  progress: 33,
  pendingItems: current.pendingItems.slice(1),
  processedItems: [...current.processedItems, current.pendingItems[0]]
}));

// Mark as complete when done
checkpoint.complete(true);
```