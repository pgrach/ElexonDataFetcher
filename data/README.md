# Bitcoin Mining Analytics Platform - Data Files

This directory contains data files and checkpoints used by the Bitcoin Mining Analytics platform.

## Directories

- `checkpoints/` - Contains checkpoint files used by the reconciliation and data processing systems
  - `daily_reconciliation_checkpoint.json` - Checkpoints for the daily reconciliation process
  - `reconciliation_checkpoint.json` - Checkpoints for the unified reconciliation system

## Important Notes

- These checkpoint files are automatically generated and updated by the application
- They contain state information that allows long-running processes to be resumed if interrupted
- Do not manually edit these files unless you understand their structure and purpose
- If a checkpoint file becomes corrupted, the corresponding process will typically create a new one