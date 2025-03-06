# Bitcoin Mining Analytics Platform - Root Files Documentation

This document provides information about the key files located in the root directory of the Bitcoin Mining Analytics platform.

## Reconciliation Files

### complete_reingestion_process.ts

This file contains the comprehensive process for reingesting Elexon API data for a specific date. It handles the entire workflow from clearing existing records to processing curtailment data and calculating Bitcoin mining potential.

#### Key Features:
- Handles API timeouts and connection issues
- Processes data in efficient batches
- Supports all 48 settlement periods and multiple miner models
- Includes comprehensive logging and verification

#### Usage:
```bash
npx tsx complete_reingestion_process.ts [date]
```

### daily_reconciliation_check.ts

This file contains the script for automatically checking the reconciliation status for recent dates and processing any missing calculations.

#### Key Features:
- Checks recent dates for missing Bitcoin calculations
- Processes any missing calculations automatically
- Supports forcing reprocessing even if no issues are found
- Uses a checkpoint system for tracking progress

#### Usage:
```bash
npx tsx daily_reconciliation_check.ts [days=2] [forceProcess=false]
```

### daily_reconciliation_checkpoint.json

This checkpoint file is used by the daily reconciliation check script to track progress between runs. It contains information about which dates have been checked and processed.

#### Structure:
```json
{
  "lastRun": "2025-03-06",
  "dates": ["2025-03-06", "2025-03-05", "2025-03-04"],
  "processedDates": [],
  "lastProcessedDate": null,
  "status": "completed",
  "startTime": "2025-03-06T10:15:23.456Z",
  "endTime": "2025-03-06T10:25:45.789Z"
}
```

### unified_reconciliation.ts

This file contains the unified reconciliation system for ensuring data integrity between curtailment records and Bitcoin calculations.

#### Key Features:
- Supports various commands for different reconciliation tasks
- Implements batch processing with configurable batch size
- Uses checkpoints for resumability
- Includes advanced retry logic with exponential backoff

#### Usage:
```bash
npx tsx unified_reconciliation.ts [command] [options]
```

### reconciliation_checkpoint.json

This checkpoint file is used by the unified reconciliation system to track progress between runs. It contains information about which dates have been processed and statistics about the reconciliation process.

#### Structure:
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

## Data Processing Files

### reingest-data.ts

This file contains a standardized tool for reingesting Elexon data for a specific date and updating curtailment records and Bitcoin calculations.

#### Key Features:
- Supports skipping Bitcoin calculation updates
- Supports skipping verification step
- Provides detailed logging during processing
- Shows help message with usage instructions

#### Usage:
```bash
npx tsx reingest-data.ts <date> [options]
```

## Configuration Files

### drizzle.config.ts

This file contains the configuration for Drizzle ORM, defining database connections and schema location.

#### Key Settings:
- Schema location: `./db/schema.ts`
- Output directory for migrations: `./migrations`
- Database driver: `pg` (PostgreSQL)
- Connection string from environment variable: `DATABASE_URL`

### tailwind.config.ts

This file contains the configuration for Tailwind CSS, defining theme settings and plugins.

#### Key Settings:
- Dark mode configuration
- Content paths for scanning for classes
- Plugin configuration (including ShadCN theme plugin)

### postcss.config.js

This file contains the configuration for PostCSS, defining plugins for CSS processing.

#### Key Settings:
- Tailwind CSS plugin
- Autoprefixer plugin

### theme.json

This file contains the configuration for the UI theme, defining colors and appearance.

#### Key Settings:
- Primary color
- Theme variant (professional, tint, vibrant)
- Appearance mode (light, dark, system)
- Border radius

## Build System Files

### vite.config.ts

This file contains the configuration for the Vite bundler, defining build settings and plugins.

#### Key Settings:
- React plugin configuration
- ShadCN theme plugin configuration
- Runtime error modal plugin configuration
- Path aliases for imports

## Other Files

### generated-icon.png

This file is the application icon shown in the browser tab.

### tsconfig.json

This file contains the TypeScript configuration for the project.

#### Key Settings:
- Compiler options
- Module resolution settings
- Type definitions
- Path aliases for imports

### LICENSE

This file contains the license information for the project.

### README.md

This file contains the main documentation for the project, including overview, features, and usage instructions.

### PROJECT_STRUCTURE.md

This file provides a high-level overview of the project structure and organization.