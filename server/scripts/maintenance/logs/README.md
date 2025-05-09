# Log Management Scripts

This directory contains scripts for managing logs in the application.

## Log Rotation

The `logRotation.ts` script provides automated log file management to prevent excessive log file accumulation. It archives older logs and organizes them by date and type.

### Features

- Archives logs older than a configurable number of days (default: 30)
- Compresses logs using gzip to save disk space
- Organizes archived logs by year, month, and log type
- Can be run manually or scheduled to run periodically

### Usage

Run the script manually:

```bash
npx tsx server/scripts/maintenance/logs/logRotation.ts
```

To schedule automatic log rotation, add this to your crontab (runs daily at 3 AM):

```
0 3 * * * cd /path/to/your/app && npx tsx server/scripts/maintenance/logs/logRotation.ts >> logs/log-rotation_$(date +\%Y-\%m-\%d).log 2>&1
```

### Configuration

You can modify the following settings in the script:

- `LOG_DIR`: The directory where logs are stored (default: './logs')
- `ARCHIVE_DIR`: The directory where archived logs will be stored (default: './logs/archives')
- `LOG_AGE_DAYS`: Archive logs older than this many days (default: 30)
- `DRY_RUN`: Set to true to simulate the rotation without actually moving files (default: false)