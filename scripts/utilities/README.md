# Bitcoin Mining Analytics Platform - Utility Scripts

This directory contains general utility scripts for the Bitcoin Mining Analytics platform.

## Purpose

The utilities directory is intended for scripts that:

1. Perform general-purpose operations not specific to data processing or reconciliation
2. Provide helper functions that might be used across different parts of the application
3. Handle administrative tasks like exports, imports, and system maintenance

## Examples of Utility Scripts

Currently, the main utility script is in the parent directory:

- `exportBmuMappingToExcel.js` - Exports BMU (Balancing Mechanism Unit) mapping data to an Excel/CSV file

Future utility scripts might include:

- Database backup and restore utilities
- Data export tools for analysis in external systems
- Performance monitoring and health check scripts
- Configuration management utilities

## Usage

Typical usage pattern for utility scripts:

```bash
# Run a utility script
npx tsx scripts/utilities/script-name.ts [arguments]
```

## Creating New Utility Scripts

When creating new utility scripts:

1. Place them in this directory
2. Add proper documentation with JSDoc or TSDoc comments
3. Include a usage example in the script header
4. Follow the project's error handling and logging patterns
5. Update this README when adding significant new utilities