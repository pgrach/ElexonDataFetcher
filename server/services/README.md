# Services

This directory contains business logic services for the application.

## Purpose

Services are responsible for:

1. Implementing business logic and domain rules
2. Coordinating with data access layers
3. Managing complex operations and processing
4. Providing reusable functionality to controllers

## Usage

Import services directly:

```typescript
import { processDailyCurtailment } from '../services/curtailmentService';
import { processSingleDay } from '../services/bitcoinService';
```

## Structure

- `curtailmentService.ts` - Handles curtailment data processing and calculations
- `bitcoinService.ts` - Manages Bitcoin mining calculations
- `windGenerationService.ts` - Processes wind generation data
- `elexon.ts` - Manages API interactions with Elexon
- `windDataUpdateService.ts` - Scheduled service for wind data updates
- `dataUpdateService.ts` - Manages data update workflows
- `historicalReconciliation.ts` - Handles reconciliation of historical data
- Other utility services

## Naming Conventions

All service files should use the `Service` suffix (e.g., `curtailmentService.ts`, not `curtailment.ts`) for consistency and clarity.

## Best Practices

- Services should be stateless when possible
- Use dependency injection for external dependencies
- Implement comprehensive error handling
- Add detailed logging for complex operations