# Controllers

This directory contains API controllers for the application.

## Purpose

Controllers are responsible for:

1. Handling HTTP requests and responses
2. Input validation and sanitization
3. Coordinating with services to fulfill requests
4. Formatting and returning appropriate responses

## Usage

Controllers should be organized by resource or domain:

```typescript
import { Router } from 'express';
import { curtailmentController } from '../controllers/curtailmentController';

const router = Router();
router.get('/api/curtailment/daily/:date', curtailmentController.getDailyCurtailment);
```

## Structure

- `curtailmentController.ts` - Handles curtailment-related API endpoints
- `bitcoinController.ts` - Handles Bitcoin calculation API endpoints
- `windGenerationController.ts` - Handles wind generation data API endpoints
- `summaryController.ts` - Handles summary-related API endpoints

## Best Practices

- Keep controllers focused on HTTP concerns
- Delegate business logic to service layers
- Use consistent error handling and response formats
- Validate inputs using middleware or validation libraries