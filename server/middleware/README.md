# Middleware

This directory contains Express middleware for the application.

## Purpose

Middleware functions are responsible for:

1. Processing HTTP requests before they reach route handlers
2. Modifying the request or response objects
3. Terminating the request-response cycle when needed
4. Applying cross-cutting concerns like logging, authentication, etc.

## Usage

Import middleware and apply to routes:

```typescript
import { Router } from 'express';
import { authenticateUser, validateRequest } from '../middleware';

const router = Router();
router.get('/api/protected', authenticateUser, validateRequest, controller.handleRequest);
```

## Structure

- `requestLogger.ts` - Logs incoming HTTP requests
- `performanceMonitor.ts` - Tracks request processing time
- `errorHandler.ts` - Global error handling middleware
- `requestValidator.ts` - Validates incoming requests
- Other utility middleware

## Best Practices

- Keep middleware focused on a single responsibility
- Chain middleware in a logical order
- Use `next()` appropriately to continue the middleware chain
- Handle errors properly and avoid uncaught exceptions