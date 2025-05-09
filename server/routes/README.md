# Routes

This directory contains API route definitions for the application.

## Purpose

Routes are responsible for:

1. Defining API endpoints and URL patterns
2. Connecting HTTP methods to controller handlers
3. Applying middleware to specific routes
4. Organizing endpoints by resource or domain

## Usage

Register routes in the main application:

```typescript
import express from 'express';
import apiRoutes from './routes';

const app = express();
app.use('/api', apiRoutes);
```

## Structure

- `index.ts` - Main routes aggregator
- `curtailmentRoutes.ts` - Curtailment-related endpoints
- `bitcoinRoutes.ts` - Bitcoin calculation endpoints
- `summaryRoutes.ts` - Summary data endpoints
- `windRoutes.ts` - Wind generation data endpoints
- Other resource-specific route files

## Best Practices

- Group related endpoints by resource
- Use consistent naming conventions
- Apply appropriate middleware for validation, auth, etc.
- Keep route files focused on routing concerns only