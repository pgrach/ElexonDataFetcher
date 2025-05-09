# Types

This directory contains TypeScript type definitions for the application.

## Purpose

Type definitions are responsible for:

1. Defining shared TypeScript interfaces and types
2. Ensuring type safety and consistency across the application
3. Documenting data structures and API contracts
4. Providing reusable type patterns

## Usage

Import types directly:

```typescript
import { ElexonBidOffer } from '../types/elexon';
import { WindGenerationData } from '../types/windGeneration';
```

## Structure

- `elexon.ts` - Elexon API related types
- `api.ts` - Common API response and request types
- `windGeneration.ts` - Wind generation data types
- `bitcoin.ts` - Bitcoin calculation related types
- Other domain-specific type files

## Best Practices

- Keep type definitions clear and focused
- Use descriptive names for types and interfaces
- Add documentation comments for complex types
- Avoid circular type dependencies