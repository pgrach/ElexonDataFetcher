# Data Models

This directory contains data models and schemas used throughout the application.

## Purpose

The models directory is responsible for:

1. Defining TypeScript interfaces and types for data structures
2. Implementing model-specific validation logic
3. Providing data access patterns and abstractions

## Usage

Import models directly from this directory:

```typescript
import { WindFarm } from "../models/windFarm";
import { BitcoinCalculation } from "../models/bitcoinCalculation";
```

## Structure

- `windFarm.ts` - Defines wind farm data structures and related utility functions
- `curtailment.ts` - Defines curtailment record structures and validations
- `bitcoin.ts` - Defines Bitcoin calculation models and related functions

## Best Practices

- Keep models focused on data structures and validation
- Avoid adding business logic to models - this belongs in the services directory
- Use TypeScript interfaces for type definitions and classes for complex models with behavior