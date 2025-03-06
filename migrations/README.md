# Bitcoin Mining Analytics Platform - Database Migrations

This directory contains SQL migration scripts for the PostgreSQL database used by the Bitcoin Mining Analytics platform.

## Migration Files

- `remove_redundant_indexes.sql` - SQL script to optimize database performance by removing redundant indexes

## Running Migrations

Migrations can be run using the migration script:

```bash
npx tsx scripts/migrations/run_migration.ts
```

The migration script in `scripts/migrations/run_migration.ts` handles:
- Reading SQL files from this directory
- Executing them against the PostgreSQL database
- Logging the results
- Error handling and transaction management

## Important Notes

- Migration files should be idempotent when possible (able to be run multiple times without causing errors)
- Always backup the database before running migrations
- Migrations should include comments explaining their purpose
- For schema changes, consider using Drizzle's migration tools instead of raw SQL