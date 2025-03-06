# Bitcoin Mining Analytics Platform - Migration Scripts

This directory contains database migration scripts for the Bitcoin Mining Analytics platform.

## Scripts

- `run_migration.ts` - PostgreSQL database migration runner that executes SQL migration scripts to create or update tables for the mining potential optimization.

## Usage

You can run the migration script directly using the npx command:

```bash
# Run database migrations
npx tsx scripts/migrations/run_migration.ts
```

The script will:
1. Connect to the PostgreSQL database using the DATABASE_URL environment variable
2. Execute SQL migration files located in the `/migrations` directory
3. Log the results of each migration
4. Handle transactions and errors appropriately

## Migration Files

The actual SQL migration files are stored in the `/migrations` directory at the root of the project. Current migration files include:

- `remove_redundant_indexes.sql` - SQL script to optimize database performance by removing redundant indexes

## Important Notes

- Always backup the database before running migrations
- Migrations should be idempotent whenever possible (able to be run multiple times without errors)
- For schema changes, consider using Drizzle ORM's migration tools instead of raw SQL