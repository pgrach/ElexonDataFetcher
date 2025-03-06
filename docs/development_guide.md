# Bitcoin Mining Analytics Platform - Development Guide

This document provides guidelines and best practices for developers working on the Bitcoin Mining Analytics platform.

## Development Environment Setup

### Prerequisites

1. Node.js (v18+)
2. PostgreSQL database
3. AWS account with DynamoDB access (for difficulty data)

### Local Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Set up environment variables:
   ```
   DATABASE_URL=postgresql://user:password@localhost:5432/bitcoin_mining
   AWS_REGION=eu-west-2
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   ELEXON_API_KEY=your_elexon_api_key
   ```
4. Start the development server:
   ```bash
   npm run dev
   ```

## Project Structure

The project is organized into the following key directories:

- `client/` - React frontend application
- `server/` - Node.js/Express backend
- `db/` - Database schemas and connection setup
- `scripts/` - Utility scripts for various tasks
- `config/` - Configuration files
- `docs/` - Documentation files

For a detailed overview, see the `PROJECT_STRUCTURE.md` file.

## Coding Standards

### General Guidelines

1. Use TypeScript for all new code
2. Use async/await for asynchronous operations
3. Write meaningful commit messages
4. Include JSDoc comments for functions and classes
5. Follow the existing code style and formatting

### Frontend Guidelines

1. Use React hooks for state management
2. Use React Query for data fetching
3. Use ShadCN UI components for consistent styling
4. Follow the component structure in `client/src/components/`
5. Place new pages in `client/src/pages/`

### Backend Guidelines

1. Follow RESTful API design principles
2. Use Drizzle ORM for database operations
3. Implement proper error handling with custom error classes
4. Log important events and errors
5. Use environment variables for configuration

## Database Guidelines

1. Define new tables in `db/schema.ts`
2. Use migrations for schema changes
3. Include indexes for frequently queried columns
4. Follow the naming conventions: snake_case for tables and columns
5. Add TypeScript types for all database entities

## API Design

1. Use RESTful routes where possible
2. Return standardized JSON responses
3. Include proper HTTP status codes
4. Validate input data with Zod schemas
5. Document new endpoints in `docs/api_endpoints.md`

## Testing Guidelines

1. Write unit tests for critical functions
2. Include integration tests for API endpoints
3. Test database operations with actual database
4. Mock external services for testing
5. Run tests before submitting pull requests

## Documentation Guidelines

1. Update documentation when making significant changes
2. Use Markdown for all documentation files
3. Include examples for API endpoints
4. Document configuration options
5. Explain complex algorithms and business logic

## Common Development Tasks

### Adding a New API Endpoint

1. Create a new route file or add to an existing one in `server/routes/`
2. Register the route in `server/routes.ts`
3. Implement controller functions in `server/controllers/`
4. Add input validation using Zod schemas
5. Document the endpoint in `docs/api_endpoints.md`

### Adding a New Database Table

1. Define the table in `db/schema.ts`
2. Create insert and select schemas using Drizzle's helper functions
3. Add TypeScript types for the table
4. Create a migration if needed
5. Update documentation in `docs/database_schema.md`

### Adding a New Frontend Page

1. Create a new page component in `client/src/pages/`
2. Add the route in `client/src/App.tsx`
3. Create necessary API hooks in a custom hook file
4. Use existing UI components from `client/src/components/ui/`
5. Add any new components to the appropriate directory

### Adding a New Script

1. Create the script in the appropriate subdirectory of `scripts/`
2. Add documentation comments explaining the purpose and usage
3. Use existing utility functions where possible
4. Add error handling and logging
5. Document the script in `docs/scripts.md`

## Performance Considerations

1. Use connection pooling for database operations
2. Implement caching for expensive operations
3. Use batch processing for large datasets
4. Add appropriate indexes to database tables
5. Optimize React components to minimize re-renders

## Security Considerations

1. Validate all user input
2. Use parameterized queries for database operations
3. Avoid storing sensitive information in code
4. Use secure environment variables for API keys
5. Implement rate limiting for public APIs

## Deployment Process

The application is deployed on Replit using the built-in deployment system:

1. Push changes to the main branch
2. Ensure all tests pass
3. Click the "Deploy" button in the Replit interface
4. Monitor logs for any deployment issues

## Troubleshooting Common Issues

### Database Connection Issues

1. Check the `DATABASE_URL` environment variable
2. Verify PostgreSQL is running
3. Check for connection limit issues
4. Look for timeout errors in the logs

### DynamoDB Issues

1. Verify AWS credentials are correct
2. Check the table name and region
3. Look for rate limiting or throttling errors
4. Check logs for DynamoDB-specific errors

### API Request Issues

1. Check for rate limiting with external APIs
2. Verify API keys are valid
3. Look for timeout errors
4. Check for changes in API response format

### Frontend Build Issues

1. Check for JavaScript errors in the console
2. Verify all dependencies are installed
3. Look for TypeScript errors
4. Check for issues with environment variables