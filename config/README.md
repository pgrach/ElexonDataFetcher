# Bitcoin Mining Analytics Platform - Configuration Directory

This directory contains configuration files for the Bitcoin Mining Analytics platform.

## Configuration Files

### Drizzle ORM Configuration

- `drizzle.config.ts` - Configuration for Drizzle ORM, defining database connections and schema location

```typescript
// Sample content of drizzle.config.ts
import { defineConfig } from 'drizzle-kit';
import { config } from 'dotenv';

config();

export default defineConfig({
  schema: './db/schema.ts',
  out: './migrations',
  driver: 'pg',
  dbCredentials: {
    connectionString: process.env.DATABASE_URL || '',
  },
  verbose: true,
  strict: true,
});
```

### Tailwind CSS Configuration

- `tailwind.config.ts` - Configuration for Tailwind CSS, defining theme settings and plugins

```typescript
// Sample content of tailwind.config.ts
import { type Config } from 'tailwindcss';
import { shadcnPlugin } from '@replit/vite-plugin-shadcn-theme-json';

export default {
  darkMode: ['class'],
  content: [
    './pages/**/*.{ts,tsx}',
    './components/**/*.{ts,tsx}',
    './app/**/*.{ts,tsx}',
    './src/**/*.{ts,tsx}',
  ],
  plugins: [
    require('tailwindcss-animate'),
    require('@tailwindcss/typography'),
    shadcnPlugin(),
  ],
} satisfies Config;
```

### PostCSS Configuration

- `postcss.config.js` - Configuration for PostCSS, defining plugins for CSS processing

```javascript
// Sample content of postcss.config.js
module.exports = {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
};
```

### Theme Configuration

- `theme.json` - Configuration for the UI theme, defining colors and appearance

```json
// Sample content of theme.json
{
  "primary": "#0070f3",
  "variant": "professional",
  "appearance": "light",
  "radius": 0.5
}
```

## Usage

These configuration files are used by various parts of the application:

1. **Build Tools**: Vite and PostCSS use these configurations during the build process
2. **Database Tools**: Drizzle ORM uses its configuration for database operations
3. **UI Framework**: The UI components use the theme configuration for styling

## Modifying Configuration

When modifying configuration files, follow these guidelines:

1. **Database Configuration**:
   - Only modify `drizzle.config.ts` if you need to change database connection settings
   - After changes, run `npm run db:push` to update the database schema

2. **UI Configuration**:
   - Modify `theme.json` to change the application's visual appearance
   - Changes to `tailwind.config.ts` should be minimal and focused on adding plugins

3. **Build Configuration**:
   - Changes to `postcss.config.js` should only be made when adding new PostCSS plugins
   - Vite configuration should generally not be modified unless absolutely necessary

## Environment-Specific Configuration

The application uses environment variables for configuration that varies between environments:

- `DATABASE_URL` - PostgreSQL connection string
- `AWS_REGION` - AWS region for DynamoDB
- `AWS_ACCESS_KEY_ID` - AWS access key for DynamoDB access
- `AWS_SECRET_ACCESS_KEY` - AWS secret key for DynamoDB access
- `ELEXON_API_KEY` - API key for Elexon API access

These environment variables should be set in the Replit environment.