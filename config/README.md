# Bitcoin Mining Analytics Platform - Configuration Files

This directory contains configuration files for the Bitcoin Mining Analytics platform.

## Configuration Files

- `drizzle.config.ts` - Configuration for the Drizzle ORM, defining database connection parameters and migration settings

- `tailwind.config.ts` - Configuration for Tailwind CSS, including theme customization, plugins, and content sources

- `postcss.config.js` - Configuration for PostCSS, which processes CSS with plugins like autoprefixer and Tailwind CSS

- `theme.json` - UI theme configuration used by the shadcn/ui components

- `tsconfig.json` - TypeScript configuration, defining compiler options, module resolution, and type definitions

## Important Notes

- When modifying these configuration files, be careful as they affect the entire application
- Use these copied configuration files as reference only
- For actual changes, modify the original files in the root directory
- After making changes to Tailwind or PostCSS configuration, you may need to restart the development server
- After making changes to the Drizzle configuration, you may need to run database migrations