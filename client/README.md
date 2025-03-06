# Bitcoin Mining Analytics Platform - Frontend

This directory contains the React frontend application for the Bitcoin Mining Analytics platform.

## Directory Structure

- `src/` - Source code for the React application
  - `src/components/` - Reusable React components
    - `src/components/ui/` - UI components (shadcn/ui based)
  - `src/pages/` - Page components for different routes
  - `src/hooks/` - Custom React hooks
  - `src/lib/` - Utility functions and configuration
- `index.html` - HTML entry point for the application

## Technology Stack

- React for UI components and state management
- Vite as the build tool and development server
- wouter for client-side routing
- TanStack Query (React Query) for data fetching and caching
- Tailwind CSS for styling with shadcn/ui components
- TypeScript for type safety

## Features

- Real-time Bitcoin mining potential visualization
- Detailed charts for curtailment data analysis
- Interactive filters for different time periods and miner models
- Responsive design for desktop and mobile devices

## Development

The application is built using modern React patterns and practices:

- Functional components with hooks
- TypeScript for type safety
- React Query for data fetching and caching
- Tailwind CSS for styling

The frontend communicates with the backend API endpoints to fetch data for visualization and analysis.