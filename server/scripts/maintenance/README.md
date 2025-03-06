# Maintenance Scripts

This directory contains scripts that need to be run periodically to maintain the system's data quality and integrity.

## Available Scripts

### updateBmuMapping.ts

Updates the BMU (Balancing Mechanism Unit) mapping from the Elexon API. This script:

- Fetches the latest wind farm data from the Elexon API
- Validates and filters for wind-only BMUs
- Updates the `bmuMapping.json` file used by the application

#### Usage

```bash
npx tsx server/scripts/maintenance/updateBmuMapping.ts
```

#### Output

The script generates a JSON file at `server/data/bmuMapping.json` containing an array of wind farm BMUs with the following structure:

```json
[
  {
    "nationalGridBmUnit": "string",
    "elexonBmUnit": "string",
    "bmUnitName": "string",
    "generationCapacity": "string",
    "fuelType": "WIND",
    "leadPartyName": "string"
  }
]
```

#### Schedule

This script should be run:
- Monthly to ensure the BMU mapping is up-to-date
- After notifications of new wind farms being added to the grid
- When investigating discrepancies in curtailment data

#### Error Handling

The script includes retry logic and will attempt to fetch data up to 3 times with exponential backoff before failing. All errors are logged to the console.