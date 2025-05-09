# BMU Mapping Files

This directory contains important data files for the wind curtailment and Bitcoin calculation system.

## bmuMapping.json

This is the **central** BMU mapping file used by all services in the application. It contains metadata about wind farm Balancing Mechanism Units (BMUs) from the Elexon API, including:

- `nationalGridBmUnit`: National Grid identifier
- `elexonBmUnit`: Elexon API identifier (used as the primary key for identifying wind farms)
- `bmUnitName`: Human-readable name
- `generationCapacity`: The generation capacity in MW
- `fuelType`: Always "WIND" in our filtered dataset
- `leadPartyName`: The company that owns/operates the BMU

## File Maintenance

This file is updated by the `server/scripts/maintenance/updateBmuMapping.ts` script, which fetches the latest BMU data from the Elexon API.

The following services reference this file:
- `server/services/elexon.ts` 
- `server/services/curtailment.ts`
- `server/services/pnDataService.ts`

**Important**: Always use the absolute path reference to maintain consistency across the codebase:
```typescript
const BMU_MAPPING_PATH = path.join(process.cwd(), "server", "data", "bmuMapping.json");
```

Do not create duplicate copies of this file in other directories, as this can lead to inconsistencies in the data processing pipeline.