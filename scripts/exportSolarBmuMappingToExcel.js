/**
 * Export Solar BMU Mapping to Excel/CSV
 * 
 * This script converts the JSON solar BMU mapping file to CSV format
 * for easier viewing and analysis in spreadsheet applications.
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Path to the JSON file
const SOLAR_BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'solarBmuMapping.json');
const OUTPUT_PATH = path.join(process.cwd(), 'solar_bmu_mapping.csv');

async function exportToCSV() {
  try {
    console.log('Reading Solar BMU mapping from:', SOLAR_BMU_MAPPING_PATH);
    
    // Read the JSON file
    const mappingContent = fs.readFileSync(SOLAR_BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    console.log(`Loaded ${bmuMapping.length} Solar BMU mappings`);
    
    // Create CSV header
    const csvHeader = 'National Grid BMU ID,Elexon BMU ID,BMU Name,Generation Capacity (MW),Fuel Type,Lead Party Name\n';
    
    // Create CSV rows
    const csvRows = bmuMapping.map(bmu => {
      // Handle potential commas in fields by enclosing in quotes
      const nationalGridBmuId = `"${bmu.nationalGridBmUnit}"`;
      const elexonBmuId = `"${bmu.elexonBmUnit}"`;
      const bmuName = `"${bmu.bmUnitName.replace(/"/g, '""')}"`;  // Replace quotes with double quotes (CSV standard)
      const generationCapacity = bmu.generationCapacity;
      const fuelType = `"${bmu.fuelType}"`;
      const leadPartyName = `"${bmu.leadPartyName.replace(/"/g, '""')}"`;
      
      return `${nationalGridBmuId},${elexonBmuId},${bmuName},${generationCapacity},${fuelType},${leadPartyName}`;
    }).join('\n');
    
    // Combine header and rows
    const csvContent = csvHeader + csvRows;
    
    // Write to CSV file
    fs.writeFileSync(OUTPUT_PATH, csvContent, 'utf8');
    
    console.log(`Successfully exported Solar BMU mapping to CSV at ${OUTPUT_PATH}`);
    console.log('You can now download this file and open it in Excel');
    
  } catch (error) {
    console.error('Error exporting Solar BMU mapping to CSV:', error);
  }
}

// Run the export function
exportToCSV();