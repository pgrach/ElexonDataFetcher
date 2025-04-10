/**
 * BMU Mapping Fix Script
 * 
 * This script resolves inconsistencies between different BMU mapping files in the system.
 * It ensures that the comprehensive mapping from server/data/bmuMapping.json is correctly
 * synchronized with data/bmu_mapping.json, which is used by different parts of the system.
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import { fileURLToPath } from 'url';

// Get the directory of the current file
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Define paths to the relevant files
const SERVER_BMU_PATH = path.join(__dirname, '..', 'server', 'data', 'bmuMapping.json');
const ROOT_BMU_PATH = path.join(__dirname, '..', 'data', 'bmu_mapping.json');

// Backup file path with timestamp
const BACKUP_PATH = path.join(
  __dirname, 
  '..', 
  'data', 
  `bmu_mapping.json.backup.${new Date().toISOString().replace(/:/g, '-')}`
);

async function fixBmuMapping() {
  console.log('=== BMU Mapping Fix Script ===');
  
  try {
    // Step 1: Check if both files exist
    const [serverExists, rootExists] = await Promise.all([
      fs.access(SERVER_BMU_PATH).then(() => true).catch(() => false),
      fs.access(ROOT_BMU_PATH).then(() => true).catch(() => false)
    ]);
    
    if (!serverExists) {
      throw new Error(`Server BMU mapping file not found at: ${SERVER_BMU_PATH}`);
    }
    
    // Step 2: Read the server/data/bmuMapping.json file
    console.log(`Reading comprehensive BMU mapping from: ${SERVER_BMU_PATH}`);
    const serverBmuContent = await fs.readFile(SERVER_BMU_PATH, 'utf8');
    let serverBmuData;
    
    try {
      serverBmuData = JSON.parse(serverBmuContent);
      console.log(`Successfully parsed server BMU data: ${serverBmuData.length} entries`);
    } catch (parseError) {
      throw new Error(`Failed to parse server BMU data: ${parseError}`);
    }
    
    // Step 3: Backup existing data/bmu_mapping.json if it exists
    if (rootExists) {
      console.log(`Creating backup of existing data/bmu_mapping.json to: ${BACKUP_PATH}`);
      await fs.copyFile(ROOT_BMU_PATH, BACKUP_PATH);
      console.log('Backup created successfully');
      
      // Read existing data
      const rootBmuContent = await fs.readFile(ROOT_BMU_PATH, 'utf8');
      let rootBmuData;
      
      try {
        rootBmuData = JSON.parse(rootBmuContent);
        console.log(`Existing data/bmu_mapping.json contains ${rootBmuData.length} entries`);
      } catch (parseError) {
        console.warn(`Could not parse existing data/bmu_mapping.json: ${parseError}`);
        console.warn('Will overwrite with server data');
      }
    } else {
      console.log('No existing data/bmu_mapping.json found, will create new file');
    }
    
    // Step 4: Ensure data directory exists
    await fs.mkdir(path.dirname(ROOT_BMU_PATH), { recursive: true });
    
    // Step 5: Write the server BMU data to the root BMU file
    console.log(`Writing comprehensive BMU data to: ${ROOT_BMU_PATH}`);
    await fs.writeFile(ROOT_BMU_PATH, serverBmuContent, 'utf8');
    
    // Step 6: Verify both files are now identical
    const rootBmuDataAfterSync = await fs.readFile(ROOT_BMU_PATH, 'utf8');
    
    if (rootBmuDataAfterSync === serverBmuContent) {
      console.log('✅ BMU mapping files successfully synchronized!');
      console.log(`Both files now contain ${serverBmuData.length} BMU entries`);
    } else {
      throw new Error('Files do not match after synchronization!');
    }
    
    console.log('\nFix summary:');
    console.log(`- Source: ${SERVER_BMU_PATH} (${serverBmuData.length} entries)`);
    console.log(`- Target: ${ROOT_BMU_PATH} (${serverBmuData.length} entries after sync)`);
    if (rootExists) {
      console.log(`- Backup: ${BACKUP_PATH}`);
    }
    console.log('\nNow you can run the reprocessing script for the desired date.');
    
  } catch (error) {
    console.error('Error fixing BMU mapping:', error);
    process.exit(1);
  }
}

// Execute the fix
fixBmuMapping().catch(console.error);