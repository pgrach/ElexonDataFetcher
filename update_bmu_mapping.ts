/**
 * Update BMU Mapping
 * 
 * This script copies the server's BMU mapping file to the data directory
 * to ensure all scripts are using the most up-to-date mapping information.
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import { fileURLToPath } from 'url';

// Get the current module's filename and directory path for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function updateBmuMapping(): Promise<boolean> {
  try {
    console.log('Updating BMU mapping file...');
    
    // Check if source file exists
    const sourcePath = path.join('server', 'data', 'bmuMapping.json');
    const targetPath = path.join('data', 'bmu_mapping.json');
    
    try {
      await fs.access(sourcePath);
    } catch (error) {
      console.error(`Source file doesn't exist: ${sourcePath}`);
      return false;
    }
    
    // Backup the existing file if it exists
    try {
      const fileStats = await fs.stat(targetPath);
      if (fileStats.isFile()) {
        // Create backup with timestamp
        const backupPath = `${targetPath}.backup.${new Date().toISOString()}`;
        await fs.copyFile(targetPath, backupPath);
        console.log(`Created backup of existing file at: ${backupPath}`);
      }
    } catch (error) {
      console.log('No existing BMU mapping file to backup');
    }
    
    // Copy the server version to the data directory
    await fs.copyFile(sourcePath, targetPath);
    
    // Verify the copy by comparing file sizes
    const sourceStats = await fs.stat(sourcePath);
    const targetStats = await fs.stat(targetPath);
    
    if (sourceStats.size === targetStats.size) {
      console.log('✅ BMU mapping updated successfully!');
      
      // Load both files to count BMU IDs
      const sourceContent = await fs.readFile(sourcePath, 'utf-8');
      const targetContent = await fs.readFile(targetPath, 'utf-8');
      
      const sourceMapping = JSON.parse(sourceContent);
      const targetMapping = JSON.parse(targetContent);
      
      console.log(`Source BMU mapping has ${Object.keys(sourceMapping).length} entries`);
      console.log(`Updated BMU mapping has ${Object.keys(targetMapping).length} entries`);
      
      return true;
    } else {
      console.error('❌ File sizes don\'t match after copy. Update may be incomplete.');
      return false;
    }
  } catch (error) {
    console.error('Error updating BMU mapping:', error);
    return false;
  }
}

async function main() {
  try {
    const success = await updateBmuMapping();
    
    if (success) {
      console.log('\nNext steps:');
      console.log('1. Run data verification for a specific date:');
      console.log('   npx tsx verify_and_fix_data.ts YYYY-MM-DD');
      console.log('2. Fix data for a specific date if needed:');
      console.log('   npx tsx verify_and_fix_data.ts YYYY-MM-DD fix');
    } else {
      console.error('\nUpdate failed. Check the error messages above.');
      process.exit(1);
    }
  } catch (error) {
    console.error('Error in update_bmu_mapping:', error);
    process.exit(1);
  }
}

// Run main if this file is being executed directly (not imported)
// ES module equivalent of the CommonJS `require.main === module` check
if (import.meta.url === `file://${__filename}`) {
  main();
}