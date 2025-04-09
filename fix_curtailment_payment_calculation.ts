/**
 * Fix Curtailment Payment Calculation Script
 * 
 * This script updates the curtailment_enhanced.ts file to fix the payment calculation
 * by adding a -1 multiplier to the payment calculation, ensuring that
 * payment values are properly signed (negative) for curtailment records.
 */

import fs from 'fs/promises';
import path from 'path';

const CURTAILMENT_SERVICE_PATH = './server/services/curtailment_enhanced.ts';

async function fixPaymentCalculation() {
  try {
    console.log(`\n=== Fixing Curtailment Payment Calculation ===\n`);
    console.log(`Updating file: ${CURTAILMENT_SERVICE_PATH}`);
    
    // Read the current file
    const fileContent = await fs.readFile(CURTAILMENT_SERVICE_PATH, 'utf8');
    
    // Look for the payment calculation line
    const OLD_CALCULATION = `const payment = volume * record.originalPrice;`;
    const NEW_CALCULATION = `const payment = volume * record.originalPrice * -1; // Multiply by -1 to ensure payment is negative`;
    
    if (!fileContent.includes(OLD_CALCULATION)) {
      console.log(`\n⚠️ Could not find the payment calculation line to update.`);
      console.log(`Please manually update the payment calculation in ${CURTAILMENT_SERVICE_PATH}.`);
      return;
    }
    
    // Replace the calculation
    const updatedContent = fileContent.replace(
      OLD_CALCULATION,
      NEW_CALCULATION
    );
    
    // Write the updated file
    await fs.writeFile(`${CURTAILMENT_SERVICE_PATH}.backup`, fileContent, 'utf8');
    console.log(`✅ Backup created at ${CURTAILMENT_SERVICE_PATH}.backup`);
    
    await fs.writeFile(CURTAILMENT_SERVICE_PATH, updatedContent, 'utf8');
    console.log(`✅ Payment calculation updated successfully!`);
    
    console.log(`\nThe payment calculation was changed from:`);
    console.log(`  ${OLD_CALCULATION}`);
    console.log(`to:`);
    console.log(`  ${NEW_CALCULATION}`);
    
    console.log(`\nTo apply this change to existing data, you'll need to re-run the processDailyCurtailment`);
    console.log(`function for affected dates.`);
    
  } catch (error) {
    console.error('Error updating payment calculation:', error);
  }
}

fixPaymentCalculation().catch(console.error);