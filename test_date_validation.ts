/**
 * Test Date Validation
 * 
 * This script tests the date validation logic in processDate and fixDateComprehensive
 */

import { processDate } from './optimized_critical_date_processor';
import { fixDateComprehensive } from './daily_reconciliation_check';

async function main() {
  console.log("=== Testing date validation ===");
  
  // Test 1: Invalid date (number only)
  console.log("\nTest 1: Invalid date (number only)");
  const test1 = await processDate("1");
  console.log("processDate result:", test1);
  
  // Test 2: Invalid date with fixDateComprehensive
  console.log("\nTest 2: Invalid date with fixDateComprehensive");
  const test2 = await fixDateComprehensive("2");
  console.log("fixDateComprehensive result:", test2);
  
  // Test 3: Valid date format
  console.log("\nTest 3: Valid date format");
  const test3 = await processDate("2025-04-02", 1, 1); // Only process period 1 to be quick
  console.log("processDate result:", test3);
  
  // Test 4: Valid date format with dashes
  console.log("\nTest 4: Valid date format with fixDateComprehensive");
  const test4 = await fixDateComprehensive("2025-04-01");
  console.log("fixDateComprehensive result:", test4);
  
  console.log("\n=== Test complete ===");
}

main().catch(error => {
  console.error("Test failed:", error);
});