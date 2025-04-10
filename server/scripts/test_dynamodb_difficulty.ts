/**
 * Test script to verify DynamoDB difficulty values
 */

import { getDifficultyData } from "../services/dynamodbService";
import fs from 'fs';
import path from 'path';

// Target date to test
const TARGET_DATE = "2025-04-01";

// Create a log file
const LOG_DIR = path.join(process.cwd(), '../logs');
const LOG_FILE = path.join(LOG_DIR, `dynamodb_test_${new Date().toISOString().replace(/:/g, '-')}.log`);

if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// Log helper function
function logMessage(message: string) {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] ${message}\n`;
  
  // Log to console and file
  console.log(message);
  fs.appendFileSync(LOG_FILE, logEntry);
}

async function main() {
  try {
    logMessage(`Testing DynamoDB difficulty retrieval for ${TARGET_DATE}...`);
    
    // Get difficulty from DynamoDB
    const difficulty = await getDifficultyData(TARGET_DATE);
    
    logMessage(`\n==============================`);
    logMessage(`RESULT FOR ${TARGET_DATE}:`);
    logMessage(`Difficulty: ${difficulty}`);
    logMessage(`Current DB Value: 113757508810853`);
    logMessage(`Same Value: ${difficulty === 113757508810853 ? 'YES' : 'NO - NEEDS UPDATE'}`);
    logMessage(`==============================\n`);
    
    // Test a few more dates for comparison
    const testDates = ["2025-03-01", "2025-02-15", "2025-01-01"];
    
    logMessage("Testing additional dates for comparison:");
    
    for (const date of testDates) {
      const difficultyValue = await getDifficultyData(date);
      logMessage(`- ${date}: ${difficultyValue}`);
    }
    
    logMessage(`\nLog file created at: ${LOG_FILE}`);
    
  } catch (error) {
    console.error("Error testing DynamoDB:", error);
  }
}

// Run the test
main()
  .then(() => {
    console.log("Test completed successfully");
    process.exit(0);
  })
  .catch(error => {
    console.error("Test failed:", error);
    process.exit(1);
  });