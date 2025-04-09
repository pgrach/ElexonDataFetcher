import { DEFAULT_DIFFICULTY } from './server/types/bitcoin';
import { getDifficultyData } from './server/services/dynamodbService';

// Add hook to capture console output
const originalConsoleInfo = console.info;
console.info = function() {
  // Filter out AWS SDK debugging info
  if (arguments[0] && typeof arguments[0] === 'string' && 
      (arguments[0].includes('[DynamoDB] Found historical difficulty') || 
       arguments[0].includes('Using default'))) {
    console.log('\n*** IMPORTANT INFO CAPTURED ***');
    console.log(...arguments);
    console.log('*** END CAPTURED INFO ***\n');
  }
  originalConsoleInfo.apply(console, arguments);
};

async function checkMarch31Difficulty() {
  try {
    const date = '2025-03-31';
    console.log(`Checking DynamoDB difficulty for ${date}`);
    
    // Get the difficulty from DynamoDB using the actual service
    console.log('Calling getDifficultyData...');
    const difficulty = await getDifficultyData(date);
    
    console.log(`\nRESULTS SUMMARY FOR ${date}:`);
    console.log('==========================================================');
    console.log(`DynamoDB difficulty: ${difficulty}`);
    
    // Check if it's using the default value
    if (difficulty === DEFAULT_DIFFICULTY) {
      console.log(`! Using DEFAULT_DIFFICULTY (${DEFAULT_DIFFICULTY}) - DynamoDB query likely failed`);
    } else {
      console.log(`✓ Using actual DynamoDB value: ${difficulty}`);
    }
    
    // Compare with the value we've been using
    const expectedDifficulty = 113757508810853;
    
    if (difficulty === expectedDifficulty) {
      console.log(`✓ MATCH: DynamoDB difficulty matches expected value (${expectedDifficulty})`);
    } else {
      console.log(`✗ MISMATCH: DynamoDB difficulty (${difficulty}) does not match expected value (${expectedDifficulty})`);
      console.log(`  Difference: ${difficulty - expectedDifficulty}`);
    }
    console.log('==========================================================');
  } catch (error) {
    console.error('Error checking difficulty:', error);
  }
}

checkMarch31Difficulty();