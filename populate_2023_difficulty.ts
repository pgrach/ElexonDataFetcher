/**
 * Populate 2023 Bitcoin Difficulty Data
 * 
 * This script ensures that historical Bitcoin difficulty data is available
 * for all dates in 2023, which is necessary for Bitcoin calculations.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { 
  DynamoDBClient, 
  PutItemCommand, 
  ScanCommand, 
  QueryCommand 
} from "@aws-sdk/client-dynamodb";
import * as fs from "fs";
import path from "path";
import { fileURLToPath } from 'url';

// Get __dirname equivalent in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const DIFFICULTY_TABLE = 'asics-dynamodb-DifficultyTable-DQ308ID3POT6';
const DIFFICULTY_DATA_FILE = path.join(__dirname, 'data', '2023_difficulty_data.json');
const FALLBACK_DIFFICULTY = 30802868313908; // Average difficulty for 2023

// Create the data directory if it doesn't exist
if (!fs.existsSync(path.join(__dirname, 'data'))) {
  fs.mkdirSync(path.join(__dirname, 'data'), { recursive: true });
}

// Initialize DynamoDB client
const dynamoDb = new DynamoDBClient({
  region: process.env.AWS_REGION || 'eu-north-1',
});

/**
 * Format date for DynamoDB
 */
function formatDateForDynamoDB(dateStr: string): string {
  return dateStr.replace(/-/g, '');
}

/**
 * Generate a random UUID
 */
function generateUUID(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * Get dates from 2023 that need difficulty data
 */
async function getDatesNeeding2023Difficulty(): Promise<string[]> {
  console.log("Finding dates in 2023 with curtailment records...");
  
  const result = await db.execute(sql`
    SELECT DISTINCT settlement_date::text
    FROM curtailment_records 
    WHERE settlement_date >= '2023-01-01' 
    AND settlement_date <= '2023-12-31'
    ORDER BY settlement_date
  `);
  
  const dates = result.rows.map((row: any) => row.settlement_date as string);
  console.log(`Found ${dates.length} dates in 2023 with curtailment records`);
  return dates;
}

/**
 * Check if difficulty data exists for a date
 */
async function checkDifficultyExists(date: string): Promise<boolean> {
  const params = {
    TableName: DIFFICULTY_TABLE,
    FilterExpression: '#date = :date',
    ExpressionAttributeNames: {
      '#date': 'Date'
    },
    ExpressionAttributeValues: {
      ':date': { S: date } // Keep date in YYYY-MM-DD format
    }
  };
  
  try {
    const command = new ScanCommand(params);
    const result = await dynamoDb.send(command);
    return (result.Items && result.Items.length > 0);
  } catch (error) {
    console.error(`Error checking difficulty for ${date}:`, error);
    return false;
  }
}

/**
 * Get 2023 difficulty data from file or create it
 */
function get2023DifficultyData(): Record<string, number> {
  // Check if the file exists
  if (fs.existsSync(DIFFICULTY_DATA_FILE)) {
    console.log("Loading 2023 difficulty data from file...");
    const data = fs.readFileSync(DIFFICULTY_DATA_FILE, 'utf8');
    return JSON.parse(data);
  }
  
  // If file doesn't exist, create sample difficulty data
  console.log("Creating default 2023 difficulty data...");
  
  // 2023 Bitcoin difficulty progression (approximate values)
  const difficultyChanges = [
    { date: '2023-01-01', difficulty: 35364525592535 },
    { date: '2023-01-15', difficulty: 37590453630840 },
    { date: '2023-01-29', difficulty: 39156598319174 },
    { date: '2023-02-12', difficulty: 38690832590.08 },
    { date: '2023-02-26', difficulty: 39156598319174 },
    { date: '2023-03-12', difficulty: 40007470271990 },
    { date: '2023-03-26', difficulty: 43054615283601 },
    { date: '2023-04-09', difficulty: 44793919940333 },
    { date: '2023-04-23', difficulty: 47887900302106 },
    { date: '2023-05-07', difficulty: 46930462650409 },
    { date: '2023-05-21', difficulty: 47887900302106 },
    { date: '2023-06-04', difficulty: 48588531889007 },
    { date: '2023-06-18', difficulty: 47887900302106 },
    { date: '2023-07-02', difficulty: 46930462650409 },
    { date: '2023-07-16', difficulty: 47887900302106 },
    { date: '2023-07-30', difficulty: 49452617000268 },
    { date: '2023-08-13', difficulty: 50542939727927 },
    { date: '2023-08-27', difficulty: 53273867359532 },
    { date: '2023-09-10', difficulty: 55602611364103 },
    { date: '2023-09-24', difficulty: 57302914230943 },
    { date: '2023-10-08', difficulty: 55602611364103 },
    { date: '2023-10-22', difficulty: 53273867359532 },
    { date: '2023-11-05', difficulty: 54192105040247 },
    { date: '2023-11-19', difficulty: 53273867359532 },
    { date: '2023-12-03', difficulty: 52587297195279 },
    { date: '2023-12-17', difficulty: 53273867359532 },
    { date: '2023-12-31', difficulty: 51866237721644 }
  ];
  
  // Interpolate difficulty for every day
  const dailyDifficulty: Record<string, number> = {};
  
  // For each period between changes, interpolate values
  for (let i = 0; i < difficultyChanges.length - 1; i++) {
    const startDate = new Date(difficultyChanges[i].date);
    const endDate = new Date(difficultyChanges[i + 1].date);
    const startDifficulty = difficultyChanges[i].difficulty;
    const endDifficulty = difficultyChanges[i + 1].difficulty;
    
    // Calculate difficulty for each day in this period
    const dayCount = Math.round((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
    const difficultyStep = (endDifficulty - startDifficulty) / dayCount;
    
    for (let day = 0; day < dayCount; day++) {
      const currentDate = new Date(startDate);
      currentDate.setDate(startDate.getDate() + day);
      const dateString = currentDate.toISOString().split('T')[0]; // Format as YYYY-MM-DD
      const difficulty = Math.round(startDifficulty + (difficultyStep * day));
      dailyDifficulty[dateString] = difficulty > 0 ? difficulty : FALLBACK_DIFFICULTY;
    }
  }
  
  // Add the last date
  const lastChange = difficultyChanges[difficultyChanges.length - 1];
  dailyDifficulty[lastChange.date] = lastChange.difficulty;
  
  // Save to file
  fs.writeFileSync(DIFFICULTY_DATA_FILE, JSON.stringify(dailyDifficulty, null, 2));
  console.log(`Saved default 2023 difficulty data to ${DIFFICULTY_DATA_FILE}`);
  
  return dailyDifficulty;
}

/**
 * Store difficulty data for a date in DynamoDB
 */
async function storeDifficultyData(date: string, difficulty: number): Promise<boolean> {
  const params = {
    TableName: DIFFICULTY_TABLE,
    Item: {
      'ID': { S: generateUUID() },
      'Date': { S: date }, // Keep the date in YYYY-MM-DD format expected by the service
      'Difficulty': { N: difficulty.toString() }, // Use 'Difficulty' (capital D) to match service expectations
      'price': { N: '25000' }, // Historical price from 2023 (approximate)
      'timestamp': { N: Math.floor(Date.now() / 1000).toString() }
    }
  };
  
  try {
    const command = new PutItemCommand(params);
    await dynamoDb.send(command);
    return true;
  } catch (error) {
    console.error(`Error storing difficulty for ${date}:`, error);
    return false;
  }
}

/**
 * Main function to populate difficulty data
 */
async function main() {
  console.log("Starting 2023 difficulty data population...");
  
  // Get dates that need difficulty data
  const dates = await getDatesNeeding2023Difficulty();
  
  // Load or create difficulty data
  const difficultyData = get2023DifficultyData();
  
  // Statistics
  let existingCount = 0;
  let addedCount = 0;
  let errorCount = 0;
  
  // Sleep function with exponential backoff
  async function sleepWithBackoff(attempt: number): Promise<void> {
    const delay = Math.min(1000 * Math.pow(2, attempt), 10000); // Max 10 seconds
    console.log(`Rate limiting: Waiting ${delay}ms before next request`);
    return new Promise(resolve => setTimeout(resolve, delay));
  }

  // Check and populate difficulty data for each date
  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    let success = false;
    let attempts = 0;
    const maxAttempts = 5;
    
    while (!success && attempts < maxAttempts) {
      try {
        // Add delay with exponential backoff based on attempt number
        if (attempts > 0) {
          await sleepWithBackoff(attempts);
        } else if (i > 0) {
          // Add small delay between requests to avoid rate limiting
          await new Promise(resolve => setTimeout(resolve, 500));
        }
        
        const exists = await checkDifficultyExists(date);
        
        if (exists) {
          console.log(`Difficulty data already exists for ${date}`);
          existingCount++;
          success = true;
        } else {
          // Get difficulty from our data or use fallback
          const difficulty = difficultyData[date] || FALLBACK_DIFFICULTY;
          
          console.log(`Adding difficulty data for ${date}: ${difficulty}`);
          success = await storeDifficultyData(date, difficulty);
          
          if (success) {
            addedCount++;
          } else {
            attempts++;
            console.log(`Failed to store difficulty for ${date}, attempt ${attempts}/${maxAttempts}`);
          }
        }
      } catch (error) {
        attempts++;
        console.error(`Error processing ${date}, attempt ${attempts}/${maxAttempts}:`, error);
        
        // Check if it's a rate limit error
        const errorString = String(error);
        if (errorString.includes('ProvisionedThroughputExceededException')) {
          console.log(`Rate limit exceeded, will retry after backoff`);
        }
      }
    }
    
    if (!success) {
      console.error(`Failed to process ${date} after ${maxAttempts} attempts`);
      errorCount++;
    }
    
    // Progress update
    if ((i + 1) % 10 === 0 || i === dates.length - 1) {
      console.log(`Progress: ${i + 1}/${dates.length} dates processed`);
      console.log(`Existing: ${existingCount}, Added: ${addedCount}, Errors: ${errorCount}`);
    }
    
    // Save progress to file every 10 dates
    if ((i + 1) % 10 === 0) {
      fs.writeFileSync(
        path.join(__dirname, 'data', 'difficulty_progress.json'), 
        JSON.stringify({ 
          lastProcessed: date, 
          existingCount, 
          addedCount, 
          errorCount,
          timestamp: new Date().toISOString()
        }, null, 2)
      );
    }
  }
  
  // Final summary
  console.log("\n===== DIFFICULTY DATA POPULATION COMPLETE =====");
  console.log(`Total dates processed: ${dates.length}`);
  console.log(`Existing records: ${existingCount}`);
  console.log(`Added records: ${addedCount}`);
  console.log(`Errors: ${errorCount}`);
}

// Run the main function
main()
  .catch(error => {
    console.error("Error during difficulty data population:", error);
    process.exit(1);
  })
  .finally(() => {
    console.log("Script finished");
  });