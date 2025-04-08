import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';
import { db } from './db';
import { sql } from 'drizzle-orm';

// Format date for the difficulty table key
function formatDateForDifficulty(dateStr: string): string {
  const date = new Date(dateStr);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

// Get difficulty data from DynamoDB
async function getDifficultyData(date: string): Promise<number> {
  const client = new DynamoDBClient({});
  const docClient = DynamoDBDocumentClient.from(client);
  
  const formattedDate = formatDateForDifficulty(date);
  console.log(`Getting difficulty for date: ${formattedDate}`);
  
  try {
    const command = new GetCommand({
      TableName: "bitcoin_network_difficulty",
      Key: {
        date: formattedDate
      }
    });
    
    const response = await docClient.send(command);
    
    if (response.Item && response.Item.difficulty) {
      console.log(`DynamoDB difficulty for ${formattedDate}: ${response.Item.difficulty}`);
      return Number(response.Item.difficulty);
    } else {
      console.log(`No difficulty data found for ${formattedDate} in DynamoDB`);
      return 0;
    }
  } catch (error) {
    console.error(`Error getting difficulty data from DynamoDB for ${formattedDate}:`, error);
    return 0;
  }
}

async function main() {
  try {
    console.log('Checking difficulties for March 2025...');
    
    // Get unique dates in March 2025 from historical_bitcoin_calculations
    const datesQuery = `
      SELECT DISTINCT settlement_date
      FROM historical_bitcoin_calculations
      WHERE settlement_date >= '2025-03-01' AND settlement_date <= '2025-03-31'
      ORDER BY settlement_date
    `;
    
    const datesResult = await db.execute(sql.raw(datesQuery));
    const dates = datesResult.rows.map(row => row.settlement_date);
    
    console.log(`Found ${dates.length} unique dates in March 2025`);
    
    for (const date of dates) {
      // Get difficulty from database
      const dbDifficultyQuery = `
        SELECT DISTINCT difficulty
        FROM historical_bitcoin_calculations
        WHERE settlement_date = '${date}'
        LIMIT 1
      `;
      
      const dbDifficultyResult = await db.execute(sql.raw(dbDifficultyQuery));
      const dbDifficulty = dbDifficultyResult.rows[0]?.difficulty;
      
      // Get difficulty from DynamoDB
      const dynamoDbDifficulty = await getDifficultyData(date);
      
      console.log(`Date: ${date}`);
      console.log(`  Database difficulty: ${dbDifficulty}`);
      console.log(`  DynamoDB difficulty: ${dynamoDbDifficulty}`);
      
      if (String(dynamoDbDifficulty) !== String(dbDifficulty)) {
        console.log(`  *** MISMATCH *** for ${date}`);
      } else {
        console.log(`  Difficulties match`);
      }
      
      console.log();
    }
    
    console.log('Done checking difficulties');
  } catch (error) {
    console.error('Error during difficulty check:', error);
  }
}

main();