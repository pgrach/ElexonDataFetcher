/**
 * DynamoDB Service
 * 
 * This service handles interactions with AWS DynamoDB for data storage and retrieval.
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { 
  DynamoDBDocumentClient, 
  GetCommand, 
  QueryCommand,
  PutCommand
} from '@aws-sdk/lib-dynamodb';
import { BitcoinDifficulty } from '../types/bitcoin';

// Configure DynamoDB client
const client = new DynamoDBClient({
  region: process.env.AWS_REGION || 'eu-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || ''
  }
});

const docClient = DynamoDBDocumentClient.from(client);

/**
 * Get Bitcoin difficulty data for a specific time range
 * 
 * @param startDate ISO date string for start date
 * @param endDate ISO date string for end date
 * @returns Promise resolving to array of difficulty records
 */
export async function getDifficultyData(
  startDate: string, 
  endDate: string
): Promise<BitcoinDifficulty[]> {
  try {
    const tableName = process.env.DYNAMODB_DIFFICULTY_TABLE || 'bitcoin-difficulty';
    
    const command = new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'timestamp BETWEEN :start AND :end',
      ExpressionAttributeValues: {
        ':start': startDate,
        ':end': endDate
      }
    });
    
    const response = await docClient.send(command);
    
    return (response.Items || []) as BitcoinDifficulty[];
  } catch (error) {
    console.error('Error fetching difficulty data from DynamoDB:', error);
    
    // Return empty array if there was an error
    return [];
  }
}

/**
 * Store Bitcoin difficulty data
 * 
 * @param difficulty The difficulty data to store
 * @returns Promise resolving to success status
 */
export async function storeDifficultyData(
  difficulty: BitcoinDifficulty
): Promise<boolean> {
  try {
    const tableName = process.env.DYNAMODB_DIFFICULTY_TABLE || 'bitcoin-difficulty';
    
    const command = new PutCommand({
      TableName: tableName,
      Item: {
        timestamp: difficulty.timestamp,
        difficulty: difficulty.difficulty
      }
    });
    
    await docClient.send(command);
    
    return true;
  } catch (error) {
    console.error('Error storing difficulty data in DynamoDB:', error);
    return false;
  }
}