import { DynamoDBClient, DescribeTableCommand, ResourceNotFoundException } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { DEFAULT_DIFFICULTY } from '../types/bitcoin';
import { parse, format } from 'date-fns';

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

// Get table name from environment variable with fallback
const DIFFICULTY_TABLE = process.env.DYNAMODB_DIFFICULTY_TABLE || "asics-dynamodb-DifficultyTable-DQ308ID3POT6";

// Initialize DynamoDB client with better configuration
const client = new DynamoDBClient({ 
  region: process.env.AWS_REGION || "us-east-1",
  logger: {
    debug: (...args) => console.debug('[DynamoDB Debug]', ...args),
    info: (...args) => console.info('[DynamoDB Info]', ...args),
    warn: (...args) => console.warn('[DynamoDB Warning]', ...args),
    error: (...args) => console.error('[DynamoDB Error]', ...args)
  },
  maxAttempts: MAX_RETRIES,
  retryMode: 'standard'
});

const docClient = DynamoDBDocumentClient.from(client, {
  marshallOptions: {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  },
});

function formatDateForDifficulty(dateStr: string): string {
  try {
    const date = parse(dateStr, 'yyyy-MM-dd', new Date());
    return format(date, 'yyyy-MM-dd');
  } catch (error) {
    console.error(`[DynamoDB] Error formatting difficulty date ${dateStr}:`, error);
    throw new Error(`Invalid date format: ${dateStr}. Expected format: YYYY-MM-DD`);
  }
}

async function verifyTableExists(tableName: string): Promise<boolean> {
  try {
    const command = new DescribeTableCommand({ TableName: tableName });
    const response = await client.send(command);
    console.info(`[DynamoDB] Table ${tableName} status:`, {
      status: response.Table?.TableStatus,
      itemCount: response.Table?.ItemCount,
      keySchema: response.Table?.KeySchema?.map(k => ({ name: k.AttributeName, type: k.KeyType }))
    });
    return true;
  } catch (error) {
    if (error instanceof ResourceNotFoundException) {
      console.error(`[DynamoDB] Table ${tableName} does not exist`);
      return false;
    }
    console.error(`[DynamoDB] Error verifying table ${tableName}:`, error);
    throw error;
  }
}

async function retryOperation<T>(operation: () => Promise<T>, attempt = 1): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    if (attempt >= MAX_RETRIES) {
      throw error;
    }

    console.warn(`[DynamoDB] Attempt ${attempt} failed, retrying in ${RETRY_DELAY_MS * attempt}ms:`, error);
    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * attempt));
    return retryOperation(operation, attempt + 1);
  }
}

export async function getDifficultyData(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDifficulty(date);
    console.info(`[DynamoDB] Fetching difficulty for date: ${formattedDate}`);

    const tableExists = await verifyTableExists(DIFFICULTY_TABLE);
    if (!tableExists) {
      console.warn(`[DynamoDB] Using default difficulty (${DEFAULT_DIFFICULTY})`);
      return DEFAULT_DIFFICULTY;
    }

    const command = new ScanCommand({
      TableName: DIFFICULTY_TABLE,
      FilterExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date"
      },
      ExpressionAttributeValues: {
        ":date": formattedDate
      }
    });

    console.debug('[DynamoDB] Executing difficulty scan:', {
      table: DIFFICULTY_TABLE,
      filter: command.input.FilterExpression,
      date: formattedDate
    });

    const response = await retryOperation(() => docClient.send(command));

    if (!response.Items?.length) {
      console.warn(`[DynamoDB] No difficulty data found for ${formattedDate}`);
      return DEFAULT_DIFFICULTY;
    }

    const difficulty = Number(response.Items[0].Difficulty);
    if (isNaN(difficulty)) {
      console.error(`[DynamoDB] Invalid difficulty value:`, response.Items[0].Difficulty);
      return DEFAULT_DIFFICULTY;
    }

    return difficulty;

  } catch (error) {
    console.error('[DynamoDB] Error fetching difficulty:', error);
    return DEFAULT_DIFFICULTY;
  }
}