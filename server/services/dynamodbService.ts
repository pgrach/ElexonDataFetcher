import { DynamoDBClient, DescribeTableCommand, ResourceNotFoundException } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { DEFAULT_DIFFICULTY } from '../types/bitcoin';
import { parse, format } from 'date-fns';

const MAX_RETRIES = 5;
const RETRY_DELAY_MS = 2000; // Increased from 1000 to 2000ms

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

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function retryOperation<T>(operation: () => Promise<T>, attempt = 1): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    if (attempt >= MAX_RETRIES) {
      throw error;
    }

    // Exponential backoff with jitter for DynamoDB
    const delay = Math.min(
      RETRY_DELAY_MS * Math.pow(2, attempt - 1) + Math.random() * 1000,
      30000 // Max delay of 30 seconds
    );

    if (error?.name === 'ProvisionedThroughputExceededException') {
      console.warn(`[DynamoDB] Throughput exceeded on attempt ${attempt}, waiting ${delay}ms before retry`);
    } else {
      console.warn(`[DynamoDB] Attempt ${attempt} failed, waiting ${delay}ms:`, error);
    }

    await sleep(delay);
    return retryOperation(operation, attempt + 1);
  }
}

export async function getDifficultyData(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDifficulty(date);
    console.info(`[DynamoDB] Fetching difficulty for date: ${formattedDate}`);

    const tableExists = await verifyTableExists(DIFFICULTY_TABLE);
    if (!tableExists) {
      console.warn(`[DynamoDB] Table ${DIFFICULTY_TABLE} does not exist, using default difficulty (${DEFAULT_DIFFICULTY})`);
      return DEFAULT_DIFFICULTY;
    }

    // First, scan the table to find records with our date
    const scanCommand = new ScanCommand({
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
      date: formattedDate,
      command: 'ScanCommand'
    });

    const scanResponse = await retryOperation(() => docClient.send(scanCommand));

    if (!scanResponse.Items?.length) {
      console.warn(`[DynamoDB] No difficulty data found for ${formattedDate}, using default: ${DEFAULT_DIFFICULTY}`);
      return DEFAULT_DIFFICULTY;
    }

    // Sort items by date (descending) to get the most recent record if multiple exist
    const sortedItems = scanResponse.Items.sort((a, b) => 
      b.Date.localeCompare(a.Date)
    );

    const difficulty = Number(sortedItems[0].Difficulty);
    console.info(`[DynamoDB] Found historical difficulty for ${formattedDate}:`, {
      difficulty: difficulty.toLocaleString(),
      id: sortedItems[0].ID,
      totalRecords: sortedItems.length
    });

    if (isNaN(difficulty)) {
      console.error(`[DynamoDB] Invalid difficulty value:`, sortedItems[0].Difficulty);
      return DEFAULT_DIFFICULTY;
    }

    return difficulty;

  } catch (error) {
    console.error('[DynamoDB] Error fetching difficulty:', error);
    return DEFAULT_DIFFICULTY;
  }
}