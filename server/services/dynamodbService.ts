import { DynamoDBClient, DescribeTableCommand, ResourceNotFoundException } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { DEFAULT_DIFFICULTY, DEFAULT_PRICE, DynamoDBHistoricalData } from '../types/bitcoin';
import { parse, format } from 'date-fns';

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

// Default table names - these should match your actual DynamoDB table names
const DIFFICULTY_TABLE = "asics-dynamodb-DifficultyTable-DQ308ID3POT6";
const PRICES_TABLE = "asics-dynamodb-PricesTable-1LXU143BUOBN";

const client = new DynamoDBClient({ 
  region: process.env.AWS_REGION || "us-east-1",
  logger: console,
  maxAttempts: MAX_RETRIES 
});

const docClient = DynamoDBDocumentClient.from(client, {
  marshallOptions: {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  },
});

function formatDateForDynamoDB(dateStr: string): string {
  try {
    const date = parse(dateStr, 'yyyy-MM-dd', new Date());
    return format(date, 'yyyy-MM-dd');
  } catch (error) {
    console.error(`[DynamoDB] Error formatting date ${dateStr}:`, error);
    throw new Error(`Invalid date format: ${dateStr}`);
  }
}

async function verifyTableExists(tableName: string): Promise<boolean> {
  try {
    const command = new DescribeTableCommand({ TableName: tableName });
    const response = await client.send(command);
    console.log(`[DynamoDB] Table ${tableName} exists:`, {
      status: response.Table?.TableStatus,
      itemCount: response.Table?.ItemCount,
      keySchema: response.Table?.KeySchema
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

async function getHistoricalDifficulty(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDynamoDB(date);
    console.log(`[DynamoDB] Fetching difficulty for date: ${formattedDate} from table: ${DIFFICULTY_TABLE}`);

    // First verify table exists
    const tableExists = await verifyTableExists(DIFFICULTY_TABLE);
    if (!tableExists) {
      console.warn(`[DynamoDB] Difficulty table ${DIFFICULTY_TABLE} not found, using default value`);
      return DEFAULT_DIFFICULTY;
    }

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

    console.log('[DynamoDB] Executing scan with parameters:', {
      TableName: scanCommand.input.TableName,
      FilterExpression: scanCommand.input.FilterExpression,
      ExpressionAttributeValues: scanCommand.input.ExpressionAttributeValues
    });

    const response = await retryOperation(() => docClient.send(scanCommand));
    console.log('[DynamoDB] Scan response:', {
      itemCount: response.Count,
      scannedCount: response.ScannedCount,
      hasItems: response.Items && response.Items.length > 0,
      firstItem: response.Items && response.Items.length > 0 ? {
        keys: Object.keys(response.Items[0]),
        date: response.Items[0].Date,
        difficulty: response.Items[0].Difficulty
      } : null
    });

    if (!response.Items || response.Items.length === 0) {
      console.warn(`[DynamoDB] No difficulty data found for date: ${formattedDate}, using default value: ${DEFAULT_DIFFICULTY}`);
      return DEFAULT_DIFFICULTY;
    }

    const matchingItem = response.Items.find(item => item.Date === formattedDate);
    if (!matchingItem) {
      console.warn(`[DynamoDB] No exact date match found for: ${formattedDate}`);
      return DEFAULT_DIFFICULTY;
    }

    const difficulty = Number(matchingItem.Difficulty);
    if (isNaN(difficulty)) {
      console.error(`[DynamoDB] Invalid difficulty value:`, matchingItem.Difficulty);
      return DEFAULT_DIFFICULTY;
    }

    return difficulty;

  } catch (error) {
    if (error instanceof ResourceNotFoundException) {
      console.error('[DynamoDB] Difficulty table not found, using default value');
      return DEFAULT_DIFFICULTY;
    }
    console.error('[DynamoDB] Error fetching historical difficulty:', error);
    return DEFAULT_DIFFICULTY;
  }
}

export async function getHistoricalData(date: string): Promise<DynamoDBHistoricalData> {
  console.log('[DynamoDB] Starting historical data fetch...', {
    date,
    awsCredentialsPresent: {
      accessKey: !!process.env.AWS_ACCESS_KEY_ID,
      secretKey: !!process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION || 'us-east-1'
    },
    tables: {
      difficulty: DIFFICULTY_TABLE,
      prices: PRICES_TABLE
    }
  });

  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      console.error('[DynamoDB] Missing AWS credentials');
      throw new Error('AWS credentials not found');
    }

    const [difficulty, price] = await Promise.all([
      getHistoricalDifficulty(date),
      getHistoricalPrice(date)
    ]);

    console.log('[DynamoDB] Retrieved data:', { 
      date,
      difficulty,
      price,
      isDefaultDifficulty: difficulty === DEFAULT_DIFFICULTY,
      isDefaultPrice: price === DEFAULT_PRICE,
      difficultyTable: DIFFICULTY_TABLE,
      pricesTable: PRICES_TABLE
    });

    return { difficulty, price };
  } catch (error) {
    console.error('[DynamoDB] Error in getHistoricalData:', error);
    throw error;
  }
}

async function retryOperation<T>(operation: () => Promise<T>): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;
      console.warn(`[DynamoDB] Attempt ${attempt} failed:`, error);
      if (attempt < MAX_RETRIES) {
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * attempt));
      }
    }
  }

  throw lastError;
}

export async function getHistoricalPrice(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDynamoDB(date);
    console.log(`[DynamoDB] Fetching price for date: ${formattedDate} from table: ${PRICES_TABLE}`);

    // First verify table exists
    const tableExists = await verifyTableExists(PRICES_TABLE);
    if (!tableExists) {
      console.warn(`[DynamoDB] Prices table ${PRICES_TABLE} not found, using default value`);
      return DEFAULT_PRICE;
    }

    const command = new QueryCommand({
      TableName: PRICES_TABLE,
      KeyConditionExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date"
      },
      ExpressionAttributeValues: {
        ":date": formattedDate
      }
    });

    const response = await docClient.send(command);
    console.log('[DynamoDB] Raw price response:', JSON.stringify(response, null, 2));

    if (!response.Items || response.Items.length === 0) {
      console.warn(`[DynamoDB] No price data found for date: ${formattedDate}, using default value: ${DEFAULT_PRICE}`);
      return DEFAULT_PRICE;
    }

    const price = Number(response.Items[0].Price);
    if (isNaN(price)) {
      console.error(`[DynamoDB] Invalid price value in response:`, response.Items[0].Price);
      return DEFAULT_PRICE;
    }

    return price;
  } catch (error) {
    if (error instanceof ResourceNotFoundException) {
      console.error('[DynamoDB] Prices table not found, using default value');
      return DEFAULT_PRICE;
    }
    console.error('[DynamoDB] Error fetching historical price:', error);
    return DEFAULT_PRICE;
  }
}