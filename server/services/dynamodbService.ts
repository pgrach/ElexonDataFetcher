import { DynamoDBClient, DescribeTableCommand, ResourceNotFoundException } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { DEFAULT_DIFFICULTY, DEFAULT_PRICE, DynamoDBHistoricalData } from '../types/bitcoin';
import { parse, format } from 'date-fns';

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

// Get table names from environment variables with fallbacks
const DIFFICULTY_TABLE = process.env.DYNAMODB_DIFFICULTY_TABLE || "asics-dynamodb-DifficultyTable-DQ308ID3POT6";
const PRICES_TABLE = process.env.DYNAMODB_PRICES_TABLE || "asics-dynamodb-PricesTable-1LXU143BUOBN";

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

function formatDateForDynamoDB(dateStr: string): string {
  try {
    const date = parse(dateStr, 'yyyy-MM-dd', new Date());
    return format(date, 'yyyy-MM-dd');
  } catch (error) {
    console.error(`[DynamoDB] Error formatting date ${dateStr}:`, error);
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

async function getHistoricalDifficulty(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDynamoDB(date);
    console.info(`[DynamoDB] Fetching difficulty for date: ${formattedDate}`);

    const tableExists = await verifyTableExists(DIFFICULTY_TABLE);
    if (!tableExists) {
      console.warn(`[DynamoDB] Using default difficulty (${DEFAULT_DIFFICULTY})`);
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

    console.debug('[DynamoDB] Executing difficulty scan:', {
      table: DIFFICULTY_TABLE,
      filter: scanCommand.input.FilterExpression,
      date: formattedDate
    });

    const response = await retryOperation(() => docClient.send(scanCommand));

    if (!response.Items?.length) {
      console.warn(`[DynamoDB] No difficulty data found for ${formattedDate}`);
      return DEFAULT_DIFFICULTY;
    }

    const matchingItem = response.Items.find(item => item.Date === formattedDate);
    if (!matchingItem) {
      console.warn(`[DynamoDB] No exact date match for ${formattedDate}`);
      return DEFAULT_DIFFICULTY;
    }

    const difficulty = Number(matchingItem.Difficulty);
    if (isNaN(difficulty)) {
      console.error(`[DynamoDB] Invalid difficulty value:`, matchingItem.Difficulty);
      return DEFAULT_DIFFICULTY;
    }

    return difficulty;

  } catch (error) {
    console.error('[DynamoDB] Error fetching difficulty:', error);
    return DEFAULT_DIFFICULTY;
  }
}

export async function getHistoricalData(date: string): Promise<DynamoDBHistoricalData> {
  console.info('[DynamoDB] Starting historical data fetch...', {
    date,
    region: process.env.AWS_REGION || 'us-east-1',
    tables: {
      difficulty: DIFFICULTY_TABLE,
      prices: PRICES_TABLE
    }
  });

  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      throw new Error('AWS credentials not configured');
    }

    const [difficulty, price] = await Promise.all([
      getHistoricalDifficulty(date),
      getHistoricalPrice(date)
    ]);

    console.info('[DynamoDB] Retrieved data:', { 
      date,
      difficulty: difficulty === DEFAULT_DIFFICULTY ? 'DEFAULT' : 'FOUND',
      price: price === DEFAULT_PRICE ? 'DEFAULT' : 'FOUND'
    });

    return { difficulty, price };
  } catch (error) {
    console.error('[DynamoDB] Historical data fetch failed:', error);
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

export async function getHistoricalPrice(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDynamoDB(date);
    console.info(`[DynamoDB] Fetching price for date: ${formattedDate}`);

    const tableExists = await verifyTableExists(PRICES_TABLE);
    if (!tableExists) {
      console.warn(`[DynamoDB] Using default price (${DEFAULT_PRICE})`);
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
    console.debug('[DynamoDB] Price response:', {
      items: response.Items?.length,
      date: formattedDate
    });

    if (!response.Items?.length) {
      console.warn(`[DynamoDB] No price data for ${formattedDate}`);
      return DEFAULT_PRICE;
    }

    const price = Number(response.Items[0].Price);
    if (isNaN(price)) {
      console.error(`[DynamoDB] Invalid price value:`, response.Items[0].Price);
      return DEFAULT_PRICE;
    }

    return price;
  } catch (error) {
    console.error('[DynamoDB] Error fetching price:', error);
    return DEFAULT_PRICE;
  }
}