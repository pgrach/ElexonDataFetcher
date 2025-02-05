import { DynamoDBClient, DescribeTableCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { DEFAULT_DIFFICULTY, DEFAULT_PRICE, DynamoDBHistoricalData } from '../types/bitcoin';
import { parse, format } from 'date-fns';

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

const client = new DynamoDBClient({ 
  region: "us-east-1",
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

const DIFFICULTY_TABLE = "asics-dynamodb-DifficultyTable-DQ308ID3POT6";
const PRICES_TABLE = "asics-dynamodb-PricesTable-1LXU143BUOBN";

function formatDateForDynamoDB(dateStr: string): string {
  try {
    const date = parse(dateStr, 'yyyy-MM-dd', new Date());
    return format(date, 'yyyy-MM-dd');
  } catch (error) {
    console.error(`[DynamoDB] Error formatting date ${dateStr}:`, error);
    throw new Error(`Invalid date format: ${dateStr}`);
  }
}

async function getHistoricalDifficulty(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDynamoDB(date);
    console.log(`[DynamoDB] Fetching difficulty for date: ${formattedDate} from table: ${DIFFICULTY_TABLE}`);

    // First, try using Scan with a filter on the Date field
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

    // Find the matching item and extract difficulty
    const matchingItem = response.Items.find(item => item.Date === formattedDate);
    if (!matchingItem) {
      console.warn(`[DynamoDB] No exact date match found for: ${formattedDate}`);
      return DEFAULT_DIFFICULTY;
    }

    console.log('[DynamoDB] Found matching item:', {
      date: matchingItem.Date,
      difficulty: matchingItem.Difficulty,
      id: matchingItem.ID
    });

    const difficulty = Number(matchingItem.Difficulty);
    if (isNaN(difficulty)) {
      console.error(`[DynamoDB] Invalid difficulty value:`, matchingItem.Difficulty);
      return DEFAULT_DIFFICULTY;
    }

    console.log(`[DynamoDB] Retrieved difficulty for ${formattedDate}: ${difficulty}`);
    return difficulty;

  } catch (error) {
    console.error('[DynamoDB] Error fetching historical difficulty:', error);
    if (error instanceof Error) {
      console.error('[DynamoDB] Error details:', {
        name: error.name,
        message: error.message,
        stack: error.stack
      });
    }
    return DEFAULT_DIFFICULTY;
  }
}

export async function getDiagnosticData(date: string): Promise<void> {
  try {
    console.log('\n[DynamoDB Diagnostic] Starting diagnostic check...');

    const describeCommand = new DescribeTableCommand({ TableName: DIFFICULTY_TABLE });
    const tableDescription = await client.send(describeCommand);
    console.log('\n[DynamoDB Diagnostic] Table Description:', {
      tableName: tableDescription.Table?.TableName,
      keySchema: tableDescription.Table?.KeySchema,
      attributeDefinitions: tableDescription.Table?.AttributeDefinitions,
      itemCount: tableDescription.Table?.ItemCount,
      tableStatus: tableDescription.Table?.TableStatus,
    });

    const formattedDate = formatDateForDynamoDB(date);

    // Try scanning with minimal filtering to see all data
    const scanCommand = new ScanCommand({
      TableName: DIFFICULTY_TABLE,
      Limit: 5  // Limit to 5 items for diagnostic purposes
    });

    console.log('[DynamoDB Diagnostic] Executing diagnostic scan');
    const scanResponse = await docClient.send(scanCommand);

    console.log('[DynamoDB Diagnostic] Sample data from table:', {
      count: scanResponse.Count,
      scannedCount: scanResponse.ScannedCount,
      items: scanResponse.Items?.slice(0, 2)  // Show first 2 items
    });

    // Now try specific date query
    const dateFilterScan = new ScanCommand({
      TableName: DIFFICULTY_TABLE,
      FilterExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date"
      },
      ExpressionAttributeValues: {
        ":date": formattedDate
      }
    });

    console.log(`[DynamoDB Diagnostic] Searching for date: ${formattedDate}`);
    const dateResponse = await docClient.send(dateFilterScan);

    console.log('[DynamoDB Diagnostic] Date query results:', {
      count: dateResponse.Count,
      scannedCount: dateResponse.ScannedCount,
      items: dateResponse.Items
    });

  } catch (error) {
    console.error('[DynamoDB Diagnostic] Error during diagnostic:', error);
    if (error instanceof Error) {
      console.error('Error details:', {
        name: error.name,
        message: error.message,
        stack: error.stack
      });
    }
  }
}

export async function getHistoricalData(date: string): Promise<DynamoDBHistoricalData> {
  console.log('[DynamoDB] Fetching both difficulty and price data...');
  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      console.error('[DynamoDB] Missing AWS credentials');
      throw new Error('AWS credentials not found');
    }

    await getDiagnosticData(date);

    const [difficulty, price] = await Promise.all([
      getHistoricalDifficulty(date),
      getHistoricalPrice(date)
    ]);

    console.log('[DynamoDB] Retrieved data:', { 
      date,
      difficulty,
      price,
      isDefaultDifficulty: difficulty === DEFAULT_DIFFICULTY,
      isDefaultPrice: price === DEFAULT_PRICE
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

    const command = new QueryCommand({
      TableName: PRICES_TABLE,
      KeyConditionExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date",
      },
      ExpressionAttributeValues: {
        ":date": formattedDate,
      },
      Limit: 1,
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

    console.log(`[DynamoDB] Retrieved price for ${formattedDate}: ${price}`);
    return price;
  } catch (error) {
    console.error('[DynamoDB] Error fetching historical price:', error);
    if (error instanceof Error) {
      console.error('[DynamoDB] Error details:', error.message);
      console.error('[DynamoDB] Error stack:', error.stack);
    }
    console.warn(`[DynamoDB] Using default price value: ${DEFAULT_PRICE}`);
    return DEFAULT_PRICE;
  }
}