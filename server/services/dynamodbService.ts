import { DynamoDBClient, DescribeTableCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
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

// Helper function to ensure consistent date formatting
function formatDateForDynamoDB(dateStr: string): string {
  try {
    const date = parse(dateStr, 'yyyy-MM-dd', new Date());
    return format(date, 'yyyy-MM-dd');
  } catch (error) {
    console.error(`[DynamoDB] Error formatting date ${dateStr}:`, error);
    throw new Error(`Invalid date format: ${dateStr}`);
  }
}

// Helper function to verify table existence
async function verifyTableExists(tableName: string): Promise<boolean> {
  try {
    const command = new DescribeTableCommand({ TableName: tableName });
    const response = await client.send(command);
    console.log(`[DynamoDB] Table ${tableName} exists:`, {
      status: response.Table?.TableStatus,
      itemCount: response.Table?.ItemCount,
      tableArn: response.Table?.TableArn
    });
    return true;
  } catch (error) {
    console.error(`[DynamoDB] Error verifying table ${tableName}:`, error);
    return false;
  }
}

// Helper function to implement retry logic
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

export async function getHistoricalDifficulty(date: string): Promise<number> {
  try {
    const formattedDate = formatDateForDynamoDB(date);
    console.log(`[DynamoDB] Fetching difficulty for date: ${formattedDate} from table: ${DIFFICULTY_TABLE}`);

    // Verify AWS credentials and table existence
    console.log('[DynamoDB] AWS credentials status:', {
      hasAccessKeyId: !!process.env.AWS_ACCESS_KEY_ID,
      hasSecretKey: !!process.env.AWS_SECRET_ACCESS_KEY,
      region: client.config.region,
      table: DIFFICULTY_TABLE
    });

    const tableExists = await verifyTableExists(DIFFICULTY_TABLE);
    if (!tableExists) {
      console.error(`[DynamoDB] Table ${DIFFICULTY_TABLE} does not exist`);
      return DEFAULT_DIFFICULTY;
    }

    const command = new QueryCommand({
      TableName: DIFFICULTY_TABLE,
      KeyConditionExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date",
      },
      ExpressionAttributeValues: {
        ":date": formattedDate,
      },
      Limit: 1,
    });

    console.log('[DynamoDB] Query command parameters:', {
      TableName: command.input.TableName,
      KeyConditionExpression: command.input.KeyConditionExpression,
      ExpressionAttributeValues: command.input.ExpressionAttributeValues
    });

    const response = await retryOperation(() => docClient.send(command));
    console.log('[DynamoDB] Raw DynamoDB response:', JSON.stringify(response, null, 2));
    console.log('[DynamoDB] Items returned:', response.Items?.length || 0);

    if (response.Items?.[0]) {
      console.log('[DynamoDB] First item:', JSON.stringify(response.Items[0], null, 2));
    } else {
      console.warn(`[DynamoDB] No items found for date: ${formattedDate}`);
    }

    if (!response.Items || response.Items.length === 0) {
      console.warn(`[DynamoDB] No difficulty data found for date: ${formattedDate}, using default value: ${DEFAULT_DIFFICULTY}`);
      return DEFAULT_DIFFICULTY;
    }

    // Log the structure of the first item to understand the actual column names
    console.log('[DynamoDB] Item structure:', Object.keys(response.Items[0]));

    // Try different possible column names for difficulty
    const difficultyValue = response.Items[0].Difficulty || response.Items[0].difficulty || response.Items[0].DIFFICULTY;

    if (difficultyValue === undefined) {
      console.error('[DynamoDB] Could not find difficulty value in response item:', response.Items[0]);
      return DEFAULT_DIFFICULTY;
    }

    const difficulty = Number(difficultyValue);
    if (isNaN(difficulty)) {
      console.error(`[DynamoDB] Invalid difficulty value in response:`, difficultyValue);
      return DEFAULT_DIFFICULTY;
    }

    console.log(`[DynamoDB] Retrieved difficulty for ${formattedDate}: ${difficulty}`);
    return difficulty;
  } catch (error) {
    console.error('[DynamoDB] Error fetching historical difficulty:', error);
    if (error instanceof Error) {
      console.error('[DynamoDB] Error details:', error.message);
      console.error('[DynamoDB] Error stack:', error.stack);
      console.error('[DynamoDB] Error name:', error.name);

      if (error.name === 'AccessDeniedException') {
        console.error('[DynamoDB] Access denied to table. Please check IAM permissions');
      } else if (error.name === 'ResourceNotFoundException') {
        console.error('[DynamoDB] Table not found. Please check table name and region');
      }
    }
    console.warn(`[DynamoDB] Using default difficulty value: ${DEFAULT_DIFFICULTY}`);
    return DEFAULT_DIFFICULTY;
  }
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

// Add diagnostic function to check table contents directly
export async function getDiagnosticData(date: string): Promise<void> {
  try {
    console.log('\n[DynamoDB Diagnostic] Starting diagnostic check...');

    // 1. Check table description
    const describeCommand = new DescribeTableCommand({ TableName: DIFFICULTY_TABLE });
    const tableDescription = await client.send(describeCommand);
    console.log('\n[DynamoDB Diagnostic] Table Description:', JSON.stringify(tableDescription.Table, null, 2));

    // 2. Try multiple date formats
    const formats = [
      date,
      format(parse(date, 'yyyy-MM-dd', new Date()), 'yyyy-MM-dd'),
      format(parse(date, 'yyyy-MM-dd', new Date()), 'YYYY-MM-DD'),
    ];

    console.log('\n[DynamoDB Diagnostic] Trying multiple date formats:', formats);

    for (const dateFormat of formats) {
      const command = new QueryCommand({
        TableName: DIFFICULTY_TABLE,
        KeyConditionExpression: "#date = :date",
        ExpressionAttributeNames: {
          "#date": "Date",
        },
        ExpressionAttributeValues: {
          ":date": dateFormat,
        },
      });

      console.log(`\n[DynamoDB Diagnostic] Querying with date format: ${dateFormat}`);
      const response = await docClient.send(command);
      console.log('[DynamoDB Diagnostic] Query response:', JSON.stringify(response, null, 2));
    }

  } catch (error) {
    console.error('[DynamoDB Diagnostic] Error during diagnostic:', error);
  }
}

export async function getHistoricalData(date: string): Promise<DynamoDBHistoricalData> {
  console.log('[DynamoDB] Fetching both difficulty and price data...');
  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      console.error('[DynamoDB] Missing AWS credentials');
      throw new Error('AWS credentials not found');
    }

    // Run diagnostics first
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