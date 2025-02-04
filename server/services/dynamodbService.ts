import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { 
  DynamoDBDocumentClient, 
  QueryCommand,
  QueryCommandInput
} from "@aws-sdk/lib-dynamodb";
import { LRUCache } from 'lru-cache';

const client = new DynamoDBClient({ region: "us-east-1" });
const docClient = DynamoDBDocumentClient.from(client);

// Cache for historical data to reduce DynamoDB costs
const cache = new LRUCache<string, HistoricalData>({
  max: 500, // Store up to 500 items
  ttl: 1000 * 60 * 60 * 24 // 24 hour TTL
});

export interface HistoricalData {
  difficulty: number;
  price: number;
}

const MAX_RETRIES = 3;
const INITIAL_BACKOFF = 1000; // 1 second

async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  retries: number = MAX_RETRIES,
  backoff: number = INITIAL_BACKOFF
): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    if (retries === 0) throw error;

    console.log(`Retrying operation after ${backoff}ms, ${retries} retries remaining`);
    await new Promise(resolve => setTimeout(resolve, backoff));

    return retryWithBackoff(
      operation,
      retries - 1,
      backoff * 2
    );
  }
}

/**
 * Get historical difficulty from DynamoDB for a specific date
 * @param date Date in YYYY-MM-DD format
 * @returns Promise<number> Historical difficulty
 * @throws Error if no data found or DynamoDB query fails
 */
export async function getHistoricalDifficulty(date: string): Promise<number> {
  try {
    const command = new QueryCommand({
      TableName: "asics-dynamodb-DifficultyTable-DQ308ID3POT6",
      KeyConditionExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date",
      },
      ExpressionAttributeValues: {
        ":date": date,
      },
      Limit: 1,
    });

    const response = await retryWithBackoff(() => docClient.send(command));

    if (!response.Items || response.Items.length === 0) {
      const error = new Error(`No difficulty data found for date: ${date}`);
      console.error(error);
      throw error;
    }

    const difficulty = Number(response.Items[0].Difficulty);
    if (isNaN(difficulty)) {
      throw new Error(`Invalid difficulty value for date: ${date}`);
    }

    return difficulty;
  } catch (error) {
    console.error('Error fetching historical difficulty:', error);
    throw error;
  }
}

/**
 * Get historical price from DynamoDB for a specific date
 * @param date Date in YYYY-MM-DD format
 * @returns Promise<number> Historical price
 * @throws Error if no data found or DynamoDB query fails
 */
export async function getHistoricalPrice(date: string): Promise<number> {
  try {
    const command = new QueryCommand({
      TableName: "asics-dynamodb-PricesTable-1LXU143BUOBN",
      KeyConditionExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date",
      },
      ExpressionAttributeValues: {
        ":date": date,
      },
      Limit: 1,
    });

    const response = await retryWithBackoff(() => docClient.send(command));

    if (!response.Items || response.Items.length === 0) {
      const error = new Error(`No price data found for date: ${date}`);
      console.error(error);
      throw error;
    }

    const price = Number(response.Items[0].Price);
    if (isNaN(price)) {
      throw new Error(`Invalid price value for date: ${date}`);
    }

    return price;
  } catch (error) {
    console.error('Error fetching historical price:', error);
    throw error;
  }
}

/**
 * Get both historical difficulty and price for a specific date
 * @param date Date in YYYY-MM-DD format
 * @returns Promise<HistoricalData>
 */
export async function getHistoricalData(date: string): Promise<HistoricalData> {
  // Check cache first
  const cacheKey = `historical-${date}`;
  const cachedData = cache.get(cacheKey);
  if (cachedData) {
    console.log(`Cache hit for date: ${date}`);
    return cachedData;
  }

  // If not in cache, fetch from DynamoDB
  try {
    const [difficulty, price] = await Promise.all([
      getHistoricalDifficulty(date),
      getHistoricalPrice(date)
    ]);

    const data: HistoricalData = { difficulty, price };

    // Store in cache
    cache.set(cacheKey, data);

    return data;
  } catch (error) {
    console.error(`Failed to fetch historical data for date ${date}:`, error);
    throw error;
  }
}