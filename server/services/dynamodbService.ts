import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { DEFAULT_DIFFICULTY, DEFAULT_PRICE, DynamoDBHistoricalData } from '../types/bitcoin';

const client = new DynamoDBClient({ 
  region: "us-east-1",
  logger: console 
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

export async function getHistoricalDifficulty(date: string): Promise<number> {
  try {
    console.log(`[DynamoDB] Fetching difficulty for date: ${date} from table: ${DIFFICULTY_TABLE}`);
    console.log('[DynamoDB] AWS credentials status:', {
      hasAccessKeyId: !!process.env.AWS_ACCESS_KEY_ID,
      hasSecretKey: !!process.env.AWS_SECRET_ACCESS_KEY
    });

    const command = new QueryCommand({
      TableName: DIFFICULTY_TABLE,
      KeyConditionExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date",
      },
      ExpressionAttributeValues: {
        ":date": date,
      },
      Limit: 1,
    });

    console.log('[DynamoDB] Sending query command for difficulty...');
    const response = await docClient.send(command);
    console.log(`[DynamoDB] Received response:`, JSON.stringify(response.Items, null, 2));

    if (!response.Items || response.Items.length === 0) {
      console.warn(`[DynamoDB] No difficulty data found for date: ${date}, using default value: ${DEFAULT_DIFFICULTY}`);
      return DEFAULT_DIFFICULTY;
    }

    const difficulty = Number(response.Items[0].Difficulty);
    console.log(`[DynamoDB] Retrieved difficulty for ${date}: ${difficulty}`);
    return difficulty;
  } catch (error) {
    console.error('[DynamoDB] Error fetching historical difficulty:', error);
    if (error instanceof Error) {
      console.error('[DynamoDB] Error details:', error.message);
      console.error('[DynamoDB] Error stack:', error.stack);
    }
    console.warn(`[DynamoDB] Using default difficulty value: ${DEFAULT_DIFFICULTY}`);
    return DEFAULT_DIFFICULTY;
  }
}

export async function getHistoricalPrice(date: string): Promise<number> {
  try {
    console.log(`[DynamoDB] Fetching price for date: ${date} from table: ${PRICES_TABLE}`);
    console.log('[DynamoDB] AWS credentials status:', {
      hasAccessKeyId: !!process.env.AWS_ACCESS_KEY_ID,
      hasSecretKey: !!process.env.AWS_SECRET_ACCESS_KEY
    });

    const command = new QueryCommand({
      TableName: PRICES_TABLE,
      KeyConditionExpression: "#date = :date",
      ExpressionAttributeNames: {
        "#date": "Date",
      },
      ExpressionAttributeValues: {
        ":date": date,
      },
      Limit: 1,
    });

    console.log('[DynamoDB] Sending query command for price...');
    const response = await docClient.send(command);
    console.log(`[DynamoDB] Received response:`, JSON.stringify(response.Items, null, 2));

    if (!response.Items || response.Items.length === 0) {
      console.warn(`[DynamoDB] No price data found for date: ${date}, using default value: ${DEFAULT_PRICE}`);
      return DEFAULT_PRICE;
    }

    const price = Number(response.Items[0].Price);
    console.log(`[DynamoDB] Retrieved price for ${date}: ${price}`);
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

export async function getHistoricalData(date: string): Promise<DynamoDBHistoricalData> {
  console.log('[DynamoDB] Fetching both difficulty and price data...');

  try {
    const [difficulty, price] = await Promise.all([
      getHistoricalDifficulty(date),
      getHistoricalPrice(date)
    ]);

    console.log('[DynamoDB] Retrieved data:', { difficulty, price });
    return { difficulty, price };
  } catch (error) {
    console.error('[DynamoDB] Error in getHistoricalData:', error);
    throw error;
  }
}