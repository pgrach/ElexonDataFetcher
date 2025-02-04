import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { DEFAULT_DIFFICULTY, DEFAULT_PRICE, DynamoDBHistoricalData } from '../types/bitcoin';

const client = new DynamoDBClient({ region: "us-east-1" });
const docClient = DynamoDBDocumentClient.from(client);

const DIFFICULTY_TABLE = "asics-dynamodb-DifficultyTable-DQ308ID3POT6";
const PRICES_TABLE = "asics-dynamodb-PricesTable-1LXU143BUOBN";

/**
 * Get historical difficulty from DynamoDB for a specific date with fallback
 * @param date Date in YYYY-MM-DD format
 * @returns Promise<number> Historical difficulty
 */
export async function getHistoricalDifficulty(date: string): Promise<number> {
  try {
    console.log(`Fetching historical difficulty for date: ${date}`);
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

    const response = await docClient.send(command);

    if (!response.Items || response.Items.length === 0) {
      console.warn(`No difficulty data found for date: ${date}, using default value`);
      return DEFAULT_DIFFICULTY;
    }

    const difficulty = Number(response.Items[0].Difficulty);
    console.log(`Retrieved difficulty for ${date}: ${difficulty}`);
    return difficulty;
  } catch (error) {
    console.error('Error fetching historical difficulty:', error);
    console.warn(`Using default difficulty value: ${DEFAULT_DIFFICULTY}`);
    return DEFAULT_DIFFICULTY;
  }
}

/**
 * Get historical price from DynamoDB for a specific date with fallback
 * @param date Date in YYYY-MM-DD format
 * @returns Promise<number> Historical price
 */
export async function getHistoricalPrice(date: string): Promise<number> {
  try {
    console.log(`Fetching historical price for date: ${date}`);
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

    const response = await docClient.send(command);

    if (!response.Items || response.Items.length === 0) {
      console.warn(`No price data found for date: ${date}, using default value`);
      return DEFAULT_PRICE;
    }

    const price = Number(response.Items[0].Price);
    console.log(`Retrieved price for ${date}: ${price}`);
    return price;
  } catch (error) {
    console.error('Error fetching historical price:', error);
    console.warn(`Using default price value: ${DEFAULT_PRICE}`);
    return DEFAULT_PRICE;
  }
}

/**
 * Get both historical difficulty and price for a specific date with fallback values
 * @param date Date in YYYY-MM-DD format
 * @returns Promise<DynamoDBHistoricalData>
 */
export async function getHistoricalData(date: string): Promise<DynamoDBHistoricalData> {
  const [difficulty, price] = await Promise.all([
    getHistoricalDifficulty(date),
    getHistoricalPrice(date)
  ]);

  return { difficulty, price };
}