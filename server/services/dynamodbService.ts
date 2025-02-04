import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({ region: "us-east-1" });
const docClient = DynamoDBDocumentClient.from(client);

export interface HistoricalData {
  difficulty?: number;
  price?: number;
}

/**
 * Get historical difficulty from DynamoDB for a specific date
 * @param date Date in YYYY-MM-DD format
 * @returns Promise<number> Historical difficulty
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

    const response = await docClient.send(command);
    
    if (!response.Items || response.Items.length === 0) {
      throw new Error(`No difficulty data found for date: ${date}`);
    }

    return Number(response.Items[0].Difficulty);
  } catch (error) {
    console.error('Error fetching historical difficulty:', error);
    throw error;
  }
}

/**
 * Get historical price from DynamoDB for a specific date
 * @param date Date in YYYY-MM-DD format
 * @returns Promise<number> Historical price
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

    const response = await docClient.send(command);
    
    if (!response.Items || response.Items.length === 0) {
      throw new Error(`No price data found for date: ${date}`);
    }

    return Number(response.Items[0].Price);
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
  const [difficulty, price] = await Promise.all([
    getHistoricalDifficulty(date),
    getHistoricalPrice(date)
  ]);

  return { difficulty, price };
}
