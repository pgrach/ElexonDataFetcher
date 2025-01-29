import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

const dynamoDb = DynamoDBDocument.from(new DynamoDB({
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
  },
  region: process.env.AWS_REGION
}));

const DIFFICULTY_TABLE = 'bitcoin_difficulty';

export async function getHistoricalDifficulty(date: Date): Promise<number | null> {
  try {
    // Format date to YYYY-MM-DD for querying
    const dateKey = date.toISOString().split('T')[0];
    
    const result = await dynamoDb.get({
      TableName: DIFFICULTY_TABLE,
      Key: {
        date: dateKey
      }
    });

    if (result.Item) {
      return Number(result.Item.difficulty);
    }
    
    return null;
  } catch (error) {
    console.error('DynamoDB Error:', error);
    return null;
  }
}
