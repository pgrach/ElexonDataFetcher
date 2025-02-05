import { DynamoDBClient, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { getHistoricalData } from '../services/dynamodbService';

async function testDynamoDBRetrieval() {
  try {
    // Try a more recent date since 2025 data might not exist yet
    console.log('Testing DynamoDB data retrieval for 2024-01-15...');

    // First inspect the price table structure and sample data
    const client = new DynamoDBClient({ 
      region: process.env.AWS_REGION || 'us-east-1'
    });

    const docClient = DynamoDBDocumentClient.from(client);

    const describeCommand = new DescribeTableCommand({
      TableName: process.env.DYNAMODB_PRICES_TABLE || 'asics-dynamodb-PricesTable-1LXU143BUOBN'
    });

    const tableInfo = await client.send(describeCommand);
    console.log('Price Table Structure:', {
      attributeDefinitions: tableInfo.Table?.AttributeDefinitions,
      keySchema: tableInfo.Table?.KeySchema,
    });

    // Get a sample record to understand the data format
    const sampleCommand = new ScanCommand({
      TableName: process.env.DYNAMODB_PRICES_TABLE || 'asics-dynamodb-PricesTable-1LXU143BUOBN',
      Limit: 1
    });

    const sampleResponse = await docClient.send(sampleCommand);
    if (sampleResponse.Items?.[0]) {
      console.log('Sample Price Record:', sampleResponse.Items[0]);
    }

    // Get another sample from difficulty table
    const difficultyCommand = new ScanCommand({
      TableName: process.env.DYNAMODB_DIFFICULTY_TABLE || 'asics-dynamodb-DifficultyTable-DQ308ID3POT6',
      Limit: 1
    });

    const difficultyResponse = await docClient.send(difficultyCommand);
    if (difficultyResponse.Items?.[0]) {
      console.log('Sample Difficulty Record:', difficultyResponse.Items[0]);
    }

    // Now try to get the actual data
    const data = await getHistoricalData('2024-01-15');

    console.log('Retrieved Data:', {
      difficulty: {
        value: data.difficulty,
        isDefault: data.difficulty === 108105433845147
      },
      price: {
        value: data.price,
        isDefault: data.price === 99212.39
      }
    });
  } catch (error) {
    console.error('Error testing DynamoDB:', error);
  }
}

testDynamoDBRetrieval();