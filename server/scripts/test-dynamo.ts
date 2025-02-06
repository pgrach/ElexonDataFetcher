import { DynamoDBClient, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { getDifficultyData } from '../services/dynamodbService';

async function testDynamoDBRetrieval() {
  try {
    // Test with January 15th, 2025
    const testDate = '2025-01-15';
    console.log(`Testing DynamoDB difficulty data retrieval for ${testDate}...`);

    // First inspect the difficulty table structure
    const client = new DynamoDBClient({ 
      region: process.env.AWS_REGION || 'us-east-1'
    });

    const docClient = DynamoDBDocumentClient.from(client);

    const describeCommand = new DescribeTableCommand({
      TableName: process.env.DYNAMODB_DIFFICULTY_TABLE || 'asics-dynamodb-DifficultyTable-DQ308ID3POT6'
    });

    const tableInfo = await client.send(describeCommand);
    console.log('Difficulty Table Structure:', {
      attributeDefinitions: tableInfo.Table?.AttributeDefinitions,
      keySchema: tableInfo.Table?.KeySchema,
      itemCount: tableInfo.Table?.ItemCount
    });

    // Get sample records to understand the data format
    const sampleCommand = new ScanCommand({
      TableName: process.env.DYNAMODB_DIFFICULTY_TABLE || 'asics-dynamodb-DifficultyTable-DQ308ID3POT6',
      Limit: 5
    });

    const sampleResponse = await docClient.send(sampleCommand);
    if (sampleResponse.Items?.length) {
      console.log('Sample Difficulty Records:', 
        sampleResponse.Items.map(item => ({
          date: item.Date,
          difficulty: item.Difficulty
        }))
      );
    }

    // Now try to get the actual historical data for January 15th
    console.log('\nTesting getDifficultyData...');
    const difficulty = await getDifficultyData(testDate);

    console.log('\nRetrieved Historical Data:', {
      date: testDate,
      difficulty: {
        value: difficulty,
        isDefault: difficulty === 108105433845147,
        formatted: difficulty.toLocaleString()
      }
    });

  } catch (error) {
    console.error('Error testing DynamoDB:', error);
  }
}

testDynamoDBRetrieval();