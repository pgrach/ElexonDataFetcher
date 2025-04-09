import { getDifficultyData } from './server/services/dynamodbService';
import { format } from 'date-fns';

interface DifficultyRecord {
  date: string;
  dynamoDbDifficulty: number;
  dbDifficulty: number | string;
  match: boolean;
  notes?: string;
}

// List of dates and difficulties we want to verify
const dates = [
  { date: '2025-03-01', difficulty: '110568428300952' },
  { date: '2025-03-04', difficulty: '108105433845147' },
  { date: '2025-03-10', difficulty: '112149504190349' },
  { date: '2025-03-20', difficulty: '55633605879865' },
  { date: '2025-03-24', difficulty: '113757508810853' },
  { date: '2025-03-28', difficulty: ['56000000000000', '113757508810853'] },
  { date: '2025-03-31', difficulty: '113757508810853' }
];

// Suppress DynamoDB logs by monkey-patching console.debug, info, warn
const originalDebug = console.debug;
const originalInfo = console.info;
const originalWarn = console.warn;
const originalError = console.error;

// Only keep original behavior for error logs, silence others from DynamoDB
console.debug = (...args: any[]) => {
  if (!args[0]?.toString().includes('[DynamoDB')) {
    originalDebug(...args);
  }
};

console.info = (...args: any[]) => {
  if (!args[0]?.toString().includes('[DynamoDB')) {
    originalInfo(...args);
  }
};

console.warn = (...args: any[]) => {
  if (!args[0]?.toString().includes('[DynamoDB')) {
    originalWarn(...args);
  }
};

console.error = (...args: any[]) => {
  // Keep errors but format them more concisely
  if (args[0]?.toString().includes('[DynamoDB')) {
    originalError('[DynamoDB Error]', args[args.length-1]);
  } else {
    originalError(...args);
  }
};

async function fetchDifficulties() {
  console.log('Fetching difficulty data from DynamoDB for March 2025...');
  console.log('This will take a minute as we query DynamoDB...\n');
  
  const results: DifficultyRecord[] = [];
  
  // Use a delay between requests to avoid rate limiting
  const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
  
  for (const { date, difficulty } of dates) {
    try {
      process.stdout.write(`Fetching difficulty for ${date}... `);
      
      // Get difficulty from DynamoDB using the service
      const dynamoDbDifficulty = await getDifficultyData(date);
      
      // If the difficulty is an array, compare with both values
      if (Array.isArray(difficulty)) {
        const matches = difficulty.map(diff => diff === dynamoDbDifficulty.toString());
        const match = matches.includes(true);
        
        results.push({
          date,
          dynamoDbDifficulty,
          dbDifficulty: difficulty.join(' or '),
          match,
          notes: match ? 
            `Matches one of the database values` : 
            `Does not match any database value`
        });
      } else {
        // Compare with single difficulty value
        const match = difficulty === dynamoDbDifficulty.toString();
        
        results.push({
          date,
          dynamoDbDifficulty,
          dbDifficulty: difficulty,
          match,
          notes: match ? 
            'Matches database value' : 
            `Database has ${difficulty}, DynamoDB has ${dynamoDbDifficulty}`
        });
      }
      
      process.stdout.write(`${dynamoDbDifficulty}\n`);
      
      // Add a delay between requests to be nice to the API
      await delay(500);
      
    } catch (error) {
      console.error(`Error fetching difficulty for ${date}:`, error);
      
      results.push({
        date,
        dynamoDbDifficulty: 0,
        dbDifficulty: Array.isArray(difficulty) ? difficulty.join(' or ') : difficulty,
        match: false,
        notes: 'Error fetching from DynamoDB'
      });
      
      await delay(500);
    }
  }
  
  // Print results in a table format
  console.log('\n=============================================================');
  console.log('RESULTS: Database vs. DynamoDB Difficulty Values');
  console.log('=============================================================');
  console.log('Date       | DynamoDB Difficulty   | Database Difficulty   | Match');
  console.log('-----------|----------------------|----------------------|-------');
  
  for (const result of results) {
    const matchSymbol = result.match ? '✓' : '✗';
    const dynamoDbDiffStr = result.dynamoDbDifficulty.toString().padEnd(20);
    const dbDiffStr = result.dbDifficulty.toString().padEnd(20);
    
    console.log(`${result.date} | ${dynamoDbDiffStr} | ${dbDiffStr} | ${matchSymbol}`);
    
    if (result.notes) {
      console.log(`           | ${result.notes}`);
    }
    
    console.log('-----------|----------------------|----------------------|-------');
  }
  
  // Summary
  const matchCount = results.filter(r => r.match).length;
  console.log(`\nSummary: ${matchCount} of ${results.length} dates match between DynamoDB and database`);
  
  if (matchCount < results.length) {
    console.log('\nMismatches detected! This suggests inconsistencies between the source of truth (DynamoDB)');
    console.log('and the values stored in the database. Consider updating the database with correct values.');
  } else {
    console.log('\nAll difficulty values match between DynamoDB and the database.');
  }
}

// Execute the function
fetchDifficulties().then(() => {
  // Restore original console methods
  console.debug = originalDebug;
  console.info = originalInfo;
  console.warn = originalWarn;
  console.error = originalError;
});