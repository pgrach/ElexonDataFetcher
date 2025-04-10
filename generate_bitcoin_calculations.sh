#!/bin/bash

# Script to generate Bitcoin calculations for any miner model and date

# Default values
DATE=$(date +%Y-%m-%d)
MINER_MODEL="S19J_PRO"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --date)
      DATE="$2"
      shift 2
      ;;
    --miner)
      MINER_MODEL="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--date YYYY-MM-DD] [--miner MINER_MODEL]"
      echo "  MINER_MODEL options: S19J_PRO, S9, M20S"
      exit 1
      ;;
  esac
done

echo "==== Starting Bitcoin Calculations Generation ===="
echo "Date: $DATE"
echo "Miner Model: $MINER_MODEL"
echo ""

# Create a temporary script file
TEMP_SCRIPT="temp_bitcoin_script_$$.ts"

cat > "$TEMP_SCRIPT" << EOF
/**
 * Generate Bitcoin Calculations
 * 
 * This script uses direct SQL queries to generate historical Bitcoin calculations
 * for a specific miner model and date, then updates the daily summary.
 */

import { db } from "@db";
import { performance } from "perf_hooks";
import { sql } from "drizzle-orm";
import { getDifficultyData } from "../../server/services/dynamodbService";
import { minerModels } from "../../server/types/bitcoin";

// Target date and miner model
const TARGET_DATE = '$DATE';
const MINER_MODEL = '$MINER_MODEL';

// Get miner specs
const MINER_SPECS = minerModels[MINER_MODEL];
if (!MINER_SPECS) {
  throw new Error(\`Unknown miner model: \${MINER_MODEL}\`);
}

// Calculate miner efficiency (J/TH)
const MINER_EFFICIENCY = MINER_SPECS.power / MINER_SPECS.hashrate;

console.log(\`Using miner specs: Hashrate \${MINER_SPECS.hashrate} TH/s, Power \${MINER_SPECS.power}W\`);
console.log(\`Calculated efficiency: \${MINER_EFFICIENCY} J/TH\`);

/**
 * Calculate Bitcoin mining potential
 */
function calculateBitcoinMiningPotential(
  volume: string | number, 
  difficulty: number
): number {
  // Energy in MWh
  const volumeValue = typeof volume === 'string' 
    ? parseFloat(volume) 
    : volume;
  
  const energy = Math.abs(volumeValue);
  
  // Convert MWh to Joules
  const joules = energy * 3600000000; // 1 MWh = 3.6 billion joules
  
  // Calculate hashing power (TH) based on miner efficiency
  const hashingPower = joules / MINER_EFFICIENCY;
  
  // Calculate Bitcoin mined
  // Formula: (hashingPower * 100000000 * 3600) / (difficulty * 2^32)
  const bitcoinMined = (hashingPower * 100000000 * 3600) / (difficulty * Math.pow(2, 32));
  
  return bitcoinMined;
}

/**
 * Process and store Bitcoin calculations
 */
async function processBitcoinCalculations(): Promise<void> {
  console.log(\`\n==== Processing \${MINER_MODEL} Bitcoin Calculations for \${TARGET_DATE} ====\n\`);
  
  try {
    // First, delete any existing calculations for this date and miner model
    console.log('Deleting any existing calculations...');
    
    await db.execute(sql\`
      DELETE FROM historical_bitcoin_calculations
      WHERE settlement_date = \${TARGET_DATE}
      AND miner_model = \${MINER_MODEL}
    \`);
    
    console.log('Deletion completed.');
    
    // Get Bitcoin difficulty for the target date
    const difficulty = await getDifficultyData(TARGET_DATE);
    console.log(\`Using Bitcoin difficulty: \${difficulty}\`);
    
    // Get negative volume curtailment records for the target date
    const curtailmentResult = await db.execute(sql\`
      SELECT 
        id, 
        settlement_date, 
        settlement_period, 
        farm_id, 
        volume
      FROM 
        curtailment_records
      WHERE 
        settlement_date = \${TARGET_DATE}
        AND volume < 0
    \`);
    
    const curtailmentRecords = curtailmentResult.rows;
    
    if (!curtailmentRecords || curtailmentRecords.length === 0) {
      throw new Error(\`No curtailment records found for \${TARGET_DATE}\`);
    }
    
    console.log(\`Found \${curtailmentRecords.length} curtailment records for \${TARGET_DATE}\`);
    
    // Process each curtailment record
    let calculationsCount = 0;
    for (const record of curtailmentRecords) {
      // Calculate Bitcoin mining potential
      const bitcoinMined = calculateBitcoinMiningPotential(record.volume, difficulty);
      
      // Insert the calculation record directly with SQL
      await db.execute(sql\`
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, 
          settlement_period, 
          farm_id, 
          miner_model, 
          bitcoin_mined, 
          difficulty
        ) VALUES (
          \${record.settlement_date}, 
          \${record.settlement_period}, 
          \${record.farm_id}, 
          \${MINER_MODEL}, 
          \${bitcoinMined.toString()}, 
          \${difficulty.toString()}
        )
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = \${bitcoinMined.toString()}, 
          difficulty = \${difficulty.toString()}
      \`);
      
      calculationsCount++;
      
      // Log progress every 100 records
      if (calculationsCount % 100 === 0) {
        console.log(\`Processed \${calculationsCount} records...\`);
      }
    }
    
    console.log(\`\nProcessed \${calculationsCount} \${MINER_MODEL} Bitcoin calculations for \${TARGET_DATE}\`);
    
    // Update the daily summary
    await updateDailySummary();
    
  } catch (error) {
    console.error(\`Error processing \${MINER_MODEL} Bitcoin calculations:\`, error);
    throw error;
  }
}

/**
 * Update the Bitcoin daily summary
 */
async function updateDailySummary(): Promise<void> {
  console.log(\`\n==== Updating \${MINER_MODEL} Bitcoin Daily Summary for \${TARGET_DATE} ====\n\`);
  
  try {
    // Calculate total Bitcoin mined for the day
    const bitcoinTotal = await db.execute(sql\`
      SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = \${TARGET_DATE}
      AND miner_model = \${MINER_MODEL}
    \`);
    
    const totalBitcoin = bitcoinTotal.rows?.[0]?.total_bitcoin;
    
    if (!totalBitcoin) {
      console.log(\`No Bitcoin total could be calculated for \${MINER_MODEL}\`);
      return;
    }
    
    // Delete existing summary if any
    await db.execute(sql\`
      DELETE FROM bitcoin_daily_summaries
      WHERE summary_date = \${TARGET_DATE}
      AND miner_model = \${MINER_MODEL}
    \`);
    
    // Insert new summary
    await db.execute(sql\`
      INSERT INTO bitcoin_daily_summaries (
        summary_date, 
        miner_model, 
        bitcoin_mined, 
        created_at, 
        updated_at
      ) VALUES (
        \${TARGET_DATE}, 
        \${MINER_MODEL}, 
        \${totalBitcoin}, 
        NOW(), 
        NOW()
      )
    \`);
    
    console.log(\`Updated Bitcoin daily summary for \${MINER_MODEL}: \${totalBitcoin} BTC\`);
    
  } catch (error) {
    console.error(\`Error updating \${MINER_MODEL} Bitcoin daily summary:\`, error);
    throw error;
  }
}

/**
 * Verify Bitcoin calculations have been created
 */
async function verifyBitcoinCalculations(): Promise<void> {
  console.log(\`\n==== Verifying \${MINER_MODEL} Bitcoin Calculations for \${TARGET_DATE} ====\n\`);
  
  // Check historical calculations
  const calculationsResult = await db.execute(sql\`
    SELECT COUNT(*) as count
    FROM historical_bitcoin_calculations
    WHERE settlement_date = \${TARGET_DATE}
    AND miner_model = \${MINER_MODEL}
  \`);
  
  const calculationsCount = calculationsResult.rows?.[0]?.count || 0;
  console.log(\`Found \${calculationsCount} \${MINER_MODEL} historical Bitcoin calculations for \${TARGET_DATE}\`);
  
  // Check daily summary
  const dailySummaryResult = await db.execute(sql\`
    SELECT *
    FROM bitcoin_daily_summaries
    WHERE summary_date = \${TARGET_DATE}
    AND miner_model = \${MINER_MODEL}
  \`);
  
  if (dailySummaryResult.rows?.length > 0) {
    console.log(\`Bitcoin daily summary for \${MINER_MODEL}: \${dailySummaryResult.rows[0].bitcoin_mined} BTC\`);
  } else {
    console.log(\`No Bitcoin daily summary found for \${MINER_MODEL}\`);
  }
}

/**
 * Main function to generate Bitcoin calculations
 */
async function main(): Promise<void> {
  const startTime = performance.now();
  
  try {
    console.log(\`\n==== Starting \${MINER_MODEL} Bitcoin Calculations Generation for \${TARGET_DATE} ====\n\`);
    
    // Process and store calculations
    await processBitcoinCalculations();
    
    // Verify calculations have been created
    await verifyBitcoinCalculations();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(\`\n==== \${MINER_MODEL} Bitcoin Calculations Generation Completed ====\`);
    console.log(\`Total execution time: \${durationSeconds} seconds\`);
    
  } catch (error) {
    console.error(\`Error during \${MINER_MODEL} Bitcoin calculations generation:\`, error);
    throw error;
  }
}

// Execute the generation process
main()
  .then(() => {
    console.log('Bitcoin calculations generation completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Bitcoin calculations generation failed with error:', error);
    process.exit(1);
  });
EOF

# Run the script
echo "Running Bitcoin calculations script..."
npx tsx "$TEMP_SCRIPT"

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Bitcoin calculations generated successfully ===="
    echo "You can verify the results using the following SQL query:"
    echo "  SELECT * FROM bitcoin_daily_summaries WHERE summary_date = '$DATE' AND miner_model = '$MINER_MODEL';"
else
    echo ""
    echo "==== Bitcoin calculations generation FAILED ===="
    echo "Please check the error messages above."
fi

# Clean up temporary file
rm "$TEMP_SCRIPT"