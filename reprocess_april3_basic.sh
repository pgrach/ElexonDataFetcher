#!/bin/bash

# Simplified script to reprocess data for April 3, 2025
# This focuses only on curtailment records and summary generation
# without attempting Bitcoin calculations

echo "Starting basic reprocessing of curtailment data for April 3, 2025..."

NODE_OPTIONS=--no-warnings node -e "
const { db } = require('./db');
const { processDailyCurtailment } = require('./server/services/curtailment_enhanced');
const { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } = require('./db/schema');
const { eq, sql } = require('drizzle-orm');

const TARGET_DATE = '2025-04-03';
const YEAR_MONTH = TARGET_DATE.substring(0, 7);
const YEAR = TARGET_DATE.substring(0, 4);

async function main() {
  try {
    console.log('======= STARTING BASIC REPROCESSING =======');
    console.log(\`Target date: \${TARGET_DATE}\`);
    
    // Step 1: Clear existing data
    console.log('\\nStep 1: Clearing existing data...');
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    console.log('Deleted existing curtailment records');
    
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    console.log('Deleted existing daily summary');
    
    // Step 2: Process new data
    console.log('\\nStep 2: Processing curtailment data from Elexon API...');
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 3: Verify results
    console.log('\\nStep 3: Verifying results...');
    const newRecords = await db
      .select({
        count: sql\`COUNT(*)\`,
        periods: sql\`COUNT(DISTINCT settlement_period)\`,
        totalVolume: sql\`SUM(ABS(volume::numeric))\`,
        totalPayment: sql\`SUM(payment::numeric)\`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(\`Processed \${newRecords[0]?.count || 0} records\`);
    console.log(\`Settlement Periods: \${newRecords[0]?.periods || 0}\`);
    console.log(\`Total Volume: \${newRecords[0]?.totalVolume || '0'} MWh\`);
    console.log(\`Total Payment: £\${newRecords[0]?.totalPayment || '0'}\`);
    
    // Check daily summary
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    if (dailySummary) {
      console.log('\\nDaily summary created:');
      console.log(\`  Energy: \${dailySummary.totalCurtailedEnergy} MWh\`);
      console.log(\`  Payment: £\${dailySummary.totalPayment}\`);
    } else {
      console.log('\\nNo daily summary created');
    }
    
    // Step 4: Update monthly summary
    if (dailySummary) {
      console.log('\\nStep 4: Updating monthly summary...');
      const monthlyStats = await db
        .select({
          totalEnergy: sql\`SUM(total_curtailed_energy::numeric)\`,
          totalPayment: sql\`SUM(total_payment::numeric)\`
        })
        .from(dailySummaries)
        .where(sql\`summary_date::text LIKE \${YEAR_MONTH + '-%'}\`);
      
      if (monthlyStats[0]?.totalEnergy) {
        await db.insert(monthlySummaries).values({
          yearMonth: YEAR_MONTH,
          totalCurtailedEnergy: monthlyStats[0].totalEnergy.toString(),
          totalPayment: monthlyStats[0].totalPayment.toString(),
          updatedAt: new Date()
        }).onConflictDoUpdate({
          target: [monthlySummaries.yearMonth],
          set: {
            totalCurtailedEnergy: monthlyStats[0].totalEnergy.toString(),
            totalPayment: monthlyStats[0].totalPayment.toString(),
            updatedAt: new Date()
          }
        });
        
        console.log(\`Updated monthly summary for \${YEAR_MONTH}\`);
        console.log(\`  Energy: \${monthlyStats[0].totalEnergy} MWh\`);
        console.log(\`  Payment: £\${monthlyStats[0].totalPayment}\`);
      }
      
      // Step 5: Update yearly summary
      console.log('\\nStep 5: Updating yearly summary...');
      const yearlyStats = await db
        .select({
          totalEnergy: sql\`SUM(total_curtailed_energy::numeric)\`,
          totalPayment: sql\`SUM(total_payment::numeric)\`
        })
        .from(monthlySummaries)
        .where(sql\`year_month::text LIKE \${YEAR + '-%'}\`);
      
      if (yearlyStats[0]?.totalEnergy) {
        await db.insert(yearlySummaries).values({
          year: YEAR,
          totalCurtailedEnergy: yearlyStats[0].totalEnergy.toString(),
          totalPayment: yearlyStats[0].totalPayment.toString(),
          updatedAt: new Date()
        }).onConflictDoUpdate({
          target: [yearlySummaries.year],
          set: {
            totalCurtailedEnergy: yearlyStats[0].totalEnergy.toString(),
            totalPayment: yearlyStats[0].totalPayment.toString(),
            updatedAt: new Date()
          }
        });
        
        console.log(\`Updated yearly summary for \${YEAR}\`);
        console.log(\`  Energy: \${yearlyStats[0].totalEnergy} MWh\`);
        console.log(\`  Payment: £\${yearlyStats[0].totalPayment}\`);
      }
    }
    
    console.log('\\n======= REPROCESSING COMPLETED SUCCESSFULLY =======');
    process.exit(0);
  } catch (error) {
    console.error('ERROR DURING REPROCESSING:', error);
    process.exit(1);
  }
}

main();
"

# Check the exit code
if [ $? -eq 0 ]; then
  echo "Basic reprocessing completed successfully!"
else
  echo "Reprocessing failed. Check the logs for details."
fi