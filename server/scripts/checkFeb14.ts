import { format } from 'date-fns';
import { fetchBidsOffers } from "../services/elexon";

const TARGET_DATE = '2022-02-14';
const CHECK_PERIODS = [1, 12, 24, 36, 48]; // Check throughout the day

async function checkFeb14Data() {
  try {
    console.log(`\n=== Checking Curtailment Data for ${TARGET_DATE} ===\n`);
    
    let totalValidRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;

    for (const period of CHECK_PERIODS) {
      try {
        console.log(`Checking period ${period}...`);
        const records = await fetchBidsOffers(TARGET_DATE, period);
        const validRecords = records.filter(record => 
          record.volume < 0 && (record.soFlag || record.cadlFlag)
        );

        if (validRecords.length > 0) {
          const periodVolume = validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
          const periodPayment = validRecords.reduce((sum, r) => sum + Math.abs(r.volume) * r.originalPrice, 0);
          
          console.log(`Period ${period}: ${validRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
          
          totalValidRecords += validRecords.length;
          totalVolume += periodVolume;
          totalPayment += periodPayment;
        } else {
          console.log(`Period ${period}: No curtailment records found`);
        }
      } catch (error) {
        console.error(`Error checking period ${period}:`, error);
      }
    }

    console.log('\nSummary:');
    console.log(`Total Records Found: ${totalValidRecords}`);
    console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total Payment: £${totalPayment.toFixed(2)}`);

  } catch (error) {
    console.error('Error during check:', error);
    process.exit(1);
  }
}

// Run check
checkFeb14Data();
