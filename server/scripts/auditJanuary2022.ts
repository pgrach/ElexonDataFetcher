import { format, eachDayOfInterval } from 'date-fns';
import { fetchBidsOffers } from "../services/elexon";
import { db } from "../db";
import { curtailment } from "@db/schema";
import { eq, and, between } from "drizzle-orm";

interface AuditResult {
  date: string;
  status: 'ok' | 'mismatch' | 'error';
  apiTotal?: number;
  dbTotal?: number;
  difference?: number;
  error?: string;
}

async function auditDay(date: string): Promise<AuditResult> {
  console.log(`\nAuditing ${date}...`);

  // Get all periods for the day
  const periods = Array.from({ length: 48 }, (_, i) => i + 1);
  let apiTotal = 0;
  let dbTotal = 0;

  try {
    // Get API data
    for (const period of periods) {
      const records = await fetchBidsOffers(date, period);
      const validRecords = records.filter(record => 
        record.volume < 0 && (record.soFlag || record.cadlFlag)
      );

      if (validRecords.length > 0) {
        const periodVolume = validRecords.reduce((sum: number, r: any) => sum + Math.abs(r.volume), 0);
        apiTotal += periodVolume;
      }
    }

    // Get DB data
    const startTime = new Date(`${date}T00:00:00Z`);
    const endTime = new Date(`${date}T23:59:59Z`);

    const dbRecords = await db.query.curtailment.findMany({
      where: and(
        between(curtailment.settlementDate, startTime, endTime)
      )
    });

    dbTotal = dbRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);

    // Compare totals
    const difference = Math.abs(apiTotal - dbTotal);
    const threshold = 0.1; // 0.1 MWh threshold for floating point differences

    if (difference > threshold) {
      console.log(`[MISMATCH] ${date}`);
      console.log(`API Total: ${apiTotal.toFixed(2)} MWh`);
      console.log(`DB Total: ${dbTotal.toFixed(2)} MWh`);
      console.log(`Difference: ${difference.toFixed(2)} MWh`);

      return {
        date,
        status: 'mismatch',
        apiTotal,
        dbTotal,
        difference
      };
    } else {
      console.log(`[OK] ${date} - API: ${apiTotal.toFixed(2)} MWh, DB: ${dbTotal.toFixed(2)} MWh`);
      return {
        date,
        status: 'ok',
        apiTotal,
        dbTotal,
        difference
      };
    }

  } catch (error) {
    console.error(`Error auditing ${date}:`, error);
    return {
      date,
      status: 'error',
      error: error instanceof Error ? error.message : 'Unknown error'
    };
  }
}

async function auditJanuary2022() {
  const startDate = new Date('2022-01-01');
  const endDate = new Date('2022-01-31');

  const days = eachDayOfInterval({ start: startDate, end: endDate });
  const results: AuditResult[] = [];

  for (const day of days) {
    const formattedDate = format(day, 'yyyy-MM-dd');
    const result = await auditDay(formattedDate);
    results.push(result);
  }

  // Summary
  console.log('\nAudit Summary:');
  const mismatches = results.filter(r => r.status === 'mismatch');
  const errors = results.filter(r => r.status === 'error');

  console.log(`Total days audited: ${results.length}`);
  console.log(`Days with mismatches: ${mismatches.length}`);
  console.log(`Days with errors: ${errors.length}`);

  if (mismatches.length > 0) {
    console.log('\nMismatched Days:');
    mismatches.forEach(m => {
      console.log(`${m.date}: API=${m.apiTotal?.toFixed(2)} MWh, DB=${m.dbTotal?.toFixed(2)} MWh`);
    });
  }

  if (errors.length > 0) {
    console.log('\nDays with Errors:');
    errors.forEach(e => console.log(`${e.date}: ${e.error}`));
  }

  // For days with mismatches, we should trigger reingestion
  if (mismatches.length > 0) {
    console.log('\nReingesting mismatched days...');
    // Here we would call the reingestion logic
  }
}

// Run the audit
auditJanuary2022().catch(console.error);