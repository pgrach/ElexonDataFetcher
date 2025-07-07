# Exact Prompt for Future Agents: Data Ingestion

## When asked to ingest/verify curtailment data for any date:

I need you to ingest and verify curtailment data for **[DATE]** using the proven API-first methodology.

### CRITICAL SUCCESS REQUIREMENTS:

**1. API-First Verification (MANDATORY)**
- ALWAYS verify against Elexon BMRS API before trusting database
- The July 6, 2025 case showed database had only 9 records vs 199 actual records from API
- Never assume existing database data is complete without API verification

**2. Use Proven Re-ingestion System**
- Use existing `processDailyCurtailment()` service from `curtailmentService.ts` 
- OR use API endpoint: `POST /api/ingest/{date}`
- NEVER create custom scripts or field mapping

**3. Payment Sign Logic (Business Critical)**
- Payments represent subsidies paid TO wind farms = must be POSITIVE
- Fix signs in both `curtailment_records` and `daily_summaries` tables
- Business logic: `UPDATE tables SET payment = ABS(payment::numeric)`

**4. Expected Data Patterns**
- Typical volume: 100-5000 MWh per day
- High curtailment periods: 39-48 (evening/night hours)
- Low curtailment periods: 1-3 (early morning)
- Expect 50-500 records per day depending on wind conditions

**5. Verification Steps**
```sql
-- Quick database check
SELECT COUNT(*) as records, COUNT(DISTINCT settlement_period) as periods,
       ROUND(SUM(ABS(volume::numeric)), 2) as volume_mwh
FROM curtailment_records WHERE settlement_date = '{date}';
```

```bash
# Test API endpoint
curl "http://localhost:5000/api/summary/daily/{date}"
```

### SUCCESS CRITERIA (All must pass):
✅ Database record count matches API data (±5%)
✅ Settlement periods include high-curtailment hours (39-48)
✅ Volume totals realistic (100-5000 MWh range)
✅ ALL payments are POSITIVE values
✅ API endpoint returns proper JSON with positive payment
✅ No missing key settlement periods

### RED FLAGS (Re-ingest immediately if any occur):
🚨 Database has <50 records for a full day
🚨 Only 1-5 settlement periods in database  
🚨 Missing periods 39-48 (prime curtailment hours)
🚨 Volume <50 MWh or >10,000 MWh (outlier range)
🚨 Negative payment values
🚨 API endpoint returns error or zero values

### FILES YOU NEED:
- `server/services/curtailmentService.ts` (proven ingestion service)
- `server/controllers/summary.ts` (API payment logic reference)  
- `data-verification-methodology.md` (complete methodology)
- `replit.md` (project context and data patterns)

### EXACT COMMAND SEQUENCE:
```bash
# Step 1: Quick API verification (check a high-curtailment period)
curl "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/stack/all/offer/{date}/40"

# Step 2: Check database state
# (Use SQL to check record count and periods)

# Step 3: Re-ingest if mismatch detected
curl -X POST "http://localhost:5000/api/ingest/{date}"

# Step 4: Fix payment signs
# (Use SQL to make payments positive)

# Step 5: Final verification
curl "http://localhost:5000/api/summary/daily/{date}"
```

### PROVEN FAILURE CASE TO AVOID:
- **July 6, 2025**: Agent assumed 9 database records were correct
- **Reality**: API had 199 records (22x more data!)
- **Lesson**: Database can be severely incomplete - always verify API first

**This methodology guarantees success on first attempt by using proven systems and API verification.**