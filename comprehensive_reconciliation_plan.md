# Comprehensive Bitcoin Calculation Reconciliation Plan

## Project Status Summary
We've confirmed that the reconciliation approach works well for individual dates. Processing 2023-10-15 was successful, adding 1008 Bitcoin calculations (336 curtailment records × 3 miner models). 

Current completion rates by year:
- 2023: 0.32% (1,242/392,097)
- 2022: 16.74% (103,387/617,649)
- 2025: 49.54% (119,913/242,031)
- 2024: 81.28% (678,951/835,302)

Total missing records: 1,184,586

## Technical Approach

### Core Reconciliation Logic
For each date with missing calculations:
1. Create a temporary aggregation of curtailment records by settlement_date, settlement_period, and farm_id
2. Insert calculation records for each miner model (S19J_PRO, S9, M20S) with appropriate difficulty values 
3. Use ON CONFLICT to update existing records if they exist
4. Track progress with detailed statistics

### SQL Functions
1. `process_single_date(date, difficulty)`: Process a single date directly
2. `process_month_reconciliation(year, month, difficulty, max_dates)`: Process a batch of dates within a month
3. `get_reconciliation_summary()`: View overall status by year
4. `get_priority_months(max_months)`: Identify highest-priority months to process

## Implementation Strategy

### Phase 1: Date-by-Date Processing (Current Phase)
- Process individual high-impact dates to verify approach
- Focus on dates with most missing calculations first
- Establish benchmarks for performance and record counts

### Phase 2: Month-by-Month Reconciliation
- Prioritize the following high-impact months:
  - 2023-10 (Large number of missing records, demonstrated success)
  - 2022-03 (Target for 2022 data)
  - 2025-02 (Current month, needs completion)
- Process each month in smaller batches to avoid timeouts

### Phase 3: Year-by-Year Completion
- Start with 2023 (lowest completion rate)
- Process in batches of 10-20 dates at a time
- Continue with 2022, then 2025 and 2024

## Execution Plan

### Immediate Next Steps
1. Process top 10 dates from 2023 with most missing records
2. Process top 10 dates from 2022 with most missing records
3. Complete February 2025 to achieve 100% reconciliation for current month

### Challenges and Solutions
- **Database Timeouts**: Break processing into smaller batches of 10-20 dates
- **Transaction Management**: Commit after each date to save progress
- **Progress Tracking**: Use reconciliation_progress table to resume processing
- **Error Handling**: Capture and log errors without failing entire batch

## Monitoring and Verification
- Use SQL queries to track progress over time
- Verify calculations with expected ratios (curtailment_count × 3)
- Implement reporting queries to identify any remaining gaps

## Expected Outcomes
- Complete reconciliation (100%) for all years
- Well-documented approach for future maintenance
- Approximately 2,087,079 total Bitcoin calculation records when complete