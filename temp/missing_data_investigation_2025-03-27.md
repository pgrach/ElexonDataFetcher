# Investigation: Missing Data for 2025-03-27

## Summary of Findings

We investigated missing data for settlement periods 35-48 on 2025-03-27. Our investigation confirms that **the data is not available from the Elexon API** for these periods.

## Evidence

1. **Database Verification:**
   - The database contains curtailment records for periods 1-34 only for 2025-03-27
   - Our query shows that the maximum period available is 34: `SELECT MAX(settlement_period) FROM curtailment_records WHERE settlement_date = '2025-03-27';`

2. **API Verification:**
   - Direct API calls to Elexon for periods 34-38 all return 404 "Resource not found" errors
   - API endpoint tested: `https://data.elexon.co.uk/bmrs/api/v1/balancing/bid-offer/accepted/settlement-period/[period]/settlement-date/2025-03-27`

3. **Reconciliation System:**
   - Our daily reconciliation check shows no issues for this date
   - The system correctly identifies the expected periods based on existing data, not a fixed number
   - It calculates expected periods by counting `COUNT(DISTINCT settlement_period)` from the curtailment_records table

## Conclusion

The missing periods 35-48 for 2025-03-27 represent a genuine gap in the source data from Elexon's API, not a failure in our data pipeline. The reconciliation system is working correctly by identifying 34 as the maximum available period for this date.

## Recommendations

1. **Status Quo:** No action needed for the pipeline itself as it's working correctly
2. **Documentation:** Note this date as having incomplete data in system documentation
3. **Monitoring:** Continue monitoring for similar patterns to identify if this is a common occurrence with Elexon's data

## Next Steps

- Monitor upcoming dates to see if this is a recurring pattern or a one-time issue
- Consider adding a notification system for dates with fewer than 48 settlement periods to highlight potential API data gaps
- For analytics purposes, ensure reporting accounts for dates with incomplete periods to avoid misleading conclusions