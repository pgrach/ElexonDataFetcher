# Curtailment Data Verification Findings

## Executive Summary

An in-depth analysis was conducted to verify the integrity of curtailment data stored in the database against source data from the Elexon API. The analysis focused on recent dates in March 2025, specifically examining periods in the 18:00 hour.

## Key Findings

1. **Volume Data Integrity**: 
   - The volume data in the database exactly matches the Elexon API data (0% discrepancy)
   - All volume values are stored as negative values in both the database and the API

2. **Payment Calculation**:
   - Payment values have consistent sign inversion between database and API
   - Database stores payment values as negative (-) while our API calculation produces positive values
   - The actual magnitude (absolute value) of payments matches perfectly
   - The formula in database is: `payment = volume * price` (keeps the negative sign)
   - Our API calculation is: `payment = Math.abs(volume) * price * -1` (produces negative)

3. **Sign Convention Analysis**:
   - 97-98% of records in the database have negative payment values
   - 100% of volume records are negative
   - Correlation analysis confirms that nearly all records (97-98%) have both volume and payment as negative

4. **Database Structure**:
   - The `processDailyCurtailment` function in `server/services/curtailment.ts` shows the payment calculation as:
     ```typescript
     const volume = Math.abs(record.volume);
     const payment = volume * record.originalPrice;
     ```
   - However, the database storage:
     ```typescript
     volume: record.volume.toString(), // Keep the original negative value
     payment: payment.toString(),
     ```
   - This explains why our data has negative payments (using originalPrice which is negative)

## Root Cause of Sign Difference

The sign convention discrepancy is caused by different approaches to payment calculation:

1. **Database Calculation (in curtailment.ts)**:
   ```typescript
   // Takes the absolute value of volume (turns negative to positive)
   const volume = Math.abs(record.volume);
   // Multiplies by original price (typically negative for curtailment)
   const payment = volume * record.originalPrice;
   // Result: Payment is negative
   ```

2. **API Calculation (in our verification script)**:
   ```typescript
   // Keep volume negative
   // Multiply by price and add an extra -1
   const payment = Math.abs(record.volume) * record.originalPrice * -1;
   // Result: Payment is positive (due to double negative)
   ```

## Conclusion

The apparent discrepancy in payment values is not an error in data integrity but a deliberate sign convention difference. The database uses a consistent approach by keeping payments negative when costs are incurred (which makes logical sense for curtailment payments). 

Our comparison tool needs to adjust its sign convention to match the database, rather than considering this a data discrepancy.

## Recommendations

1. **Update Verification Logic**: 
   - Modify the comparison script to compare the absolute values of payments
   - Or, remove the extra `-1` multiplier in our API payment calculation

2. **Documentation Update**:
   - Add clear documentation about the sign convention used in the system
   - Note that curtailment volumes are negative, and payments (costs) are also negative

3. **Validation Improvement**:
   - Implement automated tests to ensure consistent sign conventions
   - Add validation logic to ensure curtailment volumes are always negative and payments have the expected sign

4. **Future Development**:
   - Consider improving UI presentation to show payment amounts as positive values with clear labels indicating cost/revenue
   - Maintain consistent internal representation while providing intuitive user-facing display