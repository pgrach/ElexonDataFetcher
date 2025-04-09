/**
 * Utility functions for various calculations used throughout the application
 */

/**
 * Convert curtailment energy to megawatt hours
 * 
 * This is a simple pass-through function as the curtailment data
 * is already in MWh, but it's included for clarity and to allow
 * for any future unit conversions if needed.
 */
export function convertCurtailmentToMegawattHours(curtailedEnergy: number): number {
  // Curtailment data is already in MWh, so no conversion needed
  return curtailedEnergy;
}

/**
 * Convert settlement period (1-48) to hour and minute
 */
export function settlementPeriodToTime(period: number): { hour: number; minute: number } {
  if (period < 1 || period > 48) {
    throw new Error(`Invalid settlement period: ${period}. Must be between 1 and 48.`);
  }
  
  // Each period is 30 minutes
  // Period 1 = 00:00 - 00:30, Period 2 = 00:30 - 01:00, etc.
  const totalMinutes = (period - 1) * 30;
  const hour = Math.floor(totalMinutes / 60);
  const minute = totalMinutes % 60;
  
  return { hour, minute };
}

/**
 * Convert hour and minute to settlement period (1-48)
 */
export function timeToSettlementPeriod(hour: number, minute: number): number {
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    throw new Error(`Invalid time: ${hour}:${minute}`);
  }
  
  // Convert to total minutes from midnight
  const totalMinutes = hour * 60 + minute;
  
  // Calculate period (1-48)
  // Each period is 30 minutes
  const period = Math.floor(totalMinutes / 30) + 1;
  
  return period;
}

/**
 * Format settlement period as a time string (e.g., "00:00" or "13:30")
 */
export function formatSettlementPeriodTime(period: number): string {
  const { hour, minute } = settlementPeriodToTime(period);
  return `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}`;
}

/**
 * Calculate average hourly energy from a daily total
 */
export function calculateHourlyEnergy(dailyTotalMWh: number): number {
  return dailyTotalMWh / 24;
}

/**
 * Calculate daily total energy from hourly values
 */
export function calculateDailyEnergy(hourlyValues: number[]): number {
  if (hourlyValues.length !== 24) {
    throw new Error(`Expected 24 hourly values, but got ${hourlyValues.length}`);
  }
  
  return hourlyValues.reduce((sum, value) => sum + value, 0);
}

/**
 * Convert payment values from a specific currency format to a numeric value
 */
export function parsePaymentValue(paymentStr: string): number {
  // Remove currency symbol, commas, and any other non-numeric characters except decimal point and negative sign
  const numericStr = paymentStr.replace(/[^0-9.-]/g, '');
  return parseFloat(numericStr);
}

/**
 * Format a number as a currency string (e.g., "£123.45")
 */
export function formatCurrency(value: number): string {
  return `£${Math.abs(value).toFixed(2)}`;
}