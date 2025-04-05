import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * Format energy values, converting to GWh for values >= 1000
 * @param value Energy value in MWh
 * @returns Formatted string with appropriate unit (MWh or GWh)
 */
export function formatEnergy(value: number | string): string {
  const numValue = typeof value === 'string' ? Number(value) : value
  
  if (Number.isNaN(numValue)) return "0 MWh"
  
  // Convert to GWh for values with 4 or more significant digits before decimal
  if (Math.abs(numValue) >= 1000) {
    const gwh = numValue / 1000
    // Round to 1 decimal place for GWh
    return `${gwh.toFixed(1).replace(/\.0$/, '')} GWh`
  }
  
  // Keep as MWh with no decimals
  return `${Math.round(numValue).toLocaleString()} MWh`
}

/**
 * Format currency values in GBP, converting to millions for large values
 * Always display payment values as positive (for Subsidies Paid display)
 * @param value Amount in GBP
 * @returns Formatted string with £ symbol and M suffix for millions
 */
export function formatGBP(value: number | string): string {
  const numValue = typeof value === 'string' ? Number(value) : value
  
  if (Number.isNaN(numValue)) return "£0"
  
  // Always use absolute value for display (payment values are stored as negative)
  const displayValue = Math.abs(numValue)
  
  // Convert to millions for values with 7 or more digits
  if (displayValue >= 1000000) {
    const millions = displayValue / 1000000
    // Round to 1 decimal place for millions
    return `£${millions.toFixed(1).replace(/\.0$/, '')} M`
  }
  
  // Regular formatting for smaller values
  return `£${Math.round(displayValue).toLocaleString()}`
}

/**
 * Format Bitcoin values with appropriate decimal precision
 * @param value Bitcoin value
 * @returns Formatted string with appropriate decimal places
 */
export function formatBitcoin(value: number | string): string {
  const numValue = typeof value === 'string' ? Number(value) : value
  
  if (Number.isNaN(numValue)) return "0 BTC"
  
  // For values >= 1, show only 1 decimal place
  if (Math.abs(numValue) >= 1) {
    return `${numValue.toFixed(1)} BTC`
  }
  
  // For values < 1, show 2 decimal places
  return `${numValue.toFixed(2)} BTC`
}
