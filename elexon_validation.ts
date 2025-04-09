/**
 * Elexon API Validation Helper
 * 
 * This module provides a simple wrapper around the fetchBidsOffers function
 * to make it easily accessible to all the validation scripts.
 */

import { fetchBidsOffers } from "./server/services/elexon";

// Re-export the function for use in validation scripts
export { fetchBidsOffers };

// Simple helper for logging validation results
export function logValidationResult(description: string, value: any): void {
  console.log(`${description}: ${value}`);
}

// Helper for formatting currency values
export function formatCurrency(value: number): string {
  return `£${value.toFixed(2)}`;
}

// Helper for calculating percentage difference
export function calculatePercentageDifference(value1: number, value2: number): number {
  if (value1 === 0) return 0;
  return (Math.abs(value1 - value2) / value1) * 100;
}