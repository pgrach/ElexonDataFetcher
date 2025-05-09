/**
 * Curtailment Data Models
 * 
 * This file contains TypeScript interfaces and types for curtailment-related data.
 */

/**
 * Raw curtailment record from Elexon API
 */
export interface ElexonCurtailmentRecord {
  /** BMU ID */
  id: string;
  
  /** Time period */
  period: number;
  
  /** Volume in MWh (negative for curtailment) */
  volume: number;
  
  /** Original bid/offer price */
  originalPrice: number;
  
  /** Final bid/offer price */
  finalPrice: number;
  
  /** System Operator flag */
  soFlag: boolean;
  
  /** CADL flag */
  cadlFlag: boolean;
}

/**
 * Daily curtailment summary
 */
export interface DailyCurtailmentSummary {
  /** Date in YYYY-MM-DD format */
  date: string;
  
  /** Total curtailed energy in MWh */
  totalCurtailedEnergy: number;
  
  /** Total payment for curtailment in GBP */
  totalPayment: number;
  
  /** Number of settlement periods with curtailment */
  periodCount?: number;
  
  /** Number of farms curtailed */
  farmCount?: number;
}

/**
 * Monthly curtailment summary
 */
export interface MonthlyCurtailmentSummary {
  /** Year and month in YYYY-MM format */
  yearMonth: string;
  
  /** Total curtailed energy in MWh */
  totalCurtailedEnergy: number;
  
  /** Total payment for curtailment in GBP */
  totalPayment: number;
  
  /** Last updated timestamp */
  updatedAt: Date;
}

/**
 * Calculate curtailment payment
 * 
 * @param volume Volume in MWh (should be positive)
 * @param price Price per MWh in GBP
 * @returns Payment amount in GBP
 */
export function calculateCurtailmentPayment(volume: number, price: number): number {
  const absVolume = Math.abs(volume);
  return absVolume * price;
}

/**
 * Check if a record represents curtailment
 * 
 * @param record The Elexon record to check
 * @returns Boolean indicating if this is a curtailment record
 */
export function isCurtailmentRecord(record: ElexonCurtailmentRecord): boolean {
  return (
    record.volume < 0 && 
    (record.soFlag || record.cadlFlag)
  );
}