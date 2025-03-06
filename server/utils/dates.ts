/**
 * Date utilities for Bitcoin Mining Analytics platform
 * 
 * This module provides consistent date parsing, formatting, and manipulation
 * to ensure consistent date handling across the application.
 */

import { ValidationError } from './errors';

// Standard date format used across the application: YYYY-MM-DD
export const DATE_FORMAT_REGEX = /^\d{4}-\d{2}-\d{2}$/;
export const YEAR_MONTH_FORMAT_REGEX = /^\d{4}-\d{2}$/;
export const YEAR_FORMAT_REGEX = /^\d{4}$/;

/**
 * Validate a date string in YYYY-MM-DD format
 */
export function isValidDateString(dateStr: string): boolean {
  if (!DATE_FORMAT_REGEX.test(dateStr)) {
    return false;
  }
  
  const date = new Date(dateStr);
  return !isNaN(date.getTime());
}

/**
 * Validate a year-month string in YYYY-MM format 
 */
export function isValidYearMonth(yearMonth: string): boolean {
  return YEAR_MONTH_FORMAT_REGEX.test(yearMonth);
}

/**
 * Validate a year string in YYYY format
 */
export function isValidYear(year: string): boolean {
  return YEAR_FORMAT_REGEX.test(year);
}

/**
 * Parse a date string and ensure it's valid
 * Throws ValidationError if invalid
 */
export function parseDate(dateStr: string): Date {
  if (!isValidDateString(dateStr)) {
    throw new ValidationError(`Invalid date format: '${dateStr}'. Expected format: YYYY-MM-DD`);
  }
  
  return new Date(dateStr);
}

/**
 * Format a Date object to YYYY-MM-DD string
 */
export function formatDate(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  
  return `${year}-${month}-${day}`;
}

/**
 * Format a Date object to YYYY-MM string
 */
export function formatYearMonth(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  
  return `${year}-${month}`;
}

/**
 * Get yesterday's date as YYYY-MM-DD string
 */
export function getYesterday(): string {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  return formatDate(yesterday);
}

/**
 * Get dates in a range (inclusive)
 */
export function getDateRange(startDateStr: string, endDateStr: string): string[] {
  const startDate = parseDate(startDateStr);
  const endDate = parseDate(endDateStr);
  
  if (startDate > endDate) {
    throw new ValidationError('Start date must be before or equal to end date');
  }
  
  const dates: string[] = [];
  const currentDate = new Date(startDate);
  
  while (currentDate <= endDate) {
    dates.push(formatDate(currentDate));
    currentDate.setDate(currentDate.getDate() + 1);
  }
  
  return dates;
}

/**
 * Extract year and month components from a date string
 */
export function extractYearMonth(dateStr: string): { year: string; month: string } {
  const parts = dateStr.split('-');
  return {
    year: parts[0],
    month: parts[1]
  };
}

/**
 * Get the start date of a month
 */
export function getMonthStartDate(yearMonth: string): string {
  if (!isValidYearMonth(yearMonth)) {
    throw new ValidationError(`Invalid year-month format: '${yearMonth}'. Expected format: YYYY-MM`);
  }
  
  return `${yearMonth}-01`;
}

/**
 * Get the end date of a month
 */
export function getMonthEndDate(yearMonth: string): string {
  if (!isValidYearMonth(yearMonth)) {
    throw new ValidationError(`Invalid year-month format: '${yearMonth}'. Expected format: YYYY-MM`);
  }
  
  const [year, month] = yearMonth.split('-');
  // Calculate last day of month by getting the 0th day of the next month
  const lastDay = new Date(Number(year), Number(month), 0).getDate();
  
  return `${yearMonth}-${String(lastDay).padStart(2, '0')}`;
}

/**
 * Get the previous month in YYYY-MM format
 */
export function getPreviousMonth(yearMonth: string): string {
  if (!isValidYearMonth(yearMonth)) {
    throw new ValidationError(`Invalid year-month format: '${yearMonth}'. Expected format: YYYY-MM`);
  }
  
  const [year, month] = yearMonth.split('-');
  const previousMonth = new Date(Number(year), Number(month) - 2, 1);
  
  return formatYearMonth(previousMonth);
}

/**
 * Get the current month in YYYY-MM format
 */
export function getCurrentMonth(): string {
  return formatYearMonth(new Date());
}

/**
 * Get the previous day
 */
export function getPreviousDay(dateStr: string): string {
  const date = parseDate(dateStr);
  date.setDate(date.getDate() - 1);
  return formatDate(date);
}

/**
 * Check if date is within a specific number of days from today
 */
export function isWithinDays(dateStr: string, days: number): boolean {
  const date = parseDate(dateStr);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  
  const diffTime = today.getTime() - date.getTime();
  const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
  
  return diffDays >= 0 && diffDays <= days;
}

/**
 * Get the quarter from a date
 */
export function getQuarter(dateStr: string): number {
  const date = parseDate(dateStr);
  return Math.floor(date.getMonth() / 3) + 1;
}

/**
 * Get dates from previous days
 */
export function getRecentDates(days: number): string[] {
  const dates: string[] = [];
  const today = new Date();
  
  for (let i = 1; i <= days; i++) {
    const date = new Date(today);
    date.setDate(today.getDate() - i);
    dates.push(formatDate(date));
  }
  
  return dates;
}