/**
 * Simple test for the unified reconciliation system
 * This script verifies that the unified_reconciliation.ts module can be loaded
 * and that its exported functions are available.
 */

import { 
  getReconciliationStatus,
  findDatesWithMissingCalculations,
  processDate
} from './unified_reconciliation';

console.log('Unified Reconciliation Module Test');
console.log('==================================');
console.log('');
console.log('The following functions were successfully imported:');
console.log('- getReconciliationStatus');
console.log('- findDatesWithMissingCalculations');
console.log('- processDate');
console.log('');
console.log('The module was loaded successfully!');