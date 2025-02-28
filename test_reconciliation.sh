#!/bin/bash
# Test script for the unified reconciliation system

echo "====== Unified Reconciliation System Test ======"
echo ""

# Test 1: Check status
echo "Test 1: Checking current reconciliation status..."
npx tsx unified_reconciliation.ts status

# Test 2: Run analysis
echo ""
echo "Test 2: Running reconciliation analysis..."
npx tsx unified_reconciliation.ts analyze

# Test 3: Test specific date functionality
echo ""
echo "Test 3: Testing with current date..."
TODAY=$(date +%Y-%m-%d)
npx tsx unified_reconciliation.ts date $TODAY

# Test 4: Run the comprehensive test script
echo ""
echo "Test 4: Running the TypeScript test suite..."
npx tsx run_reconciliation_test.ts

echo ""
echo "====== Test suite completed ======"