#!/bin/bash

# Test script for the unified reconciliation system
# This script runs basic tests to verify the reconciliation system is working

echo "=== Unified Reconciliation System Test ==="
echo ""

# Check if the shell script exists and is executable
if [ ! -x "./unified_reconcile.sh" ]; then
  echo "❌ unified_reconcile.sh is not executable or doesn't exist"
  echo "   Run: chmod +x unified_reconcile.sh"
  exit 1
else
  echo "✅ unified_reconcile.sh is executable"
fi

# Test status command
echo ""
echo "Testing status command..."
./unified_reconcile.sh status > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo "✅ Status command executed successfully"
else
  echo "❌ Status command failed"
  exit 1
fi

# Get today's date in YYYY-MM-DD format
TODAY=$(date +%Y-%m-%d)

# Test a date command with today's date
echo ""
echo "Testing date command with $TODAY..."
echo "(This will take a few moments as it processes real data)"
./unified_reconcile.sh date "$TODAY" > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo "✅ Date command for $TODAY executed successfully"
else
  echo "❌ Date command for $TODAY failed"
  exit 1
fi

# Test analyze command
echo ""
echo "Testing analyze command..."
./unified_reconcile.sh analyze > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo "✅ Analyze command executed successfully"
else
  echo "❌ Analyze command failed"
  exit 1
fi

# All tests passed
echo ""
echo "=== All tests passed successfully ==="
echo "The reconciliation system is functioning correctly."
echo ""
echo "Available commands:"
echo "  ./unified_reconcile.sh status               - Show current status"
echo "  ./unified_reconcile.sh analyze              - Analyze issues"
echo "  ./unified_reconcile.sh date YYYY-MM-DD      - Process a date"
echo "  ./unified_reconcile.sh range START END      - Process a range"
echo "  ./unified_reconcile.sh critical YYYY-MM-DD  - Process critical date"
echo ""
echo "For more details, run: ./unified_reconcile.sh help"
exit 0