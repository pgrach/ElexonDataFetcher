#!/bin/bash

# Define date to process
DATE=${1:-"2025-04-01"}
RESUME_FLAG=""
FORCE_DELETE_FLAG=""

# Process arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --resume)
      RESUME_FLAG="--resume"
      shift
      ;;
    --force-delete)
      FORCE_DELETE_FLAG="--force-delete"
      shift
      ;;
    *)
      if [[ $1 =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        DATE=$1
      fi
      shift
      ;;
  esac
done

echo "==================================================="
echo "   RESUMABLE ELEXON REINGESTION for $DATE"
echo "==================================================="
echo "Resume mode: ${RESUME_FLAG:=No}"
echo "Force delete: ${FORCE_DELETE_FLAG:=No}"
echo "Starting reingestion process..."
npx tsx reingest_elexon_resumable.ts $DATE $RESUME_FLAG $FORCE_DELETE_FLAG