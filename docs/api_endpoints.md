# Bitcoin Mining Analytics Platform - API Endpoints

This document describes the API endpoints provided by the Bitcoin Mining Analytics platform.

## Summary Endpoints

### Get Lead Parties

```
GET /api/summary/lead-parties
```

Returns a list of all lead parties in the system.

**Response:**
```json
[
  "COMPANY_A",
  "COMPANY_B",
  "COMPANY_C"
]
```

### Get Curtailed Lead Parties

```
GET /api/summary/curtailed-lead-parties
```

Returns a list of lead parties that have curtailment records.

**Response:**
```json
[
  "COMPANY_A",
  "COMPANY_B"
]
```

### Get Daily Summary

```
GET /api/summary/daily/:date
```

Returns the curtailment summary for a specific date.

**Parameters:**
- `date`: The date in YYYY-MM-DD format

**Response:**
```json
{
  "date": "2025-03-06",
  "totalCurtailedEnergy": 47456.17,
  "totalPayment": -1043867.79
}
```

### Get Monthly Summary

```
GET /api/summary/monthly/:yearMonth
```

Returns the curtailment summary for a specific month.

**Parameters:**
- `yearMonth`: The year and month in YYYY-MM format

**Response:**
```json
{
  "yearMonth": "2025-03",
  "totalCurtailedEnergy": 356421.33,
  "totalPayment": -7843521.45
}
```

### Get Yearly Summary

```
GET /api/summary/yearly/:year
```

Returns the curtailment summary for a specific year.

**Parameters:**
- `year`: The year in YYYY format

**Response:**
```json
{
  "year": "2025",
  "totalCurtailedEnergy": 2072521.22,
  "totalPayment": -51757034.43
}
```

### Get Hourly Curtailment

```
GET /api/curtailment/hourly/:date
```

Returns hourly curtailment data for a specific date.

**Parameters:**
- `date`: The date in YYYY-MM-DD format

**Response:**
```json
[
  {
    "hour": "00:00",
    "curtailedEnergy": 1234.56
  },
  {
    "hour": "01:00",
    "curtailedEnergy": 2345.67
  }
]
```

## Mining Potential Endpoints

### Get Daily Mining Potential

```
GET /api/mining-potential/daily
```

Returns the daily mining potential for a specific date and miner model.

**Parameters:**
- `date`: The date in YYYY-MM-DD format
- `minerModel`: The miner model (S19J_PRO, S9, M20S)
- `farmId` (optional): Filter by specific farm ID

**Response:**
```json
{
  "date": "2025-03-06",
  "bitcoinMined": 36.86,
  "valueAtCurrentPrice": 2623909.38,
  "curtailedEnergy": 47456.17,
  "totalPayment": -1043867.79,
  "difficulty": 110568428300952,
  "currentPrice": 71186.04
}
```

### Get Monthly Mining Potential

```
GET /api/curtailment/monthly-mining-potential/:yearMonth
```

Returns the monthly mining potential for a specific month and miner model.

**Parameters:**
- `yearMonth`: The year and month in YYYY-MM format
- `minerModel`: The miner model (S19J_PRO, S9, M20S)
- `farmId` (optional): Filter by specific farm ID

**Response:**
```json
{
  "bitcoinMined": 262.57,
  "valueAtCurrentPrice": 18533948.33,
  "difficulty": 109916257638830.66,
  "currentPrice": 70587.31
}
```

### Get Yearly Mining Potential

```
GET /api/mining-potential/yearly/:year
```

Returns the yearly mining potential for a specific year and miner model.

**Parameters:**
- `year`: The year in YYYY format
- `minerModel`: The miner model (S19J_PRO, S9, M20S)
- `farmId` (optional): Filter by specific farm ID

**Response:**
```json
{
  "year": "2025",
  "bitcoinMined": 905.53,
  "valueAtCurrentPrice": 64460787.92,
  "curtailedEnergy": 2072521.22,
  "totalPayment": -51757034.43,
  "averageDifficulty": 109134644911480.95,
  "currentPrice": 71186.04
}
```

### Get Farm Statistics

```
GET /api/mining-potential/farm/:farmId
```

Returns statistics for a specific farm across time periods.

**Parameters:**
- `farmId`: The farm ID
- `period`: Time period (day, month, year)
- `value`: Value for the period (date, yearMonth, year)
- `minerModel`: The miner model (S19J_PRO, S9, M20S)

**Response:**
```json
{
  "farmId": "T_EDINW-1",
  "bitcoinMined": 2.43,
  "curtailedEnergy": 3128.89,
  "totalPayment": -68725.16
}
```

### Calculate Mining Potential

```
GET /api/curtailment/mining-potential
```

Calculates the potential Bitcoin mining for a given amount of energy.

**Parameters:**
- `date`: The date in YYYY-MM-DD format
- `minerModel`: The miner model (S19J_PRO, S9, M20S)
- `energy`: The curtailed energy in MWh
- `leadParty` (optional): Filter by lead party
- `farmId` (optional): Filter by farm ID

**Response:**
```json
{
  "bitcoinMined": 36.86,
  "valueAtCurrentPrice": 2623909.38,
  "difficulty": 110568428300952,
  "currentPrice": 71186.04
}
```

## Technical Endpoints

### Reconciliation Status

```
GET /api/reconciliation/status
```

Returns the current reconciliation status.

**Response:**
```json
{
  "overview": {
    "totalRecords": 123456,
    "totalCalculations": 115789,
    "missingCalculations": 7667,
    "completionPercentage": 93.79
  },
  "dateStats": [
    {
      "date": "2025-03-06",
      "expected": 2293,
      "actual": 2293,
      "missing": 0,
      "completionPercentage": 100
    }
  ]
}
```

## Error Responses

All endpoints return standardized error responses in the following format:

```json
{
  "error": {
    "message": "Error message description",
    "code": "ERROR_CODE",
    "details": {
      "additionalInfo": "Extra information about the error"
    }
  }
}
```

Common error codes:

- `VALIDATION_ERROR`: Invalid request parameters
- `NOT_FOUND`: Requested resource not found
- `DATABASE_ERROR`: Error accessing the database
- `API_ERROR`: Error communicating with external API
- `INTERNAL_ERROR`: Unexpected server error

## Authentication

Most endpoints do not require authentication. If authentication is required in the future, it will be implemented using bearer tokens in the Authorization header:

```
Authorization: Bearer <token>
```

## Rate Limiting

API rate limiting is implemented to prevent abuse. The current limits are:

- 100 requests per minute per IP address
- 1000 requests per hour per IP address

When a rate limit is exceeded, the API returns a 429 Too Many Requests response with a Retry-After header indicating how long to wait before making another request.