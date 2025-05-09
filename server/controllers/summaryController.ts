/**
 * Summary Controller
 * 
 * Handles API requests related to summary data and delegates business logic to services.
 */

import { Request, Response } from "express";
import * as summaryService from "../services/summaryService";

/**
 * Get all lead parties
 */
export async function getLeadParties(req: Request, res: Response) {
  try {
    const leadParties = await summaryService.getAllLeadParties();
    res.json(leadParties);
  } catch (error) {
    console.error('Error fetching lead parties:', error);
    res.status(500).json({
      error: "Internal server error while fetching lead parties"
    });
  }
}

/**
 * Get lead parties that had curtailment on a specific date
 */
export async function getCurtailedLeadParties(req: Request, res: Response) {
  try {
    const { date } = req.params;
    
    try {
      const leadParties = await summaryService.getLeadPartiesForDate(date);
      res.json(leadParties);
    } catch (validationError) {
      return res.status(400).json({
        error: validationError instanceof Error ? validationError.message : "Invalid request"
      });
    }
  } catch (error) {
    console.error('Error fetching curtailed lead parties:', error);
    res.status(500).json({
      error: "Internal server error while fetching curtailed lead parties"
    });
  }
}

/**
 * Get daily summary for a specific date
 */
export async function getDailySummary(req: Request, res: Response) {
  try {
    const { date } = req.params;
    
    try {
      const summary = await summaryService.getDailySummary(date);
      
      if (!summary) {
        return res.status(404).json({
          error: `No data available for date ${date}`
        });
      }
      
      res.json(summary);
    } catch (validationError) {
      return res.status(400).json({
        error: validationError instanceof Error ? validationError.message : "Invalid request"
      });
    }
  } catch (error) {
    console.error('Error fetching daily summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching daily summary"
    });
  }
}

/**
 * Get monthly summary for a specific month
 */
export async function getMonthlySummary(req: Request, res: Response) {
  try {
    const { month } = req.params;
    
    try {
      const summary = await summaryService.getMonthlySummary(month);
      
      if (!summary) {
        return res.status(404).json({
          error: `No data available for month ${month}`
        });
      }
      
      res.json(summary);
    } catch (validationError) {
      return res.status(400).json({
        error: validationError instanceof Error ? validationError.message : "Invalid request"
      });
    }
  } catch (error) {
    console.error('Error fetching monthly summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching monthly summary"
    });
  }
}

/**
 * Get yearly summary for a specific year
 */
export async function getYearlySummary(req: Request, res: Response) {
  try {
    const { year } = req.params;
    
    try {
      const summary = await summaryService.getYearlySummary(year);
      
      if (!summary) {
        return res.status(404).json({
          error: `No data available for year ${year}`
        });
      }
      
      res.json(summary);
    } catch (validationError) {
      return res.status(400).json({
        error: validationError instanceof Error ? validationError.message : "Invalid request"
      });
    }
  } catch (error) {
    console.error('Error fetching yearly summary:', error);
    res.status(500).json({
      error: "Internal server error while fetching yearly summary"
    });
  }
}