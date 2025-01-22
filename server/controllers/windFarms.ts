import { Request, Response } from "express";
import { db } from "@db";
import { windFarmLocations, curtailmentRecords } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";

export async function getWindFarmPerformance(req: Request, res: Response) {
  try {
    const { date } = req.query;

    if (!date || typeof date !== 'string') {
      return res.status(400).json({
        error: "Date parameter is required (YYYY-MM-DD format)"
      });
    }

    // Get performance data for all wind farms for the specified date
    const performanceData = await db
      .select({
        farmId: windFarmLocations.farmId,
        name: windFarmLocations.name,
        latitude: windFarmLocations.latitude,
        longitude: windFarmLocations.longitude,
        capacity: windFarmLocations.capacity,
        curtailedEnergy: sql<string>`COALESCE(SUM(${curtailmentRecords.volume}), 0)`,
        payment: sql<string>`COALESCE(SUM(${curtailmentRecords.payment}), 0)`,
      })
      .from(windFarmLocations)
      .leftJoin(
        curtailmentRecords,
        and(
          eq(curtailmentRecords.farmId, windFarmLocations.farmId),
          eq(curtailmentRecords.settlementDate, date)
        )
      )
      .groupBy(
        windFarmLocations.farmId,
        windFarmLocations.name,
        windFarmLocations.latitude,
        windFarmLocations.longitude,
        windFarmLocations.capacity
      );

    // Calculate utilization rate and format response
    const formattedData = performanceData.map(farm => {
      const curtailedEnergy = Number(farm.curtailedEnergy);
      const payment = Math.abs(Number(farm.payment)); // Convert to positive number
      const capacity = Number(farm.capacity);
      
      // Calculate utilization rate as percentage of capacity
      // Assuming 24 hours of operation
      const utilizationRate = (curtailedEnergy / (capacity * 24)) * 100;

      return {
        ...farm,
        curtailedEnergy,
        payment,
        capacity,
        utilizationRate,
      };
    });

    res.json(formattedData);
  } catch (error) {
    console.error('Error fetching wind farm performance:', error);
    res.status(500).json({
      error: "Internal server error while fetching wind farm performance"
    });
  }
}
