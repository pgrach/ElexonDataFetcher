"use client";

import { useState, useEffect } from "react";
import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";

interface FarmComparisonChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
}

export default function FarmComparisonChart({ timeframe, date, minerModel }: FarmComparisonChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd");
  const yearMonth = format(date, "yyyy-MM");
  const year = format(date, "yyyy");
  
  // Use a placeholder query that would fetch top farms data
  // In a real implementation, this would call a specific API endpoint for farm comparison
  const { data: farmsData = [], isLoading } = useQuery({
    queryKey: [`/api/lead-parties/${formattedDate}`],
    queryFn: async () => {
      // Fetch the list of lead parties (farms)
      const url = new URL(`/api/lead-parties/${formattedDate}`, window.location.origin);
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error("Failed to fetch lead parties");
      }
      
      const leadParties = await response.json();
      
      // For demonstration, we're only showing the first 5 farms with sample data
      // In a real implementation, you would fetch actual farm comparison data
      return leadParties.slice(0, 5).map((party: string, index: number) => ({
        name: party,
        curtailedEnergy: Math.random() * 1000 + 100, // This would be actual farm data
        bitcoinPotential: Math.random() * 2 + 0.1,  // This would be actual bitcoin potential data
      }));
    }
  });
  
  // Get chart title based on timeframe
  const chartTitle = 
    timeframe === "yearly" ? `Top Farms Comparison (${year})` :
    timeframe === "monthly" ? `Top Farms Comparison (${format(date, "MMMM yyyy")})` :
    `Top Farms Comparison (${format(date, "PP")})`;
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>{chartTitle}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <Skeleton className="h-[300px] w-full" />
        ) : farmsData.length === 0 ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            No farm comparison data available for this {timeframe} period
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart
              data={farmsData}
              margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" tick={{ fontSize: 12 }} />
              <YAxis 
                yAxisId="left" 
                orientation="left" 
                stroke="#8884d8" 
                tick={{ fontSize: 12 }}
                label={{ value: 'Curtailed Energy (MWh)', angle: -90, position: 'insideLeft' }}
              />
              <YAxis 
                yAxisId="right" 
                orientation="right" 
                stroke="#82ca9d" 
                tick={{ fontSize: 12 }}
                label={{ value: 'Bitcoin Potential (₿)', angle: 90, position: 'insideRight' }}
              />
              <Tooltip />
              <Legend />
              <Bar 
                yAxisId="left" 
                dataKey="curtailedEnergy" 
                name="Curtailed Energy (MWh)" 
                fill="#8884d8" 
              />
              <Bar 
                yAxisId="right" 
                dataKey="bitcoinPotential" 
                name="Bitcoin Potential (₿)" 
                fill="#82ca9d" 
              />
            </BarChart>
          </ResponsiveContainer>
        )}
        <div className="text-xs text-muted-foreground mt-2 text-center">
          Note: This is a placeholder demonstration of the farm comparison chart.
          In a production environment, this would display actual farm comparison data.
        </div>
      </CardContent>
    </Card>
  );
}