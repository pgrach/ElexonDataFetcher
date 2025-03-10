"use client";

import { useState, useEffect } from "react";
import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";

interface CurtailmentChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

export default function CurtailmentChart({ timeframe, date, minerModel, farmId }: CurtailmentChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd");
  
  // Fetch hourly data
  const { data: hourlyData = [], isLoading } = useQuery({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, farmId],
    queryFn: async () => {
      const url = new URL(`/api/curtailment/hourly/${formattedDate}`, window.location.origin);
      if (farmId) {
        url.searchParams.set("leadParty", farmId);
      }
      
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error("Failed to fetch hourly data");
      }
      
      return response.json();
    },
    enabled: timeframe === "daily" // Only fetch when in daily view
  });
  
  // Process data for the chart
  const chartData = hourlyData.map((item: any) => ({
    hour: item.hour,
    curtailedEnergy: Number(item.curtailedEnergy),
  }));
  
  // Helper for checking if an hour is in the future
  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(":").map(Number);
    const now = new Date();
    const selectedDate = new Date(date);
    
    if (format(now, "yyyy-MM-dd") === format(selectedDate, "yyyy-MM-dd")) {
      return hour > now.getHours();
    }
    return selectedDate > now;
  };
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>Hourly Breakdown</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <Skeleton className="h-[300px] w-full" />
        ) : timeframe !== "daily" ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            This chart is only available in daily view
          </div>
        ) : chartData.length === 0 ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            No curtailment data available for this date
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart
              data={chartData}
              margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="hour" 
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => value.split(":")[0] + ":00"}
              />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip 
                formatter={(value: number) => [`${value.toFixed(2)} MWh`, "Curtailed Energy"]}
                labelFormatter={(label) => `Hour: ${label}`}
              />
              <Legend />
              <Bar
                dataKey="curtailedEnergy"
                fill="#000000"
                name="Curtailed Energy (MWh)"
                // Apply different styling for future hours
                {...(chartData.some(d => isHourInFuture(d.hour)) && {
                  shape: (props: any) => {
                    const { x, y, width, height, payload } = props;
                    const inFuture = isHourInFuture(payload.hour);
                    
                    return (
                      <rect
                        key={`bar-${payload.hour}`}
                        x={x}
                        y={y}
                        width={width}
                        height={height}
                        fill={inFuture ? "#f5f5f5" : "#000000"}
                        stroke={inFuture ? "#000000" : "none"}
                        strokeWidth={1}
                        r={0}
                      />
                    );
                  }
                })}
              />
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}