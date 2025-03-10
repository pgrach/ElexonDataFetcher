"use client";

import { useState, useEffect } from "react";
import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, LabelList } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";

interface CurtailmentChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

// SVG Bitcoin logo for chart label
const BitcoinIcon = ({ x, y, value }: { x: number, y: number, value: number }) => {
  // Skip rendering if value is too small
  if (value < 0.01) return null;
  
  return (
    <g transform={`translate(${x},${y - 22})`}>
      {/* Bitcoin circle */}
      <circle cx="10" cy="10" r="10" fill="#F7931A" />
      
      {/* Bitcoin B symbol */}
      <path 
        d="M14.25,8.65c0.18-1.22-0.75-1.87-2.02-2.31l0.41-1.66l-1.01-0.25l-0.4,1.61c-0.27-0.07-0.54-0.13-0.81-0.19l0.4-1.62L9.81,4l-0.41,1.66c-0.22-0.05-0.44-0.1-0.65-0.15l0,0L7.43,5.2L7.15,6.3c0,0,0.75,0.17,0.73,0.18C8.34,6.62,8.43,6.81,8.4,6.96L7.93,8.91c0.02,0.01,0.05,0.01,0.08,0.03l-0.08-0.02L7.24,11.9c-0.05,0.11-0.17,0.28-0.45,0.22c0.01,0.02-0.73-0.18-0.73-0.18l-0.5,1.15l1.85,0.46c0.34,0.09,0.68,0.18,1.01,0.26l-0.42,1.68l1.01,0.25l0.41-1.66c0.28,0.08,0.55,0.15,0.81,0.21l-0.41,1.65l1.01,0.25l0.42-1.67c1.74,0.33,3.05,0.2,3.6-1.38c0.44-1.27,0.02-2.01-0.93-2.49C13.89,10.25,14.13,9.53,14.25,8.65z M12.19,11.79c-0.31,1.27-2.43,0.58-3.12,0.41l0.56-2.23C10.32,10.13,12.52,10.46,12.19,11.79z M12.5,8.63c-0.29,1.15-2.05,0.57-2.63,0.42l0.5-2.02C10.96,7.18,12.8,7.42,12.5,8.63z"
        fill="white" 
      />
      
      {/* Value text below the icon with more spacing */}
      <text 
        x="10" 
        y="35" 
        textAnchor="middle" 
        fontSize="11" 
        fontWeight="bold" 
        fill="#F7931A"
        style={{ textShadow: '0px 0px 2px white' }}
      >
        {value.toFixed(2)}
      </text>
    </g>
  );
};

export default function CurtailmentChart({ timeframe, date, minerModel, farmId }: CurtailmentChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd");
  const formattedYearMonth = format(date, "yyyy-MM");
  const currentYear = date.getFullYear();
  
  // Fetch hourly data for daily view
  const { data: hourlyData = [], isLoading: isHourlyLoading } = useQuery({
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
  
  // Fetch monthly data for monthly view
  const { data: monthlyData = [], isLoading: isMonthlyLoading } = useQuery({
    queryKey: [`/api/mining-potential/monthly`, currentYear, minerModel, farmId],
    queryFn: async () => {
      // Fetch data for each month of the year
      const months = [];
      const monthlyDataArray = [];
      
      for (let i = 1; i <= 12; i++) {
        const monthStr = i.toString().padStart(2, '0');
        const yearMonth = `${currentYear}-${monthStr}`;
        months.push(yearMonth);
      }
      
      // Fetch each month's data
      for (const yearMonth of months) {
        try {
          const url = new URL(`/api/mining-potential/monthly/${yearMonth}`, window.location.origin);
          url.searchParams.set("minerModel", minerModel);
          
          if (farmId) {
            url.searchParams.set("farmId", farmId);
          }
          
          const response = await fetch(url);
          if (response.ok) {
            const data = await response.json();
            monthlyDataArray.push({
              month: yearMonth,
              curtailedEnergy: data.totalCurtailedEnergy || 0,
              bitcoinMined: data.totalBitcoinMined || 0
            });
          } else {
            // If month doesn't have data yet, add empty data
            monthlyDataArray.push({
              month: yearMonth,
              curtailedEnergy: 0,
              bitcoinMined: 0
            });
          }
        } catch (error) {
          console.error(`Error fetching data for ${yearMonth}:`, error);
        }
      }
      
      return monthlyDataArray;
    },
    enabled: timeframe === "monthly" // Only fetch when in monthly view
  });
  
  // Fetch daily Bitcoin potential for daily view
  const { data: dailyBitcoin = { bitcoinMined: 0 }, isLoading: isBitcoinLoading } = useQuery({
    queryKey: [`/api/curtailment/mining-potential`, formattedDate, minerModel, farmId],
    queryFn: async () => {
      // Get daily summary first to get total energy
      const summaryUrl = new URL(`/api/summary/daily/${formattedDate}`, window.location.origin);
      if (farmId) {
        summaryUrl.searchParams.set("leadParty", farmId);
      }
      
      const summaryResponse = await fetch(summaryUrl);
      if (!summaryResponse.ok) {
        return { bitcoinMined: 0, valueAtCurrentPrice: 0 };
      }
      
      const summary = await summaryResponse.json();
      
      // Now get Bitcoin potential
      const url = new URL(`/api/curtailment/mining-potential`, window.location.origin);
      url.searchParams.set("date", formattedDate);
      url.searchParams.set("minerModel", minerModel);
      url.searchParams.set("energy", summary.totalCurtailedEnergy.toString());
      
      if (farmId) {
        url.searchParams.set("leadParty", farmId);
      }
      
      const response = await fetch(url);
      if (!response.ok) {
        return { bitcoinMined: 0, valueAtCurrentPrice: 0 };
      }
      
      return response.json();
    },
    enabled: timeframe === "daily" // Only fetch when in daily view
  });
  
  const isLoading = 
    (timeframe === "daily" && (isHourlyLoading || isBitcoinLoading)) || 
    (timeframe === "monthly" && isMonthlyLoading);
  
  // Process data for the daily chart
  const dailyChartData = hourlyData.map((item: any) => {
    const curtailedEnergy = Number(item.curtailedEnergy);
    const totalEnergy = hourlyData.reduce((sum: number, h: any) => sum + Number(h.curtailedEnergy), 0);
    
    // Calculate Bitcoin for this hour based on proportion of energy
    const bitcoinPotential = totalEnergy > 0 
      ? (curtailedEnergy / totalEnergy) * dailyBitcoin.bitcoinMined
      : 0;
      
    return {
      hour: item.hour,
      curtailedEnergy,
      bitcoinPotential
    };
  });
  
  // Process data for the monthly chart
  const monthlyChartData = monthlyData.map((item: any) => {
    const month = new Date(item.month + "-01").toLocaleString('default', { month: 'short' });
    return {
      month,
      curtailedEnergy: Number(item.curtailedEnergy),
      bitcoinMined: Number(item.bitcoinMined)
    };
  });
  
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
  
  // Helper for checking if a month is in the future
  const isMonthInFuture = (monthStr: string) => {
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const monthIndex = months.indexOf(monthStr);
    const now = new Date();
    
    if (currentYear > now.getFullYear()) return true;
    if (currentYear < now.getFullYear()) return false;
    return monthIndex > now.getMonth();
  };
  
  // Get max Bitcoin value for right Y-axis scaling (daily)
  const maxDailyBitcoin = Math.max(...dailyChartData.map((d: { bitcoinPotential: number }) => d.bitcoinPotential || 0), 0.1);
  
  // Get max Bitcoin value for right Y-axis scaling (monthly)
  const maxMonthlyBitcoin = Math.max(...monthlyChartData.map((d: { bitcoinMined: number }) => d.bitcoinMined || 0), 0.1);
  
  // Get card title based on timeframe
  const getCardTitle = () => {
    switch(timeframe) {
      case "daily":
        return "Hourly Breakdown";
      case "monthly":
        return "Monthly Breakdown";
      case "yearly":
        return "Yearly Breakdown";
      default:
        return "Breakdown";
    }
  };
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>{getCardTitle()}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <Skeleton className="h-[300px] w-full" />
        ) : timeframe === "daily" && dailyChartData.length === 0 ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            No curtailment data available for this date
          </div>
        ) : timeframe === "monthly" && monthlyChartData.length === 0 ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            No monthly data available for this year
          </div>
        ) : timeframe === "daily" ? (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart
              data={dailyChartData}
              margin={{ top: 20, right: 40, left: 20, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="hour" 
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => value.split(":")[0] + ":00"}
              />
              {/* Left Y axis for energy */}
              <YAxis 
                yAxisId="left"
                orientation="left"
                tick={{ fontSize: 12 }} 
                label={{ 
                  value: 'Curtailed Energy (MWh)', 
                  angle: -90, 
                  position: 'insideLeft',
                  style: { textAnchor: 'middle' },
                  offset: 0
                }}
              />
              {/* Right Y axis for Bitcoin */}
              <YAxis 
                yAxisId="right"
                orientation="right"
                domain={[0, Math.ceil(maxDailyBitcoin * 1.2 * 10) / 10]}
                tick={{ fontSize: 12 }}
                label={{ 
                  value: 'Potential Bitcoin Mined (₿)', 
                  angle: 90, 
                  position: 'insideRight',
                  style: { textAnchor: 'middle', fill: '#F7931A' }
                }}
                tickFormatter={(value) => value.toFixed(1)}
                stroke="#F7931A"
              />
              <Tooltip 
                formatter={(value: number, name: string) => {
                  if (name === "curtailedEnergy") {
                    return [`${value.toFixed(2)} MWh`, "Curtailed Energy"];
                  }
                  return [`₿${value.toFixed(4)}`, "Bitcoin Potential"];
                }}
                labelFormatter={(label) => `Hour: ${label}`}
              />
              <Legend />
              <Bar
                yAxisId="left"
                dataKey="curtailedEnergy"
                fill="#000000"
                name="Curtailed Energy (MWh)"
                // Apply different styling for future hours
                {...(dailyChartData.some((d: { hour: string }) => isHourInFuture(d.hour)) && {
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
              <Bar
                yAxisId="right"
                dataKey="bitcoinPotential"
                fill="transparent"
                stroke="transparent"
                name="Bitcoin Potential (₿)"
              >
                {/* Custom label to show Bitcoin icons */}
                <LabelList
                  dataKey="bitcoinPotential"
                  position="top"
                  content={(props: any) => {
                    const { x, y, value, index } = props;
                    return <BitcoinIcon x={x} y={y} value={value} key={`bitcoin-${index}`} />;
                  }}
                />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        ) : timeframe === "monthly" ? (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart
              data={monthlyChartData}
              margin={{ top: 20, right: 40, left: 20, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="month" 
                tick={{ fontSize: 12 }}
              />
              {/* Left Y axis for energy */}
              <YAxis 
                yAxisId="left"
                orientation="left"
                tick={{ fontSize: 12 }} 
                label={{ 
                  value: 'Curtailed Energy (MWh)', 
                  angle: -90, 
                  position: 'insideLeft',
                  style: { textAnchor: 'middle' },
                  offset: 0
                }}
              />
              {/* Right Y axis for Bitcoin */}
              <YAxis 
                yAxisId="right"
                orientation="right"
                domain={[0, Math.ceil(maxMonthlyBitcoin * 1.2 * 10) / 10]}
                tick={{ fontSize: 12 }}
                label={{ 
                  value: 'Bitcoin Mined (₿)', 
                  angle: 90, 
                  position: 'insideRight',
                  style: { textAnchor: 'middle', fill: '#F7931A' }
                }}
                tickFormatter={(value) => value.toFixed(1)}
                stroke="#F7931A"
              />
              <Tooltip 
                formatter={(value: number, name: string) => {
                  if (name === "curtailedEnergy") {
                    return [`${value.toLocaleString()} MWh`, "Curtailed Energy"];
                  }
                  return [`₿${value.toFixed(4)}`, "Bitcoin Mined"];
                }}
                labelFormatter={(label) => `Month: ${label}`}
              />
              <Legend />
              <Bar
                yAxisId="left"
                dataKey="curtailedEnergy"
                fill="#000000"
                name="Curtailed Energy (MWh)"
                // Apply different styling for future months
                shape={(props: any) => {
                  const { x, y, width, height, payload } = props;
                  const inFuture = isMonthInFuture(payload.month);
                  
                  return (
                    <rect
                      key={`bar-${payload.month}`}
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
                }}
              />
              <Bar
                yAxisId="right"
                dataKey="bitcoinMined"
                fill="#F7931A"
                name="Bitcoin Mined (₿)"
                // Apply different styling for future months
                shape={(props: any) => {
                  const { x, y, width, height, payload } = props;
                  const inFuture = isMonthInFuture(payload.month);
                  
                  return (
                    <rect
                      key={`bitcoin-bar-${payload.month}`}
                      x={x}
                      y={y}
                      width={width}
                      height={height}
                      fill={inFuture ? "#fce8cc" : "#F7931A"}
                      stroke={inFuture ? "#F7931A" : "none"}
                      strokeWidth={1}
                      r={0}
                    />
                  );
                }}
              />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            This chart is only available in daily and monthly views
          </div>
        )}
      </CardContent>
    </Card>
  );
}