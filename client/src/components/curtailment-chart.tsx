"use client";

import { useState, useEffect } from "react";
import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, LabelList, ReferenceLine, ReferenceArea } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";
import { formatEnergy, formatGBP, formatBitcoin } from "@/lib/utils";

interface CurtailmentChartProps {
  timeframe: string;
  date: Date | null;
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
        {formatBitcoin(value).replace(" BTC", "")}
      </text>
    </g>
  );
};

export default function CurtailmentChart({ timeframe, date, minerModel, farmId }: CurtailmentChartProps) {
  // If date is null, use current date as fallback
  const dateToUse = date || new Date();
  
  const formattedDate = format(dateToUse, "yyyy-MM-dd");
  const formattedYearMonth = format(dateToUse, "yyyy-MM");
  const currentYear = dateToUse.getFullYear();
  const selectedMonth = format(dateToUse, "MMM");  // Get the selected month abbreviation (e.g., "Mar")
  
  // Log the selected month for debugging
  console.log("Selected month for highlighting:", selectedMonth);
  
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
  
  // Fetch monthly data for monthly view - using actual API data for all months
  const { data: monthlyData = [], isLoading: isMonthlyLoading } = useQuery({
    queryKey: [`/api/monthly-chart-data`, currentYear, format(dateToUse, "yyyy-MM"), minerModel, farmId],
    queryFn: async () => {
      // Fetch data for each month of the year
      const months = [];
      const monthlyDataArray = [];
      
      for (let i = 1; i <= 12; i++) {
        const monthStr = i.toString().padStart(2, '0');
        const yearMonth = `${currentYear}-${monthStr}`;
        months.push(yearMonth);
      }
      
      // Get data from API for all months
      for (const yearMonth of months) {
        try {
          // For all months, fetch from the API
          const summaryUrl = new URL(`/api/summary/monthly/${yearMonth}`, window.location.origin);
          const bitcoinUrl = new URL(`/api/curtailment/monthly-mining-potential/${yearMonth}`, window.location.origin);
          
          // Add parameters
          if (farmId) {
            summaryUrl.searchParams.set("leadParty", farmId);
            bitcoinUrl.searchParams.set("leadParty", farmId);
          }
          
          bitcoinUrl.searchParams.set("minerModel", minerModel);
          
          console.log(`Fetching monthly summary data for ${yearMonth}...`);
          
          // Get curtailment energy data
          const summaryResponse = await fetch(summaryUrl);
          const bitcoinResponse = await fetch(bitcoinUrl);
          
          let curtailedEnergy = 0;
          let bitcoinMined = 0;
          
          if (summaryResponse.ok) {
            const summaryData = await summaryResponse.json();
            curtailedEnergy = Number(summaryData.totalCurtailedEnergy) || 0;
            console.log(`Energy data for ${yearMonth}:`, summaryData);
          }
          
          if (bitcoinResponse.ok) {
            const bitcoinData = await bitcoinResponse.json();
            bitcoinMined = Number(bitcoinData.bitcoinMined) || 0;
            console.log(`Bitcoin data for ${yearMonth}:`, bitcoinData);
          }
          
          console.log(`API values for ${yearMonth}: Energy=${formatEnergy(curtailedEnergy)}, Bitcoin=${formatBitcoin(bitcoinMined)}`);
          
          monthlyDataArray.push({
            month: yearMonth,
            curtailedEnergy: curtailedEnergy,
            bitcoinMined: bitcoinMined
          });
        } catch (error) {
          console.error(`Error fetching data for ${yearMonth}:`, error);
          
          // Add empty data for error cases
          monthlyDataArray.push({
            month: yearMonth,
            curtailedEnergy: 0,
            bitcoinMined: 0
          });
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
  console.log("Monthly data before processing:", monthlyData);
  const monthlyChartData = monthlyData
    .filter((item: any) => item && item.month) // Filter out any invalid items
    .map((item: any) => {
      const month = new Date(item.month + "-01").toLocaleString('default', { month: 'short' });
      return {
        month,
        curtailedEnergy: Number(item.curtailedEnergy) || 0,
        bitcoinMined: Number(item.bitcoinMined) || 0
      };
    });
  console.log("Processed monthly chart data:", monthlyChartData);
  
  // Helper for checking if an hour is in the future
  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(":").map(Number);
    const now = new Date();
    const selectedDate = date ? new Date(date) : now;
    
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
        ) : timeframe === "daily" && (dailyChartData.length === 0 || dailyChartData.every((item: { curtailedEnergy: number }) => item.curtailedEnergy === 0)) ? (
          <div className="flex flex-col items-center justify-center h-[300px] border border-dashed border-blue-200 rounded-md bg-blue-50/30">
            <svg className="h-16 w-16 text-blue-400 mb-2" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
              {/* Tower */}
              <rect x="47" y="55" width="6" height="35" fill="currentColor" rx="1" />
              <rect x="40" y="90" width="20" height="5" rx="2" fill="currentColor" />

              {/* Nacelle (turbine housing) */}
              <rect x="42" y="48" width="16" height="4" rx="2" fill="currentColor" transform="rotate(5, 50, 50)" />

              {/* Hub */}
              <circle cx="50" cy="50" r="3" fill="currentColor" />

              {/* Rotating blades - with animation */}
              <g style={{ transformOrigin: "50px 50px", animation: "windTurbineSpin 8s linear infinite" }}>
                {/* Blade 1 - pointing right with taper and curve */}
                <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" />
                {/* Blade 2 - rotated 120 degrees */}
                <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(120, 50, 50)" />
                {/* Blade 3 - rotated 240 degrees */}
                <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(240, 50, 50)" />
              </g>

              {/* Animation keyframes - added via style */}
              <style>{`
                @keyframes windTurbineSpin {
                  0% { transform: rotate(0deg); }
                  100% { transform: rotate(360deg); }
                }
              `}</style>
            </svg>
            <h3 className="text-lg font-medium text-blue-500">No Curtailment Events</h3>
            <p className="text-sm text-blue-400 max-w-md text-center mt-2 px-4">
              No wind farms were curtailed on this date. All available wind energy was successfully utilized by the grid.
            </p>
          </div>
        ) : timeframe === "monthly" && (monthlyChartData.length === 0 || monthlyChartData.every((item: { curtailedEnergy: number }) => item.curtailedEnergy === 0)) ? (
          <div className="flex flex-col items-center justify-center h-[300px] border border-dashed border-blue-200 rounded-md bg-blue-50/30">
            <svg className="h-16 w-16 text-blue-400 mb-2" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
              {/* Tower */}
              <rect x="47" y="55" width="6" height="35" fill="currentColor" rx="1" />
              <rect x="40" y="90" width="20" height="5" rx="2" fill="currentColor" />

              {/* Nacelle (turbine housing) */}
              <rect x="42" y="48" width="16" height="4" rx="2" fill="currentColor" transform="rotate(5, 50, 50)" />

              {/* Hub */}
              <circle cx="50" cy="50" r="3" fill="currentColor" />

              {/* Rotating blades - with animation */}
              <g style={{ transformOrigin: "50px 50px", animation: "windTurbineSpin 8s linear infinite" }}>
                {/* Blade 1 - pointing right with taper and curve */}
                <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" />
                {/* Blade 2 - rotated 120 degrees */}
                <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(120, 50, 50)" />
                {/* Blade 3 - rotated 240 degrees */}
                <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(240, 50, 50)" />
              </g>
              
              {/* Animation keyframes - added via style */}
              <style>{`
                @keyframes windTurbineSpin {
                  0% { transform: rotate(0deg); }
                  100% { transform: rotate(360deg); }
                }
              `}</style>
            </svg>
            <h3 className="text-lg font-medium text-blue-500">No Monthly Curtailment Data</h3>
            <p className="text-sm text-blue-400 max-w-md text-center mt-2 px-4">
              No curtailment events were recorded for the selected month in {currentYear}.
            </p>
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
                tickFormatter={(value) => formatBitcoin(value).replace(" BTC", "")}
                stroke="#F7931A"
              />
              <Tooltip 
                content={({ active, payload, label }) => {
                  if (active && payload && payload.length) {
                    // Find curtailed energy and bitcoin potential from payload
                    const energyItem = payload.find(p => p.name === "Curtailed Energy (MWh)");
                    const bitcoinItem = payload.find(p => p.name === "Bitcoin Potential (₿)");
                    
                    const energyValue = energyItem && typeof energyItem.value === 'number' 
                      ? formatEnergy(energyItem.value)
                      : "0 MWh";
                    const bitcoinValue = bitcoinItem && typeof bitcoinItem.value === 'number' 
                      ? formatBitcoin(bitcoinItem.value)
                      : "0.00 BTC";
                    
                    return (
                      <div className="custom-tooltip" style={{ 
                        backgroundColor: 'white', 
                        padding: '10px', 
                        border: '1px solid #ccc',
                        borderRadius: '4px'
                      }}>
                        <p className="label" style={{ margin: '0 0 5px', fontWeight: 'bold' }}>{`Hour: ${label}`}</p>
                        <p style={{ margin: '0', color: '#333' }}>{`Curtailed Energy: ${energyValue}`}</p>
                        <p style={{ margin: '0', color: '#F7931A' }}>{`Bitcoin Potential: ${bitcoinValue}`}</p>
                      </div>
                    );
                  }
                  return null;
                }}
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
              {/* Add background highlight for selected month - using a better approach with cell background */}
              <defs>
                <pattern id="selectedMonthPattern" patternUnits="userSpaceOnUse" width="100%" height="100%">
                  <rect x="0" y="0" width="100%" height="100%" fill="#f0f0f0" />
                </pattern>
              </defs>
              {/* We'll use a reference area to create a full-column highlight */}
              <ReferenceArea
                x1={selectedMonth} 
                x2={selectedMonth}
                yAxisId="left"
                strokeOpacity={0}
                fill="#f6f6f6"
                fillOpacity={0.9}
                ifOverflow="extendDomain"
              />
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
                tickFormatter={(value) => formatBitcoin(value).replace(" BTC", "")}
                stroke="#F7931A"
              />
              <Tooltip 
                content={({ active, payload, label }) => {
                  if (active && payload && payload.length) {
                    // Find curtailed energy and bitcoin potential from payload
                    const energyItem = payload.find(p => p.name === "Curtailed Energy (MWh)");
                    const bitcoinItem = payload.find(p => p.name === "Bitcoin Mined (₿)");
                    
                    const energyValue = energyItem && typeof energyItem.value === 'number' 
                      ? formatEnergy(energyItem.value)
                      : "0 MWh";
                    const bitcoinValue = bitcoinItem && typeof bitcoinItem.value === 'number' 
                      ? formatBitcoin(bitcoinItem.value)
                      : "0.00 BTC";
                    
                    return (
                      <div className="custom-tooltip" style={{ 
                        backgroundColor: 'white', 
                        padding: '10px', 
                        border: '1px solid #ccc',
                        borderRadius: '4px'
                      }}>
                        <p className="label" style={{ margin: '0 0 5px', fontWeight: 'bold' }}>{`Month: ${label}`}</p>
                        <p style={{ margin: '0', color: '#333' }}>{`Curtailed Energy: ${energyValue}`}</p>
                        <p style={{ margin: '0', color: '#F7931A' }}>{`Bitcoin Mined: ${bitcoinValue}`}</p>
                      </div>
                    );
                  }
                  return null;
                }}
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
                  const isSelectedMonth = payload.month === selectedMonth;
                  
                  // Choose fill color based on whether it's the selected month or future month
                  let fillColor = "#000000"; // Default black
                  let strokeColor = "none";
                  let strokeWidth = 1;
                  
                  if (inFuture) {
                    fillColor = "#f5f5f5"; // Light grey for future months
                    strokeColor = "#000000";
                  } else if (isSelectedMonth) {
                    // Use a darker grey fill for the selected month to make it stand out
                    fillColor = "#000000"; // Keep black for selected month bar
                    strokeColor = "#000000";
                    
                    // Debug info
                    console.log(`Highlighting month: ${payload.month} (selected: ${selectedMonth})`);
                  }
                  
                  return (
                    <rect
                      key={`bar-${payload.month}`}
                      x={x}
                      y={y}
                      width={width}
                      height={height}
                      fill={fillColor}
                      stroke={strokeColor}
                      strokeWidth={strokeWidth}
                      r={0}
                    />
                  );
                }}
              />
              <Bar
                yAxisId="right"
                dataKey="bitcoinMined"
                fill="transparent"
                stroke="transparent"
                name="Bitcoin Mined (₿)"
              >
                {/* Custom label to show Bitcoin icons */}
                <LabelList
                  dataKey="bitcoinMined"
                  position="top"
                  content={(props: any) => {
                    const { x, y, value, index } = props;
                    return <BitcoinIcon x={x} y={y} value={value} key={`bitcoin-${index}`} />;
                  }}
                />
              </Bar>
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