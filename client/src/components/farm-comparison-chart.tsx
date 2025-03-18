"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, Cell } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";
import { PoundSterling } from "lucide-react";

interface FarmComparisonChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
}

interface TopFarmData {
  name: string;
  farmId: string;
  curtailedEnergy: number;
  curtailmentPayment: number;
  bitcoinMined: number;
  bitcoinValue: number;
}

export default function FarmComparisonChart({ timeframe, date, minerModel }: FarmComparisonChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd");
  const yearMonth = format(date, "yyyy-MM");
  const year = format(date, "yyyy");
  
  // Determine period and value based on timeframe
  const period = 
    timeframe === "yearly" ? "year" : 
    timeframe === "monthly" ? "month" : "day";
  
  const value = 
    timeframe === "yearly" ? year : 
    timeframe === "monthly" ? yearMonth : formattedDate;
  
  // Fetch data from our new top-farms endpoint
  const { data: farmsData = [], isLoading } = useQuery<TopFarmData[]>({
    queryKey: ['/api/mining-potential/top-farms', period, value, minerModel],
    queryFn: async () => {
      const url = new URL('/api/mining-potential/top-farms', window.location.origin);
      url.searchParams.append('period', period);
      url.searchParams.append('value', value);
      url.searchParams.append('minerModel', minerModel);
      url.searchParams.append('limit', '5');
      
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error("Failed to fetch top farms data");
      }
      
      return response.json();
    }
  });
  
  // Transform the data for the chart
  const chartData = farmsData.map(farm => ({
    name: farm.name || farm.farmId,
    curtailmentPayment: Math.round(farm.curtailmentPayment),
    bitcoinValue: Math.round(farm.bitcoinValue)
  }));
  
  // Get chart title based on timeframe
  const chartTitle = 
    timeframe === "yearly" ? `Top 5 Curtailed Farms by Volume (${year})` :
    timeframe === "monthly" ? `Top 5 Curtailed Farms by Volume (${format(date, "MMMM yyyy")})` :
    `Top 5 Curtailed Farms by Volume (${format(date, "PP")})`;
  
  // Colors for the bars
  const curtailmentColor = "#000000"; // Black for curtailment
  const bitcoinColor = "#F7931A"; // Bitcoin orange
  
  // Format GBP values for tooltips without decimal places
  const formatGBP = (value: number) => {
    return new Intl.NumberFormat('en-GB', {
      style: 'currency',
      currency: 'GBP',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(value);
  };
  
  // Custom tooltip for the chart
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 border border-gray-200 rounded-md shadow-md">
          <p className="font-semibold">{label}</p>
          {payload.map((entry: any, index: number) => (
            <p key={`item-${index}`} style={{ color: entry.color || entry.fill }}>
              {entry.name === "Bitcoin Value" ? (
                <span className="flex items-center">
                  <span className="text-[#F7931A] mr-1">₿</span>
                  {formatGBP(entry.value)}
                </span>
              ) : (
                <span>
                  {formatGBP(entry.value)}
                </span>
              )}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>{chartTitle}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <Skeleton className="h-[300px] w-full" />
        ) : chartData.length === 0 ? (
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
            <h3 className="text-lg font-medium text-blue-500">No Wind Farms Curtailed</h3>
            <p className="text-sm text-blue-400 max-w-md text-center mt-2 px-4">
              There were no wind farms curtailed during this {timeframe} period. Try selecting a different date to view farm comparison data.
            </p>
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart
              data={chartData}
              margin={{ top: 5, right: 30, left: 60, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" tick={{ fontSize: 12 }} />
              <YAxis 
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => `£${Math.round(value).toLocaleString()}`}
                label={{ 
                  value: 'British Pounds (£)', 
                  angle: -90, 
                  position: 'insideLeft',
                  offset: 5,
                  style: { textAnchor: 'middle' },
                  dx: -30 
                }}
                width={80}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Bar 
                dataKey="curtailmentPayment" 
                name="Curtailment Payment" 
                stackId="a"
              >
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={curtailmentColor} />
                ))}
              </Bar>
              <Bar 
                dataKey="bitcoinValue" 
                name="Bitcoin Value"
                fill={bitcoinColor}
                stackId="b"
              />
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}