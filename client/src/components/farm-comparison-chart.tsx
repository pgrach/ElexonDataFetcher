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
  
  // Format GBP values for tooltips
  const formatGBP = (value: number) => {
    return new Intl.NumberFormat('en-GB', {
      style: 'currency',
      currency: 'GBP'
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
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            No farm comparison data available for this {timeframe} period
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
                tickFormatter={(value) => `£${value.toLocaleString()}`}
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
                stackId="b"
              >
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={bitcoinColor} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}