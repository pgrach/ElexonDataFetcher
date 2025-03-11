"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ReferenceLine } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";

interface FarmOpportunityComparisonChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

interface HourlyComparisonData {
  hour: string;
  curtailedEnergy: number;
  paymentAmount: number;
  bitcoinMined: number;
  currentPrice: number;
  paymentPerMwh: number;
  bitcoinValuePerMwh: number;
}

export default function FarmOpportunityComparisonChart({ 
  timeframe, 
  date, 
  minerModel, 
  farmId 
}: FarmOpportunityComparisonChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd");
  
  // Only fetch when timeframe is daily and a farm is selected
  const { data: hourlyData = [], isLoading } = useQuery<HourlyComparisonData[]>({
    queryKey: [`/api/curtailment/hourly-comparison/${formattedDate}`, farmId, minerModel],
    queryFn: async () => {
      const url = new URL(`/api/curtailment/hourly-comparison/${formattedDate}`, window.location.origin);
      url.searchParams.set("leadParty", farmId);
      url.searchParams.set("minerModel", minerModel);
      
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error("Failed to fetch hourly comparison data");
      }
      
      return response.json();
    },
    enabled: timeframe === "daily" && !!farmId // Only fetch when in daily view and a farm is selected
  });
  
  // Format GBP values for tooltips
  const formatGBP = (value: number) => {
    // For small values under 1, use 2 decimal places to show proper precision
    if (value > 0 && value < 1) {
      return new Intl.NumberFormat('en-GB', {
        style: 'currency',
        currency: 'GBP',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      }).format(value);
    }
    
    // For larger values, use no decimal places
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
      // Find data values from payload
      const curtailedEnergyItem = payload.find((p: any) => p.dataKey === "curtailedEnergy");
      const curtailedEnergy = curtailedEnergyItem ? Number(curtailedEnergyItem.value || 0) : 0;
      
      const paymentPerMwhItem = payload.find((p: any) => p.dataKey === "paymentPerMwh");
      const paymentPerMwh = paymentPerMwhItem ? Number(paymentPerMwhItem.value || 0) : 0;
      
      const bitcoinValuePerMwhItem = payload.find((p: any) => p.dataKey === "bitcoinValuePerMwh");
      const bitcoinValuePerMwh = bitcoinValuePerMwhItem ? Number(bitcoinValuePerMwhItem.value || 0) : 0;
      
      // Calculate difference
      const difference = bitcoinValuePerMwh - paymentPerMwh;
      
      // Calculate what percentage the payment is of bitcoin value
      const percentage = paymentPerMwh > 0 && bitcoinValuePerMwh > 0 
        ? ((paymentPerMwh / bitcoinValuePerMwh) * 100).toFixed(2) 
        : '0';
      
      return (
        <div className="bg-white p-3 border border-gray-200 rounded-md shadow-md">
          <p className="font-semibold">{label}</p>
          <p style={{ color: '#000000' }}>Curtailment Payment: {formatGBP(paymentPerMwh)}/MWh</p>
          <p style={{ color: '#F7931A' }}>Bitcoin Value: {formatGBP(bitcoinValuePerMwh)}/MWh</p>
          <div className="border-t pt-1 mt-1">
            <p style={{ color: difference >= 0 ? '#22c55e' : '#ef4444', fontWeight: 'bold' }}>
              Difference: {difference >= 0 ? '+' : ''}{formatGBP(difference)}/MWh
            </p>
            <p className="text-sm text-gray-500">
              Payment is {percentage}% of potential Bitcoin value
            </p>
          </div>
        </div>
      );
    }
    return null;
  };
  
  // Check if hourly data has valid values
  const hasValidData = hourlyData.some(hour => 
    hour.curtailedEnergy > 0 && (hour.paymentPerMwh > 0 || hour.bitcoinValuePerMwh > 0)
  );
  
  // Get card title based on timeframe and farm ID
  const chartTitle = farmId 
    ? `Rate Comparison: Curtailment vs. Bitcoin (${format(date, "PP")})`
    : "Select a farm to see rate comparison";
  
  // Colors for the lines
  const curtailmentColor = "#000000"; // Black for curtailment
  const bitcoinColor = "#F7931A"; // Bitcoin orange
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>{chartTitle}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <Skeleton className="h-[300px] w-full" />
        ) : !farmId ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            Please select a farm to view the comparison
          </div>
        ) : !hasValidData ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            No comparison data available for this farm on {format(date, "PP")}
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <LineChart
              data={hourlyData}
              margin={{ top: 5, right: 30, left: 60, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="hour" 
                tick={{ fontSize: 12 }}
              />
              {/* Use two separate Y axes to handle the different scales */}
              <YAxis 
                yAxisId="bitcoin"
                orientation="right"
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => `£${Math.round(value)}`}
                domain={['auto', 'auto']}
                label={{ 
                  value: 'Bitcoin Value (£/MWh)', 
                  angle: 90, 
                  position: 'insideRight',
                  style: { 
                    textAnchor: 'middle',
                    fill: bitcoinColor 
                  },
                  dy: -20
                }}
              />
              <YAxis 
                yAxisId="payment"
                orientation="left"
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => `£${value.toFixed(2)}`}
                domain={[0, 'dataMax + 1']}
                label={{ 
                  value: 'Payment (£/MWh)', 
                  angle: -90, 
                  position: 'insideLeft',
                  style: { 
                    textAnchor: 'middle',
                    fill: curtailmentColor 
                  },
                  dx: -20
                }}
                width={80}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Line 
                yAxisId="payment"
                type="monotone" 
                dataKey="paymentPerMwh" 
                name="Curtailment Payment (£/MWh)" 
                stroke={curtailmentColor}
                strokeWidth={2}
                dot={{ r: 3 }}
                activeDot={{ r: 6 }}
              />
              <Line 
                yAxisId="bitcoin"
                type="monotone" 
                dataKey="bitcoinValuePerMwh" 
                name="Bitcoin Value (£/MWh)" 
                stroke={bitcoinColor}
                strokeWidth={2}
                dot={{ r: 3 }}
                activeDot={{ r: 6 }}
              />
              {/* Add reference lines */}
              <ReferenceLine
                y={0}
                yAxisId="payment"
                stroke="#e5e5e5"
                strokeDasharray="3 3"
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}