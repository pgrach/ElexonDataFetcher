"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer, 
  Legend,
  Label
} from "recharts";
import { Skeleton } from "@/components/ui/skeleton";
import { PoundSterling } from "lucide-react";

interface FarmOpportunityComparisonChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

interface ChartData {
  timeLabel: string;
  timePeriod: number;
  curtailedEnergy: number;
  curtailmentPayment: number;
  paymentPerMwh: number;
  bitcoinMined: number;
  bitcoinValueGbp: number;
  bitcoinValuePerMwh: number;
  difficulty: number;
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-popover p-3 rounded-md shadow-md border">
        <p className="font-medium">{`Time: ${label}`}</p>
        {payload.map((entry: any, index: number) => (
          <div key={`item-${index}`} className="flex items-center gap-1 mt-1">
            <div
              className="w-3 h-3 rounded-full"
              style={{ backgroundColor: entry.color }}
            />
            <p style={{ color: entry.color }}>
              {`${entry.name}: £${entry.value.toFixed(2)}/MWh`}
            </p>
          </div>
        ))}
        {payload[0].payload.curtailedEnergy > 0 && (
          <p className="text-xs mt-2 text-muted-foreground">
            Curtailed energy: {payload[0].payload.curtailedEnergy.toFixed(2)} MWh
          </p>
        )}
      </div>
    );
  }

  return null;
};

export default function FarmOpportunityComparisonChart({ 
  timeframe, 
  date, 
  minerModel,
  farmId
}: FarmOpportunityComparisonChartProps) {
  if (!farmId || farmId === 'all') {
    return null;
  }
  
  const formattedDate = format(date, "yyyy-MM-dd");
  const yearMonth = format(date, "yyyy-MM");
  
  // Determine period and value based on timeframe
  const period = 
    timeframe === "monthly" ? "month" : "day";
  
  const value = 
    timeframe === "monthly" ? yearMonth : formattedDate;
  
  // Fetch data from our new API endpoint
  const { data: chartData = [], isLoading } = useQuery<ChartData[]>({
    queryKey: ['/api/mining-potential/farm-opportunity-comparison', farmId, period, value, minerModel],
    queryFn: async () => {
      const url = new URL(`/api/mining-potential/farm-opportunity-comparison/${farmId}`, window.location.origin);
      url.searchParams.append('period', period);
      url.searchParams.append('value', value);
      url.searchParams.append('minerModel', minerModel);
      
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error("Failed to fetch opportunity comparison data");
      }
      
      return response.json();
    },
    enabled: !!farmId && farmId !== 'all'
  });
  
  // Format data for the chart
  const formattedChartData = chartData.map(item => ({
    ...item,
    // Round values for better display
    paymentPerMwh: Math.abs(Number(item.paymentPerMwh)),  // Abs to handle negative payment values
    bitcoinValuePerMwh: Number(item.bitcoinValuePerMwh)
  }));
  
  // Get chart title based on timeframe
  const chartTitle = `Payment vs Bitcoin Opportunity (£/MWh) - ${farmId}`;
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>{chartTitle}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <Skeleton className="h-[300px] w-full" />
        ) : formattedChartData.length === 0 ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            No comparison data available for this {timeframe} period
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <LineChart
              data={formattedChartData}
              margin={{ top: 5, right: 30, left: 60, bottom: 30 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis 
                dataKey="timeLabel" 
                tick={{ fontSize: 12 }}
                angle={-45}
                textAnchor="end"
                height={60}
              />
              <YAxis 
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => `£${value.toFixed(0)}`}
                label={{ 
                  value: 'British Pounds per MWh (£/MWh)', 
                  angle: -90, 
                  position: 'insideLeft',
                  offset: -40,
                  style: { textAnchor: 'middle' },
                  dx: -10 
                }}
                width={80}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend 
                verticalAlign="top" 
                height={36}
              />
              <Line
                type="monotone"
                dataKey="paymentPerMwh"
                name="Payment"
                stroke="#000000"
                strokeWidth={2}
                dot={{ r: 4 }}
                activeDot={{ r: 6 }}
              />
              <Line
                type="monotone"
                dataKey="bitcoinValuePerMwh"
                name="Bitcoin Opportunity"
                stroke="#FFA500"
                strokeWidth={2}
                dot={{ r: 4 }}
                activeDot={{ r: 6 }}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}