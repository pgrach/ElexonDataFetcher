import React from 'react';
import { format } from 'date-fns';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertCircle, Activity, Wind, Zap } from "lucide-react";

interface CurtailmentPieChartProps {
  totalPotentialGeneration: number;
  totalCurtailedVolume: number;
  title: string;
  description: string;
  loading?: boolean;
  error?: string | null;
  date?: Date; // Add date prop
}

export default function CurtailmentPieChart({
  totalPotentialGeneration,
  totalCurtailedVolume,
  title,
  description,
  loading = false,
  error = null,
  date = new Date() // Default to current date if not provided
}: CurtailmentPieChartProps) {
  // Calculate the actual generation (potential minus curtailed)
  const actualGeneration = Math.max(totalPotentialGeneration - totalCurtailedVolume, 0);
  
  // Create data for the pie chart
  const data = [
    { name: 'Actual Generation', value: actualGeneration },
    { name: 'Curtailed Volume', value: totalCurtailedVolume }
  ];
  
  // Colors for the pie chart - updated to more professional colors
  const COLORS = ['#22c55e', '#ef4444'];
  
  // Calculate curtailment percentage
  const curtailmentPercentage = totalPotentialGeneration > 0 
    ? (totalCurtailedVolume / totalPotentialGeneration) * 100 
    : 0;

  // Format large numbers with appropriate unit suffixes
  const formatNumber = (value: number): string => {
    if (value >= 1000000) {
      return `${(value / 1000000).toFixed(2)} GWh`;
    } else if (value >= 1000) {
      return `${(value / 1000).toFixed(2)} GWh`;
    } else {
      return `${value.toFixed(2)} MWh`;
    }
  };

  // Custom tooltip to display values in MWh
  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-card border border-border p-3 rounded-lg shadow-md">
          <p className="font-semibold text-base">{payload[0].name}</p>
          <p className="text-sm mt-1">
            {formatNumber(payload[0].value)}
            <span className="text-muted-foreground ml-1">
              ({((payload[0].value / totalPotentialGeneration) * 100).toFixed(1)}%)
            </span>
          </p>
        </div>
      );
    }
    return null;
  };

  return (
    <div>
      {error ? (
        <div className="flex items-center justify-center h-60 text-center p-4 rounded-lg border border-border/30">
          <div>
            <AlertCircle className="h-10 w-10 text-destructive mx-auto mb-2" />
            <p className="text-destructive">{error}</p>
          </div>
        </div>
      ) : loading ? (
        <div className="space-y-4 py-8">
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-32 w-full" />
        </div>
      ) : (
        <div className="space-y-6">
          {/* Central visualization with minimalist details */}
          <div className="flex flex-col items-center">
            <div className="flex items-center gap-2 mb-8">
              <Activity className="text-primary h-6 w-6" />
              <h2 className="text-4xl font-bold">{curtailmentPercentage.toFixed(1)}%</h2>
              <span className="text-lg text-muted-foreground">of potential generation curtailed</span>
            </div>
            
            {/* Main pie chart visualization - larger size */}
            <div className="mx-auto max-w-3xl w-full">
              <div className="h-96 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={data}
                      cx="50%"
                      cy="50%"
                      innerRadius={110}
                      outerRadius={150}
                      paddingAngle={0}
                      dataKey="value"
                      startAngle={90}
                      endAngle={-270}
                      label={({ name, percent }) => 
                        `${name}: ${(percent * 100).toFixed(1)}%`
                      }
                      labelLine={false}
                    >
                      {data.map((entry, index) => (
                        <Cell 
                          key={`cell-${index}`} 
                          fill={COLORS[index % COLORS.length]}
                          stroke="transparent"
                        />
                      ))}
                    </Pie>
                    <Tooltip content={<CustomTooltip />} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </div>
            
            {/* Subtle date stamp */}
            <div className="text-center text-muted-foreground text-sm mt-8">
              Data for {format(date, "MMMM d, yyyy")}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}