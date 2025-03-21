import React from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertCircle } from "lucide-react";

interface CurtailmentPieChartProps {
  totalPotentialGeneration: number;
  totalCurtailedVolume: number;
  title: string;
  description: string;
  loading?: boolean;
  error?: string | null;
}

export default function CurtailmentPieChart({
  totalPotentialGeneration,
  totalCurtailedVolume,
  title,
  description,
  loading = false,
  error = null
}: CurtailmentPieChartProps) {
  // Calculate the actual generation (potential minus curtailed)
  const actualGeneration = Math.max(totalPotentialGeneration - totalCurtailedVolume, 0);
  
  // Create data for the pie chart
  const data = [
    { name: 'Actual Generation', value: actualGeneration },
    { name: 'Curtailed Volume', value: totalCurtailedVolume }
  ];
  
  // Colors for the pie chart
  const COLORS = ['#4ade80', '#f87171'];
  
  // Calculate curtailment percentage
  const curtailmentPercentage = totalPotentialGeneration > 0 
    ? (totalCurtailedVolume / totalPotentialGeneration) * 100 
    : 0;

  // Custom tooltip to display values in MWh
  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-background border border-border p-2 rounded shadow-sm">
          <p className="font-medium">{payload[0].name}</p>
          <p className="text-sm">
            {payload[0].value.toLocaleString()} MWh 
            ({((payload[0].value / totalPotentialGeneration) * 100).toFixed(1)}%)
          </p>
        </div>
      );
    }
    return null;
  };

  return (
    <Card className="w-full overflow-hidden">
      <CardHeader className="pb-2">
        <CardTitle>{title}</CardTitle>
        <p className="text-sm text-muted-foreground">{description}</p>
      </CardHeader>
      <CardContent>
        {error ? (
          <div className="flex items-center justify-center h-60 text-center p-4">
            <div>
              <AlertCircle className="h-10 w-10 text-destructive mx-auto mb-2" />
              <p className="text-destructive">{error}</p>
            </div>
          </div>
        ) : loading ? (
          <div className="space-y-4 py-8">
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-32 w-full" />
          </div>
        ) : (
          <div className="pt-4">
            <div className="flex justify-between items-center mb-4">
              <div className="flex flex-col">
                <div className="text-3xl font-bold">
                  {curtailmentPercentage.toFixed(1)}%
                </div>
                <div className="text-sm text-muted-foreground">
                  Curtailment Percentage
                </div>
              </div>
              <div className="flex flex-col items-end">
                <div className="text-xl font-medium">
                  {totalCurtailedVolume.toLocaleString()} MWh
                </div>
                <div className="text-sm text-muted-foreground">
                  Total Curtailed Volume
                </div>
              </div>
            </div>
            
            <div className="h-64 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={data}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={90}
                    paddingAngle={5}
                    dataKey="value"
                    label={({ name, percent }) => `${name} (${(percent * 100).toFixed(1)}%)`}
                    labelLine={false}
                  >
                    {data.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip content={<CustomTooltip />} />
                  <Legend />
                </PieChart>
              </ResponsiveContainer>
            </div>
            
            <div className="grid grid-cols-2 gap-4 mt-4">
              <div className="p-3 bg-muted rounded-md">
                <div className="text-sm font-medium">Total Potential</div>
                <div className="text-lg font-bold">{totalPotentialGeneration.toLocaleString()} MWh</div>
              </div>
              <div className="p-3 bg-muted rounded-md">
                <div className="text-sm font-medium">Actual Generation</div>
                <div className="text-lg font-bold">{actualGeneration.toLocaleString()} MWh</div>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}