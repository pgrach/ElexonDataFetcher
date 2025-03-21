import React from 'react';
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
    <div className="space-y-6">
      {/* Main Statistics Card */}
      <Card className="overflow-hidden bg-gradient-to-br from-background to-muted/30">
        <CardContent className="pt-6">
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
            <div>
              {/* Key metrics highlight row */}
              <div className="grid grid-cols-3 gap-4 mb-6">
                <div className="flex flex-col items-center p-4 bg-card rounded-lg shadow-sm border border-border/50">
                  <Activity className="w-5 h-5 text-primary mb-2" />
                  <span className="text-sm text-muted-foreground">Curtailment</span>
                  <span className="text-2xl font-bold mt-1">{curtailmentPercentage.toFixed(1)}%</span>
                </div>
                <div className="flex flex-col items-center p-4 bg-card rounded-lg shadow-sm border border-border/50">
                  <Zap className="w-5 h-5 text-amber-500 mb-2" />
                  <span className="text-sm text-muted-foreground">Total Curtailed</span>
                  <span className="text-2xl font-bold mt-1">{formatNumber(totalCurtailedVolume)}</span>
                </div>
                <div className="flex flex-col items-center p-4 bg-card rounded-lg shadow-sm border border-border/50">
                  <Wind className="w-5 h-5 text-emerald-500 mb-2" />
                  <span className="text-sm text-muted-foreground">Potential</span>
                  <span className="text-2xl font-bold mt-1">{formatNumber(totalPotentialGeneration)}</span>
                </div>
              </div>
              
              {/* Pie chart visualization */}
              <div className="bg-card rounded-lg p-4 border border-border/50 shadow-sm">
                <h3 className="text-lg font-medium mb-2">Generation Distribution</h3>
                <div className="h-64 w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie
                        data={data}
                        cx="50%"
                        cy="50%"
                        innerRadius={70}
                        outerRadius={90}
                        paddingAngle={2}
                        dataKey="value"
                        startAngle={90}
                        endAngle={-270}
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
                      <Legend 
                        verticalAlign="bottom"
                        height={36}
                        iconType="circle"
                        iconSize={10}
                      />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
                
                {/* Detailed stats row */}
                <div className="grid grid-cols-2 gap-4 mt-4">
                  <div className="p-3 bg-muted/50 rounded-md border border-border/30">
                    <div className="text-sm font-medium text-muted-foreground">Actual Generation</div>
                    <div className="text-lg font-bold text-emerald-600">{formatNumber(actualGeneration)}</div>
                    <div className="text-xs text-muted-foreground mt-1">
                      {((actualGeneration / totalPotentialGeneration) * 100).toFixed(1)}% of potential
                    </div>
                  </div>
                  <div className="p-3 bg-muted/50 rounded-md border border-border/30">
                    <div className="text-sm font-medium text-muted-foreground">Curtailed Energy</div>
                    <div className="text-lg font-bold text-red-500">{formatNumber(totalCurtailedVolume)}</div>
                    <div className="text-xs text-muted-foreground mt-1">
                      {curtailmentPercentage.toFixed(1)}% of potential
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}