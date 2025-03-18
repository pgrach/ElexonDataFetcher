"use client";

import { useState, useEffect } from "react";
import { format } from "date-fns";
import axios from "axios";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  LabelList,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { EnhancedChartTooltip } from "./enhanced-chart-tooltip";

interface FarmComparisonChartProps {
  date: Date;
  minerModel: string;
  timeframe: string;
  leadParty?: string;
  limit?: number;
}

interface FarmData {
  name: string;
  curtailedEnergy: number;
  bitcoinMined: number;
  totalPayment: number;
  valueAtCurrentPrice: number;
}

export default function FarmComparisonChart({ 
  date, 
  minerModel, 
  timeframe,
  leadParty,
  limit = 5
}: FarmComparisonChartProps) {
  const [data, setData] = useState<FarmData[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      
      try {
        let period, value;
        
        if (timeframe === "yearly") {
          period = "year";
          value = format(date, "yyyy");
        } else if (timeframe === "monthly") {
          period = "month";
          value = format(date, "yyyy-MM");
        } else {
          period = "day";
          value = format(date, "yyyy-MM-dd");
        }
        
        const params: Record<string, string | number> = {
          period,
          value,
          minerModel,
          limit
        };
        
        if (leadParty) {
          params.leadParty = leadParty;
        }
        
        const response = await axios.get('/api/mining-potential/top-farms', { params });
        
        // Process and transform the data
        const processedData = response.data.map((farm: any) => ({
          name: farm.name.length > 15 ? farm.name.substring(0, 13) + '...' : farm.name,
          fullName: farm.name,
          curtailedEnergy: farm.curtailedEnergy,
          bitcoinMined: farm.bitcoinMined,
          totalPayment: farm.totalPayment,
          valueAtCurrentPrice: farm.valueAtCurrentPrice,
          valueRatio: farm.valueAtCurrentPrice / farm.totalPayment
        }));
        
        setData(processedData);
      } catch (err: any) {
        console.error("Error fetching farm comparison data:", err);
        setError("Failed to load farm comparison data. Please try again.");
      } finally {
        setLoading(false);
      }
    };
    
    fetchData();
  }, [date, minerModel, timeframe, leadParty, limit]);
  
  const periodText = timeframe === "yearly" 
    ? "year" 
    : timeframe === "monthly" 
      ? "month" 
      : "day";
      
  const dateLabel = timeframe === "yearly"
    ? format(date, "yyyy")
    : timeframe === "monthly"
      ? format(date, "MMMM yyyy")
      : format(date, "PP");
  
  return (
    <Card className="shadow-md border-gray-200">
      <CardHeader className="border-b border-gray-100 bg-gray-50/50">
        <CardTitle className="text-lg font-medium text-gray-800">
          Top Wind Farms by Curtailment
        </CardTitle>
        <CardDescription className="text-xs text-gray-500">
          Showing top {limit} farms for {dateLabel} {leadParty ? `owned by ${leadParty}` : ""}
          using {minerModel} miner model
        </CardDescription>
      </CardHeader>
      <CardContent className="p-6">
        {loading ? (
          <div className="h-72 w-full flex items-center justify-center bg-gray-50 rounded-md">
            <p className="text-gray-500">Loading farm comparison data...</p>
          </div>
        ) : error ? (
          <div className="h-72 w-full flex items-center justify-center bg-red-50 rounded-md border border-red-100">
            <p className="text-red-600">{error}</p>
          </div>
        ) : data.length === 0 ? (
          <div className="h-72 w-full flex items-center justify-center bg-gray-50 rounded-md">
            <p className="text-gray-500">No farm comparison data available for this period.</p>
          </div>
        ) : (
          <div className="h-80 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={data}
                margin={{ top: 20, right: 30, left: 20, bottom: 50 }}
                layout="vertical"
              >
                <CartesianGrid horizontal strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis 
                  type="number"
                  tick={{ fontSize: 12, fill: '#4b5563' }}
                  tickLine={{ stroke: '#d1d5db' }}
                  axisLine={{ stroke: '#d1d5db' }}
                  tickFormatter={(value) => {
                    if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
                    if (value >= 1000) return `${(value / 1000).toFixed(0)}k`;
                    return value.toString();
                  }}
                  label={{ 
                    value: 'Curtailed Energy (MWh)', 
                    position: 'insideBottom', 
                    offset: -10,
                    fill: '#4b5563',
                    fontSize: 14,
                    fontWeight: 500,
                  }}
                />
                <YAxis 
                  dataKey="name" 
                  type="category"
                  width={100}
                  tick={{ fontSize: 12, fill: '#4b5563' }}
                  tickLine={false}
                />
                <Tooltip content={<EnhancedChartTooltip title="Farm" />} />
                <Legend 
                  iconType="circle"
                  iconSize={10}
                  wrapperStyle={{
                    fontSize: '13px',
                    fontWeight: 500,
                    paddingTop: '10px',
                    paddingBottom: '5px'
                  }}
                />
                <Bar 
                  dataKey="curtailedEnergy" 
                  name="Curtailed Energy (MWh)" 
                  fill="#3b82f6" 
                  radius={[0, 4, 4, 0]}
                  barSize={20}
                >
                  <LabelList 
                    dataKey="curtailedEnergy" 
                    position="right" 
                    formatter={(value: number) => {
                      if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
                      if (value >= 1000) return `${(value / 1000).toFixed(0)}k`;
                      return value.toFixed(0);
                    }}
                    style={{
                      fontSize: 12,
                      fill: '#4b5563',
                      fontWeight: 500
                    }}
                  />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
        
        {/* Chart footer with additional information */}
        <div className="mt-4 pt-3 border-t border-gray-100 text-sm text-gray-500">
          <p className="flex items-center">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4 mr-2 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Chart shows farms with the most curtailed energy for this {periodText}. Hover for more details.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}