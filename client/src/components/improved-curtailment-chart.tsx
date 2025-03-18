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
  ReferenceLine,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { EnhancedChartTooltip } from "./enhanced-chart-tooltip";

interface ImprovedCurtailmentChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

// Custom tooltip component with improved readability
const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white p-3 shadow-lg border border-gray-200 rounded-md">
        <p className="text-base font-semibold text-gray-800 mb-1.5 border-b border-gray-100 pb-1.5">
          {label}
        </p>
        
        <div className="text-sm space-y-2">
          {payload.map((entry: any, index: number) => {
            let formattedValue = entry.value;
            const dataKey = entry.dataKey;
            
            if (dataKey === "curtailedEnergy") {
              formattedValue = `${Number(entry.value).toLocaleString()} MWh`;
            } else if (dataKey === "bitcoinMined") {
              formattedValue = `₿${Number(entry.value).toFixed(2)}`;
            }
            
            return (
              <div key={`tooltip-${index}`} className="flex items-center">
                <span 
                  className="inline-block w-3 h-3 mr-2 rounded-sm" 
                  style={{ backgroundColor: entry.color }}
                />
                <span className="font-medium mr-2 text-gray-700">
                  {dataKey === "curtailedEnergy" ? "Curtailed Energy:" : "Bitcoin Potential:"}
                </span>
                <span className="text-gray-800">
                  {formattedValue}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    );
  }
  
  return null;
};

export default function ImprovedCurtailmentChart({ 
  timeframe, 
  date, 
  minerModel, 
  farmId 
}: ImprovedCurtailmentChartProps) {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);
  
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      
      try {
        // Adjust endpoint based on timeframe
        let endpoint = "";
        let params: any = { minerModel };
        
        if (farmId) {
          params.farmId = farmId;
        }
        
        if (timeframe === "yearly") {
          endpoint = `/api/curtailment/yearly-mining-potential/${format(date, "yyyy")}`;
        } else if (timeframe === "monthly") {
          // For monthly view, we need to get all months for the year
          endpoint = `/api/curtailment/monthly-breakdown/${format(date, "yyyy")}`;
          setSelectedMonth(format(date, "MMM")); // Highlight selected month
        } else {
          // Daily view
          endpoint = `/api/curtailment/daily-mining-potential/${format(date, "yyyy-MM-dd")}`;
        }
        
        const response = await axios.get(endpoint, { params });
        
        // Process data based on timeframe
        let processedData = [];
        
        if (timeframe === "yearly") {
          // Yearly breakdown by month
          processedData = response.data.monthlyBreakdown || [];
        } else if (timeframe === "monthly") {
          // Monthly breakdown - transform data for chart
          const months = response.data || [];
          processedData = months.map((item: any) => ({
            month: format(new Date(item.month + "-01"), "MMM"),
            curtailedEnergy: item.curtailedEnergy,
            bitcoinMined: item.bitcoinMined
          }));
        } else {
          // Daily breakdown by hour
          processedData = response.data.hourlyBreakdown || [];
          processedData = processedData.map((item: any) => ({
            hour: `${item.hour}:00`,
            curtailedEnergy: item.curtailedEnergy,
            bitcoinMined: item.bitcoinMined
          }));
        }
        
        console.log("Processed chart data:", processedData);
        setData(processedData);
      } catch (err: any) {
        console.error("Error fetching chart data:", err);
        setError("Failed to load chart data. Please try again.");
      } finally {
        setLoading(false);
      }
    };
    
    fetchData();
  }, [timeframe, date, minerModel, farmId]);
  
  // Determine chart labels based on timeframe
  const chartTitle = timeframe === "yearly" 
    ? "Yearly Breakdown by Month" 
    : timeframe === "monthly" 
      ? "Monthly Breakdown" 
      : "Daily Breakdown by Hour";
      
  // Determine x-axis label
  const xAxisLabel = timeframe === "yearly" || timeframe === "monthly" 
    ? "Month" 
    : "Hour";
    
  // Title describing the current view
  const viewDescription = timeframe === "yearly"
    ? `${format(date, "yyyy")} Monthly Breakdown`
    : timeframe === "monthly"
      ? `${format(date, "MMMM yyyy")} Breakdown`
      : `${format(date, "PP")} Hourly Breakdown`;
      
  // Custom bar background for highlighting the current month in monthly view
  const getBarBackground = (entry: any) => {
    if (timeframe === "monthly" && selectedMonth && entry.month === selectedMonth) {
      return "rgba(59, 130, 246, 0.1)"; // Light blue background
    }
    return "transparent";
  };
  
  return (
    <Card className="shadow-md border-gray-200">
      <CardHeader className="border-b border-gray-100 bg-gray-50/50">
        <CardTitle className="text-lg font-medium text-gray-800">{chartTitle}</CardTitle>
        <CardDescription className="text-xs text-gray-500">
          {viewDescription}{farmId ? ` for farm ${farmId}` : ""}. 
          Using {minerModel} miner model.
        </CardDescription>
      </CardHeader>
      <CardContent className="p-6">
        {loading ? (
          <div className="h-72 w-full flex items-center justify-center bg-gray-50 rounded-md">
            <p className="text-gray-500">Loading chart data...</p>
          </div>
        ) : error ? (
          <div className="h-72 w-full flex items-center justify-center bg-red-50 rounded-md border border-red-100">
            <p className="text-red-600">{error}</p>
          </div>
        ) : data.length === 0 ? (
          <div className="h-72 w-full flex items-center justify-center bg-gray-50 rounded-md">
            <p className="text-gray-500">No data available for this period.</p>
          </div>
        ) : (
          <div className="h-80 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={data}
                margin={{ top: 20, right: 30, left: 20, bottom: 45 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis 
                  dataKey={timeframe === "yearly" || timeframe === "monthly" ? "month" : "hour"} 
                  tick={{ fontSize: 12, fill: '#4b5563' }}
                  tickLine={{ stroke: '#d1d5db' }}
                  axisLine={{ stroke: '#d1d5db' }}
                  label={{ 
                    value: xAxisLabel, 
                    position: 'insideBottom', 
                    offset: -10,
                    fill: '#4b5563',
                    fontSize: 14,
                    fontWeight: 500,
                  }}
                  height={60}
                />
                <YAxis 
                  yAxisId="left"
                  tick={{ fontSize: 12, fill: '#4b5563' }}
                  tickFormatter={(value) => {
                    if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
                    if (value >= 1000) return `${(value / 1000).toFixed(0)}k`;
                    return value.toString();
                  }}
                  label={{ 
                    value: 'Curtailed Energy (MWh)', 
                    angle: -90, 
                    position: 'insideLeft',
                    fill: '#3b82f6',
                    fontSize: 13,
                    fontWeight: 500,
                    dx: -15
                  }}
                  width={80}
                />
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tick={{ fontSize: 12, fill: '#4b5563' }}
                  tickFormatter={(value) => {
                    if (value >= 1000) return `${(value / 1000).toFixed(1)}k`;
                    if (value >= 100) return value.toFixed(0);
                    return value.toFixed(1);
                  }}
                  label={{ 
                    value: 'Bitcoin Potential (₿)', 
                    angle: 90, 
                    position: 'insideRight',
                    fill: '#f59e0b',
                    fontSize: 13,
                    fontWeight: 500,
                    dx: 15
                  }}
                  width={80}
                />
                <Tooltip content={<EnhancedChartTooltip />} />
                <Legend 
                  formatter={(value) => {
                    if (value === "curtailedEnergy") return "Curtailed Energy (MWh)";
                    if (value === "bitcoinMined") return "Potential Bitcoin (₿)";
                    return value;
                  }}
                  iconType="circle"
                  iconSize={10}
                  wrapperStyle={{
                    fontSize: '13px',
                    fontWeight: 500,
                    paddingTop: '10px',
                    paddingBottom: '5px'
                  }}
                />
                {/* Add background highlight for selected month using a different approach */}
                {timeframe === "monthly" && selectedMonth && (
                  <CartesianGrid
                    horizontal={false}
                    verticalPoints={data
                      .filter(entry => entry.month === selectedMonth)
                      .map(entry => {
                        const monthIndex = data.findIndex(d => d.month === entry.month);
                        return monthIndex;
                      })}
                    stroke="#3b82f620"
                    strokeWidth={30}
                  />
                )}
                <Bar
                  yAxisId="left"
                  dataKey="curtailedEnergy"
                  name="Curtailed Energy (MWh)"
                  fill="#3b82f6"
                  radius={[4, 4, 0, 0]}
                  barSize={timeframe === "daily" ? 15 : 30}
                />
                <Bar
                  yAxisId="right"
                  dataKey="bitcoinMined"
                  name="Potential Bitcoin (₿)"
                  fill="#f59e0b"
                  radius={[4, 4, 0, 0]}
                  barSize={timeframe === "daily" ? 15 : 30}
                />
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
            {timeframe === "monthly" && selectedMonth ? 
              `${selectedMonth} is highlighted. ` : 
              ""
            }
            Chart shows {timeframe === "yearly" ? "monthly" : timeframe === "monthly" ? "monthly" : "hourly"} curtailed energy and potential Bitcoin.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}