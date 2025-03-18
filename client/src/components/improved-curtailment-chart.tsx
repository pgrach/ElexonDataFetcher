"use client";

import { useState, useEffect } from "react";
import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, LabelList, ReferenceLine, ReferenceArea } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";

interface CurtailmentChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

// SVG Bitcoin logo for chart label with improved visibility
const BitcoinIcon = ({ x, y, value }: { x: number, y: number, value: number }) => {
  // Skip rendering if value is too small
  if (value < 0.01) return null;
  
  return (
    <g transform={`translate(${x},${y - 22})`}>
      {/* Background glow for better visibility */}
      <circle cx="10" cy="10" r="14" fill="#F7931A" opacity={0.2} />
      
      {/* Bitcoin circle */}
      <circle cx="10" cy="10" r="10" fill="#F7931A" />
      
      {/* Bitcoin B symbol */}
      <path 
        d="M14.25,8.65c0.18-1.22-0.75-1.87-2.02-2.31l0.41-1.66l-1.01-0.25l-0.4,1.61c-0.27-0.07-0.54-0.13-0.81-0.19l0.4-1.62L9.81,4l-0.41,1.66c-0.22-0.05-0.44-0.1-0.65-0.15l0,0L7.43,5.2L7.15,6.3c0,0,0.75,0.17,0.73,0.18C8.34,6.62,8.43,6.81,8.4,6.96L7.93,8.91c0.02,0.01,0.05,0.01,0.08,0.03l-0.08-0.02L7.24,11.9c-0.05,0.11-0.17,0.28-0.45,0.22c0.01,0.02-0.73-0.18-0.73-0.18l-0.5,1.15l1.85,0.46c0.34,0.09,0.68,0.18,1.01,0.26l-0.42,1.68l1.01,0.25l0.41-1.66c0.28,0.08,0.55,0.15,0.81,0.21l-0.41,1.65l1.01,0.25l0.42-1.67c1.74,0.33,3.05,0.2,3.6-1.38c0.44-1.27,0.02-2.01-0.93-2.49C13.89,10.25,14.13,9.53,14.25,8.65z M12.19,11.79c-0.31,1.27-2.43,0.58-3.12,0.41l0.56-2.23C10.32,10.13,12.52,10.46,12.19,11.79z M12.5,8.63c-0.29,1.15-2.05,0.57-2.63,0.42l0.5-2.02C10.96,7.18,12.8,7.42,12.5,8.63z"
        fill="white" 
      />
      
      {/* Value background for better readability */}
      <rect 
        x="-15" 
        y="25" 
        width="50" 
        height="22" 
        rx="4"
        fill="#FFFFFF" 
        opacity="0.9"
      />
      
      {/* Value text below the icon with improved spacing and visibility */}
      <text 
        x="10" 
        y="38" 
        textAnchor="middle" 
        fontSize="12" 
        fontWeight="bold" 
        fill="#F7931A"
      >
        {value.toFixed(2)}
      </text>
    </g>
  );
};

// Enhanced tooltip for better readability
const CustomDailyTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    const energyValue = Number(payload[0].value).toLocaleString(undefined, { 
      maximumFractionDigits: 2 
    });
    const bitcoinValue = Number(payload[1].value).toFixed(4);

    return (
      <div className="bg-white p-3 shadow-md border border-gray-200 rounded-md">
        <p className="text-base font-semibold border-b pb-1 mb-2">{`Hour: ${label}`}</p>
        <div className="text-sm space-y-1">
          <p><span className="inline-block w-4 h-2 bg-primary mr-2 rounded-sm"></span>
            <span className="font-medium">Energy:</span> {energyValue} MWh
          </p>
          <p><span className="inline-block w-4 h-2 bg-[#F7931A] mr-2 rounded-sm"></span>
            <span className="font-medium">Bitcoin:</span> ₿{bitcoinValue}
          </p>
        </div>
      </div>
    );
  }
  return null;
};

// Enhanced tooltip for monthly data
const CustomMonthlyTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    const energyValue = Number(payload[0].value).toLocaleString(undefined, { 
      maximumFractionDigits: 0 
    });
    const bitcoinValue = Number(payload[1].value).toFixed(2);

    return (
      <div className="bg-white p-3 shadow-md border border-gray-200 rounded-md">
        <p className="text-base font-semibold border-b pb-1 mb-2">{`Month: ${label}`}</p>
        <div className="text-sm space-y-1">
          <p><span className="inline-block w-4 h-2 bg-primary mr-2 rounded-sm"></span>
            <span className="font-medium">Energy:</span> {energyValue} MWh
          </p>
          <p><span className="inline-block w-4 h-2 bg-[#F7931A] mr-2 rounded-sm"></span>
            <span className="font-medium">Bitcoin:</span> ₿{bitcoinValue}
          </p>
        </div>
      </div>
    );
  }
  return null;
};

// Enhanced Legend component
const EnhancedLegend = ({ payload }: any) => {
  return (
    <ul className="flex justify-center gap-6 pt-4 pb-2">
      {payload.map((entry: any, index: number) => (
        <li key={`item-${index}`} className="flex items-center">
          <span 
            className="inline-block w-4 h-4 mr-2 rounded" 
            style={{ 
              backgroundColor: entry.color,
              border: "1px solid rgba(0,0,0,0.1)"
            }}
          />
          <span className="text-sm font-medium">
            {entry.value === "curtailedEnergy" ? "Curtailed Energy (MWh)" : 
             entry.value === "bitcoinPotential" ? "Potential Bitcoin (₿)" :
             entry.value === "bitcoinMined" ? "Bitcoin Mined (₿)" : entry.value}
          </span>
        </li>
      ))}
    </ul>
  );
};

export default function ImprovedCurtailmentChart({ timeframe, date, minerModel, farmId }: CurtailmentChartProps) {
  // [KEEP THE SAME DATA FETCHING CODE]
  // NOTE: This is a demonstration component to show improved visualization
  // The actual implementation would use the same data fetching logic as curtailment-chart.tsx

  // Placeholder data for demonstration
  // In real implementation, this would be replaced with the actual data fetching logic
  
  // Example data for hourly view - replace with actual data fetching in real implementation
  const dailyChartData = [
    { hour: "00:00", curtailedEnergy: 120, bitcoinPotential: 0.25 },
    { hour: "01:00", curtailedEnergy: 180, bitcoinPotential: 0.37 },
    { hour: "02:00", curtailedEnergy: 220, bitcoinPotential: 0.45 },
    { hour: "03:00", curtailedEnergy: 250, bitcoinPotential: 0.52 },
    { hour: "04:00", curtailedEnergy: 290, bitcoinPotential: 0.60 },
    { hour: "05:00", curtailedEnergy: 350, bitcoinPotential: 0.72 },
    { hour: "06:00", curtailedEnergy: 320, bitcoinPotential: 0.66 },
    { hour: "07:00", curtailedEnergy: 280, bitcoinPotential: 0.58 },
    { hour: "08:00", curtailedEnergy: 250, bitcoinPotential: 0.52 },
    { hour: "09:00", curtailedEnergy: 200, bitcoinPotential: 0.41 },
    { hour: "10:00", curtailedEnergy: 180, bitcoinPotential: 0.37 },
    { hour: "11:00", curtailedEnergy: 150, bitcoinPotential: 0.31 }
  ];

  // Example data for monthly view - replace with actual data fetching in real implementation
  const monthlyChartData = [
    { month: "Jan", curtailedEnergy: 54000, bitcoinMined: 42.5 },
    { month: "Feb", curtailedEnergy: 114000, bitcoinMined: 21.7 },
    { month: "Mar", curtailedEnergy: 55600, bitcoinMined: 39.5 },
    { month: "Apr", curtailedEnergy: 0, bitcoinMined: 0 },
    { month: "May", curtailedEnergy: 0, bitcoinMined: 0 },
    { month: "Jun", curtailedEnergy: 0, bitcoinMined: 0 },
    { month: "Jul", curtailedEnergy: 0, bitcoinMined: 0 },
    { month: "Aug", curtailedEnergy: 0, bitcoinMined: 0 },
    { month: "Sept", curtailedEnergy: 0, bitcoinMined: 0 },
    { month: "Oct", curtailedEnergy: 0, bitcoinMined: 0 },
    { month: "Nov", curtailedEnergy: 0, bitcoinMined: 0 },
    { month: "Dec", curtailedEnergy: 0, bitcoinMined: 0 }
  ];

  // Get max Bitcoin value for right Y-axis scaling
  const maxDailyBitcoin = Math.max(...dailyChartData.map(d => d.bitcoinPotential || 0), 0.1);
  const maxMonthlyBitcoin = Math.max(...monthlyChartData.map(d => d.bitcoinMined || 0), 0.1);
  const selectedMonth = "Mar"; // This would be determined dynamically in the real implementation
  
  // Get card title based on timeframe
  const getCardTitle = () => {
    switch(timeframe) {
      case "daily": return "Hourly Energy & Bitcoin Breakdown";
      case "monthly": return "Monthly Curtailment & Bitcoin Breakdown";
      case "yearly": return "Yearly Breakdown";
      default: return "Energy & Bitcoin Breakdown";
    }
  };

  return (
    <Card className="shadow-md border-gray-200">
      <CardHeader className="border-b border-gray-100 bg-gray-50/50">
        <CardTitle className="text-lg font-medium text-gray-800">{getCardTitle()}</CardTitle>
      </CardHeader>
      <CardContent className="pt-6">
        {timeframe === "daily" ? (
          <ResponsiveContainer width="100%" height={340}>
            <BarChart
              data={dailyChartData}
              margin={{ top: 20, right: 40, left: 20, bottom: 30 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
              <XAxis 
                dataKey="hour" 
                tick={{ fontSize: 12, fill: "#666" }}
                tickFormatter={(value) => value.split(":")[0] + ":00"}
                axisLine={{ stroke: '#ccc' }}
                tickLine={{ stroke: '#ccc' }}
                padding={{ left: 10, right: 10 }}
                label={{ 
                  value: 'Hour of Day', 
                  position: 'insideBottom', 
                  offset: -15,
                  fill: '#666',
                  fontSize: 12,
                  fontWeight: 500
                }}
              />
              {/* Left Y axis for energy */}
              <YAxis 
                yAxisId="left"
                orientation="left"
                tick={{ fontSize: 12, fill: "#666" }} 
                axisLine={{ stroke: '#ccc' }}
                tickLine={{ stroke: '#ccc' }}
                label={{ 
                  value: 'Curtailed Energy (MWh)', 
                  angle: -90, 
                  position: 'insideLeft',
                  style: { 
                    textAnchor: 'middle',
                    fill: '#666',
                    fontSize: 12,
                    fontWeight: 500
                  },
                  offset: 0
                }}
              />
              {/* Right Y axis for Bitcoin */}
              <YAxis 
                yAxisId="right"
                orientation="right"
                domain={[0, Math.ceil(maxDailyBitcoin * 1.2 * 10) / 10]}
                tick={{ fontSize: 12, fill: "#666" }}
                axisLine={{ stroke: '#ccc' }}
                tickLine={{ stroke: '#ccc' }}
                tickFormatter={(value) => value.toFixed(2)}
                label={{ 
                  value: 'Potential Bitcoin (₿)', 
                  angle: 90, 
                  position: 'insideRight',
                  style: { 
                    textAnchor: 'middle',
                    fill: '#F7931A',
                    fontSize: 12,
                    fontWeight: 500
                  },
                  offset: 0
                }}
              />
              <Tooltip content={<CustomDailyTooltip />} />
              
              {/* Custom enhanced legend */}
              <Legend content={<EnhancedLegend />} />
              
              {/* Bars for curtailed energy */}
              <Bar 
                yAxisId="left"
                dataKey="curtailedEnergy" 
                fill="var(--primary)"
                radius={[4, 4, 0, 0]}
                name="Curtailed Energy (MWh)"
              />
              
              {/* Line for Bitcoin potential */}
              <Bar
                yAxisId="right"
                dataKey="bitcoinPotential"
                fill="#F7931A"
                radius={[4, 4, 0, 0]}
                name="Potential Bitcoin (₿)"
                minPointSize={2}
                barSize={20}
              >
                {/* Custom Bitcoin labels */}
                <LabelList 
                  dataKey="bitcoinPotential" 
                  position="top" 
                  content={BitcoinIcon} 
                />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        ) : timeframe === "monthly" ? (
          <ResponsiveContainer width="100%" height={340}>
            <BarChart
              data={monthlyChartData}
              margin={{ top: 20, right: 40, left: 20, bottom: 30 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
              <XAxis 
                dataKey="month" 
                tick={{ fontSize: 12, fill: "#666" }}
                axisLine={{ stroke: '#ccc' }}
                tickLine={{ stroke: '#ccc' }}
                padding={{ left: 10, right: 10 }}
                label={{
                  value: 'Month (2025)',
                  position: 'insideBottom',
                  offset: -15,
                  fill: '#666',
                  fontSize: 12,
                  fontWeight: 500
                }}
              />
              {/* Left Y axis for energy */}
              <YAxis 
                yAxisId="left"
                orientation="left"
                tick={{ fontSize: 12, fill: "#666" }}
                axisLine={{ stroke: '#ccc' }}
                tickLine={{ stroke: '#ccc' }}
                tickFormatter={(value) => value >= 1000 ? `${(value/1000).toFixed(0)}k` : value}
                label={{ 
                  value: 'Curtailed Energy (MWh)', 
                  angle: -90, 
                  position: 'insideLeft',
                  style: { 
                    textAnchor: 'middle',
                    fill: '#666',
                    fontSize: 12,
                    fontWeight: 500
                  },
                  offset: 5
                }}
              />
              {/* Right Y axis for Bitcoin */}
              <YAxis 
                yAxisId="right"
                orientation="right"
                domain={[0, Math.ceil(maxMonthlyBitcoin * 1.2)]}
                tick={{ fontSize: 12, fill: "#666" }}
                axisLine={{ stroke: '#ccc' }}
                tickLine={{ stroke: '#ccc' }}
                tickFormatter={(value) => value.toFixed(0)}
                label={{ 
                  value: 'Bitcoin Mined (₿)', 
                  angle: 90, 
                  position: 'insideRight',
                  style: { 
                    textAnchor: 'middle',
                    fill: '#F7931A',
                    fontSize: 12,
                    fontWeight: 500
                  },
                  offset: 5
                }}
              />
              <Tooltip content={<CustomMonthlyTooltip />} />
              
              {/* Custom enhanced legend */}
              <Legend content={<EnhancedLegend />} />
              
              {/* Reference area for highlighting the selected month */}
              {selectedMonth && (
                <ReferenceArea 
                  x1={selectedMonth} 
                  x2={selectedMonth} 
                  fill="#f0f9ff" 
                  fillOpacity={0.6} 
                  stroke="#3b82f6"
                  strokeWidth={1}
                  strokeOpacity={0.5}
                />
              )}
              
              {/* Bars for curtailed energy */}
              <Bar 
                yAxisId="left"
                dataKey="curtailedEnergy" 
                fill="var(--primary)"
                radius={[4, 4, 0, 0]}
                name="Curtailed Energy (MWh)"
              />
              
              {/* Bars for Bitcoin mined */}
              <Bar
                yAxisId="right"
                dataKey="bitcoinMined"
                fill="#F7931A"
                radius={[4, 4, 0, 0]}
                name="Bitcoin Mined (₿)"
                barSize={20}
              >
                {/* Custom Bitcoin labels */}
                <LabelList 
                  dataKey="bitcoinMined" 
                  position="top" 
                  content={BitcoinIcon} 
                />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        ) : null}
        
        {/* Note section below chart for additional context */}
        <div className="mt-4 pt-3 border-t border-gray-100 text-sm text-gray-500">
          <p className="flex items-center">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4 mr-2 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            {timeframe === "daily" 
              ? "Each bar shows hourly curtailed energy and potential Bitcoin that could have been mined."
              : "Chart shows monthly curtailed energy and potential Bitcoin mining opportunity."}
          </p>
        </div>
      </CardContent>
    </Card>
  );
}