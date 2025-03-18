"use client";

import React from 'react';

interface EnhancedTooltipProps {
  active?: boolean;
  payload?: any[];
  label?: string;
  title?: string;
  currencySymbol?: string;
}

/**
 * Enhanced chart tooltip with better readability and consistent styling
 * 
 * Features:
 * - Clear section headers with larger type
 * - Consistent spacing and borders
 * - Color-coded indicators matching chart elements
 * - Proper number formatting with units
 * - Improved contrast and shadow for better visibility
 * 
 * @param active Whether the tooltip is active
 * @param payload The data payload from the chart
 * @param label The label for the active data point
 * @param title Custom title for the tooltip header (defaults to "Data")
 * @param currencySymbol Currency symbol for monetary values (defaults to "£")
 */
export const EnhancedChartTooltip: React.FC<EnhancedTooltipProps> = ({ 
  active, 
  payload, 
  label,
  title = "Data Point",
  currencySymbol = "£"
}) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white p-3 shadow-lg border border-gray-200 rounded-md max-w-xs">
        <div className="border-b border-gray-100 pb-1.5 mb-2">
          <p className="text-base font-semibold text-gray-800">{title}: <span className="text-primary">{label}</span></p>
        </div>
        
        <div className="text-sm space-y-2">
          {payload.map((entry, index) => {
            // Format number based on type
            let formattedValue: string;
            const value = entry.value;
            
            if (typeof value === 'number') {
              const dataKey = entry.dataKey || '';
              
              // Bitcoin values (small decimals, typically < 1000)
              if (dataKey.toLowerCase().includes('bitcoin') || entry.name?.toLowerCase().includes('bitcoin')) {
                formattedValue = `₿${value.toFixed(value < 1 ? 4 : 2)}`;
              }
              // Monetary values (use currency symbol, thousands separator)
              else if (dataKey.toLowerCase().includes('value') || 
                      dataKey.toLowerCase().includes('payment') || 
                      dataKey.toLowerCase().includes('subsid')) {
                formattedValue = `${currencySymbol}${value.toLocaleString(undefined, {maximumFractionDigits: 0})}`;
              }
              // Energy values (use MWh, no decimals for large numbers)
              else if (dataKey.toLowerCase().includes('energy')) {
                formattedValue = `${value.toLocaleString(undefined, {maximumFractionDigits: 0})} MWh`;
              }
              // Percentage values
              else if (dataKey.toLowerCase().includes('percent') || dataKey.toLowerCase().includes('ratio')) {
                formattedValue = `${value.toFixed(2)}%`;
              }
              // Default numeric formatting
              else {
                formattedValue = value.toLocaleString(undefined, {
                  maximumFractionDigits: value < 10 ? 2 : 0
                });
              }
            } else {
              formattedValue = String(value);
            }
            
            return (
              <div key={`tooltip-item-${index}`} className="flex items-center">
                <span 
                  className="inline-block w-3 h-3 mr-2 rounded-sm flex-shrink-0" 
                  style={{ backgroundColor: entry.color || '#8884d8' }}
                />
                <span className="font-medium mr-2 text-gray-700">
                  {entry.name || entry.dataKey}:
                </span>
                <span className="text-gray-800">
                  {formattedValue}
                </span>
              </div>
            );
          })}
        </div>
        
        {/* Optional bottom section for additional context */}
        {payload[0]?.payload?.note && (
          <div className="mt-2 pt-2 border-t border-gray-100 text-xs text-gray-500">
            {payload[0].payload.note}
          </div>
        )}
      </div>
    );
  }
  
  return null;
};

/**
 * Enhanced chart legend with better formatting and consistency
 */
export const EnhancedChartLegend: React.FC<{payload?: any[]}> = ({ 
  payload 
}) => {
  if (!payload || payload.length === 0) return null;
  
  // Map of dataKey/value to more readable display names
  const nameMap: Record<string, string> = {
    'curtailedEnergy': 'Curtailed Energy (MWh)',
    'bitcoinPotential': 'Potential Bitcoin (₿)',
    'bitcoinMined': 'Bitcoin Mined (₿)',
    'curtailmentPayment': 'Subsidy Payment (£)',
    'bitcoinValue': 'Bitcoin Value (£)',
    'volume': 'Volume (MWh)',
    'ratio': 'Value Ratio'
  };
  
  return (
    <div className="flex justify-center items-center flex-wrap gap-x-8 gap-y-2 pt-4 pb-2">
      {payload.map((entry, index) => (
        <div key={`legend-${index}`} className="flex items-center">
          <span 
            className="inline-block w-4 h-4 mr-2 rounded" 
            style={{ 
              backgroundColor: entry.color || '#8884d8',
              border: "1px solid rgba(0,0,0,0.1)"
            }}
          />
          <span className="text-sm font-medium text-gray-700">
            {nameMap[entry.value] || nameMap[entry.dataKey] || entry.value}
          </span>
        </div>
      ))}
    </div>
  );
};

export default function EnhancedChartComponents() {
  // This component serves as documentation and an example - not for direct use
  
  // Example data to show tooltip formatting
  const samplePayload = [
    { 
      name: "Curtailed Energy", 
      value: 24500, 
      dataKey: "curtailedEnergy",
      color: "#2563eb"
    },
    { 
      name: "Bitcoin Mined", 
      value: 12.56, 
      dataKey: "bitcoinMined",
      color: "#F7931A"
    }
  ];
  
  return (
    <div className="p-6 space-y-6">
      <div>
        <h2 className="text-lg font-medium mb-4">Enhanced Chart Tooltip Example</h2>
        <div className="border p-4 rounded-md inline-block">
          <EnhancedChartTooltip 
            active={true} 
            payload={samplePayload} 
            label="Mar 2025" 
            title="Month"
          />
        </div>
      </div>
    </div>
  );
}