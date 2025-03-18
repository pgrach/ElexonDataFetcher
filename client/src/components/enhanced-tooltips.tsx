"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

// Enhanced tooltip for better readability in charts
export const EnhancedTooltip = ({ active, payload, label, title = "Data Point" }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-white p-3 shadow-lg border border-gray-200 rounded-md max-w-xs">
        <p className="text-base font-semibold border-b pb-1 mb-2">{title}: {label}</p>
        <div className="text-sm space-y-2">
          {payload.map((entry: any, index: number) => (
            <p key={`tooltip-${index}`} className="flex items-center">
              <span 
                className="inline-block w-3 h-3 mr-2 rounded-sm" 
                style={{ backgroundColor: entry.color }}
              />
              <span className="font-medium mr-2">{entry.name || entry.dataKey}:</span>
              <span className="text-gray-800">
                {typeof entry.value === 'number' && entry.unit
                  ? `${entry.value.toLocaleString(undefined, {
                      maximumFractionDigits: entry.unit === '%' || entry.unit === '₿' ? 2 : 0,
                    })} ${entry.unit}`
                  : entry.value
                }
              </span>
            </p>
          ))}
        </div>
      </div>
    );
  }
  return null;
};

// Enhanced Legend with better formatting
export const EnhancedLegend = ({ payload }: any) => {
  if (!payload || payload.length === 0) return null;
  
  return (
    <div className="flex justify-center items-center flex-wrap gap-x-8 gap-y-2 pt-2 pb-1">
      {payload.map((entry: any, index: number) => (
        <div key={`legend-${index}`} className="flex items-center">
          <span 
            className="inline-block w-4 h-4 mr-2 rounded" 
            style={{ 
              backgroundColor: entry.color,
              border: "1px solid rgba(0,0,0,0.1)"
            }}
          />
          <span className="text-sm font-medium text-gray-700">
            {entry.value === "curtailedEnergy" ? "Curtailed Energy (MWh)" : 
             entry.value === "bitcoinPotential" ? "Potential Bitcoin (₿)" :
             entry.value === "bitcoinMined" ? "Bitcoin Mined (₿)" :
             entry.value === "curtailmentPayment" ? "Subsidy Payment (£)" :
             entry.value === "bitcoinValue" ? "Bitcoin Value (£)" :
             entry.value}
          </span>
        </div>
      ))}
    </div>
  );
};

// Examples of how these enhanced components improve readability
export default function EnhancedTooltipExamples() {
  // Sample data to showcase the tooltips
  const samplePayload = [
    { 
      name: "Curtailed Energy", 
      value: 24500, 
      unit: "MWh", 
      color: "#2563eb", 
      dataKey: "curtailedEnergy" 
    },
    { 
      name: "Bitcoin Mined", 
      value: 12.56, 
      unit: "₿", 
      color: "#F7931A", 
      dataKey: "bitcoinMined" 
    }
  ];

  const legendPayload = [
    { value: "curtailedEnergy", color: "#2563eb", type: "rect" },
    { value: "bitcoinMined", color: "#F7931A", type: "rect" },
  ];

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Enhanced Tooltip Example</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="p-4 border rounded-md">
            <EnhancedTooltip 
              active={true} 
              payload={samplePayload} 
              label="Mar" 
              title="Month"
            />
          </div>
          <p className="text-sm text-gray-500 mt-3">
            Tooltips now include a clear title, consistent styling, and properly formatted values with units.
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Enhanced Legend Example</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="p-4 border rounded-md">
            <EnhancedLegend payload={legendPayload} />
          </div>
          <p className="text-sm text-gray-500 mt-3">
            Legends now feature more readable labels, proper spacing, and consistent styling.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}