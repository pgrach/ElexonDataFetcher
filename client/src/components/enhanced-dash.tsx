"use client";

import { useState } from "react";
import { format } from "date-fns";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Wind, Battery, Bitcoin, ArrowRightLeft, Calendar } from "lucide-react";
import { DatePicker } from "@/components/date-picker";
import TimeframeSelector from "@/components/timeframe-selector";
import MinerModelSelector from "@/components/miner-model-selector";
import FarmSelector from "@/components/farm-selector";

// This is an enhanced version of the dashboard that integrates all readability improvements 
// It doesn't modify existing components but serves as a template for improving readability

export default function EnhancedDashboard() {
  // State variables (would be linked to actual data when implemented)
  const [date, setDate] = useState<Date>(new Date());
  const [timeframe, setTimeframe] = useState<string>("monthly");
  const [minerModel, setMinerModel] = useState<string>("S19J_PRO");
  const [farmId, setFarmId] = useState<string>("");
  
  // Helper for displaying timeframe-specific text
  const timeframeLabel =
    timeframe === "yearly"
      ? format(date, "yyyy")
      : timeframe === "monthly"
        ? format(date, "MMMM yyyy")
        : format(date, "PP");
        
  return (
    <div className="container mx-auto py-4 space-y-6">
      {/* Enhanced page header with clearer typography */}
      <div className="space-y-2 pb-4 border-b border-gray-200">
        <h1 className="text-2xl font-bold text-gray-800">Bitcoin Mining Analytics</h1>
        <p className="text-sm text-gray-600">
          Analyze wind farm curtailment and potential Bitcoin mining opportunities.
        </p>
      </div>
      
      {/* Enhanced filter bar with improved spacing and contrast */}
      <Card className="shadow-sm border-gray-200">
        <CardContent className="p-4">
          <div className="grid gap-4 md:grid-cols-4">
            <div className="space-y-1">
              <label className="text-sm font-medium text-gray-700 flex items-center">
                <Calendar className="h-4 w-4 mr-1 text-primary" />
                <span>Date</span>
              </label>
              <DatePicker date={date} onDateChange={(newDate) => newDate && setDate(newDate)} />
            </div>
            
            <div className="space-y-1">
              <label className="text-sm font-medium text-gray-700">Timeframe</label>
              <TimeframeSelector 
                value={timeframe} 
                onValueChange={setTimeframe} 
                date={date}
              />
            </div>
            
            <div className="space-y-1">
              <label className="text-sm font-medium text-gray-700">Miner Model</label>
              <MinerModelSelector
                value={minerModel}
                onValueChange={setMinerModel}
              />
            </div>
            
            <div className="space-y-1">
              <label className="text-sm font-medium text-gray-700">Wind Farm</label>
              <FarmSelector
                value={farmId}
                onValueChange={setFarmId}
                timeframe={timeframe}
                date={date}
              />
            </div>
          </div>
        </CardContent>
      </Card>
      
      {/* Enhanced summary cards with improved readability */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {/* Energy Curtailed Card */}
        <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
            <div className="flex justify-between items-center">
              <CardTitle className="text-sm font-medium text-gray-700">
                Energy Curtailed
              </CardTitle>
              <Wind className="h-4 w-4 text-primary" />
            </div>
            <CardDescription className="text-xs text-gray-500">
              Total wasted wind energy
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-4">
            <div className="space-y-1">
              <div className="text-2xl font-bold text-gray-800">
                556k <span className="text-base font-medium">MWh</span>
              </div>
              <div className="flex flex-col">
                <div className="flex items-center">
                  <span className="inline-block w-2 h-2 rounded-full bg-primary mr-2"></span>
                  <p className="text-xs text-gray-500">
                    Untapped energy resource
                  </p>
                </div>
                <div className="mt-2 pt-2 border-t border-gray-100">
                  <div className="text-xs text-gray-500">
                    That's enough to power approximately{" "}
                    <span className="font-medium text-primary">
                      167k homes
                    </span>{" "}
                    for a month
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Subsidies Paid Card */}
        <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
            <div className="flex justify-between items-center">
              <CardTitle className="text-sm font-medium text-gray-700">
                Subsidies Paid
              </CardTitle>
              <Battery className="h-4 w-4 text-red-500" />
            </div>
            <CardDescription className="text-xs text-gray-500">
              Consumer cost for curtailment
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-4">
            <div className="space-y-1">
              <div className="text-2xl font-bold text-red-500">
                £11.5M
              </div>
              <div className="flex flex-col">
                <div className="flex items-center">
                  <span className="inline-block w-2 h-2 rounded-full bg-red-500 mr-2"></span>
                  <p className="text-xs text-gray-500">
                    Paid to idle wind farms
                  </p>
                </div>
                <div className="mt-2 pt-2 border-t border-gray-100">
                  <div className="text-xs text-gray-500">
                    Approximately{" "}
                    <span className="font-medium text-red-500">
                      £21
                    </span>{" "}
                    per MWh of curtailed energy
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Potential Bitcoin Card */}
        <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
            <div className="flex justify-between items-center">
              <CardTitle className="text-sm font-medium text-gray-700">
                Potential Bitcoin
              </CardTitle>
              <Bitcoin className="h-4 w-4 text-[#F7931A]" />
            </div>
            <CardDescription className="text-xs text-gray-500">
              Mining using S19J PRO
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-4">
            <div className="space-y-1">
              <div>
                <div className="text-2xl font-bold text-[#F7931A]">
                  394.68 <span className="text-base font-medium">₿</span>
                </div>
                <div className="text-sm text-gray-600 mt-0">
                  ≈ £25.8M
                </div>
              </div>
              
              <div className="mt-2 pt-2 border-t border-gray-100">
                <div className="text-xs text-gray-500">
                  <span className="font-medium text-[#F7931A]">
                    0.710 ₿
                  </span>{" "}
                  per GWh of curtailed energy
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Value Ratio Card */}
        <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
          <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
            <div className="flex justify-between items-center">
              <CardTitle className="text-sm font-medium text-gray-700">
                Value Ratio
              </CardTitle>
              <ArrowRightLeft className="h-4 w-4 text-green-500" />
            </div>
            <CardDescription className="text-xs text-gray-500">
              Bitcoin value vs. subsidy cost
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-4">
            <div className="space-y-1">
              <div className="text-2xl font-bold text-green-500">
                2.25×
              </div>
              
              <div className="flex flex-col">
                <div className="flex items-center gap-1">
                  <span className="inline-block w-2 h-2 rounded-full bg-green-500 mr-1"></span>
                  <p className="text-xs text-gray-500">
                    <span className="font-medium text-green-500">High value</span> from mining
                  </p>
                </div>
                
                <div className="mt-2 pt-2 border-t border-gray-100">
                  <div className="text-xs text-gray-500">
                    Bitcoin value is{" "}
                    <span className="font-medium text-green-500">
                      2.25×
                    </span>{" "}
                    the subsidy payment
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
      
      {/* Enhanced chart container */}
      <Card className="shadow-md border-gray-200">
        <CardHeader className="border-b border-gray-100 bg-gray-50/50">
          <CardTitle className="text-lg font-medium text-gray-800">Monthly Curtailment & Bitcoin Breakdown</CardTitle>
          <CardDescription className="text-xs text-gray-500">
            Chart shows monthly energy curtailment and potential Bitcoin mining for 2025. Hover over bars for details.
          </CardDescription>
        </CardHeader>
        <CardContent className="p-6">
          {/* Chart would be included here - using placeholder to indicate */}
          <div className="h-80 border border-dashed border-gray-200 rounded-md flex items-center justify-center bg-gray-50">
            <div className="text-center">
              <p className="text-gray-500">Chart displays with enhanced readability features:</p>
              <ul className="text-sm text-gray-600 list-disc list-inside mt-2 space-y-1 text-left max-w-md mx-auto">
                <li>Larger, more readable axis labels</li>
                <li>Improved color contrast for data points</li>
                <li>Enhanced tooltips with better formatting</li>
                <li>Clearer legends with more descriptive labels</li>
                <li>Consistent spacing and typography</li>
              </ul>
            </div>
          </div>
          
          {/* Note section below chart for additional context */}
          <div className="mt-4 pt-3 border-t border-gray-100 text-sm text-gray-500">
            <p className="flex items-center">
              <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4 mr-2 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              Chart shows monthly curtailed energy and potential Bitcoin mining opportunity.
              March 2025 data is highlighted.
            </p>
          </div>
        </CardContent>
      </Card>
      
      {/* Footer with data source information */}
      <div className="text-xs text-gray-400 pt-2 border-t border-gray-100">
        <p>Data sources: Elexon API for curtailment data, current Bitcoin network difficulty, and market prices.</p>
      </div>
    </div>
  );
}