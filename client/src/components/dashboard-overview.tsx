"use client";

import { useState, useEffect } from "react";
import { format, isValid } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { FilterBar } from "@/components/ui/filter-bar";
import TimeframeSelector from "@/components/timeframe-selector";
import CurtailmentChart from "@/components/curtailment-chart";
import FarmComparisonChart from "@/components/farm-comparison-chart";
import FarmOpportunityComparisonChart from "@/components/farm-opportunity-comparison-chart";
import CurtailmentPercentageChart from "@/components/curtailment-percentage-chart";
import BitcoinPotentialTable from "@/components/bitcoin-potential-table";
import MinerModelSelector from "@/components/miner-model-selector";
import { DatePicker } from "@/components/date-picker";
import SummaryCards from "@/components/summary-cards";
import FarmSelector from "@/components/farm-selector";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
// CurtailmentInfoBanner removed as requested
// Guide component removed as requested

export default function DashboardOverview() {
  // State
  const [date, setDate] = useState<Date | null>(null);
  const [selectedLeadParty, setSelectedLeadParty] = useState<string | null>(null);
  const [selectedMinerModel, setSelectedMinerModel] = useState("S19J_PRO");
  const [selectedFarm, setSelectedFarm] = useState("all"); // 'all' represents all farms
  
  // Initially set to monthly, but we'll update this based on data availability
  const [timeframe, setTimeframe] = useState("monthly");
  
  // Removed Lead Party selector for Curtailment Analysis

  // Fetch the latest date with curtailment data
  const { data: latestDateData, isLoading: isLatestDateLoading } = useQuery({
    queryKey: ['/api/latest-date'],
    queryFn: async () => {
      const response = await fetch('/api/latest-date');
      if (!response.ok) {
        // If we can't get the latest date, default to today
        return { date: new Date().toISOString().split('T')[0] };
      }
      return response.json();
    },
    // Default to today if the query fails
    placeholderData: { date: new Date().toISOString().split('T')[0] }
  });

  // Set the initial date once we have the latest date
  useEffect(() => {
    if (latestDateData?.date && !date) {
      const newDate = new Date(latestDateData.date);
      // Ensure it's a valid date
      if (isValid(newDate)) {
        setDate(newDate);
      } else {
        // Fallback to today if invalid
        setDate(new Date());
      }
    }
  }, [latestDateData, date]);

  // Derived values - use fallback to today if date is null
  const formattedDate = date ? format(date, "yyyy-MM-dd") : format(new Date(), "yyyy-MM-dd");
  
  // Check if there's data available - will be used to conditionally show/hide charts for daily view
  const dailyDataCheck = useQuery({
    queryKey: [`/api/summary/daily/${formattedDate}`, "data-check"],
    queryFn: async () => {
      const response = await fetch(`/api/summary/daily/${formattedDate}`);
      if (!response.ok) {
        return false;
      }
      const data = await response.json();
      return Number(data.totalCurtailedEnergy) > 0;
    },
    // Default to false (no data) until we know otherwise
    placeholderData: false
  });
  
  // Check if daily data is available on initial load, then set timeframe accordingly
  useEffect(() => {
    // Only run this effect once on initial component mount
    const checkDailyData = async () => {
      try {
        const response = await fetch(`/api/summary/daily/${formattedDate}`);
        if (response.ok) {
          const data = await response.json();
          if (Number(data.totalCurtailedEnergy) > 0) {
            // We have daily data, set to daily view
            setTimeframe("daily");
          }
          // Otherwise keep the default "monthly" view
        }
      } catch (error) {
        console.error("Error checking daily data:", error);
        // On error, keep monthly view
      }
    };
    
    checkDailyData();
  }, [formattedDate]); // Only run when component mounts and if date changes
  
  // Only hide charts when it's a daily view AND there's no data
  // For monthly and yearly views, always show charts as they typically have data
  const shouldShowCharts = timeframe !== "daily" || dailyDataCheck.data;

  // Fetch lead parties for the filters
  const { data: curtailedLeadParties = [] } = useQuery<string[]>({
    queryKey: [`/api/lead-parties/${formattedDate}`],
    enabled: !!formattedDate && isValid(date),
  });

  // Handle farm/lead party selection
  const handleFarmChange = (value: string) => {
    console.log("Farm/Lead Party selected:", value);
    setSelectedFarm(value);
    
    // If all farms are selected, clear the lead party filter
    if (value === 'all') {
      setSelectedLeadParty(null);
    } 
    // Otherwise use the selected value as a lead party name
    else {
      setSelectedLeadParty(value);
    }
  };

  // Determine the parameter to use for API calls
  // We now use selectedLeadParty for all non-"all" cases
  const farmIdToUse = selectedLeadParty || "";

  return (
    <div className="min-h-screen">
      {/* Filter bar */}
      <div className="border-b bg-card p-4 sticky top-0 z-10">
        <div className="container mx-auto">
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:gap-6">
            <TimeframeSelector 
              value={timeframe} 
              onValueChange={setTimeframe} 
            />
            <DatePicker 
              date={date} 
              onDateChange={newDate => newDate && setDate(newDate)} 
            />
            <MinerModelSelector 
              value={selectedMinerModel} 
              onValueChange={setSelectedMinerModel} 
            />
            <FarmSelector
              value={selectedFarm}
              onValueChange={handleFarmChange}
              timeframe={timeframe}
              date={date}
            />
          </div>
        </div>
      </div>

      <div className="container mx-auto py-8">
        <h1 className="text-4xl font-bold mb-8">CurtailCoin Dashboard</h1>
        
        {/* Educational components removed as requested */}
        
        {/* Summary cards */}
        <SummaryCards 
          timeframe={timeframe}
          date={date}
          minerModel={selectedMinerModel}
          farmId={farmIdToUse}
        />
        
        {/* Only show tabs when there's data (or if we're in monthly/yearly view) */}
        {shouldShowCharts && (
          <>
            {/* Tabs for different analyses */}
            <Tabs defaultValue="charts" className="mt-10">
              <TabsList className="grid grid-cols-3 mb-8 text-lg">
                <TabsTrigger value="charts" className="py-3">Charts & Visualizations</TabsTrigger>
                <TabsTrigger value="curtailment" className="py-3">Curtailment Analysis</TabsTrigger>
                <TabsTrigger value="data" className="py-3">Data Tables</TabsTrigger>
              </TabsList>
              
              <TabsContent value="charts" className="space-y-8">
                {/* Curtailment Chart */}
                <CurtailmentChart 
                  timeframe={timeframe}
                  date={date}
                  minerModel={selectedMinerModel}
                  farmId={farmIdToUse}
                />
                
                {/* Show either Farm Comparison or Farm Opportunity Comparison based on farm selection */}
                {selectedFarm === 'all' ? (
                  // When no specific farm is selected, show the general farm comparison
                  <FarmComparisonChart
                    timeframe={timeframe}
                    date={date}
                    minerModel={selectedMinerModel}
                  />
                ) : (
                  // When a specific farm is selected, show the opportunity comparison
                  <FarmOpportunityComparisonChart
                    timeframe={timeframe}
                    date={date}
                    minerModel={selectedMinerModel}
                    farmId={selectedFarm}
                  />
                )}
              </TabsContent>
              
              <TabsContent value="curtailment" className="space-y-8">
                {/* Simplified view - no redundant title */}
                <CurtailmentPercentageChart 
                  date={date}
                  leadPartyName={undefined}
                  farmId={undefined}
                />
              </TabsContent>
              
              <TabsContent value="data" className="space-y-8">
                {/* Bitcoin Potential Table with integrated farm data */}
                <BitcoinPotentialTable
                  timeframe={timeframe}
                  date={date}
                  minerModel={selectedMinerModel}
                  farmId={farmIdToUse}
                />
              </TabsContent>
            </Tabs>
          </>
        )}
      </div>
    </div>
  );
}