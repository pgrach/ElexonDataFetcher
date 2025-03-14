"use client";

import { useState } from "react";
import { format, isValid } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { FilterBar } from "@/components/ui/filter-bar";
import TimeframeSelector from "@/components/timeframe-selector";
import CurtailmentChart from "@/components/curtailment-chart";
import FarmComparisonChart from "@/components/farm-comparison-chart";
import FarmOpportunityComparisonChart from "@/components/farm-opportunity-comparison-chart";
import CurtailmentPercentageChart from "@/components/curtailment-percentage-chart";
import LeadPartyCurtailmentChart from "@/components/lead-party-curtailment-chart";
import BitcoinPotentialTable from "@/components/bitcoin-potential-table";
import MinerModelSelector from "@/components/miner-model-selector";
import { DatePicker } from "@/components/date-picker";
import SummaryCards from "@/components/summary-cards";
import FarmSelector from "@/components/farm-selector";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function DashboardOverview() {
  // State
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2022-01-01");
    return today < startDate ? startDate : today;
  });
  const [selectedLeadParty, setSelectedLeadParty] = useState<string | null>(null);
  const [selectedMinerModel, setSelectedMinerModel] = useState("S19J_PRO");
  const [selectedFarm, setSelectedFarm] = useState("all"); // 'all' represents all farms
  const [timeframe, setTimeframe] = useState("daily");

  // Derived values
  const formattedDate = format(date, "yyyy-MM-dd");

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
        
        {/* Summary cards */}
        <SummaryCards 
          timeframe={timeframe}
          date={date}
          minerModel={selectedMinerModel}
          farmId={farmIdToUse}
        />
        
        {/* Tabs for different analyses */}
        <Tabs defaultValue="charts" className="mt-10">
          <TabsList className="grid grid-cols-3 mb-8">
            <TabsTrigger value="charts">Charts & Visualizations</TabsTrigger>
            <TabsTrigger value="curtailment">Curtailment Analytics</TabsTrigger>
            <TabsTrigger value="data">Data Tables</TabsTrigger>
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
            {/* Lead Party Curtailment Chart (Shows overall curtailment by lead party) */}
            <LeadPartyCurtailmentChart
              timeframe={timeframe === "daily" ? "daily" : timeframe === "monthly" ? "monthly" : "yearly"}
              date={date}
            />
            
            {/* Individual BMU Curtailment Charts (For reference) */}
            <CurtailmentPercentageChart
              timeframe={timeframe === "daily" ? "daily" : timeframe === "monthly" ? "monthly" : "yearly"}
              date={date}
              leadParty={selectedLeadParty || "Viking Energy Wind Farm LLP"}
              farmId={selectedFarm !== "all" ? selectedFarm : undefined}
            />
          </TabsContent>
          
          <TabsContent value="data">
            {/* Bitcoin Potential Table */}
            <BitcoinPotentialTable
              timeframe={timeframe}
              date={date}
              minerModel={selectedMinerModel}
              farmId={farmIdToUse}
            />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}