"use client";

import { useState } from "react";
import { format, isValid } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { FilterBar } from "@/components/ui/filter-bar";
import TimeframeSelector from "@/components/timeframe-selector";
import CurtailmentChart from "@/components/curtailment-chart";
import FarmComparisonChart from "@/components/farm-comparison-chart";
import BitcoinPotentialTable from "@/components/bitcoin-potential-table";
import MinerModelSelector from "@/components/miner-model-selector";
import { DatePicker } from "@/components/date-picker";
import SummaryCards from "@/components/summary-cards";
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
  const [timeframe, setTimeframe] = useState("daily");

  // Derived values
  const formattedDate = format(date, "yyyy-MM-dd");

  // Fetch lead parties for the filters
  const { data: curtailedLeadParties = [] } = useQuery<string[]>({
    queryKey: [`/api/lead-parties/${formattedDate}`],
    enabled: !!formattedDate && isValid(date),
  });

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
          farmId={selectedLeadParty || ""}
        />
        
        {/* Tabs for different analyses */}
        <Tabs defaultValue="charts" className="mt-10">
          <TabsList className="grid grid-cols-2 mb-8">
            <TabsTrigger value="charts">Charts & Visualizations</TabsTrigger>
            <TabsTrigger value="data">Data Tables</TabsTrigger>
          </TabsList>
          
          <TabsContent value="charts" className="space-y-8">
            {/* Curtailment Chart */}
            <CurtailmentChart 
              timeframe={timeframe}
              date={date}
              minerModel={selectedMinerModel}
              farmId={selectedLeadParty || ""}
            />
            
            {/* Farm Comparison Chart */}
            <FarmComparisonChart
              timeframe={timeframe}
              date={date}
              minerModel={selectedMinerModel}
            />
          </TabsContent>
          
          <TabsContent value="data">
            {/* Bitcoin Potential Table */}
            <BitcoinPotentialTable
              timeframe={timeframe}
              date={date}
              minerModel={selectedMinerModel}
              farmId={selectedLeadParty || ""}
            />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}