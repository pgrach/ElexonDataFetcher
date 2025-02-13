"use client"

import { useState } from "react"
import { format } from "date-fns"
import { FilterBar } from "@/components/ui/filter-bar"
import SummaryReports from "@/components/SummaryReports"
import DataVisualization from "@/components/DataVisualization"

export default function Home() {
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2022-01-01");
    return today < startDate ? startDate : today;
  });

  const [selectedLeadParty, setSelectedLeadParty] = useState<string | null>(null);
  const [selectedMinerModel, setSelectedMinerModel] = useState("S19J_PRO");

  return (
    <div className="min-h-screen">
      <FilterBar
        date={date}
        onDateChange={(newDate) => newDate && setDate(newDate)}
        selectedLeadParty={selectedLeadParty}
        onLeadPartyChange={(value) => setSelectedLeadParty(value || null)}
        curtailedLeadParties={[]} 
        selectedMinerModel={selectedMinerModel}
        onMinerModelChange={setSelectedMinerModel}
      />

      <div className="container mx-auto py-8">
        <h1 className="text-4xl font-bold mb-8">CurtailCoin</h1>

        <div className="space-y-6">
          <SummaryReports
            date={date}
            selectedLeadParty={selectedLeadParty}
            selectedMinerModel={selectedMinerModel}
          />

          <DataVisualization
            date={date}
            selectedLeadParty={selectedLeadParty}
            selectedMinerModel={selectedMinerModel}
          />
        </div>
      </div>
    </div>
  );
}