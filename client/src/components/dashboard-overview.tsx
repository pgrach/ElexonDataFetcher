"use client"

import { useState } from "react"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs"
import { SummaryCards } from "../components/summary-cards"
import { TimeframeSelector } from "../components/timeframe-selector"
import { DatePicker } from "../components/date-picker"
import { MinerModelSelector } from "../components/miner-model-selector"
import { CurtailmentChart } from "../components/curtailment-chart"
import { BitcoinPotentialTable } from "../components/bitcoin-potential-table"
import { FarmComparisonChart } from "../components/farm-comparison-chart"

// Mock data - replace with actual API calls using react-query
const mockCurtailmentData = [
  { hour: '00:00', curtailedEnergy: 120, bitcoinMined: 0.00012 },
  { hour: '01:00', curtailedEnergy: 145, bitcoinMined: 0.00015 },
  { hour: '02:00', curtailedEnergy: 130, bitcoinMined: 0.00013 },
  { hour: '03:00', curtailedEnergy: 110, bitcoinMined: 0.00011 },
  { hour: '04:00', curtailedEnergy: 90, bitcoinMined: 0.00009 },
  { hour: '05:00', curtailedEnergy: 85, bitcoinMined: 0.00008 },
  { hour: '06:00', curtailedEnergy: 100, bitcoinMined: 0.00010 },
  { hour: '07:00', curtailedEnergy: 140, bitcoinMined: 0.00014 },
  { hour: '08:00', curtailedEnergy: 160, bitcoinMined: 0.00016 },
  { hour: '09:00', curtailedEnergy: 180, bitcoinMined: 0.00018 },
  { hour: '10:00', curtailedEnergy: 210, bitcoinMined: 0.00021 },
  { hour: '11:00', curtailedEnergy: 230, bitcoinMined: 0.00023 },
  { hour: '12:00', curtailedEnergy: 250, bitcoinMined: 0.00025 },
  { hour: '13:00', curtailedEnergy: 240, bitcoinMined: 0.00024 },
  { hour: '14:00', curtailedEnergy: 220, bitcoinMined: 0.00022 },
  { hour: '15:00', curtailedEnergy: 200, bitcoinMined: 0.00020 },
  { hour: '16:00', curtailedEnergy: 190, bitcoinMined: 0.00019 },
  { hour: '17:00', curtailedEnergy: 170, bitcoinMined: 0.00017 },
  { hour: '18:00', curtailedEnergy: 150, bitcoinMined: 0.00015 },
  { hour: '19:00', curtailedEnergy: 130, bitcoinMined: 0.00013 },
  { hour: '20:00', curtailedEnergy: 120, bitcoinMined: 0.00012 },
  { hour: '21:00', curtailedEnergy: 110, bitcoinMined: 0.00011 },
  { hour: '22:00', curtailedEnergy: 100, bitcoinMined: 0.00010 },
  { hour: '23:00', curtailedEnergy: 90, bitcoinMined: 0.00009 },
];

const mockBitcoinTable = [
  { period: '00:00-00:30', energy: 60, payment: -5000, bitcoin: 0.00006, value: 1200 },
  { period: '00:30-01:00', energy: 55, payment: -4800, bitcoin: 0.000055, value: 1100 },
  { period: '01:00-01:30', energy: 70, payment: -6000, bitcoin: 0.00007, value: 1400 },
  { period: '01:30-02:00', energy: 80, payment: -7000, bitcoin: 0.00008, value: 1600 },
  { period: '02:00-02:30', energy: 65, payment: -5500, bitcoin: 0.000065, value: 1300 },
  { period: '02:30-03:00', energy: 50, payment: -4500, bitcoin: 0.00005, value: 1000 },
];

const mockFarmData = [
  { name: 'Wind Farm A', energy: 1200, bitcoin: 0.12, payment: -120000 },
  { name: 'Wind Farm B', energy: 950, bitcoin: 0.095, payment: -95000 },
  { name: 'Wind Farm C', energy: 800, bitcoin: 0.08, payment: -80000 },
  { name: 'Wind Farm D', energy: 1500, bitcoin: 0.15, payment: -150000 },
  { name: 'Wind Farm E', energy: 600, bitcoin: 0.06, payment: -60000 },
];

export default function DashboardOverview() {
  const [timeframe, setTimeframe] = useState<"today" | "month" | "year">("today")
  const [date, setDate] = useState<Date>(new Date())
  const [selectedFarm, setSelectedFarm] = useState<string | null>(null)
  const [minerModel, setMinerModel] = useState<string>("S19J_PRO")
  const [activeTab, setActiveTab] = useState<string>("overview")

  // Calculate total values for summary cards
  const totalEnergy = 3670; // MWh
  const totalSubsidies = 350000; // £
  const bitcoinPotential = 0.367; // BTC
  const bitcoinValue = 7340000; // £

  return (
    <div className="container mx-auto p-6 space-y-8">
      <div className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Bitcoin Mining Potential</h1>
        <p className="text-muted-foreground">
          Monitor curtailed wind energy and potential Bitcoin mining revenue
        </p>
      </div>

      <div className="flex flex-wrap gap-4 items-center justify-between">
        <TimeframeSelector selected={timeframe} onChange={setTimeframe} />
        
        <div className="flex flex-wrap gap-2 items-center">
          <DatePicker date={date} onDateChange={(newDate) => newDate && setDate(newDate)} />
          <MinerModelSelector value={minerModel} onValueChange={setMinerModel} />
        </div>
      </div>

      <SummaryCards
        energyCurtailed={totalEnergy}
        subsidiesPaid={totalSubsidies}
        bitcoinPotential={bitcoinPotential}
        bitcoinValue={bitcoinValue}
        minerModel={minerModel}
      />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
        <TabsList>
          <TabsTrigger value="overview">Curtailment Overview</TabsTrigger>
          <TabsTrigger value="settlement">Settlement Periods</TabsTrigger>
          <TabsTrigger value="farms">Wind Farms</TabsTrigger>
        </TabsList>
        
        <TabsContent value="overview" className="space-y-4">
          <div className="rounded-md border">
            <div className="p-4 border-b">
              <h3 className="font-medium">Hourly Curtailment & Bitcoin Potential</h3>
              <p className="text-sm text-muted-foreground">
                Visualization of curtailed energy and potential Bitcoin mining over 24 hours
              </p>
            </div>
            <div className="p-4">
              <CurtailmentChart data={mockCurtailmentData} />
            </div>
          </div>
        </TabsContent>
        
        <TabsContent value="settlement" className="space-y-4">
          <div className="rounded-md border">
            <div className="p-4 border-b">
              <h3 className="font-medium">Settlement Period Details</h3>
              <p className="text-sm text-muted-foreground">
                Detailed breakdown of energy, payments, and Bitcoin potential by settlement period
              </p>
            </div>
            <div className="p-4">
              <BitcoinPotentialTable data={mockBitcoinTable} />
            </div>
          </div>
        </TabsContent>
        
        <TabsContent value="farms" className="space-y-4">
          <div className="rounded-md border">
            <div className="p-4 border-b">
              <h3 className="font-medium">Wind Farm Comparison</h3>
              <p className="text-sm text-muted-foreground">
                Comparison of curtailed energy and Bitcoin potential across different wind farms
              </p>
            </div>
            <div className="p-4">
              <FarmComparisonChart data={mockFarmData} />
            </div>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  )
}