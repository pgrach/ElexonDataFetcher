import { useState } from "react"
import { DatePicker } from "@/components/date-picker"
import TimeframeSelector from "@/components/timeframe-selector"
import MinerModelSelector from "@/components/miner-model-selector"
import SummaryCards from "@/components/summary-cards"
import CurtailmentChart from "@/components/curtailment-chart"
import FarmComparisonChart from "@/components/farm-comparison-chart"
import BitcoinPotentialTable from "@/components/bitcoin-potential-table"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

export default function DashboardOverview() {
  const [date, setDate] = useState<Date>(new Date())
  const [timeframe, setTimeframe] = useState<string>("daily")
  const [minerModel, setMinerModel] = useState<string>("S19J_PRO")
  const [farmId, setFarmId] = useState<string>("all")
  const [tab, setTab] = useState<string>("overview")
  
  // Fetch the list of farms (lead parties)
  const [farms, setFarms] = useState<string[]>([
    "all", 
    "Moray West Wind Farm Ltd",
    "Dogger Bank Wind Farm Ltd",
    "Dunvegan Wind Farm Ltd",
    "Creag Riabhach Wind Farm Ltd",
    "Kilgallioch Wind Farm Ltd"
  ])
  
  // Handlers for controls
  const handleDateChange = (newDate: Date | undefined) => {
    if (newDate) {
      setDate(newDate)
    }
  }
  
  const handleTimeframeChange = (value: string) => {
    setTimeframe(value)
  }
  
  const handleMinerModelChange = (value: string) => {
    setMinerModel(value)
  }
  
  const handleFarmChange = (value: string) => {
    setFarmId(value)
  }
  
  return (
    <div className="container mx-auto py-6 space-y-8">
      <div className="flex flex-col space-y-2">
        <h1 className="text-3xl font-bold">Wind Farm Analytics Dashboard</h1>
        <p className="text-muted-foreground">
          Analyze curtailment data and Bitcoin mining potential for wind farms
        </p>
      </div>

      <div className="flex flex-col md:flex-row gap-4 items-start md:items-center justify-between">
        <Select value={farmId} onValueChange={handleFarmChange}>
          <SelectTrigger className="w-[200px]">
            <SelectValue placeholder="Select Wind Farm" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Wind Farms</SelectItem>
            {farms.filter(farm => farm !== "all").map((farm) => (
              <SelectItem key={farm} value={farm}>
                {farm.length > 30 ? farm.substring(0, 30) + '...' : farm}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <div className="flex flex-col sm:flex-row gap-2">
          <DatePicker date={date} onDateChange={handleDateChange} />
          <TimeframeSelector value={timeframe} onValueChange={handleTimeframeChange} />
          <MinerModelSelector value={minerModel} onValueChange={handleMinerModelChange} />
        </div>
      </div>

      <SummaryCards 
        timeframe={timeframe} 
        date={date} 
        minerModel={minerModel} 
        farmId={farmId} 
      />

      <Tabs defaultValue="overview" value={tab} onValueChange={setTab}>
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="comparison">Farm Comparison</TabsTrigger>
          <TabsTrigger value="details">Mining Details</TabsTrigger>
        </TabsList>
        
        <TabsContent value="overview" className="space-y-6">
          <CurtailmentChart 
            timeframe={timeframe}
            date={date} 
            minerModel={minerModel}
            farmId={farmId}
          />
        </TabsContent>
        
        <TabsContent value="comparison" className="space-y-6">
          <FarmComparisonChart 
            timeframe={timeframe}
            date={date}
            minerModel={minerModel}
          />
        </TabsContent>
        
        <TabsContent value="details" className="space-y-6">
          <BitcoinPotentialTable 
            timeframe={timeframe}
            date={date}
            minerModel={minerModel}
            farmId={farmId}
          />
        </TabsContent>
      </Tabs>
    </div>
  )
}