import { useState } from "react"
import { format } from "date-fns"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { DatePicker } from "@/components/date-picker"
import SummaryCards from "@/components/summary-cards"
import CurtailmentChart from "@/components/curtailment-chart"
import FarmComparisonChart from "@/components/farm-comparison-chart"
import MinerModelSelector from "@/components/miner-model-selector"
import BitcoinPotentialTable from "@/components/bitcoin-potential-table"
import TimeframeSelector from "@/components/timeframe-selector"
import { useQuery } from "@tanstack/react-query"

export default function DashboardOverview() {
  const [selectedTimeframe, setSelectedTimeframe] = useState("daily")
  const [selectedDate, setSelectedDate] = useState(new Date())
  const [selectedMiner, setSelectedMiner] = useState("S19J_PRO")
  const [selectedFarm, setSelectedFarm] = useState("all")

  const formattedDate = format(selectedDate, "yyyy-MM-dd")
  
  // Fetch available lead parties (farms) for the selected date
  const { data: curtailedLeadParties = [] } = useQuery<string[]>({
    queryKey: [`/api/lead-parties/${formattedDate}`],
    enabled: !!formattedDate,
  })

  return (
    <div className="container mx-auto py-6 space-y-8">
      <div className="flex flex-col space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Wind Curtailment to Bitcoin Mining</h1>
        <p className="text-muted-foreground">
          Analyze the potential of using curtailed wind farm energy for Bitcoin mining
        </p>
      </div>

      <div className="flex flex-col md:flex-row gap-4 items-start md:items-center justify-between">
        <TimeframeSelector value={selectedTimeframe} onValueChange={setSelectedTimeframe} />

        <div className="flex flex-col sm:flex-row gap-2">
          <DatePicker date={selectedDate} onDateChange={(date) => date && setSelectedDate(date)} />

          <Select value={selectedFarm} onValueChange={setSelectedFarm}>
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Select Farm" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Farms</SelectItem>
              {curtailedLeadParties.map((party) => (
                <SelectItem key={party} value={party}>
                  {party}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <MinerModelSelector value={selectedMiner} onValueChange={setSelectedMiner} />
        </div>
      </div>

      <SummaryCards
        timeframe={selectedTimeframe}
        date={selectedDate}
        minerModel={selectedMiner}
        farmId={selectedFarm}
      />

      <Tabs defaultValue="overview" className="w-full">
        <TabsList className="grid w-full md:w-auto grid-cols-3">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="farms">Farm Comparison</TabsTrigger>
          <TabsTrigger value="details">Detailed Data</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="space-y-6">
          <CurtailmentChart
            timeframe={selectedTimeframe}
            date={selectedDate}
            minerModel={selectedMiner}
            farmId={selectedFarm}
          />

          <Card>
            <CardHeader>
              <CardTitle>Curtailment vs Mining Revenue</CardTitle>
              <CardDescription>
                Comparison between curtailment payments and potential Bitcoin mining revenue
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-[350px]">
                <div className="flex items-center justify-center h-full text-muted-foreground">
                  Revenue comparison data visualization will appear here
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="farms" className="space-y-6">
          <FarmComparisonChart 
            timeframe={selectedTimeframe} 
            date={selectedDate} 
            minerModel={selectedMiner} 
          />
        </TabsContent>

        <TabsContent value="details" className="space-y-6">
          <BitcoinPotentialTable
            timeframe={selectedTimeframe}
            date={selectedDate}
            minerModel={selectedMiner}
            farmId={selectedFarm}
          />
        </TabsContent>
      </Tabs>
    </div>
  )
}