import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from "@/components/ui/card"
import { ChartContainer } from "@/components/ui/chart"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

interface FarmComparisonChartProps {
  timeframe: string
  date: Date
  minerModel: string
}

interface FarmData {
  farmId: string
  curtailedEnergy: number
  bitcoinPotential: number
  potentialValue: number
  curtailmentPayment: number
}

export default function FarmComparisonChart({ timeframe, date, minerModel }: FarmComparisonChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd")
  const yearMonth = format(date, "yyyy-MM")
  const year = format(date, "yyyy")
  
  // Endpoint for farm comparison data
  const endpoint = timeframe === "daily" 
    ? `/api/farm-comparison/daily/${formattedDate}`
    : timeframe === "monthly"
      ? `/api/farm-comparison/monthly/${yearMonth}`
      : `/api/farm-comparison/yearly/${year}`
  
  // Fetch comparison data for farms
  const { data, isLoading, error } = useQuery<FarmData[]>({
    queryKey: [endpoint, minerModel],
    queryFn: async () => {
      // This is a placeholder - in a real implementation, this endpoint would return comparison data
      // For now, we'll simulate this data with the daily curtailment data by lead party
      const leadPartiesUrl = new URL(`/api/lead-parties/${formattedDate}`, window.location.origin)
      const leadPartiesResponse = await fetch(leadPartiesUrl)
      if (!leadPartiesResponse.ok) {
        throw new Error("Failed to fetch lead parties")
      }
      
      const leadParties = await leadPartiesResponse.json()
      
      // Get data for top 5 farms by curtailed energy
      // This is simplified - in a real implementation, we'd call an endpoint that provides this data directly
      const comparisonData = await Promise.all(
        leadParties.slice(0, 5).map(async (leadParty: string) => {
          // Get curtailment data for this farm
          const summaryUrl = new URL(`/api/summary/daily/${formattedDate}`, window.location.origin)
          summaryUrl.searchParams.set("leadParty", leadParty)
          const summaryResponse = await fetch(summaryUrl)
          
          if (!summaryResponse.ok) {
            return null
          }
          
          const summary = await summaryResponse.json()
          
          // Get Bitcoin potential for this farm
          const bitcoinUrl = new URL("/api/curtailment/mining-potential", window.location.origin)
          bitcoinUrl.searchParams.set("date", formattedDate)
          bitcoinUrl.searchParams.set("minerModel", minerModel)
          bitcoinUrl.searchParams.set("energy", summary.totalCurtailedEnergy.toString())
          bitcoinUrl.searchParams.set("leadParty", leadParty)
          
          const bitcoinResponse = await fetch(bitcoinUrl)
          if (!bitcoinResponse.ok) {
            return null
          }
          
          const bitcoinData = await bitcoinResponse.json()
          
          return {
            farmId: leadParty,
            curtailedEnergy: summary.totalCurtailedEnergy,
            bitcoinPotential: bitcoinData.bitcoinMined,
            potentialValue: bitcoinData.valueAtCurrentPrice,
            curtailmentPayment: summary.totalPayment
          }
        })
      )
      
      // Filter out nulls and sort by curtailed energy
      return comparisonData
        .filter(data => data !== null)
        .sort((a, b) => b.curtailedEnergy - a.curtailedEnergy)
    },
    // Disable fetching actual farm comparison data for now
    enabled: false
  })
  
  // Create mock data for demonstration
  const mockData = [
    { farmId: "Moray West", curtailedEnergy: 7850, bitcoinPotential: 6.23, potentialValue: 398000, curtailmentPayment: -220000 },
    { farmId: "Dunvegan", curtailedEnergy: 3900, bitcoinPotential: 3.10, potentialValue: 198500, curtailmentPayment: -110000 },
    { farmId: "Dogger Bank", curtailedEnergy: 2740, bitcoinPotential: 2.17, potentialValue: 139000, curtailmentPayment: -76000 },
    { farmId: "Moray East", curtailedEnergy: 2100, bitcoinPotential: 1.67, potentialValue: 106700, curtailmentPayment: -59000 },
    { farmId: "Creag Riabhach", curtailedEnergy: 1900, bitcoinPotential: 1.51, potentialValue: 96500, curtailmentPayment: -53000 },
  ]

  const chartData = data || mockData
  
  // Format the farm names for display
  const formatFarmName = (name: string) => {
    if (name.length > 15) {
      return name.substring(0, 12) + '...'
    }
    return name
  }

  // Prepare data for chart
  const preparedData = chartData.map(farm => ({
    ...farm,
    farmName: formatFarmName(farm.farmId),
    btcValue: Math.abs(farm.potentialValue),
    payment: Math.abs(farm.curtailmentPayment),
  }))

  return (
    <Card>
      <CardHeader>
        <CardTitle>Farm Comparison</CardTitle>
        <CardDescription>
          {timeframe === "daily" 
            ? `Top farms by curtailed energy on ${format(date, "MMMM d, yyyy")}`
            : timeframe === "monthly"
              ? `Top farms by curtailed energy in ${format(date, "MMMM yyyy")}`
              : `Top farms by curtailed energy in ${format(date, "yyyy")}`
          }
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex justify-center items-center h-80">
            <div className="animate-pulse">Loading farm comparison data...</div>
          </div>
        ) : error ? (
          <div className="flex justify-center items-center h-80 text-red-500">
            Error loading farm comparison data
          </div>
        ) : (
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={preparedData}
                margin={{ top: 20, right: 30, left: 20, bottom: 70 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis 
                  dataKey="farmName" 
                  angle={-45} 
                  textAnchor="end" 
                  height={70} 
                  interval={0}
                />
                <YAxis yAxisId="left" orientation="left" stroke="#8884d8" />
                <YAxis yAxisId="right" orientation="right" stroke="#82ca9d" />
                <Tooltip formatter={(value: any, name: string) => {
                  if (name === "Energy (MWh)") return [value.toLocaleString(), name]
                  if (name === "Bitcoin (BTC)") return [value.toFixed(4), name]
                  return [value.toLocaleString(), name]
                }} />
                <Legend />
                <Bar 
                  yAxisId="left" 
                  dataKey="curtailedEnergy" 
                  name="Energy (MWh)" 
                  fill="#8884d8" 
                />
                <Bar 
                  yAxisId="right" 
                  dataKey="bitcoinPotential" 
                  name="Bitcoin (BTC)" 
                  fill="#82ca9d" 
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  )
}