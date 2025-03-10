import { useState, useEffect } from "react"
import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { 
  ResponsiveContainer,
  BarChart,
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  Legend 
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
}

export default function FarmComparisonChart({ timeframe, date, minerModel }: FarmComparisonChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd")
  const yearMonth = format(date, "yyyy-MM")
  const year = format(date, "yyyy")
  
  const [chartData, setChartData] = useState<any[]>([])
  
  // Get lead parties (farms) that had curtailment
  const { data: leadParties = [] } = useQuery<string[]>({
    queryKey: [`/api/lead-parties/${timeframe === "daily" ? formattedDate : timeframe === "monthly" ? yearMonth : year}`],
    queryFn: async () => {
      const url = new URL(
        `/api/lead-parties/${timeframe === "daily" ? formattedDate : timeframe === "monthly" ? yearMonth : year}`,
        window.location.origin
      )
      
      const response = await fetch(url)
      if (!response.ok) {
        return []
      }
      
      return response.json()
    },
    enabled: true
  })
  
  // This is a simplified implementation. In a real application, you would fetch farm-specific data
  // for each leadParty and combine it. Here we're simulating the data.
  useEffect(() => {
    if (leadParties.length > 0) {
      const processData = async () => {
        // In a real implementation, you would fetch actual data for each farm
        // This is a placeholder that would be replaced with real API calls
        const data = await Promise.all(
          leadParties.slice(0, 10).map(async (farm) => {
            try {
              // Example API call to get farm data - this should be implemented based on your API
              const url = new URL(
                `/api/farm/${farm}`, 
                window.location.origin
              )
              url.searchParams.set("timeframe", timeframe)
              url.searchParams.set("date", timeframe === "daily" ? formattedDate : timeframe === "monthly" ? yearMonth : year)
              url.searchParams.set("minerModel", minerModel)
              
              // This is where you would make the actual API call 
              // For now, we'll just use random values for the demo
              return {
                name: farm,
                curtailedEnergy: Math.random() * 1000, // Replace with actual data
                bitcoinPotential: Math.random() * 5 // Replace with actual data
              }
            } catch (error) {
              console.error(`Error fetching data for farm ${farm}:`, error)
              return {
                name: farm,
                curtailedEnergy: 0,
                bitcoinPotential: 0
              }
            }
          })
        )
        
        setChartData(data)
      }
      
      processData()
    } else {
      setChartData([])
    }
  }, [leadParties, timeframe, formattedDate, yearMonth, year, minerModel])
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>Farm Comparison</CardTitle>
        <CardDescription>
          Curtailment and Bitcoin mining potential by farm
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          {leadParties.length === 0 ? (
            <div className="flex items-center justify-center h-full">
              No farm data available for the selected period
            </div>
          ) : chartData.length === 0 ? (
            <div className="flex items-center justify-center h-full">
              Loading farm comparison data...
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={chartData}
                margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
                layout="vertical"
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" />
                <YAxis 
                  dataKey="name" 
                  type="category" 
                  width={120}
                  tick={{ fontSize: 12 }}
                />
                <Tooltip />
                <Legend />
                <Bar dataKey="curtailedEnergy" name="Curtailed Energy (MWh)" fill="#0ea5e9" />
                <Bar dataKey="bitcoinPotential" name="Bitcoin Potential (BTC)" fill="#f59e0b" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </CardContent>
    </Card>
  )
}