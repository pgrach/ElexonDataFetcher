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
  Legend,
  ReferenceLine
} from "recharts"

interface CurtailmentChartProps {
  timeframe: string
  date: Date
  minerModel: string
  farmId: string
}

interface HourlyData {
  hour: string
  curtailedEnergy: number
}

export default function CurtailmentChart({ timeframe, date, minerModel, farmId }: CurtailmentChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd")
  
  const { data: hourlyData, isLoading } = useQuery<HourlyData[]>({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, farmId !== "all" ? farmId : null],
    queryFn: async () => {
      const url = new URL(`/api/curtailment/hourly/${formattedDate}`, window.location.origin)
      if (farmId && farmId !== "all") {
        url.searchParams.set("leadParty", farmId)
      }
      
      const response = await fetch(url)
      if (!response.ok) {
        throw new Error("Failed to fetch hourly data")
      }
      
      return response.json()
    },
    enabled: timeframe === "daily"
  })

  // For future: implement monthly/yearly chart data fetching based on timeframe
  
  const chartData = hourlyData?.map(item => ({
    hour: item.hour,
    energy: item.curtailedEnergy,
  })) || []
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>Hourly Curtailment & Bitcoin Potential</CardTitle>
        <CardDescription>
          {timeframe === "daily" 
            ? `Average hourly breakdown for ${format(date, "PPP")}`
            : timeframe === "monthly"
              ? `Daily breakdown for ${format(date, "MMMM yyyy")}`
              : `Monthly breakdown for ${format(date, "yyyy")}`
          }
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          {isLoading ? (
            <div className="flex items-center justify-center h-full">Loading chart data...</div>
          ) : chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="hour" />
                <YAxis yAxisId="left" orientation="left" label={{ value: 'Energy (MWh)', angle: -90, position: 'insideLeft' }} />
                <Tooltip />
                <Legend />
                <Bar yAxisId="left" dataKey="energy" name="Curtailed Energy (MWh)" fill="#0ea5e9" />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex items-center justify-center h-full">No data available for the selected date</div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}