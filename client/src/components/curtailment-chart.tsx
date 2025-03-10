import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from "@/components/ui/card"
import { 
  ChartContainer, 
  ChartTooltip 
} from "@/components/ui/chart"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  XAxis,
  YAxis,
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
  isFuture?: boolean
}

export default function CurtailmentChart({ timeframe, date, minerModel, farmId }: CurtailmentChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd")
  
  const { data: hourlyData, isLoading } = useQuery<HourlyData[]>({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, farmId !== 'all' ? farmId : null],
    queryFn: async () => {
      const url = new URL(`/api/curtailment/hourly/${formattedDate}`, window.location.origin)
      if (farmId !== 'all') {
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

  // Function to check if an hour is in the future
  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(":").map(Number)
    const now = new Date()
    const selectedDate = new Date(date)

    if (format(now, "yyyy-MM-dd") === format(selectedDate, "yyyy-MM-dd")) {
      return hour > now.getHours()
    }
    return selectedDate > now
  }

  // Prepare chart data - we removed the future hour marking to fix TS errors
  const chartData = hourlyData || []

  return (
    <Card>
      <CardHeader>
        <CardTitle>Hourly Curtailment</CardTitle>
        <CardDescription>
          {timeframe === "daily" 
            ? `Wind farm curtailment by hour for ${format(date, "MMMM d, yyyy")}`
            : timeframe === "monthly"
              ? `Wind farm curtailment for ${format(date, "MMMM yyyy")}`
              : `Wind farm curtailment for ${format(date, "yyyy")}`
          }
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex justify-center items-center h-80">
            <div className="animate-pulse">Loading curtailment data...</div>
          </div>
        ) : !chartData || chartData.length === 0 ? (
          <div className="flex justify-center items-center h-80 text-muted-foreground">
            No hourly data available for the selected date
          </div>
        ) : (
          <div className="h-80">
            <ChartContainer
              config={{
                curtailedEnergy: {
                  label: "Curtailed Energy (MWh)",
                  color: "hsl(185, 70%, 50%)"
                }
              }}
            >
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={chartData}
                  margin={{ top: 10, right: 30, left: 0, bottom: 30 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="hour"
                    interval={0}
                    angle={-45}
                    tickMargin={10}
                    height={70}
                  />
                  <YAxis />
                  <ChartTooltip />
                  <Legend />
                  <Bar
                    dataKey="curtailedEnergy"
                    fill="hsl(185, 70%, 50%)"
                    fillOpacity={0.8}
                    stroke="hsl(185, 70%, 35%)"
                    strokeWidth={1}
                    name="Curtailed Energy (MWh)"
                  />
                </BarChart>
              </ResponsiveContainer>
            </ChartContainer>
          </div>
        )}
      </CardContent>
    </Card>
  )
}