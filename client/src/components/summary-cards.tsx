import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Bitcoin, Wind, PoundSterling } from "lucide-react"

interface SummaryCardsProps {
  timeframe: string
  date: Date
  minerModel: string
  farmId: string
}

export default function SummaryCards({ timeframe, date, minerModel, farmId }: SummaryCardsProps) {
  const formattedDate = format(date, "yyyy-MM-dd")
  const yearMonth = format(date, "yyyy-MM")
  const year = format(date, "yyyy")
  
  // API endpoint selection based on timeframe
  const summaryEndpoint = timeframe === "daily" 
    ? `/api/summary/daily/${formattedDate}`
    : timeframe === "monthly" 
      ? `/api/summary/monthly/${yearMonth}`
      : `/api/summary/yearly/${year}`
  
  const bitcoinEndpoint = timeframe === "daily"
    ? `/api/curtailment/mining-potential`
    : timeframe === "monthly"
      ? `/api/curtailment/monthly-mining-potential/${yearMonth}`
      : `/api/mining-potential/yearly/${year}`

  // Get summary data
  const { data: summaryData, isLoading: isSummaryLoading } = useQuery({
    queryKey: [summaryEndpoint, farmId !== "all" ? farmId : null],
    queryFn: async () => {
      const url = new URL(summaryEndpoint, window.location.origin)
      if (farmId && farmId !== "all") {
        url.searchParams.set("leadParty", farmId)
      }

      const response = await fetch(url)
      if (!response.ok) {
        if (response.status === 404) {
          return {
            totalCurtailedEnergy: 0,
            totalPayment: 0
          }
        }
        throw new Error(`API Error: ${response.status}`)
      }

      return response.json()
    },
    enabled: !!date
  })

  // Get bitcoin calculation data
  const { data: bitcoinData, isLoading: isBitcoinLoading } = useQuery({
    queryKey: [bitcoinEndpoint, minerModel, summaryData?.totalCurtailedEnergy, farmId !== "all" ? farmId : null],
    queryFn: async () => {
      if (timeframe === "daily") {
        if (!summaryData?.totalCurtailedEnergy) {
          return {
            bitcoinMined: 0,
            valueAtCurrentPrice: 0,
            difficulty: 0,
            price: 0,
            currentPrice: 0
          }
        }

        const url = new URL(bitcoinEndpoint, window.location.origin)
        url.searchParams.set("date", formattedDate)
        url.searchParams.set("minerModel", minerModel)
        url.searchParams.set("energy", summaryData.totalCurtailedEnergy.toString())
        if (farmId && farmId !== "all") {
          url.searchParams.set("leadParty", farmId)
        }

        const response = await fetch(url)
        if (!response.ok) {
          throw new Error("Failed to fetch mining potential")
        }

        return response.json()
      } else {
        // For monthly and yearly data
        const url = new URL(bitcoinEndpoint, window.location.origin)
        url.searchParams.set("minerModel", minerModel)
        if (farmId && farmId !== "all") {
          url.searchParams.set("leadParty", farmId)
        }

        const response = await fetch(url)
        if (!response.ok) {
          throw new Error(`Failed to fetch ${timeframe} mining potential`)
        }

        return response.json()
      }
    },
    enabled: timeframe === "daily" ? !!summaryData?.totalCurtailedEnergy : true
  })

  return (
    <div className="grid gap-4 md:grid-cols-3">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Energy Curtailed</CardTitle>
          <Wind className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isSummaryLoading ? (
            <div className="text-2xl font-bold animate-pulse">Loading...</div>
          ) : (
            <>
              <div className="text-2xl font-bold">
                {(summaryData?.totalCurtailedEnergy || 0).toLocaleString()} MWh
              </div>
              <p className="text-xs text-muted-foreground">Wasted energy that could be utilized</p>
            </>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Subsidies Paid</CardTitle>
          <PoundSterling className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isSummaryLoading ? (
            <div className="text-2xl font-bold animate-pulse">Loading...</div>
          ) : (
            <>
              <div className="text-2xl font-bold text-red-600">
                £{Math.abs(summaryData?.totalPayment || 0).toLocaleString()}
              </div>
              <p className="text-xs text-muted-foreground">Consumer cost for idle wind farms</p>
            </>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Potential Bitcoin</CardTitle>
          <Bitcoin className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isBitcoinLoading ? (
            <div className="text-2xl font-bold animate-pulse">Loading...</div>
          ) : (
            <>
              <div className="text-2xl font-bold">{(bitcoinData?.bitcoinMined || 0).toFixed(4)} BTC</div>
              <div className="text-sm font-medium text-muted-foreground">
                ≈ £{(bitcoinData?.valueAtCurrentPrice || 0).toLocaleString()}
              </div>
              <p className="text-xs text-muted-foreground mt-1">Using {minerModel.replace("_", " ")} miners</p>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}