import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Bitcoin, Battery, Wind } from "lucide-react"

interface SummaryCardsProps {
  timeframe: string
  date: Date
  minerModel: string
  farmId: string
}

interface BitcoinCalculation {
  bitcoinMined: number
  valueAtCurrentPrice: number
  difficulty: number | null
  price: number
  currentPrice: number
}

interface YearlyBitcoinCalculation {
  bitcoinMined: number
  valueAtCurrentPrice: number
  curtailedEnergy: number
  totalPayment: number
  difficulty: number
  currentPrice: number
  year: string
}

interface SummaryData {
  totalCurtailedEnergy: number
  totalPayment: number
}

export default function SummaryCards({ timeframe, date, minerModel, farmId }: SummaryCardsProps) {
  const formattedDate = format(date, "yyyy-MM-dd")
  const yearMonth = format(date, "yyyy-MM")
  const year = format(date, "yyyy")
  
  // Generate API endpoints based on timeframe
  const summaryEndpoint = timeframe === "daily" 
    ? `/api/summary/daily/${formattedDate}`
    : timeframe === "monthly"
      ? `/api/summary/monthly/${yearMonth}`
      : `/api/summary/yearly/${year}`
      
  const bitcoinEndpoint = timeframe === "daily"
    ? "/api/curtailment/mining-potential"
    : timeframe === "monthly"
      ? `/api/curtailment/monthly-mining-potential/${yearMonth}`
      : `/api/mining-potential/yearly/${year}`

  // Fetch summary data (curtailment energy and payment)
  const { 
    data: summaryData,
    isLoading: isSummaryLoading,
    error: summaryError
  } = useQuery<SummaryData>({
    queryKey: [summaryEndpoint, farmId !== 'all' ? farmId : null],
    queryFn: async () => {
      const url = new URL(summaryEndpoint, window.location.origin)
      if (farmId !== 'all') {
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
    }
  })

  // Fetch Bitcoin calculation data
  const {
    data: bitcoinData,
    isLoading: isBitcoinLoading,
    error: bitcoinError
  } = useQuery<BitcoinCalculation | YearlyBitcoinCalculation>({
    queryKey: [bitcoinEndpoint, minerModel, summaryData?.totalCurtailedEnergy, farmId !== 'all' ? farmId : null],
    queryFn: async () => {
      const url = new URL(bitcoinEndpoint, window.location.origin)
      
      // Add appropriate parameters based on timeframe
      if (timeframe === "daily") {
        url.searchParams.set("date", formattedDate)
        url.searchParams.set("minerModel", minerModel)
        if (summaryData?.totalCurtailedEnergy) {
          url.searchParams.set("energy", summaryData.totalCurtailedEnergy.toString())
        }
      } else {
        url.searchParams.set("minerModel", minerModel)
      }
      
      if (farmId !== 'all') {
        url.searchParams.set("leadParty", farmId)
      }
      
      const response = await fetch(url)
      if (!response.ok) {
        throw new Error("Failed to fetch mining potential")
      }
      
      return response.json()
    },
    enabled: !!summaryData
  })

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            {farmId !== 'all' ? "Farm Curtailed Energy" : "Total Curtailed Energy"}
          </CardTitle>
          <Wind className="h-5 w-5 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <div>
              {isSummaryLoading ? (
                <div className="text-2xl font-bold animate-pulse">Loading...</div>
              ) : summaryError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : summaryData ? (
                <div className="text-2xl font-bold">
                  {Number(summaryData.totalCurtailedEnergy).toLocaleString()} MWh
                </div>
              ) : (
                <div className="text-sm text-muted-foreground">No data available</div>
              )}
              <p className="text-xs text-muted-foreground mt-1">
                {farmId !== 'all' ? (
                  <>Energy curtailed for {farmId}</>
                ) : (
                  <>Total energy curtailed</>
                )}{' '}
                {timeframe === "daily"
                  ? `on ${format(date, "PP")}`
                  : timeframe === "monthly"
                    ? `in ${format(date, "MMMM yyyy")}`
                    : `in ${year}`
                }
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Bitcoin Mining Potential</CardTitle>
          <Bitcoin className="h-5 w-5 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <div>
              {isBitcoinLoading ? (
                <div className="text-2xl font-bold animate-pulse">Loading...</div>
              ) : bitcoinError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : bitcoinData ? (
                <div className="text-2xl font-bold text-[#F7931A]">
                  ₿{bitcoinData.bitcoinMined.toFixed(8)}
                </div>
              ) : (
                <div className="text-sm text-muted-foreground">No data available</div>
              )}
              <p className="text-xs text-muted-foreground mt-1">
                Bitcoin that could be mined with {minerModel.replace("_", " ")} miners
              </p>
            </div>

            <div>
              <div className="text-sm font-medium">Value (GBP)</div>
              {isBitcoinLoading ? (
                <div className="text-xl font-bold animate-pulse">Loading...</div>
              ) : bitcoinError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : bitcoinData ? (
                <div className="text-xl font-bold">
                  £{bitcoinData.valueAtCurrentPrice.toLocaleString('en-GB', { maximumFractionDigits: 2 })}
                </div>
              ) : (
                <div className="text-sm text-muted-foreground">No data available</div>
              )}
              <p className="text-xs text-muted-foreground mt-1">
                At current Bitcoin price
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Curtailment Payment</CardTitle>
          <Battery className="h-5 w-5 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <div>
              {isSummaryLoading ? (
                <div className="text-2xl font-bold animate-pulse">Loading...</div>
              ) : summaryError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : summaryData ? (
                <div className="text-2xl font-bold">
                  £{Number(summaryData.totalPayment).toLocaleString()}
                </div>
              ) : (
                <div className="text-sm text-muted-foreground">No data available</div>
              )}
              <p className="text-xs text-muted-foreground mt-1">
                Payment for curtailment
                {timeframe === "daily"
                  ? ` on ${format(date, "PP")}`
                  : timeframe === "monthly"
                    ? ` in ${format(date, "MMMM yyyy")}`
                    : ` in ${year}`
                }
              </p>
            </div>

            <div>
              <div className="text-sm font-medium">Value Comparison</div>
              {isBitcoinLoading || isSummaryLoading ? (
                <div className="text-xl font-bold animate-pulse">Loading...</div>
              ) : bitcoinError || summaryError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : (bitcoinData && summaryData) ? (
                <div className="text-xl font-bold">
                  {bitcoinData.valueAtCurrentPrice > Math.abs(summaryData.totalPayment) ? (
                    <span className="text-green-600">+£{(bitcoinData.valueAtCurrentPrice - Math.abs(summaryData.totalPayment)).toLocaleString('en-GB', { maximumFractionDigits: 2 })}</span>
                  ) : (
                    <span className="text-red-600">-£{(Math.abs(summaryData.totalPayment) - bitcoinData.valueAtCurrentPrice).toLocaleString('en-GB', { maximumFractionDigits: 2 })}</span>
                  )}
                </div>
              ) : (
                <div className="text-sm text-muted-foreground">No data available</div>
              )}
              <p className="text-xs text-muted-foreground mt-1">
                Mining value vs. curtailment payment
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}