import { Circle, Coins, PoundSterling } from "lucide-react"

interface SummaryCardsProps {
  energyCurtailed: number
  subsidiesPaid: number
  bitcoinPotential: number
  bitcoinValue: number
  minerModel: string
  isLoading?: boolean
}

export function SummaryCards({
  energyCurtailed,
  subsidiesPaid,
  bitcoinPotential,
  bitcoinValue,
  minerModel,
  isLoading = false
}: SummaryCardsProps) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
      {/* Energy Curtailed Card */}
      <div className="rounded-lg border p-4">
        <div className="flex justify-between items-start">
          <div>
            <p className="text-sm text-muted-foreground flex items-center">
              <Circle className="h-4 w-4 mr-1 stroke-primary" />
              Energy Curtailed
            </p>
            {isLoading ? (
              <div className="h-8 w-36 bg-muted animate-pulse rounded mt-2" />
            ) : (
              <h3 className="text-2xl font-semibold mt-1">
                {energyCurtailed.toLocaleString()} MWh
              </h3>
            )}
            <p className="text-xs text-muted-foreground mt-1">
              Wasted energy that could be utilized
            </p>
          </div>
        </div>
      </div>

      {/* Subsidies Paid Card */}
      <div className="rounded-lg border p-4">
        <div className="flex justify-between items-start">
          <div>
            <p className="text-sm text-muted-foreground flex items-center">
              <PoundSterling className="h-4 w-4 mr-1 text-red-500" />
              Subsidies Paid
            </p>
            {isLoading ? (
              <div className="h-8 w-36 bg-muted animate-pulse rounded mt-2" />
            ) : (
              <h3 className="text-2xl font-semibold mt-1 text-red-500">
                £{Math.abs(subsidiesPaid).toLocaleString()}
              </h3>
            )}
            <p className="text-xs text-muted-foreground mt-1">
              Consumer cost for idle wind farms
            </p>
          </div>
        </div>
      </div>

      {/* Potential Bitcoin Card */}
      <div className="rounded-lg border p-4">
        <div className="flex justify-between items-start">
          <div>
            <p className="text-sm text-muted-foreground flex items-center">
              <Coins className="h-4 w-4 mr-1 text-[#F7931A]" />
              Potential Bitcoin
            </p>
            {isLoading ? (
              <div className="h-8 w-36 bg-muted animate-pulse rounded mt-2" />
            ) : (
              <>
                <h3 className="text-2xl font-semibold mt-1 text-[#F7931A]">
                  {bitcoinPotential.toFixed(4)} BTC
                </h3>
                <p className="text-xs text-muted-foreground mt-1">
                  = £{bitcoinValue.toLocaleString()}
                </p>
                <p className="text-xs text-muted-foreground mt-1">
                  Using {minerModel.replace("_", " ")} miners
                </p>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}