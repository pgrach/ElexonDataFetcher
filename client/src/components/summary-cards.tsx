import { 
  Zap, 
  PoundSterling, 
  Bitcoin, 
  TrendingUp,
  Loader2 
} from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card"
import { formatNumber, formatCurrency, formatBitcoin } from "../lib/utils"

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
  const getModelDisplayName = (model: string) => {
    switch (model) {
      case "S19J_PRO": return "Antminer S19J Pro"
      case "S9": return "Antminer S9"
      case "M20S": return "Whatsminer M20S"
      default: return model
    }
  }

  return (
    <div className="grid gap-4 grid-cols-1 sm:grid-cols-2 lg:grid-cols-4">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Energy Curtailed
          </CardTitle>
          <Zap className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <>
              <div className="text-2xl font-bold">{formatNumber(energyCurtailed)} MWh</div>
              <p className="text-xs text-muted-foreground">
                Using {getModelDisplayName(minerModel)} miners
              </p>
            </>
          )}
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Subsidies Paid
          </CardTitle>
          <PoundSterling className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <>
              <div className="text-2xl font-bold text-red-500">
                {formatCurrency(subsidiesPaid)}
              </div>
              <p className="text-xs text-muted-foreground">
                Paid to wind farms for curtailment
              </p>
            </>
          )}
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Bitcoin Potential
          </CardTitle>
          <Bitcoin className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <>
              <div className="text-2xl font-bold">
                {formatBitcoin(bitcoinPotential)} BTC
              </div>
              <p className="text-xs text-muted-foreground">
                Potential mining with curtailed energy
              </p>
            </>
          )}
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Bitcoin Value
          </CardTitle>
          <TrendingUp className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <>
              <div className="text-2xl font-bold">
                {formatCurrency(bitcoinValue)}
              </div>
              <p className="text-xs text-muted-foreground">
                Potential value at current price
              </p>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}