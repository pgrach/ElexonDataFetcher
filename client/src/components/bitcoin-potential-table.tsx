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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

interface BitcoinPotentialTableProps {
  timeframe: string
  date: Date
  minerModel: string
  farmId: string
}

interface BitcoinPotentialData {
  farm: string
  curtailedEnergy: number
  bitcoinPotential: number
  potentialValue: number
  curtailmentPayment: number
}

export default function BitcoinPotentialTable({ timeframe, date, minerModel, farmId }: BitcoinPotentialTableProps) {
  const formattedDate = format(date, "yyyy-MM-dd")
  const yearMonth = format(date, "yyyy-MM")
  const year = format(date, "yyyy")
  
  const endpoint = timeframe === "daily" 
    ? `/api/mining-potential/daily?date=${formattedDate}&minerModel=${minerModel}`
    : timeframe === "monthly"
      ? `/api/mining-potential/monthly/${yearMonth}?minerModel=${minerModel}`
      : `/api/mining-potential/yearly/${year}?minerModel=${minerModel}`
      
  const { 
    data: tableData, 
    isLoading, 
    error 
  } = useQuery<BitcoinPotentialData[]>({
    queryKey: [endpoint, farmId !== 'all' ? farmId : null],
    queryFn: async () => {
      const url = new URL(endpoint, window.location.origin)
      if (farmId !== 'all') {
        url.searchParams.set("leadParty", farmId)
      }
      
      const response = await fetch(url)
      if (!response.ok) {
        throw new Error("Failed to fetch mining potential data")
      }
      
      return response.json()
    },
    // Disable fetching for now as the endpoint is not available
    enabled: false
  })

  // Mock data for demonstration
  const mockData = [
    { farm: "Moray West Wind Farm Ltd", curtailedEnergy: 7850, bitcoinPotential: 6.23, potentialValue: 398000, curtailmentPayment: 220000 },
    { farm: "Dogger Bank Wind Farm Ltd", curtailedEnergy: 2740, bitcoinPotential: 2.17, potentialValue: 139000, curtailmentPayment: 76000 },
    { farm: "Dunvegan Wind Farm Ltd", curtailedEnergy: 3900, bitcoinPotential: 3.10, potentialValue: 198500, curtailmentPayment: 110000 },
    { farm: "Moray East Wind Farm Ltd", curtailedEnergy: 2100, bitcoinPotential: 1.67, potentialValue: 106700, curtailmentPayment: 59000 },
    { farm: "Creag Riabhach Wind Farm Ltd", curtailedEnergy: 1900, bitcoinPotential: 1.51, potentialValue: 96500, curtailmentPayment: 53000 },
    { farm: "Kilgallioch Wind Farm Ltd", curtailedEnergy: 1500, bitcoinPotential: 1.19, potentialValue: 76200, curtailmentPayment: 42000 },
  ]

  // Apply filter for selected farm
  const filteredData = farmId === 'all' 
    ? mockData 
    : mockData.filter(item => item.farm === farmId)

  return (
    <Card>
      <CardHeader>
        <CardTitle>Bitcoin Mining Potential</CardTitle>
        <CardDescription>
          {timeframe === "daily" 
            ? `Potential Bitcoin mining from curtailed energy on ${format(date, "MMMM d, yyyy")} with ${minerModel.replace("_", " ")} miners`
            : timeframe === "monthly"
              ? `Potential Bitcoin mining from curtailed energy in ${format(date, "MMMM yyyy")} with ${minerModel.replace("_", " ")} miners`
              : `Potential Bitcoin mining from curtailed energy in ${format(date, "yyyy")} with ${minerModel.replace("_", " ")} miners`
          }
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex justify-center items-center h-80">
            <div className="animate-pulse">Loading mining potential data...</div>
          </div>
        ) : error ? (
          <div className="flex justify-center items-center text-red-500">
            Error loading mining potential data
          </div>
        ) : (
          <div className="overflow-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Wind Farm</TableHead>
                  <TableHead className="text-right">Curtailed Energy (MWh)</TableHead>
                  <TableHead className="text-right">Bitcoin Potential (BTC)</TableHead>
                  <TableHead className="text-right">BTC Value (GBP)</TableHead>
                  <TableHead className="text-right">Curtailment Payment (GBP)</TableHead>
                  <TableHead className="text-right">Difference (GBP)</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredData.map((item, index) => (
                  <TableRow key={index}>
                    <TableCell className="font-medium">{item.farm}</TableCell>
                    <TableCell className="text-right">{item.curtailedEnergy.toLocaleString()}</TableCell>
                    <TableCell className="text-right">{item.bitcoinPotential.toFixed(8)}</TableCell>
                    <TableCell className="text-right">£{item.potentialValue.toLocaleString()}</TableCell>
                    <TableCell className="text-right">£{item.curtailmentPayment.toLocaleString()}</TableCell>
                    <TableCell className="text-right">
                      {item.potentialValue > item.curtailmentPayment ? (
                        <span className="text-green-600">+£{(item.potentialValue - item.curtailmentPayment).toLocaleString()}</span>
                      ) : (
                        <span className="text-red-600">-£{(item.curtailmentPayment - item.potentialValue).toLocaleString()}</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  )
}