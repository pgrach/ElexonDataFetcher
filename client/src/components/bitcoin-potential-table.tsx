import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
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
  
  // Generate API endpoint based on timeframe
  const apiEndpoint = timeframe === "daily" 
    ? `/api/farm-data/daily/${formattedDate}`
    : timeframe === "monthly"
      ? `/api/farm-data/monthly/${yearMonth}`
      : `/api/farm-data/yearly/${year}`
  
  const { data, isLoading, error } = useQuery<BitcoinPotentialData[]>({
    queryKey: [apiEndpoint, minerModel],
    queryFn: async () => {
      const url = new URL(apiEndpoint, window.location.origin)
      url.searchParams.set("minerModel", minerModel)
      
      // This is a placeholder for the actual API endpoint
      // In a real implementation, you would call your actual API endpoint
      
      // For now, we'll return a placeholder array of data for demonstration
      // This should be replaced with actual data from a real API call
      
      // Simulate an API call delay
      await new Promise(resolve => setTimeout(resolve, 500))
      
      // This would be the actual API call
      // const response = await fetch(url.toString())
      // if (!response.ok) {
      //   throw new Error("Failed to fetch data")
      // }
      // return response.json()
      
      // For now, let's return an empty array
      return []
    },
    enabled: false // Disabled until a real API endpoint is available
  })
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>Detailed Bitcoin Potential</CardTitle>
        <CardDescription>
          {timeframe === "daily" 
            ? `Mining potential breakdown for ${format(date, "PPP")}`
            : timeframe === "monthly"
              ? `Mining potential summary for ${format(date, "MMMM yyyy")}`
              : `Mining potential summary for ${format(date, "yyyy")}`
          }
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex justify-center py-4">Loading detailed data...</div>
        ) : error ? (
          <div className="text-red-500 py-4">Error loading data</div>
        ) : data && data.length > 0 ? (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Farm</TableHead>
                <TableHead className="text-right">Energy (MWh)</TableHead>
                <TableHead className="text-right">Bitcoin (BTC)</TableHead>
                <TableHead className="text-right">BTC Value (£)</TableHead>
                <TableHead className="text-right">Payments (£)</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((item, index) => (
                <TableRow key={index}>
                  <TableCell className="font-medium">{item.farm}</TableCell>
                  <TableCell className="text-right">{item.curtailedEnergy.toLocaleString()}</TableCell>
                  <TableCell className="text-right">{item.bitcoinPotential.toFixed(8)}</TableCell>
                  <TableCell className="text-right">{item.potentialValue.toLocaleString('en-GB', { maximumFractionDigits: 2 })}</TableCell>
                  <TableCell className="text-right">{item.curtailmentPayment.toLocaleString('en-GB', { maximumFractionDigits: 2 })}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <div className="text-center py-8 text-muted-foreground">
            No detailed data available for the selected period.
            <br />
            Please select a different date or timeframe.
          </div>
        )}
      </CardContent>
    </Card>
  )
}