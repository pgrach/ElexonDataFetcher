import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

interface BitcoinPotentialItem {
  period: string;
  energy: number;
  payment: number;
  bitcoin: number;
  value: number;
}

interface BitcoinPotentialTableProps {
  data: BitcoinPotentialItem[];
  isLoading?: boolean;
}

export function BitcoinPotentialTable({
  data,
  isLoading = false
}: BitcoinPotentialTableProps) {
  if (isLoading) {
    return (
      <div className="w-full flex items-center justify-center py-12">
        <p className="text-muted-foreground animate-pulse">Loading data...</p>
      </div>
    )
  }

  if (!data || data.length === 0) {
    return (
      <div className="w-full flex items-center justify-center py-12">
        <p className="text-muted-foreground">No data available for selected period</p>
      </div>
    )
  }

  return (
    <div className="w-full overflow-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Period</TableHead>
            <TableHead className="text-right">Energy (MWh)</TableHead>
            <TableHead className="text-right">Payment (£)</TableHead>
            <TableHead className="text-right">Bitcoin (BTC)</TableHead>
            <TableHead className="text-right">Value (£)</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((item) => (
            <TableRow key={item.period}>
              <TableCell className="font-medium">{item.period}</TableCell>
              <TableCell className="text-right">{item.energy.toLocaleString()}</TableCell>
              <TableCell className="text-right text-red-500">
                £{Math.abs(item.payment).toLocaleString()}
              </TableCell>
              <TableCell className="text-right text-[#F7931A]">
                {item.bitcoin.toFixed(8)}
              </TableCell>
              <TableCell className="text-right text-[#F7931A]">
                £{item.value.toLocaleString()}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}