import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table"
import { Skeleton } from "../components/ui/skeleton"
import { formatNumber, formatCurrency, formatBitcoin } from "../lib/utils"

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
      <div className="w-full">
        <div className="space-y-2">
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
        </div>
      </div>
    );
  }

  return (
    <div className="w-full overflow-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Settlement Period</TableHead>
            <TableHead>Energy (MWh)</TableHead>
            <TableHead>Payment (£)</TableHead>
            <TableHead>Bitcoin (BTC)</TableHead>
            <TableHead>Value (£)</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((item, index) => (
            <TableRow key={index}>
              <TableCell className="font-medium">{item.period}</TableCell>
              <TableCell>{formatNumber(item.energy)}</TableCell>
              <TableCell className="text-red-500">{formatCurrency(item.payment)}</TableCell>
              <TableCell>{formatBitcoin(item.bitcoin)}</TableCell>
              <TableCell>{formatCurrency(item.value)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}