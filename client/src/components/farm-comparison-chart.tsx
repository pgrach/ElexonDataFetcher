import { Skeleton } from "../components/ui/skeleton"
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from "recharts"
import { formatNumber, formatCurrency, formatBitcoin } from "../lib/utils"

interface FarmData {
  name: string;
  energy: number;
  bitcoin: number;
  payment: number;
}

interface FarmComparisonChartProps {
  data: FarmData[];
  isLoading?: boolean;
}

export function FarmComparisonChart({
  data,
  isLoading = false
}: FarmComparisonChartProps) {
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="rounded-lg border bg-background p-2 shadow-sm text-xs">
          <p className="font-semibold">{label}</p>
          <p className="text-muted-foreground">
            Curtailed: {formatNumber(payload[0].value)} MWh
          </p>
          <p className="text-muted-foreground">
            Bitcoin: {formatBitcoin(payload[1].value)} BTC
          </p>
          <p className="text-muted-foreground text-red-500">
            Payment: {formatCurrency(payload[2].value)}
          </p>
        </div>
      );
    }
    return null;
  };

  if (isLoading) {
    return <Skeleton className="h-[300px] w-full" />;
  }

  return (
    <ResponsiveContainer width="100%" height={400}>
      <BarChart
        data={data}
        margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
        barGap={0}
        barCategoryGap={20}
      >
        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
        <XAxis dataKey="name" />
        <YAxis yAxisId="left" orientation="left" />
        <YAxis yAxisId="right" orientation="right" />
        <Tooltip content={<CustomTooltip />} />
        <Legend />
        <Bar 
          yAxisId="left"
          name="Curtailed Energy (MWh)" 
          dataKey="energy" 
          fill="#2563eb" 
        />
        <Bar 
          yAxisId="right"
          name="Bitcoin Potential (BTC)" 
          dataKey="bitcoin" 
          fill="#f59e0b" 
        />
        <Bar 
          yAxisId="left"
          name="Subsidy Payment (£)" 
          dataKey="payment" 
          fill="#dc2626" 
        />
      </BarChart>
    </ResponsiveContainer>
  )
}