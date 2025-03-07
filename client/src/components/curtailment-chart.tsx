import { Skeleton } from "../components/ui/skeleton"
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Line,
  ComposedChart
} from "recharts"
import { formatNumber, formatBitcoin } from "../lib/utils"

interface HourlyData {
  hour: string
  curtailedEnergy: number
  bitcoinMined?: number
}

interface CurtailmentChartProps {
  data: HourlyData[]
  showBitcoin?: boolean
  isLoading?: boolean
}

export function CurtailmentChart({
  data,
  showBitcoin = true,
  isLoading = false
}: CurtailmentChartProps) {
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="rounded-lg border bg-background p-2 shadow-sm text-xs">
          <p className="font-semibold">{label}</p>
          <p className="text-muted-foreground">
            Curtailed: {formatNumber(payload[0].value)} MWh
          </p>
          {showBitcoin && payload[1] && (
            <p className="text-muted-foreground">
              Bitcoin: {formatBitcoin(payload[1].value)} BTC
            </p>
          )}
        </div>
      );
    }
    return null;
  };

  if (isLoading) {
    return <Skeleton className="h-[300px] w-full" />;
  }

  return (
    <ResponsiveContainer width="100%" height={350}>
      {showBitcoin ? (
        <ComposedChart
          data={data}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
          <XAxis dataKey="hour" />
          <YAxis yAxisId="left" />
          <YAxis yAxisId="right" orientation="right" />
          <Tooltip content={<CustomTooltip />} />
          <Legend />
          <Bar
            yAxisId="left"
            name="Curtailed Energy (MWh)"
            dataKey="curtailedEnergy"
            fill="#2563eb"
            barSize={20}
          />
          <Line
            yAxisId="right"
            name="Bitcoin Mined (BTC)"
            type="monotone"
            dataKey="bitcoinMined"
            stroke="#f59e0b"
            strokeWidth={2}
            dot={{ r: 3 }}
          />
        </ComposedChart>
      ) : (
        <BarChart
          data={data}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
          <XAxis dataKey="hour" />
          <YAxis />
          <Tooltip content={<CustomTooltip />} />
          <Legend />
          <Bar
            name="Curtailed Energy (MWh)"
            dataKey="curtailedEnergy"
            fill="#2563eb"
            barSize={20}
          />
        </BarChart>
      )}
    </ResponsiveContainer>
  )
}