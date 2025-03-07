import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts"

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
  if (isLoading) {
    return (
      <div className="h-[400px] w-full flex items-center justify-center bg-muted/20 rounded-lg">
        <p className="text-muted-foreground animate-pulse">Loading farm comparison data...</p>
      </div>
    )
  }

  if (!data || data.length === 0) {
    return (
      <div className="h-[400px] w-full flex items-center justify-center bg-muted/20 rounded-lg">
        <p className="text-muted-foreground">No farm comparison data available</p>
      </div>
    )
  }

  return (
    <div className="h-[400px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={data}
          margin={{ top: 20, right: 30, left: 30, bottom: 60 }}
        >
          <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.2} />
          <XAxis 
            dataKey="name" 
            tick={{ fontSize: 11 }}
            tickMargin={8}
            axisLine={{ stroke: 'hsl(var(--border))' }}
            tickLine={false}
            interval={0}
            angle={-45}
            textAnchor="end"
          />
          <YAxis 
            yAxisId="left"
            tick={{ fontSize: 12 }}
            tickMargin={8}
            axisLine={{ stroke: 'hsl(var(--border))' }}
            tickLine={false}
            domain={[0, 'auto']}
            allowDecimals={false}
            label={{ 
              value: 'Energy (MWh)', 
              angle: -90, 
              position: 'insideLeft',
              style: { textAnchor: 'middle', fill: 'hsl(var(--muted-foreground))' },
              offset: -5,
            }}
          />
          <YAxis 
            yAxisId="right"
            orientation="right"
            tick={{ fontSize: 12 }}
            tickMargin={8}
            axisLine={{ stroke: 'hsl(var(--border))' }}
            tickLine={false}
            domain={[0, 'auto']}
            allowDecimals={false}
            label={{ 
              value: 'Bitcoin (BTC)', 
              angle: 90, 
              position: 'insideRight',
              style: { textAnchor: 'middle', fill: 'hsl(var(--muted-foreground))' }, 
              offset: -5
            }}
          />
          <Tooltip />
          <Legend />
          <Bar 
            dataKey="energy" 
            fill="hsl(var(--primary))" 
            yAxisId="left" 
            name="Curtailed Energy (MWh)" 
          />
          <Bar 
            dataKey="bitcoin" 
            fill="#F7931A" 
            yAxisId="right" 
            name="Potential Bitcoin (BTC)" 
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}