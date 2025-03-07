import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts"

interface HourlyData {
  hour: string;
  curtailedEnergy: number;
  bitcoinMined?: number;
}

interface CurtailmentChartProps {
  data: HourlyData[];
  showBitcoin?: boolean;
  isLoading?: boolean;
}

export function CurtailmentChart({
  data,
  showBitcoin = true,
  isLoading = false
}: CurtailmentChartProps) {
  if (isLoading) {
    return (
      <div className="h-[400px] w-full flex items-center justify-center bg-muted/20 rounded-lg">
        <p className="text-muted-foreground animate-pulse">Loading chart data...</p>
      </div>
    )
  }

  if (!data || data.length === 0) {
    return (
      <div className="h-[400px] w-full flex items-center justify-center bg-muted/20 rounded-lg">
        <p className="text-muted-foreground">No data available for selected period</p>
      </div>
    )
  }

  return (
    <div className="h-[400px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="curtailedEnergy" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.3} />
              <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0} />
            </linearGradient>
            {showBitcoin && (
              <linearGradient id="bitcoinMined" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#F7931A" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#F7931A" stopOpacity={0} />
              </linearGradient>
            )}
          </defs>
          <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.2} />
          <XAxis 
            dataKey="hour" 
            tick={{ fontSize: 12 }}
            tickMargin={8}
            axisLine={{ stroke: 'hsl(var(--border))' }}
            tickLine={false}
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
              value: 'MWh', 
              angle: -90, 
              position: 'insideLeft',
              style: { textAnchor: 'middle', fill: 'hsl(var(--muted-foreground))' },
              offset: -5,
            }}
          />
          {showBitcoin && (
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
                value: 'BTC', 
                angle: 90, 
                position: 'insideRight',
                style: { textAnchor: 'middle', fill: 'hsl(var(--muted-foreground))' }, 
                offset: -5
              }}
            />
          )}
          <Tooltip />
          <Area 
            type="monotone" 
            dataKey="curtailedEnergy" 
            stroke="hsl(var(--primary))" 
            fillOpacity={1}
            fill="url(#curtailedEnergy)" 
            yAxisId="left"
            name="Curtailed Energy (MWh)"
          />
          {showBitcoin && (
            <Area 
              type="monotone" 
              dataKey="bitcoinMined" 
              stroke="#F7931A" 
              fillOpacity={1}
              fill="url(#bitcoinMined)" 
              yAxisId="right"
              name="Bitcoin Potential (BTC)"
            />
          )}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}