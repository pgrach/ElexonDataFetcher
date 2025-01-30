import * as React from "react"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Bar,
  ComposedChart
} from "recharts"
import { ChartContainer, ChartTooltip } from "./chart"

type DualAxisProps = {
  data: Array<{
    name: string;
    curtailedEnergy: number;
    bitcoinMined: number;
  }>;
  chartConfig: {
    curtailedEnergy: {
      label: string;
      theme: {
        light: string;
        dark: string;
      };
    };
    bitcoinMined: {
      label: string;
      theme: {
        light: string;
        dark: string;
      };
    };
  };
}

export const DualAxisChart: React.FC<DualAxisProps> = ({ data, chartConfig }) => {
  return (
    <ChartContainer config={chartConfig} className="h-[400px]">
      <ComposedChart
        data={data}
        margin={{ top: 20, right: 60, left: 60, bottom: 20 }}
      >
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          dataKey="name"
          interval={2}
          className="text-sm [&_.recharts-cartesian-axis-line]:stroke-border [&_.recharts-cartesian-axis-tick-line]:stroke-border"
        />
        {/* Primary Y-Axis (Left) - Curtailed Energy */}
        <YAxis
          yAxisId="left"
          orientation="left"
          label={{ 
            value: chartConfig.curtailedEnergy.label,
            angle: -90,
            position: 'insideLeft',
            offset: -40,
            style: { fontSize: 12 }
          }}
          className="text-sm [&_.recharts-cartesian-axis-line]:stroke-border [&_.recharts-cartesian-axis-tick-line]:stroke-border"
        />
        {/* Secondary Y-Axis (Right) - Bitcoin Mined */}
        <YAxis
          yAxisId="right"
          orientation="right"
          label={{ 
            value: chartConfig.bitcoinMined.label,
            angle: 90,
            position: 'insideRight',
            offset: -40,
            style: { fontSize: 12 }
          }}
          domain={[0, 'auto']}
          className="text-sm [&_.recharts-cartesian-axis-line]:stroke-border [&_.recharts-cartesian-axis-tick-line]:stroke-border"
        />
        <Tooltip
          content={({ active, payload }) => {
            if (!active || !payload?.length) return null;
            return (
              <div className="rounded-lg border bg-background p-2 shadow-md">
                <div className="grid gap-2">
                  <div className="flex items-center gap-2">
                    <div className="h-2 w-2 rounded-full bg-primary" />
                    <span className="font-medium">{payload[0].payload.name}</span>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    {chartConfig.curtailedEnergy.label}: {Number(payload[0].value).toFixed(2)} MWh
                  </div>
                  <div className="text-sm text-[#F7931A]">
                    {chartConfig.bitcoinMined.label}: ₿{Number(payload[1]?.value).toFixed(8)}
                  </div>
                </div>
              </div>
            );
          }}
        />
        <Bar
          dataKey="curtailedEnergy"
          yAxisId="left"
          fill="var(--color-curtailedEnergy)"
        />
        <Line
          type="monotone"
          dataKey="bitcoinMined"
          yAxisId="right"
          stroke="var(--color-bitcoinMined)"
          strokeWidth={2}
          dot={false}
        />
      </ComposedChart>
    </ChartContainer>
  )
}