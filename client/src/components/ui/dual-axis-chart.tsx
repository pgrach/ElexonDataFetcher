import * as React from "react"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts"
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "./chart"

// Example data structure for dual axis
type DualAxisData = {
  name: string
  temperature: number
  humidity: number
}

const exampleData: DualAxisData[] = [
  { name: "Jan", temperature: 20, humidity: 45 },
  { name: "Feb", temperature: 22, humidity: 48 },
  { name: "Mar", temperature: 25, humidity: 52 },
  { name: "Apr", temperature: 21, humidity: 55 },
]

export const DualAxisChart = () => {
  const chartConfig = {
    temperature: {
      label: "Temperature (°C)",
      theme: {
        light: "#ef4444",
        dark: "#dc2626",
      },
    },
    humidity: {
      label: "Humidity (%)",
      theme: {
        light: "#3b82f6",
        dark: "#2563eb",
      },
    },
  }

  return (
    <ChartContainer config={chartConfig} className="h-[400px]">
      <LineChart
        data={exampleData}
        margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
      >
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          dataKey="name"
          className="text-sm [&_.recharts-cartesian-axis-line]:stroke-border [&_.recharts-cartesian-axis-tick-line]:stroke-border"
        />
        {/* Primary Y-Axis (Left) - Temperature */}
        <YAxis
          yAxisId="temperature"
          orientation="left"
          domain={['auto', 'auto']}
          label={{ value: "Temperature (°C)", angle: -90, position: 'insideLeft' }}
          className="text-sm [&_.recharts-cartesian-axis-line]:stroke-border [&_.recharts-cartesian-axis-tick-line]:stroke-border"
        />
        {/* Secondary Y-Axis (Right) - Humidity */}
        <YAxis
          yAxisId="humidity"
          orientation="right"
          domain={[0, 100]}
          label={{ value: "Humidity (%)", angle: 90, position: 'insideRight' }}
          className="text-sm [&_.recharts-cartesian-axis-line]:stroke-border [&_.recharts-cartesian-axis-tick-line]:stroke-border"
        />
        <ChartTooltip
          content={({ active, payload }) => (
            <ChartTooltipContent
              active={active}
              payload={payload}
              nameKey="name"
              labelKey="name"
            />
          )}
        />
        <Line
          type="monotone"
          dataKey="temperature"
          yAxisId="temperature"
          stroke="var(--color-temperature)"
          strokeWidth={2}
          dot={false}
        />
        <Line
          type="monotone"
          dataKey="humidity"
          yAxisId="humidity"
          stroke="var(--color-humidity)"
          strokeWidth={2}
          dot={false}
        />
      </LineChart>
    </ChartContainer>
  )
}