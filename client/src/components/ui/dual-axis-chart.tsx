import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Scatter
} from "recharts";

interface ChartConfig {
  leftAxis: {
    label: string;
    dataKey: string;
    color: string;
  };
  rightAxis: {
    label: string;
    dataKey: string;
    color: string;
  };
}

interface DualAxisChartProps {
  data: any[];
  chartConfig: ChartConfig;
}

interface ScatterProps {
  cx: number;
  cy: number;
  fill: string;
  payload: { [key: string]: number };
}


export const DualAxisChart = ({ data, chartConfig }: DualAxisChartProps) => {
  return (
    <ResponsiveContainer width="100%" height={400}>
      <ComposedChart data={data} margin={{ top: 20, right: 60, left: 60, bottom: 20 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
        <XAxis 
          dataKey="hour"
          interval={3}
          tick={{ fontSize: 12, fill: 'currentColor' }}
          className="text-sm font-medium"
        />
        <YAxis
          yAxisId="left"
          orientation="left"
          label={{ 
            value: chartConfig.leftAxis.label, 
            angle: -90, 
            position: 'insideLeft',
            dy: 40,
            dx: -10,
            className: "text-sm font-medium fill-current"
          }}
          tick={{ fill: 'currentColor' }}
        />
        <YAxis
          yAxisId="right"
          orientation="right"
          label={{ 
            value: chartConfig.rightAxis.label, 
            angle: 90, 
            position: 'insideRight',
            dy: 40,
            dx: 10,
            className: "text-sm font-medium",
            fill: chartConfig.rightAxis.color
          }}
          tick={{ fill: chartConfig.rightAxis.color }}
        />
        <Tooltip 
          contentStyle={{ 
            backgroundColor: 'hsl(var(--background))',
            border: '1px solid hsl(var(--border))',
            borderRadius: '0.5rem'
          }}
          labelClassName="font-medium"
        />
        <Legend />
        <Bar
          yAxisId="left"
          dataKey={chartConfig.leftAxis.dataKey}
          fill={chartConfig.leftAxis.color}
          name={chartConfig.leftAxis.label}
          radius={[4, 4, 0, 0]}
        />
        <Scatter
          yAxisId="right"
          dataKey={chartConfig.rightAxis.dataKey}
          fill={chartConfig.rightAxis.color}
          name={chartConfig.rightAxis.label}
          shape={(props: unknown) => {
            const { cx, cy, fill, payload } = props as ScatterProps;
            const value = payload[chartConfig.rightAxis.dataKey];
            if (value === 0) return null;

            return (
              <g transform={`translate(${cx - 8}, ${cy - 8})`}>
                {/* Outer circle (coin) */}
                <circle 
                  cx="8" 
                  cy="8" 
                  r="8" 
                  fill={fill} 
                  stroke={fill}
                  strokeWidth={1}
                />
                {/* Inner circle for the metallic effect */}
                <circle 
                  cx="8" 
                  cy="8" 
                  r="7" 
                  fill={fill} 
                  stroke="#ffffff"
                  strokeWidth={0.5}
                  opacity={0.3}
                />
                {/* Bitcoin symbol */}
                <text
                  x="8"
                  y="11"
                  textAnchor="middle"
                  fill="#ffffff"
                  fontSize="10px"
                  fontWeight="bold"
                >
                  ₿
                </text>
              </g>
            );
          }}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
};