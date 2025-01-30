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

export const DualAxisChart = ({ data, chartConfig }: DualAxisChartProps) => {
  return (
    <ResponsiveContainer width="100%" height={400}>
      <ComposedChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 20 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
        <XAxis 
          dataKey="hour" // Changed dataKey to "hour"
          interval={2}
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
            className: "text-sm font-medium fill-current"
          }}
          tick={{ fill: 'currentColor' }}
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
          shape={(props) => {
            const { cx, cy, fill } = props;
            const value = props.payload[chartConfig.rightAxis.dataKey];
            // Don't render if value is 0
            if (value === 0) return null;
            // Render a larger circle for non-zero values
            return (
              <circle 
                cx={cx} 
                cy={cy} 
                r={6} 
                fill={fill} 
                stroke={fill}
                strokeWidth={2}
              />
            );
          }}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
};