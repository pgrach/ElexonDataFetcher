import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
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
          dataKey="name" 
          className="text-sm font-medium"
          tick={{ fill: 'currentColor' }}
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
        <Line
          yAxisId="right"
          type="monotone"
          dataKey={chartConfig.rightAxis.dataKey}
          stroke={chartConfig.rightAxis.color}
          name={chartConfig.rightAxis.label}
          dot={false}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
};
