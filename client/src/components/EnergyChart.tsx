import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import type { AggregatedData } from '@/types';

interface EnergyChartProps {
  data: AggregatedData[];
}

export function EnergyChart({ data }: EnergyChartProps) {
  // Transform hour numbers to readable time format
  const formattedData = data.map(item => ({
    ...item,
    time: `${item.hour.toString().padStart(2, '0')}:00`,
  }));

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={formattedData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="time" />
        <YAxis yAxisId="left" label={{ value: 'Curtailed Energy (MWh)', angle: -90, position: 'insideLeft' }} />
        <YAxis yAxisId="right" orientation="right" label={{ value: 'Potential BTC', angle: 90, position: 'insideRight' }} />
        <Tooltip />
        <Legend />
        <Bar yAxisId="left" dataKey="curtailedEnergy" fill="#0284c7" name="Curtailed Energy (MWh)" />
        <Bar yAxisId="right" dataKey="potentialBtc" fill="#f59e0b" name="Potential BTC" />
      </BarChart>
    </ResponsiveContainer>
  );
}
