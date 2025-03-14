import { useState, useEffect } from 'react';
import { BarChart, Bar, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import axios from 'axios';
import { format } from 'date-fns';

interface CurtailmentPercentageChartProps {
  date: Date;
  leadPartyName?: string;
  farmId?: string;
}

interface FarmCurtailmentData {
  farmId: string;
  totalPotentialGeneration: number;
  totalCurtailedVolume: number;
  curtailmentPercentage: number;
}

interface LeadPartyCurtailmentData {
  leadPartyName: string;
  date: string;
  farms: FarmCurtailmentData[];
  totalPotentialGeneration: number;
  totalCurtailedVolume: number;
  overallCurtailmentPercentage: number;
}

interface SingleFarmCurtailmentData {
  farmId: string;
  date: string;
  leadPartyName: string;
  totalPotentialGeneration: number;
  totalCurtailedVolume: number;
  curtailmentPercentage: number;
  detailedPeriods: Array<{
    period: number;
    potentialGeneration: number;
    curtailedVolume: number;
    curtailmentPercentage: number;
  }>;
}

export default function CurtailmentPercentageChart({ date, leadPartyName, farmId }: CurtailmentPercentageChartProps) {
  const [data, setData] = useState<any[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [viewType, setViewType] = useState<'farm' | 'period'>(farmId ? 'period' : 'farm');
  const [title, setTitle] = useState<string>('Curtailment Percentage');
  
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      
      try {
        const formattedDate = format(date, 'yyyy-MM-dd');
        let response;
        let chartData: any[] = [];
        
        if (farmId) {
          // Fetch data for a specific farm
          response = await axios.get<SingleFarmCurtailmentData>(
            `/api/production/curtailment-percentage/farm/${farmId}/${formattedDate}`
          );
          
          // For single farm view, we'll show period-by-period data
          setTitle(`${response.data.farmId} Curtailment by Period`);
          
          if (viewType === 'period') {
            // Transform period data for the chart
            chartData = response.data.detailedPeriods.map(period => ({
              name: `P${period.period}`,
              potentialGeneration: period.potentialGeneration,
              curtailedVolume: period.curtailedVolume,
              curtailmentPercentage: period.curtailmentPercentage
            }));
          } else {
            // Single data point for farm view
            chartData = [{
              name: response.data.farmId,
              potentialGeneration: response.data.totalPotentialGeneration,
              curtailedVolume: response.data.totalCurtailedVolume,
              curtailmentPercentage: response.data.curtailmentPercentage
            }];
          }
        } else if (leadPartyName) {
          // Fetch data for all farms of a lead party
          response = await axios.get<LeadPartyCurtailmentData>(
            `/api/production/curtailment-percentage/lead-party/${encodeURIComponent(leadPartyName)}/${formattedDate}`
          );
          
          setTitle(`${response.data.leadPartyName} Farms Curtailment`);
          
          // Transform farm data for the chart
          chartData = response.data.farms.map(farm => ({
            name: farm.farmId,
            potentialGeneration: farm.totalPotentialGeneration,
            curtailedVolume: farm.totalCurtailedVolume,
            curtailmentPercentage: farm.curtailmentPercentage
          }));
          
          // Add the overall total as the last bar
          chartData.push({
            name: 'Total',
            potentialGeneration: response.data.totalPotentialGeneration,
            curtailedVolume: response.data.totalCurtailedVolume,
            curtailmentPercentage: response.data.overallCurtailmentPercentage,
            isTotal: true
          });
        } else {
          throw new Error('Either farmId or leadPartyName must be provided');
        }
        
        setData(chartData);
      } catch (err) {
        console.error('Error fetching curtailment data:', err);
        setError('Failed to load curtailment data. Please try again later.');
      } finally {
        setLoading(false);
      }
    };
    
    fetchData();
  }, [date, leadPartyName, farmId, viewType]);
  
  // Custom tooltip to show all relevant data
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const item = payload[0].payload;
      return (
        <div className="bg-background border rounded-md p-3 shadow-md">
          <p className="font-semibold">{label}</p>
          <p>Potential Generation: {item.potentialGeneration.toFixed(2)} MWh</p>
          <p>Curtailed Volume: {item.curtailedVolume.toFixed(2)} MWh</p>
          <p className="font-bold">Curtailment: {item.curtailmentPercentage.toFixed(2)}%</p>
        </div>
      );
    }
    return null;
  };
  
  const toggleViewType = () => {
    if (farmId) {
      setViewType(viewType === 'farm' ? 'period' : 'farm');
    }
  };
  
  if (loading) {
    return (
      <Card className="w-full h-[400px] flex items-center justify-center">
        <CardContent>
          <div className="text-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-800 mx-auto"></div>
            <p className="mt-2">Loading curtailment data...</p>
          </div>
        </CardContent>
      </Card>
    );
  }
  
  if (error) {
    return (
      <Card className="w-full">
        <CardHeader>
          <CardTitle>Error</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center text-red-500">
            <p>{error}</p>
          </div>
        </CardContent>
      </Card>
    );
  }
  
  return (
    <Card className="w-full">
      <CardHeader>
        <div className="flex justify-between items-center">
          <div>
            <CardTitle>{title}</CardTitle>
            <CardDescription>
              {format(date, 'MMMM d, yyyy')}
            </CardDescription>
          </div>
          {farmId && (
            <button 
              onClick={toggleViewType}
              className="px-3 py-1 rounded bg-primary text-primary-foreground text-sm"
            >
              {viewType === 'period' ? 'Show Farm Total' : 'Show By Period'}
            </button>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              width={500}
              height={300}
              data={data}
              margin={{
                top: 5,
                right: 30,
                left: 20,
                bottom: 25,
              }}
            >
              <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
              <XAxis 
                dataKey="name" 
                angle={-45} 
                textAnchor="end" 
                height={60} 
                tick={{ fontSize: 12 }}
              />
              <YAxis 
                label={{ 
                  value: 'Curtailment %', 
                  angle: -90, 
                  position: 'insideLeft',
                  style: { textAnchor: 'middle' },
                }}
                domain={[0, 100]}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Bar 
                dataKey="curtailmentPercentage" 
                name="Curtailment %" 
                fill="#8884d8"
              >
                {data?.map((entry, index) => (
                  <Cell 
                    key={`cell-${index}`} 
                    fill={entry.isTotal ? '#ff7e67' : '#8884d8'} 
                    stroke={entry.isTotal ? '#ff5e57' : '#7771d8'}
                    strokeWidth={entry.isTotal ? 2 : 1}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}