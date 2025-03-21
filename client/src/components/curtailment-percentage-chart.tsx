import { useState, useEffect } from "react";
import { format } from "date-fns";
import axios from "axios";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { AlertCircle } from "lucide-react";

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
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<any[]>([]);
  const [chartTitle, setChartTitle] = useState<string>("Curtailment Percentage");
  const [chartDescription, setChartDescription] = useState<string>("");

  useEffect(() => {
    async function fetchCurtailmentData() {
      setLoading(true);
      setError(null);
      
      try {
        const formattedDate = format(date, "yyyy-MM-dd");
        
        // If farm ID is provided, fetch specific farm data
        if (farmId) {
          const encodedFarmId = encodeURIComponent(farmId);
          const { data } = await axios.get<SingleFarmCurtailmentData>(
            `/api/production/curtailment-percentage/farm/${encodedFarmId}/${formattedDate}`
          );
          
          setChartTitle(`${farmId} Curtailment Analysis`);
          setChartDescription(`${data.curtailmentPercentage.toFixed(1)}% of potential generation curtailed on ${formattedDate}`);
          
          // Transform period data for chart
          const chartData = data.detailedPeriods.map(period => ({
            name: `Period ${period.period}`,
            potentialMWh: Number(period.potentialGeneration.toFixed(1)),
            curtailedMWh: Number(period.curtailedVolume.toFixed(1)),
            percentage: Number(period.curtailmentPercentage.toFixed(1))
          }));
          
          setData(chartData);
        } 
        // If lead party name is provided, fetch lead party data
        else if (leadPartyName && leadPartyName !== "All Lead Parties") {
          const encodedLeadParty = encodeURIComponent(leadPartyName);
          const { data } = await axios.get<LeadPartyCurtailmentData>(
            `/api/production/curtailment-percentage/lead-party/${encodedLeadParty}/${formattedDate}`
          );
          
          setChartTitle(`${leadPartyName} Curtailment Analysis`);
          setChartDescription(`${data.overallCurtailmentPercentage.toFixed(1)}% of potential generation curtailed on ${formattedDate}`);
          
          // Transform farm data for chart
          const chartData = data.farms.map(farm => ({
            name: farm.farmId,
            potentialMWh: Number(farm.totalPotentialGeneration.toFixed(1)),
            curtailedMWh: Number(farm.totalCurtailedVolume.toFixed(1)),
            percentage: Number(farm.curtailmentPercentage.toFixed(1))
          }));
          
          setData(chartData);
        } 
        // If neither farmId nor leadPartyName (or "All Lead Parties"), show aggregate data by lead party
        else {
          // Fetch all lead parties with curtailment data for the specified date
          const { data: leadParties } = await axios.get(
            `/api/production/curtailed-lead-parties`, {
              params: { date: formattedDate }
            }
          );
          
          const aggregateData = await Promise.all(
            leadParties.map(async (party: { leadPartyName: string }) => {
              const encodedLeadParty = encodeURIComponent(party.leadPartyName);
              const { data } = await axios.get<LeadPartyCurtailmentData>(
                `/api/production/curtailment-percentage/lead-party/${encodedLeadParty}/${formattedDate}`
              );
              
              return {
                name: party.leadPartyName,
                potentialMWh: Number(data.totalPotentialGeneration.toFixed(1)),
                curtailedMWh: Number(data.totalCurtailedVolume.toFixed(1)),
                percentage: Number(data.overallCurtailmentPercentage.toFixed(1))
              };
            })
          );
          
          setChartTitle("All Lead Parties Curtailment Analysis");
          setChartDescription(`Curtailment percentages by lead party on ${formattedDate}`);
          
          setData(aggregateData);
        }
      } catch (err) {
        console.error("Error fetching curtailment data:", err);
        
        // Handle specific error cases
        if (axios.isAxiosError(err) && err.response?.status === 404) {
          setError("No curtailment data found for this selection");
        } else {
          setError("Failed to load curtailment data");
        }
        
        setData([]);
      } finally {
        setLoading(false);
      }
    }
    
    fetchCurtailmentData();
  }, [date, leadPartyName, farmId]);
  
  const getBarColors = (count: number) => {
    const baseColors = [
      "#0088FE", "#00C49F", "#FFBB28", "#FF8042", "#A28CFF", 
      "#FF6B6B", "#4ECDC4", "#45ADA8", "#547980", "#594F4F"
    ];
    
    // If we need more colors than in the base array, generate them
    if (count <= baseColors.length) {
      return baseColors.slice(0, count);
    } else {
      const colors = [...baseColors];
      for (let i = baseColors.length; i < count; i++) {
        // Generate random colors for additional items
        const r = Math.floor(Math.random() * 200) + 55; // 55-255 for better visibility
        const g = Math.floor(Math.random() * 200) + 55;
        const b = Math.floor(Math.random() * 200) + 55;
        colors.push(`rgb(${r}, ${g}, ${b})`);
      }
      return colors;
    }
  };
  
  const renderCustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-card text-card-foreground p-4 border shadow-sm rounded-md">
          <p className="font-medium text-base">{label}</p>
          <p className="text-base mt-1">
            <span className="font-medium">Potential:</span> {payload[0].value} MWh
          </p>
          <p className="text-base mt-1">
            <span className="font-medium">Curtailed:</span> {payload[1].value} MWh
          </p>
          <p className="text-base mt-1">
            <span className="font-medium">Percentage:</span> {payload[2].value}%
          </p>
        </div>
      );
    }
    
    return null;
  };
  
  const colors = getBarColors(data?.length || 0);
  
  return (
    <Card className="w-full h-full">
      <CardHeader>
        <CardTitle className="flex justify-between items-center text-2xl">
          {chartTitle}
          {loading && <Badge variant="outline">Loading...</Badge>}
        </CardTitle>
        <p className="text-lg text-muted-foreground">{chartDescription}</p>
      </CardHeader>
      <CardContent>
        {error ? (
          <div className="flex flex-col items-center justify-center h-80 text-center">
            <AlertCircle className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-lg text-muted-foreground">{error}</p>
          </div>
        ) : (
          <div className="h-80">
            {!loading && data && data.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={data}
                  margin={{ top: 5, right: 30, left: 20, bottom: 65 }}
                >
                  <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                  <XAxis 
                    dataKey="name" 
                    angle={-45} 
                    textAnchor="end" 
                    tick={{ fontSize: 10 }}
                    height={60} 
                  />
                  <YAxis yAxisId="left" label={{ value: 'Energy (MWh)', angle: -90, position: 'insideLeft' }} />
                  <YAxis yAxisId="right" orientation="right" label={{ value: 'Percentage (%)', angle: -90, position: 'insideRight' }} />
                  <Tooltip content={renderCustomTooltip} />
                  <Legend />
                  <Bar 
                    yAxisId="left"
                    name="Potential Generation" 
                    dataKey="potentialMWh" 
                    fill="#8884d8" 
                    opacity={0.7}
                  />
                  <Bar 
                    yAxisId="left"
                    name="Curtailed Energy" 
                    dataKey="curtailedMWh" 
                    fill="#82ca9d" 
                    opacity={0.9}
                  />
                  <Bar 
                    yAxisId="right"
                    name="Curtailment %" 
                    dataKey="percentage" 
                    fill="#ff7300"
                  >
                    {data.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            ) : !loading ? (
              <div className="flex flex-col items-center justify-center h-full text-center">
                <p className="text-lg text-muted-foreground">No data to display</p>
              </div>
            ) : null}
          </div>
        )}
      </CardContent>
    </Card>
  );
}