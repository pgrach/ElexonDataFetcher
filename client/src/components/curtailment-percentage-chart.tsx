import { useState, useEffect } from "react";
import { format } from "date-fns";
import axios from "axios";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { AlertCircle } from "lucide-react";
import CurtailmentPieChart from "@/components/curtailment-pie-chart";

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
  const [totalPotentialGeneration, setTotalPotentialGeneration] = useState<number>(0);
  const [totalCurtailedVolume, setTotalCurtailedVolume] = useState<number>(0);
  const [totalWindGeneration, setTotalWindGeneration] = useState<number>(0);
  const [showPieChart, setShowPieChart] = useState<boolean>(false);

  useEffect(() => {
    async function fetchCurtailmentData() {
      setLoading(true);
      setError(null);
      
      try {
        const formattedDate = format(date, "yyyy-MM-dd");
        
        if (farmId) {
          // For specific farm, still use the detailed API
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
        else if (leadPartyName && leadPartyName !== "All Lead Parties") {
          // For specific lead party, still use the detailed API
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
        else {
          // For "All Farms" view, use the same endpoint as the main dashboard
          // to ensure consistent data between dashboard summary and analysis
          const { data: summaryData } = await axios.get(`/api/summary/daily/${formattedDate}`);
          
          // Use the daily summary data for curtailment and wind generation values
          const totalCurtailed = Number(summaryData.totalCurtailedEnergy);
          const totalWindGeneration = Number(summaryData.totalWindGeneration || 0);
          
          // For actual vs. curtailed calculation, use the real wind generation data
          // instead of estimating from a percentage
          let totalPotential = totalCurtailed + totalWindGeneration;
          
          // Calculate curtailment percentage from actual values
          let overallPercentage = totalPotential > 0 
            ? (totalCurtailed / totalPotential) * 100
            : 0;
          
          // Store calculated values
          setTotalCurtailedVolume(totalCurtailed);
          setTotalPotentialGeneration(totalPotential);
          setTotalWindGeneration(totalWindGeneration);
          setShowPieChart(true);
          
          setChartTitle("Wind Farm Curtailment");
          setChartDescription("");
          
          // We don't need lead party breakdown in this minimal view
          setData([]);
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
    <div className="w-full h-full">
      {/* Clean page header section */}
      <div className="mb-6">
        {loading && (
          <div className="flex items-center gap-2 mb-3">
            <Badge variant="outline" className="animate-pulse bg-muted px-3 py-1">
              <span className="inline-block h-4 w-4 rounded-full border-2 border-current border-r-transparent animate-spin mr-2"></span>
              Loading data...
            </Badge>
          </div>
        )}
      </div>

      {error ? (
        <div className="flex flex-col items-center justify-center h-80 text-center bg-card rounded-lg border border-border/40 shadow-sm p-6">
          <AlertCircle className="h-12 w-12 text-destructive mb-4" />
          <p className="text-xl font-medium mb-2">Unable to Load Data</p>
          <p className="text-muted-foreground">{error}</p>
        </div>
      ) : (
        <div className="h-auto">
          {!loading && showPieChart && totalPotentialGeneration > 0 ? (
            // Show improved pie chart for "All Farms" view without redundant title/description
            <div>
              <CurtailmentPieChart
                totalPotentialGeneration={totalPotentialGeneration}
                totalCurtailedVolume={totalCurtailedVolume}
                totalWindGeneration={totalWindGeneration}
                title={chartTitle}
                description={chartDescription}
                loading={loading}
                error={error}
                date={date}
              />
            </div>
          ) : !loading && data && data.length > 0 ? (
            // Show bar chart for specific farm or lead party
            <Card className="w-full overflow-hidden bg-gradient-to-br from-background to-muted/30">
              <CardHeader>
                <CardTitle className="text-2xl font-bold tracking-tight">{chartTitle}</CardTitle>
                <p className="text-muted-foreground">{chartDescription}</p>
              </CardHeader>
              <CardContent>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={data}
                      margin={{ top: 5, right: 30, left: 20, bottom: 65 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
                      <XAxis 
                        dataKey="name" 
                        angle={-45} 
                        textAnchor="end" 
                        tick={{ fontSize: 11 }}
                        height={60} 
                        tickMargin={10}
                        stroke="#888888"
                      />
                      <YAxis 
                        yAxisId="left" 
                        label={{ value: 'Energy (MWh)', angle: -90, position: 'insideLeft', style: {textAnchor: 'middle'} }} 
                        stroke="#888888"
                      />
                      <YAxis 
                        yAxisId="right" 
                        orientation="right" 
                        label={{ value: 'Percentage (%)', angle: -90, position: 'insideRight', style: {textAnchor: 'middle'} }} 
                        stroke="#888888"
                      />
                      <Tooltip content={renderCustomTooltip} />
                      <Legend wrapperStyle={{ paddingTop: '10px' }} />
                      <Bar 
                        yAxisId="left"
                        name="Potential Generation" 
                        dataKey="potentialMWh" 
                        fill="#4f46e5" 
                        radius={[4, 4, 0, 0]}
                        opacity={0.8}
                      />
                      <Bar 
                        yAxisId="left"
                        name="Curtailed Energy" 
                        dataKey="curtailedMWh" 
                        fill="#ef4444" 
                        radius={[4, 4, 0, 0]}
                        opacity={0.8}
                      />
                      <Bar 
                        yAxisId="right"
                        name="Curtailment %" 
                        dataKey="percentage" 
                        fill="#f59e0b"
                        radius={[4, 4, 0, 0]}
                      >
                        {data.map((entry, index) => (
                          <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          ) : !loading ? (
            <div className="flex flex-col items-center justify-center h-80 text-center bg-card rounded-lg border border-border/40 shadow-sm p-6">
              <p className="text-xl font-medium mb-2">No Data Available</p>
              <p className="text-muted-foreground">No curtailment data found for the selected filters.</p>
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}