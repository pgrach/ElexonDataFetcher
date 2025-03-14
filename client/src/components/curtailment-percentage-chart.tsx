import { useQuery } from "@tanstack/react-query";
import axios from "axios";
import { format } from "date-fns";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

interface CurtailmentPercentageChartProps {
  timeframe: string;
  date: Date;
  leadParty?: string;
  farmId?: string;
}

interface FarmData {
  farmId: string;
  capacity: number;
  curtailedEnergy: number;
  curtailmentPercentage: number;
}

interface LeadPartyData {
  leadPartyName: string;
  farms: FarmData[];
  totalCapacity: number;
  totalPotentialGeneration: number;
  totalCurtailedEnergy: number;
  overallCurtailmentPercentage: number;
  periodCount: number;
  timeframe: string;
  value: string;
}

interface TopFarmData {
  farmId: string;
  leadPartyName: string;
  capacity: number;
  curtailedEnergy: number;
  totalPotentialGeneration: number;
  curtailmentPercentage: number;
}

export default function CurtailmentPercentageChart({ 
  timeframe, 
  date, 
  leadParty = "Viking Energy Wind Farm LLP", 
  farmId 
}: CurtailmentPercentageChartProps) {

  const formattedDate = timeframe === "daily" 
    ? format(date, "yyyy-MM-dd")
    : timeframe === "monthly" 
      ? format(date, "yyyy-MM") 
      : format(date, "yyyy");

  const period = timeframe === "daily" ? "day" : timeframe === "monthly" ? "month" : "year";

  // Get curtailment data for a specific lead party
  const { data: leadPartyData, isLoading: isLeadPartyLoading } = useQuery({
    queryKey: ["curtailment-analytics", "lead-party", leadParty, period, formattedDate],
    queryFn: async () => {
      const response = await axios.get<LeadPartyData>(
        `/api/curtailment-analytics/lead-party/${leadParty}?period=${period}&value=${formattedDate}`
      );
      return response.data;
    },
    enabled: !!leadParty,
  });

  // Get top curtailed farms data
  const { data: topFarmsData, isLoading: isTopFarmsLoading } = useQuery({
    queryKey: ["curtailment-analytics", "top-farms", period, formattedDate],
    queryFn: async () => {
      const response = await axios.get<TopFarmData[]>(
        `/api/curtailment-analytics/top-farms?period=${period}&value=${formattedDate}&limit=10`
      );
      return response.data;
    },
  });

  if (isLeadPartyLoading || isTopFarmsLoading) {
    return (
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <CardTitle className="flex justify-between">
              <Skeleton className="h-6 w-1/2" />
            </CardTitle>
            <CardDescription>
              <Skeleton className="h-4 w-2/3" />
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Skeleton className="h-[300px] w-full" />
          </CardContent>
        </Card>
      </div>
    );
  }

  // Prepare data for the lead party chart
  const leadPartyChartData = leadPartyData?.farms.map(farm => ({
    name: farm.farmId,
    capacity: farm.capacity,
    curtailedEnergy: Number(farm.curtailedEnergy.toFixed(2)),
    curtailmentPercentage: Number(farm.curtailmentPercentage.toFixed(2)),
  }));

  // Prepare data for the top farms chart
  const topFarmsChartData = topFarmsData?.map(farm => ({
    name: farm.farmId,
    leadParty: farm.leadPartyName,
    capacity: farm.capacity,
    curtailedEnergy: Number(farm.curtailedEnergy.toFixed(2)),
    curtailmentPercentage: Number(farm.curtailmentPercentage.toFixed(2)),
  }));

  // Different colors for bars
  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#a569bd', '#5dade2', '#58d68d', '#f4d03f', '#e67e22', '#ec7063'];

  return (
    <div className="space-y-8">
      {/* Lead Party Curtailment Percentage Chart */}
      {leadPartyData && (
        <Card>
          <CardHeader>
            <CardTitle>Curtailment Percentage for {leadPartyData.leadPartyName}</CardTitle>
            <CardDescription>
              {timeframe === "daily" 
                ? `Data for ${format(date, "MMMM d, yyyy")}`
                : timeframe === "monthly" 
                  ? `Data for ${format(date, "MMMM yyyy")}`
                  : `Data for ${format(date, "yyyy")}`
              }
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="mb-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm text-muted-foreground">Total Capacity</p>
                  <p className="text-2xl font-bold">{leadPartyData.totalCapacity.toFixed(2)} MW</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Overall Curtailment</p>
                  <p className="text-2xl font-bold">{leadPartyData.overallCurtailmentPercentage.toFixed(2)}%</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Total Curtailed Energy</p>
                  <p className="text-2xl font-bold">{leadPartyData.totalCurtailedEnergy.toFixed(2)} MWh</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Total Potential Generation</p>
                  <p className="text-2xl font-bold">{leadPartyData.totalPotentialGeneration.toFixed(2)} MWh</p>
                </div>
              </div>
            </div>
            <div className="h-[300px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={leadPartyChartData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 70 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="name" 
                    angle={-45} 
                    textAnchor="end" 
                    height={70}
                  />
                  <YAxis
                    label={{ value: "Curtailment %", angle: -90, position: "insideLeft" }}
                    domain={[0, 100]}
                  />
                  <Tooltip 
                    formatter={(value, name) => {
                      if (name === "curtailmentPercentage") return [`${value}%`, "Curtailment"];
                      if (name === "curtailedEnergy") return [`${value} MWh`, "Curtailed Energy"];
                      return [value, name];
                    }}
                  />
                  <Legend />
                  <Bar 
                    dataKey="curtailmentPercentage" 
                    name="Curtailment %" 
                    fill="#0088FE"
                  >
                    {leadPartyChartData?.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Top Farms Curtailment Percentage Chart */}
      {topFarmsData && (
        <Card>
          <CardHeader>
            <CardTitle>Top 10 Curtailed Wind Farms</CardTitle>
            <CardDescription>
              {timeframe === "daily" 
                ? `For ${format(date, "MMMM d, yyyy")}`
                : timeframe === "monthly" 
                  ? `For ${format(date, "MMMM yyyy")}`
                  : `For ${format(date, "yyyy")}`
              }
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="h-[300px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={topFarmsChartData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 70 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="name" 
                    angle={-45} 
                    textAnchor="end" 
                    height={70}
                  />
                  <YAxis
                    label={{ value: "Curtailment %", angle: -90, position: "insideLeft" }}
                    domain={[0, 100]}
                  />
                  <Tooltip 
                    formatter={(value, name) => {
                      if (name === "curtailmentPercentage") return [`${value}%`, "Curtailment %"];
                      if (name === "curtailedEnergy") return [`${value} MWh`, "Curtailed Energy"];
                      if (name === "leadParty") return [value, "Lead Party"];
                      if (name === "capacity") return [`${value} MW`, "Capacity"];
                      return [value, name];
                    }}
                  />
                  <Legend />
                  <Bar 
                    dataKey="curtailmentPercentage" 
                    name="Curtailment %" 
                    fill="#0088FE"
                  >
                    {topFarmsChartData?.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}