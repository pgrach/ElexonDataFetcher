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

interface LeadPartyCurtailmentChartProps {
  timeframe: string;
  date: Date;
}

interface LeadPartyData {
  leadPartyName: string;
  totalCapacity: number;
  totalPotentialGeneration: number;
  totalCurtailedEnergy: number;
  overallCurtailmentPercentage: number;
}

export default function LeadPartyCurtailmentChart({ 
  timeframe, 
  date
}: LeadPartyCurtailmentChartProps) {

  const formattedDate = timeframe === "daily" 
    ? format(date, "yyyy-MM-dd")
    : timeframe === "monthly" 
      ? format(date, "yyyy-MM") 
      : format(date, "yyyy");

  const period = timeframe === "daily" ? "day" : timeframe === "monthly" ? "month" : "year";

  // First, get the list of lead parties that have curtailment data for this date
  const { data: leadParties, isLoading: isLeadPartiesLoading } = useQuery({
    queryKey: ["lead-parties", formattedDate],
    queryFn: async () => {
      const response = await axios.get<string[]>(`/api/lead-parties/${formattedDate}`);
      return response.data;
    },
  });

  // Then, fetch curtailment data for each lead party
  const { data: leadPartyData, isLoading: isLeadPartyDataLoading } = useQuery({
    queryKey: ["lead-party-curtailment", period, formattedDate, leadParties?.length],
    queryFn: async () => {
      if (!leadParties || leadParties.length === 0) return [];
      
      // Fetch data for each lead party in parallel
      const leadPartyPromises = leadParties.map(async (leadParty) => {
        try {
          const response = await axios.get(`/api/curtailment-analytics/lead-party/${encodeURIComponent(leadParty)}?period=${period}&value=${formattedDate}`);
          return {
            leadPartyName: response.data.leadPartyName,
            totalCapacity: response.data.totalCapacity,
            totalPotentialGeneration: response.data.totalPotentialGeneration,
            totalCurtailedEnergy: response.data.totalCurtailedEnergy,
            overallCurtailmentPercentage: response.data.overallCurtailmentPercentage
          };
        } catch (error) {
          console.error(`Error fetching data for ${leadParty}:`, error);
          return null;
        }
      });
      
      // Wait for all requests to complete
      const results = await Promise.all(leadPartyPromises);
      
      // Filter out any failed requests and sort by curtailment percentage (highest first)
      return results
        .filter((result): result is LeadPartyData => result !== null)
        .sort((a, b) => b.overallCurtailmentPercentage - a.overallCurtailmentPercentage);
    },
    enabled: !!leadParties && leadParties.length > 0,
  });

  const isLoading = isLeadPartiesLoading || isLeadPartyDataLoading;

  if (isLoading) {
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

  // Prepare chart data
  const chartData = leadPartyData?.map(party => ({
    name: party.leadPartyName,
    capacity: Number(party.totalCapacity.toFixed(2)),
    curtailedEnergy: Number(party.totalCurtailedEnergy.toFixed(2)),
    curtailmentPercentage: Number(party.overallCurtailmentPercentage.toFixed(2)),
    potentialGeneration: Number(party.totalPotentialGeneration.toFixed(2))
  })) || [];

  // Different colors for bars
  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#a569bd', '#5dade2', '#58d68d', '#f4d03f', '#e67e22', '#ec7063'];

  return (
    <div className="space-y-8">
      {/* Lead Party Curtailment Percentage Chart */}
      <Card>
        <CardHeader>
          <CardTitle>Wind Farm Curtailment Percentage by Lead Party</CardTitle>
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
          <div className="mb-2">
            <p className="text-sm text-muted-foreground">
              Each bar represents the overall curtailment percentage for a lead party, combining data from all their BMUs
            </p>
          </div>
          {chartData.length === 0 ? (
            <div className="p-8 text-center">
              <p className="text-lg font-medium text-muted-foreground">No curtailment data available for this period</p>
            </div>
          ) : (
            <div className="h-[400px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={chartData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 70 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="name" 
                    angle={-45} 
                    textAnchor="end" 
                    height={100}
                    label={{ value: "Lead Party", position: "insideBottom", offset: -10 }}
                  />
                  <YAxis
                    label={{ value: "Curtailment %", angle: -90, position: "insideLeft" }}
                    domain={[0, 100]}
                  />
                  <Tooltip 
                    formatter={(value, name) => {
                      if (name === "curtailmentPercentage") return [`${value}%`, "Curtailment %"];
                      if (name === "curtailedEnergy") return [`${value} MWh`, "Curtailed Energy"];
                      if (name === "capacity") return [`${value} MW`, "Capacity"];
                      if (name === "potentialGeneration") return [`${value} MWh`, "Potential Generation"];
                      return [value, name];
                    }}
                    labelFormatter={(value) => `${value}`}
                  />
                  <Legend />
                  <Bar 
                    dataKey="curtailmentPercentage" 
                    name="Curtailment %" 
                    fill="#0088FE"
                  >
                    {chartData?.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}