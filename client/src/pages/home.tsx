import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { Calendar } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Calendar as CalendarIcon, Factory } from "lucide-react";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ResizablePanelGroup, ResizablePanel, ResizableHandle } from "@/components/ui/resizable";
import { ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent } from "@/components/ui/chart";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid } from "recharts";

interface DailySummary {
  date: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
  recordTotals: {
    totalVolume: number;
    totalPayment: number;
  };
}

interface MonthlySummary {
  yearMonth: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
  dailyTotals: {
    totalCurtailedEnergy: number;
    totalPayment: number;
  };
}

interface HourlyData {
  hour: string;
  curtailedEnergy: number;
}

interface FarmDailySummary {
  farmId: string;
  summaryDate: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
  averageOriginalPrice: number;
  averageFinalPrice: number;
  curtailmentEvents: number;
  soFlaggedEvents: number;
  cadlFlaggedEvents: number;
}

interface FarmDateRangeSummary {
  farmId: string;
  startDate: string;
  endDate: string;
  periodTotals: {
    totalCurtailedEnergy: number;
    totalPayment: number;
    totalEvents: number;
    daysWithCurtailment: number;
    averageOriginalPrice: number;
    averageFinalPrice: number;
  };
  dailySummaries: FarmDailySummary[];
}

// Mock data for farm IDs - replace with actual API call later
const FARM_IDS = ["FARM001", "FARM002", "FARM003", "FARM004", "FARM005"];

export default function Home() {
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2023-01-01");
    return today < startDate ? startDate : today;
  });

  const [selectedFarmId, setSelectedFarmId] = useState<string>(FARM_IDS[0]);

  const { data: dailyData, isLoading: isDailyLoading } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${format(date, 'yyyy-MM-dd')}`],
    enabled: !!date
  });

  const { data: monthlyData, isLoading: isMonthlyLoading } = useQuery<MonthlySummary>({
    queryKey: [`/api/summary/monthly/${format(date, 'yyyy-MM')}`],
    enabled: !!date
  });

  const { data: hourlyData, isLoading: isHourlyLoading } = useQuery<HourlyData[]>({
    queryKey: [`/api/curtailment/hourly/${format(date, 'yyyy-MM-dd')}`],
    enabled: !!date
  });

  const { data: farmDailyData, isLoading: isFarmDailyLoading } = useQuery<FarmDailySummary>({
    queryKey: [`/api/farms/${selectedFarmId}/summary/${format(date, 'yyyy-MM-dd')}`],
    enabled: !!date && !!selectedFarmId
  });

  const { data: farmRangeData, isLoading: isFarmRangeLoading } = useQuery<FarmDateRangeSummary>({
    queryKey: [`/api/farms/${selectedFarmId}/summaries`],
    queryFn: async () => {
      const startDate = format(new Date(date.getFullYear(), date.getMonth(), 1), 'yyyy-MM-dd');
      const endDate = format(new Date(date.getFullYear(), date.getMonth() + 1, 0), 'yyyy-MM-dd');
      const response = await fetch(`/api/farms/${selectedFarmId}/summaries?startDate=${startDate}&endDate=${endDate}`);
      if (!response.ok) throw new Error('Failed to fetch farm range data');
      return response.json();
    },
    enabled: !!date && !!selectedFarmId
  });

  const chartConfig = {
    curtailedEnergy: {
      label: "Curtailed Energy (MWh)",
      color: "hsl(var(--primary))"
    }
  };

  // Function to check if a given hour is in the future
  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(':').map(Number);
    const now = new Date();
    const selectedDate = new Date(date);

    if (format(now, 'yyyy-MM-dd') === format(selectedDate, 'yyyy-MM-dd')) {
      return hour > now.getHours();
    }
    return selectedDate > now;
  };

  return (
    <div className="container mx-auto py-8">
      <h1 className="text-4xl font-bold mb-8">Wind Farm Curtailment Data</h1>

      <ResizablePanelGroup direction="horizontal" className="min-h-[800px] rounded-lg border">
        <ResizablePanel defaultSize={25}>
          <div className="p-4 space-y-4">
            <Card>
              <CardHeader>
                <CardTitle>Select Farm</CardTitle>
              </CardHeader>
              <CardContent>
                <Select value={selectedFarmId} onValueChange={setSelectedFarmId}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select a farm" />
                  </SelectTrigger>
                  <SelectContent>
                    {FARM_IDS.map(farmId => (
                      <SelectItem key={farmId} value={farmId}>
                        {farmId}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Select Date</CardTitle>
              </CardHeader>
              <CardContent>
                <Calendar
                  mode="single"
                  selected={date}
                  onSelect={(newDate) => newDate && setDate(newDate)}
                  disabled={(date) => {
                    const startDate = new Date("2023-01-01");
                    startDate.setHours(0, 0, 0, 0);
                    const currentDate = new Date();
                    return date < startDate || date > currentDate;
                  }}
                />
              </CardContent>
            </Card>
          </div>
        </ResizablePanel>

        <ResizableHandle withHandle />

        <ResizablePanel defaultSize={75}>
          <div className="p-4 space-y-8">
            <div className="grid md:grid-cols-2 gap-4">
              {/* Overall Summary Cards */}
              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">
                    Monthly Curtailed Energy
                  </CardTitle>
                  <CalendarIcon className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                  {isMonthlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold">
                      {Number(monthlyData.totalCurtailedEnergy).toLocaleString()} MWh
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No monthly data available</div>
                  )}
                  <div className="text-xs text-muted-foreground mt-1">
                    Total curtailed energy for {format(date, 'MMMM yyyy')}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">
                    Monthly Payment
                  </CardTitle>
                  <Battery className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                  {isMonthlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold">
                      £{Number(monthlyData.totalPayment).toLocaleString()}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No monthly data available</div>
                  )}
                  <div className="text-xs text-muted-foreground mt-1">
                    Total payment for {format(date, 'MMMM yyyy')}
                  </div>
                </CardContent>
              </Card>

              {/* Farm-level Summary Cards */}
              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">
                    Farm Daily Curtailed Energy
                  </CardTitle>
                  <Factory className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                  {isFarmDailyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : farmDailyData ? (
                    <div className="text-2xl font-bold">
                      {Number(farmDailyData.totalCurtailedEnergy).toLocaleString()} MWh
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No farm data available</div>
                  )}
                  <div className="text-xs text-muted-foreground mt-1">
                    Farm {selectedFarmId} curtailment for {format(date, 'MMM d, yyyy')}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">
                    Farm Daily Events
                  </CardTitle>
                  <Wind className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                  {isFarmDailyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : farmDailyData ? (
                    <div>
                      <div className="text-2xl font-bold">
                        {farmDailyData.curtailmentEvents}
                      </div>
                      <div className="text-xs text-muted-foreground mt-1">
                        SO Flagged: {farmDailyData.soFlaggedEvents} | CADL Flagged: {farmDailyData.cadlFlaggedEvents}
                      </div>
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No event data available</div>
                  )}
                  <div className="text-xs text-muted-foreground mt-1">
                    Events for {format(date, 'MMM d, yyyy')}
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Charts */}
            <div className="grid gap-8">
              {/* Hourly Curtailment Chart */}
              <Card>
                <CardHeader>
                  <CardTitle>Hourly Curtailment</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-[300px] w-full">
                    {isHourlyLoading ? (
                      <div className="h-full flex items-center justify-center">
                        <div className="animate-pulse">Loading chart data...</div>
                      </div>
                    ) : hourlyData ? (
                      <ChartContainer config={chartConfig}>
                        <BarChart 
                          data={hourlyData}
                          margin={{ top: 20, right: 30, left: 60, bottom: 20 }}
                        >
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis 
                            dataKey="hour" 
                            interval={2}
                            tick={{ fontSize: 12 }}
                          />
                          <YAxis
                            label={{ 
                              value: 'Curtailed Energy (MWh)', 
                              angle: -90, 
                              position: 'insideLeft',
                              offset: -40,
                              style: { fontSize: 12 }
                            }}
                            tick={{ fontSize: 12 }}
                          />
                          <ChartTooltip
                            content={({ active, payload }) => {
                              if (!active || !payload?.length) return null;
                              const data = payload[0];
                              const hour = data.payload.hour;
                              const value = Number(data.value);

                              let message = "";
                              if (isHourInFuture(hour)) {
                                message = "Data not available yet";
                              } else if (value === 0) {
                                message = "No curtailment detected";
                              } else {
                                message = `${value.toFixed(2)} MWh`;
                              }

                              return (
                                <div className="rounded-lg border bg-background p-2 shadow-md">
                                  <div className="grid gap-2">
                                    <div className="flex items-center gap-2">
                                      <div className="h-2 w-2 rounded-full bg-primary" />
                                      <span className="font-medium">{hour}</span>
                                    </div>
                                    <div className="text-sm text-muted-foreground">
                                      {message}
                                    </div>
                                  </div>
                                </div>
                              );
                            }}
                          />
                          <Bar
                            dataKey="curtailedEnergy"
                            name="Curtailed Energy"
                            fill="hsl(var(--primary))"
                          />
                          <ChartLegend
                            content={({ payload }) => (
                              <ChartLegendContent payload={payload} />
                            )}
                          />
                        </BarChart>
                      </ChartContainer>
                    ) : (
                      <div className="h-full flex items-center justify-center text-muted-foreground">
                        No hourly data available
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>

              {/* Farm Monthly Trend Chart */}
              <Card>
                <CardHeader>
                  <CardTitle>Farm Monthly Trend</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-[300px] w-full">
                    {isFarmRangeLoading ? (
                      <div className="h-full flex items-center justify-center">
                        <div className="animate-pulse">Loading chart data...</div>
                      </div>
                    ) : farmRangeData ? (
                      <ChartContainer config={chartConfig}>
                        <BarChart 
                          data={farmRangeData.dailySummaries}
                          margin={{ top: 20, right: 30, left: 60, bottom: 20 }}
                        >
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis 
                            dataKey="summaryDate" 
                            interval={2}
                            tick={{ fontSize: 12 }}
                          />
                          <YAxis
                            label={{ 
                              value: 'Curtailed Energy (MWh)', 
                              angle: -90, 
                              position: 'insideLeft',
                              offset: -40,
                              style: { fontSize: 12 }
                            }}
                            tick={{ fontSize: 12 }}
                          />
                          <ChartTooltip />
                          <Bar
                            dataKey="totalCurtailedEnergy"
                            name="Curtailed Energy"
                            fill="hsl(var(--primary))"
                          />
                          <ChartLegend />
                        </BarChart>
                      </ChartContainer>
                    ) : (
                      <div className="h-full flex items-center justify-center text-muted-foreground">
                        No farm trend data available
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        </ResizablePanel>
      </ResizablePanelGroup>
    </div>
  );
}