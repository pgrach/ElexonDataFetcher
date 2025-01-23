import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { Calendar } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Calendar as CalendarIcon } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
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

interface FarmData {
  id: string;
  name: string;
  location: string;
  capacity: number;
  curtailedEnergy: number;
  payment: number;
}

export default function Home() {
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2023-01-01");
    return today < startDate ? startDate : today;
  });

  const [selectedFarm, setSelectedFarm] = useState<string>("");

  const { data: dailyData, isLoading: isDailyLoading, error: dailyError } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${format(date, 'yyyy-MM-dd')}${selectedFarm ? `/${selectedFarm}` : ''}`],
    enabled: !!date
  });

  const { data: monthlyData, isLoading: isMonthlyLoading, error: monthlyError } = useQuery<MonthlySummary>({
    queryKey: [`/api/summary/monthly/${format(date, 'yyyy-MM')}${selectedFarm ? `/${selectedFarm}` : ''}`],
    enabled: !!date
  });

  const { data: hourlyData, isLoading: isHourlyLoading } = useQuery<HourlyData[]>({
    queryKey: [`/api/curtailment/hourly/${format(date, 'yyyy-MM-dd')}${selectedFarm ? `/${selectedFarm}` : ''}`],
    enabled: !!date
  });

  const { data: farms } = useQuery<FarmData[]>({
    queryKey: ['/api/farms'],
  });

  // Chart configuration that matches the ChartConfig type
  const chartConfig = {
    curtailedEnergy: {
      label: "Curtailed Energy",
      color: "hsl(var(--primary))"
    }
  };

  // Function to check if a given hour is in the future
  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(':').map(Number);
    const now = new Date();
    const selectedDate = new Date(date);

    // If the date is today, check the hour
    if (format(now, 'yyyy-MM-dd') === format(selectedDate, 'yyyy-MM-dd')) {
      return hour > now.getHours();
    }

    // If the date is in the future, all hours are in the future
    return selectedDate > now;
  };

  const renderDataCards = () => (
    <>
      <div className="grid md:grid-cols-2 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              {selectedFarm ? "Farm Monthly Curtailed Energy" : "Monthly Curtailed Energy"}
            </CardTitle>
            <CalendarIcon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {isMonthlyLoading ? (
              <div className="text-2xl font-bold animate-pulse">Loading...</div>
            ) : monthlyError ? (
              <div className="text-sm text-red-500">Failed to load monthly data</div>
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
              {selectedFarm ? "Farm Monthly Payment" : "Monthly Payment"}
            </CardTitle>
            <Battery className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {isMonthlyLoading ? (
              <div className="text-2xl font-bold animate-pulse">Loading...</div>
            ) : monthlyError ? (
              <div className="text-sm text-red-500">Failed to load monthly data</div>
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
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              {selectedFarm ? "Farm Daily Curtailed Energy" : "Daily Curtailed Energy"}
            </CardTitle>
            <Wind className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {isDailyLoading ? (
              <div className="text-2xl font-bold animate-pulse">Loading...</div>
            ) : dailyError ? (
              <div className="text-sm text-red-500">Failed to load daily data</div>
            ) : dailyData ? (
              <div className="text-2xl font-bold">
                {Number(dailyData.totalCurtailedEnergy).toLocaleString()} MWh
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">No daily data available</div>
            )}
            <div className="text-xs text-muted-foreground mt-1">
              Daily curtailed energy for {format(date, 'MMM d, yyyy')}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">
              {selectedFarm ? "Farm Daily Payment" : "Daily Payment"}
            </CardTitle>
            <Battery className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            {isDailyLoading ? (
              <div className="text-2xl font-bold animate-pulse">Loading...</div>
            ) : dailyError ? (
              <div className="text-sm text-red-500">Failed to load daily data</div>
            ) : dailyData ? (
              <div className="text-2xl font-bold">
                £{Number(dailyData.totalPayment).toLocaleString()}
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">No daily data available</div>
            )}
            <div className="text-xs text-muted-foreground mt-1">
              Daily payment for {format(date, 'MMM d, yyyy')}
            </div>
          </CardContent>
        </Card>
      </div>
    </>
  );

  return (
    <div className="container mx-auto py-8">
      <h1 className="text-4xl font-bold mb-8">Wind Farm Curtailment Data</h1>

      <Tabs defaultValue="aggregate" className="mb-8">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="aggregate">Aggregate View</TabsTrigger>
          <TabsTrigger value="farm-specific">Farm Specific</TabsTrigger>
        </TabsList>

        <TabsContent value="aggregate" className="mt-4">
          <div className="grid md:grid-cols-[300px,1fr] gap-8">
            <div className="space-y-4">
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

            <div className="space-y-8">
              {renderDataCards()}
              <div className="h-[400px] w-full">
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
            </div>
          </div>
        </TabsContent>

        <TabsContent value="farm-specific" className="mt-4">
          <div className="grid md:grid-cols-[300px,1fr] gap-8">
            <div className="space-y-4">
              <Card>
                <CardHeader>
                  <CardTitle>Select Farm</CardTitle>
                </CardHeader>
                <CardContent>
                  <Select value={selectedFarm} onValueChange={setSelectedFarm}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select a wind farm" />
                    </SelectTrigger>
                    <SelectContent>
                      {farms?.map((farm) => (
                        <SelectItem key={farm.id} value={farm.id}>
                          {farm.name}
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

            <div className="space-y-8">
              {selectedFarm ? (
                renderDataCards()
              ) : (
                <div className="flex items-center justify-center h-[200px] text-muted-foreground">
                  Please select a wind farm to view its data
                </div>
              )}
              {selectedFarm && (
                <div className="h-[400px] w-full">
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
              )}
            </div>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}