import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, isValid } from "date-fns";
import { Calendar } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Calendar as CalendarIcon, Building } from "lucide-react";
import { ChartContainer, ChartTooltip } from "@/components/ui/chart";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, ResponsiveContainer } from "recharts";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface DailySummary {
  date: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
  leadParty: string | null;
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

export default function Home() {
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2023-01-01");
    return today < startDate ? startDate : today;
  });
  const [selectedLeadParty, setSelectedLeadParty] = useState<string | null>(null);

  // Fetch lead parties for the dropdown
  const { data: leadParties = [] } = useQuery<string[]>({
    queryKey: ['/api/lead-parties'],
  });

  const formattedDate = format(date, 'yyyy-MM-dd');

  const { data: dailyData, isLoading: isDailyLoading, error: dailyError } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${formattedDate}`, selectedLeadParty],
    queryFn: async () => {
      // Validate date before making the request
      if (!isValid(date)) {
        throw new Error('Invalid date selected');
      }

      const url = new URL(`/api/summary/daily/${formattedDate}`, window.location.origin);
      if (selectedLeadParty && selectedLeadParty !== 'all') {
        url.searchParams.set('leadParty', selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!formattedDate && isValid(date)
  });


  // Monthly data query remains unchanged
  const { data: monthlyData, isLoading: isMonthlyLoading, error: monthlyError } = useQuery<MonthlySummary>({
    queryKey: [`/api/summary/monthly/${format(date, 'yyyy-MM')}`],
    enabled: !!date && isValid(date)
  });

  // Fetch hourly data with improved error handling
  const { data: hourlyData, isLoading: isHourlyLoading } = useQuery<HourlyData[]>({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, selectedLeadParty],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error('Invalid date selected');
      }

      const url = new URL(`/api/curtailment/hourly/${formattedDate}`, window.location.origin);
      if (selectedLeadParty) {
        url.searchParams.set('leadParty', selectedLeadParty);
      }
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch hourly data');
      }
      return response.json();
    },
    enabled: !!formattedDate && isValid(date)
  });

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

          <Card>
            <CardHeader>
              <CardTitle>Select Farm</CardTitle>
            </CardHeader>
            <CardContent>
              <Select
                value={selectedLeadParty || 'all'}
                onValueChange={(value) => setSelectedLeadParty(value === 'all' ? null : value)}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="All Farms" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Farms</SelectItem>
                  {leadParties.map((party) => (
                    <SelectItem key={party} value={party}>
                      {party}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </CardContent>
          </Card>
        </div>

        <div className="space-y-8">
          <div className="grid md:grid-cols-2 gap-4">
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
                  Monthly Payment
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
                  {selectedLeadParty ? 'Farm Curtailed Energy' : 'Daily Curtailed Energy'}
                </CardTitle>
                <Wind className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                {isDailyLoading ? (
                  <div className="text-2xl font-bold animate-pulse">Loading...</div>
                ) : dailyError ? (
                  <div className="text-sm text-red-500">
                    {dailyError instanceof Error ? dailyError.message : 'Failed to load daily data'}
                  </div>
                ) : dailyData ? (
                  <div className="text-2xl font-bold">
                    {Number(dailyData.totalCurtailedEnergy).toLocaleString()} MWh
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No daily data available</div>
                )}
                <div className="text-xs text-muted-foreground mt-1">
                  {selectedLeadParty ? (
                    <>Farm curtailed energy for {selectedLeadParty}</>
                  ) : (
                    <>Daily curtailed energy for {format(date, 'MMM d, yyyy')}</>
                  )}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">
                  {selectedLeadParty ? 'Farm Payment' : 'Daily Payment'}
                </CardTitle>
                <Building className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                {isDailyLoading ? (
                  <div className="text-2xl font-bold animate-pulse">Loading...</div>
                ) : dailyError ? (
                  <div className="text-sm text-red-500">
                    {dailyError instanceof Error ? dailyError.message : 'Failed to load daily data'}
                  </div>
                ) : dailyData ? (
                  <div className="text-2xl font-bold">
                    £{Number(dailyData.totalPayment).toLocaleString()}
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No daily data available</div>
                )}
                <div className="text-xs text-muted-foreground mt-1">
                  {selectedLeadParty ? (
                    <>Farm payment for {selectedLeadParty}</>
                  ) : (
                    <>Daily payment for {format(date, 'MMM d, yyyy')}</>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>
                Hourly Curtailment
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-[400px] w-full">
                {isHourlyLoading ? (
                  <div className="h-full flex items-center justify-center">
                    <div className="animate-pulse">Loading chart data...</div>
                  </div>
                ) : hourlyData ? (
                  <ResponsiveContainer width="100%" height="100%">
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
                        fill="hsl(var(--primary))"
                      />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-full flex items-center justify-center text-muted-foreground">
                    No hourly data available
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}