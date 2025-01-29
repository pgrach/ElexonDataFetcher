import React, { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, isValid } from "date-fns";
import { Calendar } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Calendar as CalendarIcon, Building } from "lucide-react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip
} from "recharts";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

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

interface YearlySummary {
  year: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
}

function Home() {
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2023-01-01");
    return today < startDate ? startDate : today;
  });
  const [selectedLeadParty, setSelectedLeadParty] = useState<string | null>(null);

  const formattedDate = format(date, 'yyyy-MM-dd');

  const { data: curtailedLeadParties = [] } = useQuery<string[]>({
    queryKey: [`/api/lead-parties/${formattedDate}`],
    enabled: !!formattedDate && isValid(date)
  });

  useEffect(() => {
    if (selectedLeadParty && !curtailedLeadParties.includes(selectedLeadParty)) {
      setSelectedLeadParty(null);
    }
  }, [formattedDate, curtailedLeadParties, selectedLeadParty]);

  const { data: dailyData, isLoading: isDailyLoading, error: dailyError } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${formattedDate}`, selectedLeadParty],
    queryFn: async () => {
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

  const { data: monthlyData, isLoading: isMonthlyLoading, error: monthlyError } = useQuery<MonthlySummary>({
    queryKey: [`/api/summary/monthly/${format(date, 'yyyy-MM')}`, selectedLeadParty],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error('Invalid date selected');
      }

      const url = new URL(`/api/summary/monthly/${format(date, 'yyyy-MM')}`, window.location.origin);
      if (selectedLeadParty && selectedLeadParty !== 'all') {
        url.searchParams.set('leadParty', selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!date && isValid(date)
  });

  const { data: yearlyData, isLoading: isYearlyLoading, error: yearlyError } = useQuery<YearlySummary>({
    queryKey: [`/api/summary/yearly/${format(date, 'yyyy')}`, selectedLeadParty],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error('Invalid date selected');
      }

      const url = new URL(`/api/summary/yearly/${format(date, 'yyyy')}`, window.location.origin);
      if (selectedLeadParty && selectedLeadParty !== 'all') {
        url.searchParams.set('leadParty', selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!date && isValid(date)
  });

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

  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(':').map(Number);
    const now = new Date();
    const selectedDate = new Date(date);

    if (format(now, 'yyyy-MM-dd') === format(selectedDate, 'yyyy-MM-dd')) {
      return hour > now.getHours();
    }
    return selectedDate > now;
  };

  const CustomTooltip = ({ active, payload, label }: any) => {
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
  };

  return (
    <div className="container mx-auto py-8">
      <div className="mb-8">
        <h1 className="text-4xl font-bold mb-4">Wind Farm Curtailment Dashboard</h1>

        <div className="flex flex-col sm:flex-row gap-4 p-4 bg-muted rounded-lg">
          <Popover>
            <PopoverTrigger asChild>
              <Button
                variant="outline"
                className={cn(
                  "w-full sm:w-[240px] justify-start text-left font-normal",
                  !date && "text-muted-foreground"
                )}
              >
                <CalendarIcon className="mr-2 h-4 w-4" />
                {date ? format(date, "PPP") : <span>Pick a date</span>}
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="start">
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
                initialFocus
              />
            </PopoverContent>
          </Popover>

          <Select
            value={selectedLeadParty || 'all'}
            onValueChange={(value) => setSelectedLeadParty(value === 'all' ? null : value)}
          >
            <SelectTrigger className="w-full sm:w-[240px]">
              <SelectValue placeholder="Select Wind Farm" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Farms</SelectItem>
              {curtailedLeadParties.map((party) => (
                <SelectItem key={party} value={party}>
                  {party}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          {curtailedLeadParties.length === 0 && (
            <p className="text-sm text-muted-foreground my-auto">
              No farms were curtailed on this date
            </p>
          )}
        </div>
      </div>

      <div className="space-y-8">
        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
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
              <p className="text-xs text-muted-foreground mt-1">
                {selectedLeadParty ? (
                  <>Farm curtailed energy for {selectedLeadParty}</>
                ) : (
                  <>Daily curtailed energy for {format(date, 'MMM d, yyyy')}</>
                )}
              </p>
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
              <p className="text-xs text-muted-foreground mt-1">
                {selectedLeadParty ? (
                  <>Farm payment for {selectedLeadParty}</>
                ) : (
                  <>Daily payment for {format(date, 'MMM d, yyyy')}</>
                )}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty ? 'Farm Monthly Energy' : 'Monthly Energy'}
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
              <p className="text-xs text-muted-foreground mt-1">
                {format(date, 'MMMM yyyy')}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty ? 'Farm Monthly Payment' : 'Monthly Payment'}
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
              <p className="text-xs text-muted-foreground mt-1">
                {format(date, 'MMMM yyyy')}
              </p>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Hourly Curtailment</CardTitle>
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
                    <Tooltip content={<CustomTooltip />} />
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
  );
}

export default Home;