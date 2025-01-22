import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { Calendar } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Calendar as CalendarIcon } from "lucide-react";

// Define the API response types based on our schema
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

export default function Home() {
  const [date, setDate] = useState<Date>(new Date("2024-07-01")); // Default to July 2024

  const { data: dailyData, isLoading: isDailyLoading, error: dailyError } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${format(date, 'yyyy-MM-dd')}`],
    enabled: !!date
  });

  const { data: monthlyData, isLoading: isMonthlyLoading, error: monthlyError } = useQuery<MonthlySummary>({
    queryKey: [`/api/summary/monthly/${format(date, 'yyyy-MM')}`],
    enabled: !!date
  });

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
                  // Allow dates from July 2024 to December 2024
                  return date < new Date("2024-07-01") || date > new Date("2024-12-31");
                }}
              />
            </CardContent>
          </Card>
        </div>

        <div className="space-y-8">
          {/* Monthly Summary Section */}
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

          {/* Daily Summary Section */}
          <div className="grid md:grid-cols-2 gap-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">
                  Daily Curtailed Energy
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
                  Daily Payment
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
        </div>
      </div>
    </div>
  );
}