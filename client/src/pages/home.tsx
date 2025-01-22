import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { Calendar } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Calendar as CalendarIcon, TrendingUp } from "lucide-react";

// Keep existing interfaces
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
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2023-01-01");
    return today < startDate ? startDate : today;
  });

  const { data: dailyData, isLoading: isDailyLoading, error: dailyError } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${format(date, 'yyyy-MM-dd')}`],
    enabled: !!date
  });

  const { data: monthlyData, isLoading: isMonthlyLoading, error: monthlyError } = useQuery<MonthlySummary>({
    queryKey: [`/api/summary/monthly/${format(date, 'yyyy-MM')}`],
    enabled: !!date
  });

  return (
    <div className="min-h-screen bg-gradient-to-b from-background to-accent/5">
      <div className="container mx-auto px-4 py-6 lg:py-8">
        <header className="mb-8">
          <h1 className="text-3xl md:text-4xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-primary to-primary/70">
            Wind Farm Curtailment Dashboard
          </h1>
          <p className="text-muted-foreground mt-2">
            Monitor and analyze wind farm curtailment data and payments
          </p>
        </header>

        <div className="grid lg:grid-cols-[280px,1fr] gap-6">
          {/* Calendar Section */}
          <div className="space-y-4">
            <Card className="border-accent/20 shadow-lg hover:shadow-xl transition-shadow duration-200">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <CalendarIcon className="h-5 w-5 text-primary" />
                  <span>Select Date</span>
                </CardTitle>
              </CardHeader>
              <CardContent className="px-3 pb-3">
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
                  className="w-full"
                />
              </CardContent>
            </Card>
          </div>

          {/* Statistics Grid */}
          <div className="space-y-6">
            {/* Monthly Summary Cards */}
            <div className="grid sm:grid-cols-2 gap-4">
              <Card className="border-accent/20 shadow-lg hover:shadow-xl transition-all duration-200 hover:scale-[1.02]">
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">
                    Monthly Curtailed Energy
                  </CardTitle>
                  <Wind className="h-5 w-5 text-primary" />
                </CardHeader>
                <CardContent>
                  {isMonthlyLoading ? (
                    <div className="h-8 w-32 bg-muted/20 animate-pulse rounded" />
                  ) : monthlyError ? (
                    <div className="text-sm text-destructive">Failed to load data</div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold text-primary">
                      {Number(monthlyData.totalCurtailedEnergy).toLocaleString()} MWh
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-2">
                    Total curtailed energy for {format(date, 'MMMM yyyy')}
                  </p>
                </CardContent>
              </Card>

              <Card className="border-accent/20 shadow-lg hover:shadow-xl transition-all duration-200 hover:scale-[1.02]">
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">
                    Monthly Payment
                  </CardTitle>
                  <TrendingUp className="h-5 w-5 text-primary" />
                </CardHeader>
                <CardContent>
                  {isMonthlyLoading ? (
                    <div className="h-8 w-32 bg-muted/20 animate-pulse rounded" />
                  ) : monthlyError ? (
                    <div className="text-sm text-destructive">Failed to load data</div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold text-primary">
                      £{Number(monthlyData.totalPayment).toLocaleString()}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-2">
                    Total payment for {format(date, 'MMMM yyyy')}
                  </p>
                </CardContent>
              </Card>
            </div>

            {/* Daily Summary Cards */}
            <div className="grid sm:grid-cols-2 gap-4">
              <Card className="border-accent/20 shadow-lg hover:shadow-xl transition-all duration-200 hover:scale-[1.02]">
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">
                    Daily Curtailed Energy
                  </CardTitle>
                  <Battery className="h-5 w-5 text-primary" />
                </CardHeader>
                <CardContent>
                  {isDailyLoading ? (
                    <div className="h-8 w-32 bg-muted/20 animate-pulse rounded" />
                  ) : dailyError ? (
                    <div className="text-sm text-destructive">Failed to load data</div>
                  ) : dailyData ? (
                    <div className="text-2xl font-bold text-primary">
                      {Number(dailyData.totalCurtailedEnergy).toLocaleString()} MWh
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-2">
                    Daily curtailed energy for {format(date, 'MMM d, yyyy')}
                  </p>
                </CardContent>
              </Card>

              <Card className="border-accent/20 shadow-lg hover:shadow-xl transition-all duration-200 hover:scale-[1.02]">
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                  <CardTitle className="text-sm font-medium">
                    Daily Payment
                  </CardTitle>
                  <TrendingUp className="h-5 w-5 text-primary" />
                </CardHeader>
                <CardContent>
                  {isDailyLoading ? (
                    <div className="h-8 w-32 bg-muted/20 animate-pulse rounded" />
                  ) : dailyError ? (
                    <div className="text-sm text-destructive">Failed to load data</div>
                  ) : dailyData ? (
                    <div className="text-2xl font-bold text-primary">
                      £{Number(dailyData.totalPayment).toLocaleString()}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-2">
                    Daily payment for {format(date, 'MMM d, yyyy')}
                  </p>
                </CardContent>
              </Card>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}