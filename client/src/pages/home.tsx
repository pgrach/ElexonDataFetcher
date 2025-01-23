import { useEffect, useState } from "react";
import { useLocation } from "wouter";
import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Bitcoin, PoundSterling } from "lucide-react";
import { Calendar } from "@/components/ui/calendar";
import type { DailySummary, MonthlySummary } from "@/types";
import { useToast } from "@/hooks/use-toast";

export default function Home() {
  const [location, navigate] = useLocation();
  const { toast } = useToast();

  // Get date from URL or use today
  const urlDate = location.split("/")[1];
  const initialDate = urlDate ? new Date(urlDate) : new Date();
  const formattedDate = format(initialDate, "yyyy-MM-dd");

  const {
    data: dailyData,
    error: dailyError,
    isLoading: isDailyLoading,
  } = useQuery<DailySummary>({
    queryKey: [`/api/summary/${formattedDate}`],
    enabled: !!formattedDate,
  });

  const {
    data: monthlyData,
    error: monthlyError,
    isLoading: isMonthlyLoading,
  } = useQuery<MonthlySummary>({
    queryKey: [`/api/summary/monthly/${format(initialDate, "yyyy-MM")}`],
    enabled: !!formattedDate,
  });

  useEffect(() => {
    if (dailyError || monthlyError) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to load data",
      });
    }
  }, [dailyError, monthlyError, toast]);

  const handleDateChange = (date: Date) => {
    navigate(`/${format(date, "yyyy-MM-dd")}`);
  };

  return (
    <div className="min-h-screen bg-gradient-to-b from-sky-100 to-white dark:from-sky-900 dark:to-gray-900">
      <div className="container mx-auto p-4 space-y-8">
        {/* Header */}
        <header className="py-6">
          <div className="flex flex-col gap-6">
            <div>
              <h1 className="text-4xl font-bold text-sky-900 dark:text-sky-100 flex items-center gap-2">
                <Wind className="h-8 w-8 text-sky-500" />
                Wind Farm Curtailment Dashboard
              </h1>
              <p className="text-xl text-sky-700 dark:text-sky-300">
                Monitoring and Analysis Platform
              </p>
            </div>
            <div className="flex flex-col sm:flex-row gap-4">
              <Card className="w-full sm:w-auto">
                <CardHeader>
                  <CardTitle className="text-lg">Select Date</CardTitle>
                </CardHeader>
                <CardContent>
                  <Calendar
                    mode="single"
                    selected={initialDate}
                    onSelect={(newDate) => newDate && handleDateChange(newDate)}
                    disabled={(date) => {
                      const startDate = new Date("2023-01-01");
                      startDate.setHours(0, 0, 0, 0);
                      return date < startDate || date > new Date();
                    }}
                  />
                </CardContent>
              </Card>
            </div>
          </div>
        </header>

        {/* Stats Grid */}
        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">
                Daily Curtailed Energy
              </CardTitle>
              <Wind className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              {isDailyLoading ? (
                <div className="text-2xl font-bold animate-pulse">Loading...</div>
              ) : dailyError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : (
                <div className="text-2xl font-bold">
                  {Number(dailyData?.totalCurtailedEnergy || 0).toLocaleString()} MWh
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">
                Monthly Curtailed Energy
              </CardTitle>
              <Battery className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              {isMonthlyLoading ? (
                <div className="text-2xl font-bold animate-pulse">Loading...</div>
              ) : monthlyError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : (
                <div className="text-2xl font-bold">
                  {Number(monthlyData?.totalCurtailedEnergy || 0).toLocaleString()} MWh
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">
                Potential BTC
              </CardTitle>
              <Bitcoin className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              {isDailyLoading ? (
                <div className="text-2xl font-bold animate-pulse">Loading...</div>
              ) : dailyError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : (
                <div className="text-2xl font-bold">
                  {Number(dailyData?.totalPotentialBtc || 0).toFixed(8)} BTC
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">
                Daily Payment
              </CardTitle>
              <PoundSterling className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              {isDailyLoading ? (
                <div className="text-2xl font-bold animate-pulse">Loading...</div>
              ) : dailyError ? (
                <div className="text-sm text-red-500">Failed to load data</div>
              ) : (
                <div className="text-2xl font-bold">
                  £{Number(dailyData?.totalPayment || 0).toLocaleString()}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Chart Section - To be implemented */}
        {dailyData && !dailyError && (
          <Card>
            <CardHeader>
              <CardTitle>Hourly Breakdown</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-[400px] flex items-center justify-center text-muted-foreground">
                Chart component will be implemented here
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}