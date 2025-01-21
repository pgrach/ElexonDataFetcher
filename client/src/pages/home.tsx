import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { Calendar } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery } from "lucide-react";

export default function Home() {
  const [date, setDate] = useState<Date>(new Date("2024-12-01"));

  const { data, isLoading, error } = useQuery({
    queryKey: [`/api/summary/daily/${format(date, 'yyyy-MM-dd')}`],
    onError: (error) => {
      console.error('API Error:', error);
    }
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
                  // Allow dates from Dec 2024 to today
                  return date < new Date("2024-12-01") || date > new Date();
                }}
              />
            </CardContent>
          </Card>
        </div>

        <div className="space-y-4">
          <div className="grid md:grid-cols-2 gap-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">
                  Total Curtailed Energy
                </CardTitle>
                <Wind className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="text-2xl font-bold animate-pulse">Loading...</div>
                ) : error ? (
                  <div className="text-sm text-red-500">Failed to load data</div>
                ) : data ? (
                  <div className="text-2xl font-bold">
                    {Number(data.totalCurtailedEnergy).toLocaleString()} MWh
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No data available</div>
                )}
                <div className="text-xs text-muted-foreground mt-1">
                  Total curtailed energy for {format(date, 'MMM d, yyyy')}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">
                  Total Payment
                </CardTitle>
                <Battery className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="text-2xl font-bold animate-pulse">Loading...</div>
                ) : error ? (
                  <div className="text-sm text-red-500">Failed to load data</div>
                ) : data ? (
                  <div className="text-2xl font-bold">
                    £{Number(data.totalPayment).toLocaleString()}
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No data available</div>
                )}
                <div className="text-xs text-muted-foreground mt-1">
                  Total payment for {format(date, 'MMM d, yyyy')}
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}