import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, parseISO } from "date-fns";
import { Calendar } from "@/components/ui/calendar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, TrendingUp } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

interface DailySummary {
  summaryDate: string;
  totalCurtailedEnergy: string;
  totalPayment: string;
  createdAt: string;
}

export default function Home() {
  const [date, setDate] = useState<Date>(new Date("2024-12-31"));

  // Query for selected date's data
  const { data: dailyData } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${format(date, 'yyyy-MM-dd')}`],
    enabled: !!date,
  });

  // Query for December 2024 data
  const { data: decemberData } = useQuery<DailySummary[]>({
    queryKey: ['/api/summary/december-2024'],
  });

  const chartData = decemberData?.map(summary => ({
    date: format(parseISO(summary.summaryDate), 'MMM d'),
    energy: parseFloat(summary.totalCurtailedEnergy),
    payment: Math.abs(parseFloat(summary.totalPayment))
  }));

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
                  return date < new Date("2024-12-01") || date > new Date("2024-12-31");
                }}
              />
            </CardContent>
          </Card>
        </div>

        <div className="space-y-4">
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">
                  Total Curtailed Energy
                </CardTitle>
                <Wind className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                {dailyData ? (
                  <div className="text-2xl font-bold">
                    {Number(dailyData.totalCurtailedEnergy).toLocaleString()} MWh
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">Select a date</div>
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
                {dailyData ? (
                  <div className="text-2xl font-bold">
                    £{Math.abs(Number(dailyData.totalPayment)).toLocaleString()}
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">Select a date</div>
                )}
                <div className="text-xs text-muted-foreground mt-1">
                  Total payment for {format(date, 'MMM d, yyyy')}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">
                  December 2024 Overview
                </CardTitle>
                <TrendingUp className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                {decemberData ? (
                  <div className="text-2xl font-bold">
                    {decemberData.length} Days
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">Loading...</div>
                )}
                <div className="text-xs text-muted-foreground mt-1">
                  Days with curtailment data
                </div>
              </CardContent>
            </Card>
          </div>

          {chartData && (
            <Card>
              <CardHeader>
                <CardTitle>December 2024 Curtailment Trends</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="h-[400px] mt-4">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="date" />
                      <YAxis yAxisId="left" />
                      <YAxis yAxisId="right" orientation="right" />
                      <Tooltip />
                      <Line 
                        yAxisId="left"
                        type="monotone" 
                        dataKey="energy" 
                        stroke="hsl(var(--primary))" 
                        name="Energy (MWh)"
                      />
                      <Line 
                        yAxisId="right"
                        type="monotone" 
                        dataKey="payment" 
                        stroke="hsl(var(--destructive))" 
                        name="Payment (£)"
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}