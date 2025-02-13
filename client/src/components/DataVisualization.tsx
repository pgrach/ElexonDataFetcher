"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import { DualAxisChart } from "@/components/ui/dual-axis-chart"

interface DataVisualizationProps {
  date: Date;
  selectedLeadParty: string | null;
  selectedMinerModel: string;
}

interface HourlyData {
  hour: string;
  curtailedEnergy: number;
}

export default function DataVisualization({ date, selectedLeadParty, selectedMinerModel }: DataVisualizationProps) {
  const formattedDate = format(date, "yyyy-MM-dd")

  const { data: dailyData } = useQuery<{
    totalCurtailedEnergy: number;
    totalPayment: number;
  }>({
    queryKey: [`/api/summary/daily/${formattedDate}`, selectedLeadParty],
    enabled: !!formattedDate,
  })

  const { data: bitcoinPotential } = useQuery({
    queryKey: [
      `/api/curtailment/mining-potential`,
      selectedMinerModel,
      dailyData?.totalCurtailedEnergy,
      selectedLeadParty,
      formattedDate 
    ],
    enabled: !!formattedDate && !!dailyData?.totalCurtailedEnergy,
  })

  const { data: hourlyData, isLoading: isHourlyLoading } = useQuery<HourlyData[]>({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, selectedLeadParty],
    enabled: !!formattedDate,
  })

  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(":").map(Number);
    const now = new Date();
    const selectedDate = new Date(date);

    if (format(now, "yyyy-MM-dd") === format(selectedDate, "yyyy-MM-dd")) {
      return hour > now.getHours();
    }
    return selectedDate > now;
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Hourly Curtailment Analysis</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-[400px]">
          {isHourlyLoading ? (
            <div className="h-full flex items-center justify-center">
              <div className="animate-pulse">Loading chart data...</div>
            </div>
          ) : hourlyData ? (
            <DualAxisChart
              data={hourlyData.map((hour) => ({
                hour: `${hour.hour.split(":")[0].padStart(2, "0")}:00`,
                curtailedEnergy: isHourInFuture(hour.hour)
                  ? 0
                  : hour.curtailedEnergy,
                bitcoinMined: isHourInFuture(hour.hour)
                  ? 0
                  : (hour.curtailedEnergy *
                      (bitcoinPotential?.bitcoinMined ?? 0)) /
                    (dailyData?.totalCurtailedEnergy ?? 1),
              }))}
              chartConfig={{
                leftAxis: {
                  label: "Curtailed Energy (MWh)",
                  dataKey: "curtailedEnergy",
                  color: "hsl(var(--primary))",
                },
                rightAxis: {
                  label: "Potential Bitcoin Mined (₿)",
                  dataKey: "bitcoinMined",
                  color: "#F7931A",
                },
              }}
            />
          ) : (
            <div className="h-full flex items-center justify-center text-muted-foreground">
              No hourly data available
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
