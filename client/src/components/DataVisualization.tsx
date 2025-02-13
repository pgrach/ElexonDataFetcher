"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import { DualAxisChart } from "@/components/ui/dual-axis-chart"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

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
    queryFn: async () => {
      const response = await fetch(`/api/summary/daily/${formattedDate}${selectedLeadParty ? `?leadParty=${selectedLeadParty}` : ''}`);
      if (response.status === 404) {
        return { totalCurtailedEnergy: 0, totalPayment: 0 };
      }
      if (!response.ok) {
        throw new Error('Failed to fetch daily data');
      }
      return response.json();
    },
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
    queryFn: async () => {
      const url = new URL("/api/curtailment/mining-potential", window.location.origin);
      url.searchParams.set("date", formattedDate);
      url.searchParams.set("minerModel", selectedMinerModel);
      if (selectedLeadParty) {
        url.searchParams.set("leadParty", selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch mining potential');
      }
      return response.json();
    },
    enabled: !!formattedDate && !!dailyData?.totalCurtailedEnergy,
  })

  const { data: hourlyData, isLoading: isHourlyLoading } = useQuery<HourlyData[]>({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, selectedLeadParty],
    queryFn: async () => {
      const url = new URL(`/api/curtailment/hourly/${formattedDate}`, window.location.origin);
      if (selectedLeadParty) {
        url.searchParams.set("leadParty", selectedLeadParty);
      }
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch hourly data');
      }
      return response.json();
    },
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
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle>Hourly Curtailment Analysis</CardTitle>
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger className="text-sm text-muted-foreground">
              Network Difficulty: {bitcoinPotential?.difficulty ? bitcoinPotential.difficulty.toLocaleString() : 'Not available'}
            </TooltipTrigger>
            <TooltipContent>
              <p>Current BTC Price: £{(bitcoinPotential?.currentPrice || 0).toLocaleString()}</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
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