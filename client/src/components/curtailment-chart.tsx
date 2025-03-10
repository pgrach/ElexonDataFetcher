import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DualAxisChart } from "@/components/ui/dual-axis-chart";

interface CurtailmentChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

export default function CurtailmentChart({ timeframe, date, minerModel, farmId }: CurtailmentChartProps) {
  const formattedDate = format(date, "yyyy-MM-dd");
  
  // Fetch hourly data for the chart
  const { data: hourlyData = [], isLoading } = useQuery({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, farmId],
    enabled: timeframe === "daily" && !!formattedDate,
  });

  // Function to check if an hour is in the future
  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(":").map(Number);
    const now = new Date();
    const selectedDate = new Date(date);

    if (format(now, "yyyy-MM-dd") === format(selectedDate, "yyyy-MM-dd")) {
      return hour > now.getHours();
    }
    return selectedDate > now;
  };

  const chartData = hourlyData.map((hour: any) => ({
    hour: `${hour.hour.split(":")[0].padStart(2, "0")}:00`,
    curtailedEnergy: isHourInFuture(hour.hour) ? null : hour.curtailedEnergy,
    bitcoinPotential: isHourInFuture(hour.hour) ? null : hour.curtailedEnergy * 0.00077, // Simplified calculation for demo
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle>Hourly Curtailment & Bitcoin Potential</CardTitle>
        <CardDescription>
          Average hourly breakdown for the selected period
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          {isLoading ? (
            <div className="flex h-full items-center justify-center">
              <div className="text-muted-foreground">Loading chart data...</div>
            </div>
          ) : chartData.length > 0 ? (
            <DualAxisChart
              data={chartData}
              chartConfig={{
                leftAxis: {
                  label: "Curtailed Energy (MWh)",
                  dataKey: "curtailedEnergy",
                  color: "#10b981",
                },
                rightAxis: {
                  label: "Bitcoin Potential (BTC)",
                  dataKey: "bitcoinPotential",
                  color: "#f59e0b",
                },
              }}
            />
          ) : (
            <div className="flex h-full items-center justify-center">
              <div className="text-muted-foreground">No data available for this period</div>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}