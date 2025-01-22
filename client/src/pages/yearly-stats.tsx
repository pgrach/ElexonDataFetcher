import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { useToast } from "@/hooks/use-toast";

interface YearlySummaryResponse {
  year: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
  monthlyTotals: {
    totalCurtailedEnergy: number;
    totalPayment: number;
  };
}

export default function YearlyStats() {
  const { toast } = useToast();
  const currentYear = new Date().getFullYear().toString();

  const { data: yearlyStats, isLoading, error } = useQuery<YearlySummaryResponse>({
    queryKey: [`/api/summary/yearly/${currentYear}`],
  });

  if (error) {
    toast({
      variant: "destructive",
      title: "Error",
      description: "Failed to load yearly statistics"
    });
  }

  if (isLoading) {
    return (
      <div className="container mx-auto p-4">
        <Card>
          <CardHeader>
            <CardTitle>Loading yearly statistics...</CardTitle>
          </CardHeader>
          <CardContent>
            <Progress value={100} className="animate-pulse" />
          </CardContent>
        </Card>
      </div>
    );
  }

  if (!yearlyStats) {
    return (
      <div className="container mx-auto p-4">
        <Card>
          <CardHeader>
            <CardTitle>No data available</CardTitle>
          </CardHeader>
          <CardContent>
            No yearly statistics found for {currentYear}
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="container mx-auto p-4">
      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Total Curtailed Energy {currentYear}</CardTitle>
          </CardHeader>
          <CardContent className="text-2xl font-bold">
            {yearlyStats.totalCurtailedEnergy.toFixed(2)} MWh
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Total Payments {currentYear}</CardTitle>
          </CardHeader>
          <CardContent className="text-2xl font-bold">
            £{yearlyStats.totalPayment.toFixed(2)}
          </CardContent>
        </Card>

        <Card className="md:col-span-2">
          <CardHeader>
            <CardTitle>Verification</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              <p>Monthly Totals:</p>
              <div className="grid grid-cols-2 gap-2">
                <div>Energy: {yearlyStats.monthlyTotals.totalCurtailedEnergy.toFixed(2)} MWh</div>
                <div>Payments: £{yearlyStats.monthlyTotals.totalPayment.toFixed(2)}</div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}