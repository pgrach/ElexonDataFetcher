import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

interface FarmComparisonChartProps {
  timeframe: string;
  date: Date;
  minerModel: string;
}

export default function FarmComparisonChart({ timeframe, date, minerModel }: FarmComparisonChartProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Farm Comparison</CardTitle>
        <CardDescription>
          Compare curtailment and Bitcoin potential across different wind farms
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          <div className="flex h-full items-center justify-center">
            <div className="text-muted-foreground">Farm comparison visualization will be shown here</div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}