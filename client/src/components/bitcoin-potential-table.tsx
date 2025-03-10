import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

interface BitcoinPotentialTableProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

export default function BitcoinPotentialTable({
  timeframe,
  date,
  minerModel,
  farmId,
}: BitcoinPotentialTableProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Detailed Bitcoin Mining Data</CardTitle>
        <CardDescription>
          Full breakdown of curtailment and potential Bitcoin mining statistics
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="h-[350px]">
          <div className="flex h-full items-center justify-center">
            <div className="text-muted-foreground">Detailed data table will be shown here</div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}