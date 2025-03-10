"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface BitcoinPotentialTableProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string; // Keeping farmId in prop interface for backward compatibility
}

export default function BitcoinPotentialTable({
  timeframe,
  date,
  minerModel,
  farmId,
}: BitcoinPotentialTableProps) {
  const formattedDate = format(date, "yyyy-MM-dd");
  const yearMonth = format(date, "yyyy-MM");
  const year = format(date, "yyyy");
  
  // Determine which bitcoin potential endpoint to use based on timeframe
  const bitcoinEndpoint = 
    timeframe === "yearly" ? `/api/mining-potential/yearly/${year}` :
    timeframe === "monthly" ? `/api/curtailment/monthly-mining-potential/${yearMonth}` :
    `/api/curtailment/mining-potential`;
  
  // Determine which summary endpoint to use based on timeframe  
  const summaryEndpoint = 
    timeframe === "yearly" ? `/api/summary/yearly/${year}` :
    timeframe === "monthly" ? `/api/summary/monthly/${yearMonth}` :
    `/api/summary/daily/${formattedDate}`;
  
  // Fetch summary data
  const { data: summaryData = {}, isLoading: isSummaryLoading } = useQuery({
    queryKey: [summaryEndpoint, farmId],
    queryFn: async () => {
      const url = new URL(summaryEndpoint, window.location.origin);
      if (farmId) {
        url.searchParams.set("leadParty", farmId);
      }
      
      const response = await fetch(url);
      if (!response.ok) {
        if (response.status === 404) {
          return { totalCurtailedEnergy: 0, totalPayment: 0 };
        }
        throw new Error(`API Error: ${response.status}`);
      }
      
      return response.json();
    }
  });
  
  // Fetch bitcoin data
  const { data: bitcoinData = {}, isLoading: isBitcoinLoading } = useQuery({
    queryKey: [bitcoinEndpoint, minerModel, farmId, summaryData.totalCurtailedEnergy],
    queryFn: async () => {
      const url = new URL(bitcoinEndpoint, window.location.origin);
      url.searchParams.set("minerModel", minerModel);
      
      if (farmId) {
        url.searchParams.set("leadParty", farmId);
      }
      
      // For daily view, we need to pass the energy value
      if (timeframe === "daily" && summaryData.totalCurtailedEnergy) {
        url.searchParams.set("date", formattedDate);
        url.searchParams.set("energy", summaryData.totalCurtailedEnergy.toString());
      }
      
      const response = await fetch(url);
      if (!response.ok) {
        if (response.status === 404) {
          return { 
            bitcoinMined: 0, 
            valueAtCurrentPrice: 0, 
            difficulty: 0, 
            price: 0,
            currentPrice: 0 
          };
        }
        throw new Error(`Failed to fetch mining potential`);
      }
      
      return response.json();
    },
    enabled: !!summaryData.totalCurtailedEnergy || timeframe !== "daily"
  });
  
  // Get title based on timeframe and farmId
  const title = 
    farmId ? 
      (timeframe === "yearly" ? `Farm Bitcoin Data (${year})` : 
       timeframe === "monthly" ? `Farm Bitcoin Data (${format(date, "MMMM yyyy")})` : 
       `Farm Bitcoin Data (${format(date, "PP")})`) :
      (timeframe === "yearly" ? `Bitcoin Mining Potential (${year})` : 
       timeframe === "monthly" ? `Bitcoin Mining Potential (${format(date, "MMMM yyyy")})` : 
       `Bitcoin Mining Potential (${format(date, "PP")})`);
  
  // Load state
  const isLoading = isSummaryLoading || isBitcoinLoading;
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
          </div>
        ) : (
          <Table>
            <TableCaption>Bitcoin mining data using {minerModel.replace("_", " ")} miners</TableCaption>
            <TableHeader>
              <TableRow>
                <TableHead>Metric</TableHead>
                <TableHead className="text-right">Value</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <TableRow>
                <TableCell>Curtailed Energy</TableCell>
                <TableCell className="text-right">{Number(summaryData.totalCurtailedEnergy).toLocaleString()} MWh</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>Bitcoin Potential</TableCell>
                <TableCell className="text-right">₿{Number(bitcoinData.bitcoinMined).toFixed(8)}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>Bitcoin Value (at current price)</TableCell>
                <TableCell className="text-right">£{Number(bitcoinData.valueAtCurrentPrice).toLocaleString('en-GB', { maximumFractionDigits: 2 })}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>Current Bitcoin Price</TableCell>
                <TableCell className="text-right">£{Number(bitcoinData.currentPrice).toLocaleString('en-GB', { maximumFractionDigits: 2 })}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>Curtailment Payment</TableCell>
                <TableCell className="text-right">£{Number(summaryData.totalPayment).toLocaleString()}</TableCell>
              </TableRow>
              {/* Add ratio comparison */}
              <TableRow>
                <TableCell className="font-medium">Value Ratio (Bitcoin/Curtailment)</TableCell>
                <TableCell className="text-right font-medium">
                  {summaryData.totalPayment !== 0 ? 
                    (Number(bitcoinData.valueAtCurrentPrice) / Math.abs(Number(summaryData.totalPayment))).toFixed(2) : 
                    'N/A'}x
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}