"use client";

import { useQuery } from "@tanstack/react-query";
import { format } from "date-fns";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertTriangle, TrendingUp, TrendingDown, Minus } from "lucide-react";

interface CurtailmentComparisonCardProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

export default function CurtailmentComparisonCard({ 
  timeframe, 
  date, 
  minerModel, 
  farmId 
}: CurtailmentComparisonCardProps) {
  // Format dates based on timeframe
  const formattedDate = format(date, "yyyy-MM-dd");
  const yearMonth = format(date, "yyyy-MM");
  const year = format(date, "yyyy");
  
  // Determine which endpoints to use based on timeframe
  const summaryEndpoint = 
    timeframe === "yearly" ? `/api/summary/yearly/${year}` :
    timeframe === "monthly" ? `/api/summary/monthly/${yearMonth}` :
    `/api/summary/daily/${formattedDate}`;
  
  const bitcoinEndpoint = 
    timeframe === "yearly" ? `/api/mining-potential/yearly/${year}` :
    timeframe === "monthly" ? `/api/curtailment/monthly-mining-potential/${yearMonth}` :
    `/api/curtailment/mining-potential`;
  
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
  
  // Calculate difference and percentage
  const curtailmentPayment = Number(summaryData.totalPayment) || 0;
  const bitcoinValue = Number(bitcoinData.valueAtCurrentPrice) || 0;
  const difference = bitcoinValue - curtailmentPayment;
  const percentageDiff = curtailmentPayment > 0 
    ? ((difference / curtailmentPayment) * 100) 
    : 0;
  
  // Determine if Bitcoin mining is better
  const isBitcoinBetter = difference > 0;
  
  // Helper for displaying timeframe-specific text
  const timeframeLabel = 
    timeframe === "yearly" ? format(date, "yyyy") :
    timeframe === "monthly" ? format(date, "MMMM yyyy") :
    format(date, "PP");
  
  return (
    <Card className={isBitcoinBetter ? "border-green-500/30" : 
                      difference < 0 ? "border-red-500/30" : "border-primary/20"}>
      <CardHeader className="pb-2">
        <CardTitle className="text-lg flex items-center justify-between">
          <span>Payment vs. Mining Value Comparison</span>
          {isBitcoinBetter ? (
            <TrendingUp className="h-5 w-5 text-green-500" />
          ) : difference < 0 ? (
            <TrendingDown className="h-5 w-5 text-red-500" />
          ) : (
            <Minus className="h-5 w-5 text-primary" />
          )}
        </CardTitle>
      </CardHeader>
      
      <CardContent>
        {isSummaryLoading || isBitcoinLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-24 w-full" />
          </div>
        ) : (
          <>
            <div className="grid grid-cols-2 gap-4 mb-4">
              <div className="flex flex-col">
                <span className="text-sm text-muted-foreground">Current System:</span>
                <span className="text-xl font-semibold">£{Math.round(curtailmentPayment).toLocaleString()}</span>
                <span className="text-xs text-muted-foreground">Curtailment payment</span>
              </div>
              
              <div className="flex flex-col">
                <span className="text-sm text-muted-foreground">Alternative:</span>
                <span className={`text-xl font-semibold ${isBitcoinBetter ? "text-green-500" : ""}`}>
                  £{Math.round(bitcoinValue).toLocaleString()}
                </span>
                <span className="text-xs text-muted-foreground">Bitcoin mining value</span>
              </div>
            </div>
            
            <div className={`p-3 rounded-md ${
              isBitcoinBetter 
                ? "bg-green-500/10 border border-green-500/20" 
                : difference < 0 
                  ? "bg-red-500/10 border border-red-500/20"
                  : "bg-primary/10 border border-primary/20"
            }`}>
              <div className="flex items-center gap-2 mb-1">
                {Math.abs(percentageDiff) > 5 && (
                  <AlertTriangle className={`h-4 w-4 ${
                    isBitcoinBetter ? "text-green-500" : "text-red-500"
                  }`} />
                )}
                <h3 className="font-semibold">
                  {isBitcoinBetter
                    ? "Bitcoin mining would be more profitable!"
                    : difference < 0
                      ? "Curtailment payments are currently higher"
                      : "Values are approximately equal"}
                </h3>
              </div>
              
              <p className="text-sm">
                {isBitcoinBetter ? (
                  <>
                    Mining Bitcoin instead of accepting curtailment payments would have generated <strong>£{Math.abs(Math.round(difference)).toLocaleString()} more value</strong> ({Math.abs(Math.round(percentageDiff))}% increase) during this period.
                  </>
                ) : difference < 0 ? (
                  <>
                    Curtailment payments were <strong>£{Math.abs(Math.round(difference)).toLocaleString()} higher</strong> than potential Bitcoin mining value during this period.
                  </>
                ) : (
                  <>
                    The two values are approximately equal for this period.
                  </>
                )}
              </p>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}