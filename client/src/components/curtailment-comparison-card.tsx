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
  
  // Check if we have no data for this date
  const hasNoCurtailmentData = 
    !isSummaryLoading && 
    !isBitcoinLoading && 
    curtailmentPayment === 0 && 
    bitcoinValue === 0;

  return (
    <Card className={hasNoCurtailmentData ? "border-blue-500/30" :
            isBitcoinBetter ? "border-green-500/30" : 
            difference < 0 ? "border-red-500/30" : "border-primary/20"}>
      <CardHeader className="pb-2">
        <CardTitle className="text-lg flex items-center justify-between">
          <span>Payment vs. Mining Value Comparison</span>
          {hasNoCurtailmentData ? (
            <Minus className="h-5 w-5 text-blue-500" />
          ) : isBitcoinBetter ? (
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
        ) : hasNoCurtailmentData ? (
          // Special state for when no curtailment occurred
          <div className="bg-blue-50/30 border border-dashed border-blue-200 p-4 rounded-md flex flex-col items-center">
            <div className="flex justify-center mb-2">
              <svg className="h-12 w-12 text-blue-400" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                {/* Tower */}
                <rect x="48" y="52" width="4" height="38" fill="currentColor" />
                <path d="M46 90 L54 90 L56 95 L44 95 Z" fill="currentColor" />
                
                {/* Nacelle (turbine housing) */}
                <rect x="45" y="48" width="10" height="5" rx="1" fill="currentColor" />
                
                {/* Hub */}
                <circle cx="50" cy="50" r="2.5" fill="currentColor" />
                
                {/* Three blades with proper wind turbine shape */}
                <g style={{ transformOrigin: "50px 50px", animation: "windTurbineSpin 8s linear infinite" }}>
                  {/* Blade 1 - pointing right */}
                  <path d="M50 50 L90 45 Q92 42 88 40 L52 48 Z" fill="currentColor" />
                  
                  {/* Blade 2 - pointing bottom left */}
                  <path d="M50 50 L30 85 Q26 87 25 83 L47 53 Z" fill="currentColor" 
                    transform="rotate(120, 50, 50)" />
                  
                  {/* Blade 3 - pointing top left */}
                  <path d="M50 50 L30 85 Q26 87 25 83 L47 53 Z" fill="currentColor" 
                    transform="rotate(240, 50, 50)" />
                </g>
                
                {/* Animation keyframes - added via style */}
                <style>{`
                  @keyframes windTurbineSpin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                  }
                `}</style>
              </svg>
            </div>
            <h3 className="font-semibold text-lg text-blue-500 mb-2">No Curtailment Events Today</h3>
            <p className="text-sm text-blue-400 text-center mb-3">
              No wind farms were curtailed on this date, meaning all available wind energy was utilized by the grid.
            </p>
            <p className="text-sm text-muted-foreground text-center">
              Try selecting a different date to see curtailment events and potential Bitcoin mining comparisons.
            </p>
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