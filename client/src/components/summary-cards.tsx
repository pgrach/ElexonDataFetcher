"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Bitcoin, Calendar, Building } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

interface SummaryCardsProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

export default function SummaryCards({ timeframe, date, minerModel, farmId }: SummaryCardsProps) {
  // Format dates based on timeframe
  const formattedDate = format(date, "yyyy-MM-dd");
  const yearMonth = format(date, "yyyy-MM");
  const year = format(date, "yyyy");
  
  // Determine which summary to fetch based on timeframe
  const summaryEndpoint = 
    timeframe === "yearly" ? `/api/summary/yearly/${year}` :
    timeframe === "monthly" ? `/api/summary/monthly/${yearMonth}` :
    `/api/summary/daily/${formattedDate}`;
  
  // Determine which bitcoin potential to fetch based on timeframe
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
  
  // Helper for displaying timeframe-specific text
  const timeframeLabel = 
    timeframe === "yearly" ? format(date, "yyyy") :
    timeframe === "monthly" ? format(date, "MMMM yyyy") :
    format(date, "PP");
  
  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
      {/* Curtailed Energy Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            {farmId ? "Farm Curtailed Energy" : "Curtailed Energy"}
          </CardTitle>
          <Wind className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isSummaryLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="text-2xl font-bold">
              {Number(summaryData.totalCurtailedEnergy).toLocaleString()} MWh
            </div>
          )}
          <p className="text-xs text-muted-foreground mt-1">
            {farmId ? `Farm energy for ${timeframeLabel}` : `Total energy for ${timeframeLabel}`}
          </p>
        </CardContent>
      </Card>
      
      {/* Payment Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            {farmId ? "Farm Payment" : "Curtailment Payment"}
          </CardTitle>
          <Battery className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isSummaryLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="text-2xl font-bold">
              £{Number(summaryData.totalPayment).toLocaleString()}
            </div>
          )}
          <p className="text-xs text-muted-foreground mt-1">
            {farmId ? `Payment for ${timeframeLabel}` : `Total payment for ${timeframeLabel}`}
          </p>
        </CardContent>
      </Card>
      
      {/* Bitcoin Mining Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Bitcoin Mining Potential
          </CardTitle>
          <Bitcoin className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isBitcoinLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="text-2xl font-bold text-[#F7931A]">
              ₿{Number(bitcoinData.bitcoinMined).toFixed(8)}
            </div>
          )}
          <p className="text-xs text-muted-foreground mt-1">
            With {minerModel.replace("_", " ")} miners
          </p>
        </CardContent>
      </Card>
      
      {/* Value Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Bitcoin Value
          </CardTitle>
          <Calendar className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isBitcoinLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="text-2xl font-bold text-[#F7931A]">
              £{Number(bitcoinData.valueAtCurrentPrice).toLocaleString('en-GB', { 
                maximumFractionDigits: 2 
              })}
            </div>
          )}
          <p className="text-xs text-muted-foreground mt-1">
            Estimated value at current BTC price
          </p>
        </CardContent>
      </Card>
    </div>
  );
}