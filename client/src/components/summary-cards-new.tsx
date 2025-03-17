"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Bitcoin, Calendar, ArrowRightLeft } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

interface SummaryCardsProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

export default function SummaryCards({
  timeframe,
  date,
  minerModel,
  farmId,
}: SummaryCardsProps) {
  // Format dates based on timeframe
  const formattedDate = format(date, "yyyy-MM-dd");
  const yearMonth = format(date, "yyyy-MM");
  const year = format(date, "yyyy");

  // Determine which summary to fetch based on timeframe
  const summaryEndpoint =
    timeframe === "yearly"
      ? `/api/summary/yearly/${year}`
      : timeframe === "monthly"
        ? `/api/summary/monthly/${yearMonth}`
        : `/api/summary/daily/${formattedDate}`;

  // Determine which bitcoin potential to fetch based on timeframe
  const bitcoinEndpoint =
    timeframe === "yearly"
      ? `/api/mining-potential/yearly/${year}`
      : timeframe === "monthly"
        ? `/api/curtailment/monthly-mining-potential/${yearMonth}`
        : `/api/curtailment/mining-potential`;

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
    },
  });

  // Fetch bitcoin data
  const { data: bitcoinData = {}, isLoading: isBitcoinLoading } = useQuery({
    queryKey: [
      bitcoinEndpoint,
      minerModel,
      farmId,
      summaryData.totalCurtailedEnergy,
    ],
    queryFn: async () => {
      const url = new URL(bitcoinEndpoint, window.location.origin);
      url.searchParams.set("minerModel", minerModel);

      if (farmId) {
        url.searchParams.set("leadParty", farmId);
      }

      // For daily view, we need to pass the energy value
      if (timeframe === "daily" && summaryData.totalCurtailedEnergy) {
        url.searchParams.set("date", formattedDate);
        url.searchParams.set(
          "energy",
          summaryData.totalCurtailedEnergy.toString(),
        );
      }

      const response = await fetch(url);
      if (!response.ok) {
        if (response.status === 404) {
          return {
            bitcoinMined: 0,
            valueAtCurrentPrice: 0,
            difficulty: 0,
            price: 0,
            currentPrice: 0,
          };
        }
        throw new Error(`Failed to fetch mining potential`);
      }

      return response.json();
    },
    enabled: !!summaryData.totalCurtailedEnergy || timeframe !== "daily",
  });

  // Helper for displaying timeframe-specific text
  const timeframeLabel =
    timeframe === "yearly"
      ? format(date, "yyyy")
      : timeframe === "monthly"
        ? format(date, "MMMM yyyy")
        : format(date, "PP");

  // Check if there's no data for the selected date
  const hasCurtailmentData = !isSummaryLoading && Number(summaryData.totalCurtailedEnergy) > 0;
  
  // If there's no data, show a message instead of empty cards
  if (!hasCurtailmentData && !isSummaryLoading) {
    return (
      <Card className="mb-8">
        <CardContent className="pt-6">
          <div className="flex flex-col items-center justify-center p-6 text-center">
            <div className="relative h-20 w-20 mb-4">
              <svg
                viewBox="0 0 100 100"
                className="absolute inset-0 text-blue-400"
                xmlns="http://www.w3.org/2000/svg"
              >
                {/* Wind turbine icon with animation */}
                <circle cx="50" cy="50" r="40" fill="currentColor" opacity="0.1" />
                
                {/* Tower */}
                <rect x="47" y="55" width="6" height="35" fill="currentColor" rx="1" />
                <rect x="40" y="90" width="20" height="5" rx="2" fill="currentColor" />

                {/* Nacelle (turbine housing) */}
                <rect x="42" y="48" width="16" height="4" rx="2" fill="currentColor" transform="rotate(5, 50, 50)" />

                {/* Hub */}
                <circle cx="50" cy="50" r="3" fill="currentColor" />

                {/* Rotating blades with animation */}
                <g style={{ transformOrigin: "50px 50px", animation: "windTurbineSpin 12s linear infinite" }}>
                  <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" />
                  <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(120, 50, 50)" />
                  <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(240, 50, 50)" />
                </g>

                {/* Calendar icon for date selection hint */}
                <g transform="translate(75, 75) scale(0.25)">
                  <rect x="10" y="10" width="80" height="80" rx="4" fill="#ffffff" stroke="currentColor" strokeWidth="6" />
                  <rect x="10" y="10" width="80" height="20" fill="currentColor" rx="4" />
                  <rect x="25" y="45" width="10" height="10" fill="currentColor" />
                  <rect x="45" y="45" width="10" height="10" fill="currentColor" />
                  <rect x="65" y="45" width="10" height="10" fill="currentColor" />
                  <rect x="25" y="65" width="10" height="10" fill="currentColor" />
                  <rect x="45" y="65" width="10" height="10" fill="currentColor" />
                </g>

                {/* Animation keyframes */}
                <style>{`
                  @keyframes windTurbineSpin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                  }
                `}</style>
              </svg>
            </div>
            <h3 className="text-lg font-semibold mb-2">No Curtailment Data for {timeframeLabel}</h3>
            <p className="text-muted-foreground max-w-lg">
              There were no curtailment events during this period. Try selecting a different date to see wind farm curtailment 
              data and potential Bitcoin mining comparisons.
            </p>
            <div className="flex items-center gap-2 mt-4 text-primary">
              <Calendar className="h-4 w-4" />
              <span className="text-sm font-medium">Try selecting a different date</span>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  // Regular view when data is available
  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
      {/* Energy Curtailed Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Energy Curtailed
          </CardTitle>
          <Wind className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isSummaryLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="text-2xl font-bold">
              {Number.isNaN(Number(summaryData.totalCurtailedEnergy))
                ? "0 MWh"
                : `${Math.round(Number(summaryData.totalCurtailedEnergy)).toLocaleString()} MWh`}
            </div>
          )}
          <p className="text-xs text-muted-foreground mt-1">
            Wasted energy that could be utilized
          </p>
        </CardContent>
      </Card>

      {/* Subsidies Paid Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Subsidies Paid
          </CardTitle>
          <Battery className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isSummaryLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="text-2xl font-bold text-red-500">
              {Number.isNaN(Number(summaryData.totalPayment))
                ? "£0"
                : `£${Math.round(Number(summaryData.totalPayment)).toLocaleString()}`}
            </div>
          )}
          <p className="text-xs text-muted-foreground mt-1">
            Consumer cost for idle wind farms
          </p>
        </CardContent>
      </Card>

      {/* Potential Bitcoin Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Potential Bitcoin
          </CardTitle>
          <Bitcoin className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isBitcoinLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <>
              <div className="text-2xl font-bold text-[#F7931A]">
                {Number.isNaN(Number(bitcoinData.bitcoinMined))
                  ? "0 BTC"
                  : `${Number(bitcoinData.bitcoinMined).toFixed(4)} BTC`}
              </div>
              <div className="text-sm text-muted-foreground mt-1">
                ≈ £{Math.round(Number(bitcoinData.valueAtCurrentPrice)).toLocaleString("en-GB")}
              </div>
            </>
          )}
          <p className="text-xs text-muted-foreground mt-1">
            Using {minerModel.replace("_", " ")} miners
          </p>
        </CardContent>
      </Card>

      {/* Value Ratio Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            Value Ratio
          </CardTitle>
          <ArrowRightLeft className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          {isBitcoinLoading || isSummaryLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <>
              <div className="text-2xl font-bold text-green-500">
                {Number.isNaN(Number(bitcoinData.valueAtCurrentPrice)) || 
                 Number.isNaN(Number(summaryData.totalPayment)) ||
                 Number(summaryData.totalPayment) === 0
                  ? "0.00x"
                  : `${(Number(bitcoinData.valueAtCurrentPrice) / Number(summaryData.totalPayment)).toFixed(2)}x`}
              </div>
              <div className="flex items-center space-x-1 mt-1">
                <div className="text-xs text-muted-foreground">
                  Higher ratio = better value from mining
                </div>
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}