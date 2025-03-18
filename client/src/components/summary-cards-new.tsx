"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Bitcoin, Calendar, ArrowRightLeft } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { formatEnergy, formatGBP, formatBitcoin } from "@/lib/utils";

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
      <div className="space-y-4 mb-8">
        {/* Time period badge/label */}
        <div className="flex justify-center items-center">
          <div className="inline-flex items-center justify-center px-4 py-1.5 rounded-full bg-muted">
            <Calendar className="h-4 w-4 mr-2" />
            <span className="text-sm font-medium">Data for {timeframeLabel}</span>
          </div>
        </div>
      
        <Card className="mb-4">
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

                  {/* Animation keyframes */}
                  <style>{`
                    @keyframes windTurbineSpin {
                      0% { transform: rotate(0deg); }
                      100% { transform: rotate(360deg); }
                    }
                  `}</style>
                </svg>
              </div>
              <h3 className="text-lg font-semibold mb-2">No Curtailment Data Available</h3>
              <p className="text-muted-foreground max-w-lg">
                There were no curtailment events during this period. Try selecting a different date to see wind farm curtailment 
                data and potential Bitcoin mining comparisons.
              </p>
              <div className="flex items-center gap-2 mt-4 text-primary">
                <Calendar className="h-4 w-4" />
                <span className="text-sm font-medium">Try selecting a different date or timeframe</span>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Regular view when data is available
  return (
    <div className="space-y-4 mb-8">
      {/* Time period badge/label */}
      <div className="flex justify-center items-center">
        <div className="inline-flex items-center justify-center px-4 py-1.5 rounded-full bg-muted">
          <Calendar className="h-4 w-4 mr-2" />
          <span className="text-sm font-medium">Data for {timeframeLabel}</span>
        </div>
      </div>
      
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {/* Energy Curtailed Card */}
        <Card className="overflow-hidden">
          <div className="p-6">
            <div className="flex justify-between items-start mb-2">
              <div>
                <h3 className="text-sm font-medium text-muted-foreground">Energy Curtailed</h3>
                <p className="text-xs text-muted-foreground">Total wasted wind energy</p>
              </div>
              <div className="w-7 h-7 rounded-full bg-blue-100 flex items-center justify-center">
                <Wind className="h-4 w-4 text-blue-600" />
              </div>
            </div>
            
            {isSummaryLoading ? (
              <Skeleton className="h-8 w-32 mb-1" />
            ) : (
              <div className="mt-4">
                <div className="text-2xl font-bold text-blue-600">
                  {Number.isNaN(Number(summaryData.totalCurtailedEnergy))
                    ? "0 MWh"
                    : formatEnergy(Number(summaryData.totalCurtailedEnergy))}
                </div>
                <div className="flex items-center mt-1">
                  <span className="inline-block mr-1 text-blue-600">•</span>
                  <span className="text-xs text-muted-foreground">Untapped energy resource</span>
                </div>
                <p className="text-xs text-muted-foreground mt-2">
                  {Number(summaryData.totalCurtailedEnergy) > 0 
                    ? `That's enough to power approximately ${Math.round(Number(summaryData.totalCurtailedEnergy) / 3.4)} homes for a month`
                    : "No wasted energy recorded for this period"}
                </p>
              </div>
            )}
          </div>
        </Card>

        {/* Subsidies Paid Card */}
        <Card className="overflow-hidden">
          <div className="p-6">
            <div className="flex justify-between items-start mb-2">
              <div>
                <h3 className="text-sm font-medium text-muted-foreground">Subsidies Paid</h3>
                <p className="text-xs text-muted-foreground">Consumer cost for curtailment</p>
              </div>
              <div className="w-7 h-7 rounded-full bg-red-100 flex items-center justify-center">
                <Battery className="h-4 w-4 text-red-600" />
              </div>
            </div>
            
            {isSummaryLoading ? (
              <Skeleton className="h-8 w-32 mb-1" />
            ) : (
              <div className="mt-4">
                <div className="text-2xl font-bold text-red-600">
                  {Number.isNaN(Number(summaryData.totalPayment))
                    ? "£0"
                    : formatGBP(Number(summaryData.totalPayment))}
                </div>
                <div className="flex items-center mt-1">
                  <span className="inline-block mr-1 text-red-600">•</span>
                  <span className="text-xs text-muted-foreground">Paid to idle wind farms</span>
                </div>
                <p className="text-xs text-muted-foreground mt-2">
                  {Number(summaryData.totalCurtailedEnergy) > 0 && Number(summaryData.totalPayment) > 0
                    ? `Approximately £${(Number(summaryData.totalPayment) / Number(summaryData.totalCurtailedEnergy)).toFixed(2)} per MWh of curtailed energy`
                    : "No subsidy payments recorded for this period"}
                </p>
              </div>
            )}
          </div>
        </Card>

        {/* Potential Bitcoin Card */}
        <Card className="overflow-hidden">
          <div className="p-6">
            <div className="flex justify-between items-start mb-2">
              <div>
                <h3 className="text-sm font-medium text-muted-foreground">Potential Bitcoin</h3>
                <p className="text-xs text-muted-foreground">Mining using {minerModel.replace("_", " ")}</p>
              </div>
              <div className="w-7 h-7 rounded-full bg-amber-100 flex items-center justify-center">
                <Bitcoin className="h-4 w-4 text-amber-600" />
              </div>
            </div>
            
            {isBitcoinLoading ? (
              <Skeleton className="h-8 w-32 mb-1" />
            ) : (
              <div className="mt-4">
                <div className="text-2xl font-bold text-amber-600">
                  {Number.isNaN(Number(bitcoinData.bitcoinMined))
                    ? "0 BTC"
                    : formatBitcoin(Number(bitcoinData.bitcoinMined))}
                </div>
                <div className="flex items-center mt-1">
                  <span className="inline-block mr-1 text-amber-600">•</span>
                  <span className="text-xs text-muted-foreground">≈ {formatGBP(Number(bitcoinData.valueAtCurrentPrice))}</span>
                </div>
                <p className="text-xs text-muted-foreground mt-2">
                  {Number(bitcoinData.bitcoinMined) > 0 && Number(summaryData.totalCurtailedEnergy) > 0
                    ? `Potential value: £${(Number(bitcoinData.valueAtCurrentPrice) / Number(summaryData.totalCurtailedEnergy)).toFixed(2)} per MWh of curtailed energy`
                    : "Potential value from wasted energy"}
                </p>
              </div>
            )}
          </div>
        </Card>

        {/* Value Ratio Card */}
        <Card className="overflow-hidden">
          <div className="p-6">
            <div className="flex justify-between items-start mb-2">
              <div>
                <h3 className="text-sm font-medium text-muted-foreground">Value Ratio</h3>
                <p className="text-xs text-muted-foreground">Bitcoin value vs. subsidy cost</p>
              </div>
              <div className="w-7 h-7 rounded-full bg-green-100 flex items-center justify-center">
                <ArrowRightLeft className="h-4 w-4 text-green-600" />
              </div>
            </div>
            
            {isBitcoinLoading || isSummaryLoading ? (
              <Skeleton className="h-8 w-32 mb-1" />
            ) : (
              <div className="mt-4">
                <div className="text-2xl font-bold text-green-600">
                  {Number.isNaN(Number(bitcoinData.valueAtCurrentPrice)) || 
                   Number.isNaN(Number(summaryData.totalPayment)) ||
                   Number(summaryData.totalPayment) === 0
                    ? "0.00×"
                    : `${(Number(bitcoinData.valueAtCurrentPrice) / Number(summaryData.totalPayment)).toFixed(2)}×`}
                </div>
                <div className="flex items-center mt-1">
                  <span className="inline-block mr-1 text-green-600">•</span>
                  <span className="text-xs text-muted-foreground">High value from mining</span>
                </div>
                <p className="text-xs text-muted-foreground mt-2">
                  Bitcoin value is {Number.isNaN(Number(bitcoinData.valueAtCurrentPrice)) || 
                   Number.isNaN(Number(summaryData.totalPayment)) ||
                   Number(summaryData.totalPayment) === 0
                    ? "0.00"
                    : (Number(bitcoinData.valueAtCurrentPrice) / Number(summaryData.totalPayment)).toFixed(2)}× the subsidy payment
                </p>
              </div>
            )}
          </div>
        </Card>
      </div>
    </div>
  );
}