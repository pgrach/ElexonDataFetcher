"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Wind,
  Bitcoin,
  Calendar,
  ArrowRightLeft,
  PoundSterling,
  Receipt,
} from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { formatEnergy, formatGBP, formatBitcoin } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

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
  const hasCurtailmentData =
    !isSummaryLoading && Number(summaryData.totalCurtailedEnergy) > 0;

  // If there's no data, show a message instead of empty cards
  if (!hasCurtailmentData && !isSummaryLoading) {
    return (
      <div className="space-y-4 mb-8">
        {/* Time period badge/label */}
        <div className="flex justify-center items-center">
          <div className="inline-flex items-center justify-center px-4 py-1.5 rounded-full bg-muted">
            <Calendar className="h-4 w-4 mr-2" />
            <span className="text-sm font-medium">
              Data for {timeframeLabel}
            </span>
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
                  <circle
                    cx="50"
                    cy="50"
                    r="40"
                    fill="currentColor"
                    opacity="0.1"
                  />

                  {/* Tower */}
                  <rect
                    x="47"
                    y="55"
                    width="6"
                    height="35"
                    fill="currentColor"
                    rx="1"
                  />
                  <rect
                    x="40"
                    y="90"
                    width="20"
                    height="5"
                    rx="2"
                    fill="currentColor"
                  />

                  {/* Nacelle (turbine housing) */}
                  <rect
                    x="42"
                    y="48"
                    width="16"
                    height="4"
                    rx="2"
                    fill="currentColor"
                    transform="rotate(5, 50, 50)"
                  />

                  {/* Hub */}
                  <circle cx="50" cy="50" r="3" fill="currentColor" />

                  {/* Rotating blades with animation */}
                  <g
                    style={{
                      transformOrigin: "50px 50px",
                      animation: "windTurbineSpin 12s linear infinite",
                    }}
                  >
                    <path
                      d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z"
                      fill="currentColor"
                    />
                    <path
                      d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z"
                      fill="currentColor"
                      transform="rotate(120, 50, 50)"
                    />
                    <path
                      d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z"
                      fill="currentColor"
                      transform="rotate(240, 50, 50)"
                    />
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
              <h3 className="text-lg font-semibold mb-2">
                No Wind Curtailment Detected{" "}
              </h3>
              <p className="text-muted-foreground max-w-lg">
                There were no curtailment events during this period. Try
                selecting a different date to see wind farm curtailment data and
                potential Bitcoin mining comparisons.
              </p>
              <div className="flex items-center gap-2 mt-4 text-primary">
                <Calendar className="h-4 w-4" />
                <span className="text-sm font-medium">
                  Try selecting a different date or timeframe
                </span>
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
        <Card className="overflow-hidden border-t-4 border-t-blue-500 shadow-md">
          <CardHeader className="pb-2">
            <div className="flex justify-between items-center">
              <CardTitle className="text-lg font-semibold">
                Energy Curtailed
              </CardTitle>
              <div className="p-2 rounded-full bg-blue-100 flex items-center justify-center">
                <Wind className="h-5 w-5 text-blue-600" />
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {isSummaryLoading ? (
              <Skeleton className="h-12 w-36 mb-2" />
            ) : (
              <>
                <div className="text-4xl font-bold text-blue-600 mb-2">
                  {Number.isNaN(Number(summaryData.totalCurtailedEnergy))
                    ? "0 MWh"
                    : formatEnergy(Number(summaryData.totalCurtailedEnergy))}
                </div>
                <div className="flex items-center">
                  <div className="h-3 w-3 rounded-full bg-blue-600 mr-2"></div>
                  <p className="text-base text-muted-foreground">
                    Untapped energy resource
                  </p>
                </div>
                {Number(summaryData.totalCurtailedEnergy) > 0 && (
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <p className="text-sm text-muted-foreground mt-2 border-t pt-2 border-dashed border-slate-200">
                          Powers ~{Math.round(Number(summaryData.totalCurtailedEnergy) / 3.4)} homes/month
                        </p>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p className="text-xs">
                          This energy could power approximately {Math.round(Number(summaryData.totalCurtailedEnergy) / 3.4)} homes for an entire month
                        </p>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                )}
              </>
            )}
          </CardContent>
        </Card>

        {/* Subsidies Paid Card */}
        <Card className="overflow-hidden border-t-4 border-t-red-500 shadow-md">
          <CardHeader className="pb-2">
            <div className="flex justify-between items-center">
              <CardTitle className="text-lg font-semibold">
                Subsidies Paid
              </CardTitle>
              <div className="p-2 rounded-full bg-red-100 flex items-center justify-center">
                <PoundSterling className="h-5 w-5 text-red-600" />
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {isSummaryLoading ? (
              <Skeleton className="h-12 w-36 mb-2" />
            ) : (
              <>
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <div className="text-4xl font-bold text-red-600 mb-2">
                        {Number.isNaN(Number(summaryData.totalPayment))
                          ? "£0"
                          : formatGBP(Number(summaryData.totalPayment))}
                      </div>
                    </TooltipTrigger>
                    <TooltipContent>
                      <p className="text-sm">
                        Full amount: £
                        {Number.isNaN(Number(summaryData.totalPayment))
                          ? "0"
                          : Number(summaryData.totalPayment).toLocaleString(
                              undefined,
                              { maximumFractionDigits: 2 }
                            )}
                      </p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
                <div className="flex items-center">
                  <div className="h-3 w-3 rounded-full bg-red-600 mr-2"></div>
                  <p className="text-base text-muted-foreground">
                    Paid to idle wind farms
                  </p>
                </div>
                {Number(summaryData.totalCurtailedEnergy) > 0 &&
                  Number(summaryData.totalPayment) > 0 && (
                    <p className="text-sm text-muted-foreground mt-2 border-t pt-2 border-dashed border-slate-200">
                      £{(Number(summaryData.totalPayment) / Number(summaryData.totalCurtailedEnergy)).toFixed(2)} per MWh
                    </p>
                  )}
              </>
            )}
          </CardContent>
        </Card>

        {/* Potential Bitcoin Card */}
        <Card className="overflow-hidden border-t-4 border-t-amber-500 shadow-md">
          <CardHeader className="pb-2">
            <div className="flex justify-between items-center">
              <CardTitle className="text-lg font-semibold">
                Potential Bitcoin
              </CardTitle>
              <div className="p-2 rounded-full bg-amber-100 flex items-center justify-center">
                <Bitcoin className="h-5 w-5 text-amber-600" />
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {isBitcoinLoading ? (
              <Skeleton className="h-12 w-36 mb-2" />
            ) : (
              <>
                <div className="text-4xl font-bold text-amber-600 mb-2">
                  {Number.isNaN(Number(bitcoinData.bitcoinMined))
                    ? "0 BTC"
                    : formatBitcoin(Number(bitcoinData.bitcoinMined))}
                </div>
                <div className="flex items-center">
                  <div className="h-3 w-3 rounded-full bg-amber-600 mr-2"></div>
                  <p className="text-base text-muted-foreground">
                    ≈ {formatGBP(Number(bitcoinData.valueAtCurrentPrice))}
                  </p>
                </div>
                {Number(bitcoinData.bitcoinMined) > 0 &&
                  Number(summaryData.totalCurtailedEnergy) > 0 && (
                    <p className="text-sm text-muted-foreground mt-2 border-t pt-2 border-dashed border-slate-200">
                      £{(Number(bitcoinData.valueAtCurrentPrice) / Number(summaryData.totalCurtailedEnergy)).toFixed(2)} per MWh
                    </p>
                  )}
              </>
            )}
          </CardContent>
        </Card>

        {/* Value Ratio Card */}
        <Card className="overflow-hidden border-t-4 shadow-md">
          {(() => {
            // Calculate ratio for card styling
            const valueRatio =
              Number.isNaN(Number(bitcoinData.valueAtCurrentPrice)) ||
              Number.isNaN(Number(summaryData.totalPayment)) ||
              Number(summaryData.totalPayment) === 0
                ? 0
                : Number(bitcoinData.valueAtCurrentPrice) /
                  Number(summaryData.totalPayment);

            // Use green for ratios >= 1.0 and slate for < 1.0
            const borderColor = valueRatio >= 1.0 ? "border-t-green-500" : "border-t-slate-500";
            
            return (
              <div className={borderColor}>
                <CardHeader className="pb-2">
                  <div className="flex justify-between items-center">
                    <CardTitle className="text-lg font-semibold">
                      Value Ratio
                    </CardTitle>
                    <div className={`p-2 rounded-full ${valueRatio >= 1.0 ? "bg-green-100" : "bg-slate-100"} flex items-center justify-center`}>
                      <ArrowRightLeft className={`h-5 w-5 ${valueRatio >= 1.0 ? "text-green-600" : "text-slate-600"}`} />
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  {isBitcoinLoading || isSummaryLoading ? (
                    <Skeleton className="h-12 w-36 mb-2" />
                  ) : (
                    <>
                      <div className={`text-4xl font-bold mb-2 ${valueRatio >= 1.0 ? "text-green-600" : "text-slate-600"}`}>
                        {valueRatio === 0
                          ? "0.00×"
                          : `${valueRatio.toFixed(2)}×`}
                      </div>
                      <div className="flex items-center">
                        <div className={`h-3 w-3 rounded-full mr-2 ${valueRatio >= 1.0 ? "bg-green-600" : "bg-slate-600"}`}></div>
                        <p className="text-base text-muted-foreground">
                          {valueRatio >= 1.0 ? "High value from mining" : "Subsidies exceed mining value"}
                        </p>
                      </div>
                      {valueRatio > 0 && (
                        <p className="text-sm text-muted-foreground mt-2 border-t pt-2 border-dashed border-slate-200">
                          {valueRatio >= 1.0
                            ? `Bitcoin > subsidy by ${valueRatio.toFixed(2)}×`
                            : `Subsidy > Bitcoin by ${(1 / valueRatio).toFixed(2)}×`}
                        </p>
                      )}
                    </>
                  )}
                </CardContent>
              </div>
            );
          })()}
        </Card>
      </div>
    </div>
  );
}