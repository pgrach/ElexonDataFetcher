"use client";

import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Bitcoin, Calendar, Building, ArrowRightLeft } from "lucide-react";
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
                : formatEnergy(Number(summaryData.totalCurtailedEnergy))}
            </div>
          )}
          {Number(summaryData.totalCurtailedEnergy) === 0 ? (
            <div className="flex items-center mt-1 space-x-2">
              <div className="relative h-6 w-6 text-blue-400">
                <svg
                  viewBox="0 0 100 100"
                  className="absolute inset-0"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  {/* Tower */}
                  <rect x="47" y="55" width="6" height="35" fill="currentColor" rx="1" />
                  <rect x="40" y="90" width="20" height="5" rx="2" fill="currentColor" />

                  {/* Nacelle (turbine housing) */}
                  <rect x="42" y="48" width="16" height="4" rx="2" fill="currentColor" transform="rotate(5, 50, 50)" />

                  {/* Hub */}
                  <circle cx="50" cy="50" r="3" fill="currentColor" />

                  {/* Rotating blades - with animation */}
                  <g style={{ transformOrigin: "50px 50px", animation: "windTurbineSpin 8s linear infinite" }}>
                    {/* Blade 1 - pointing right with taper and curve */}
                    <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" />
                    {/* Blade 2 - rotated 120 degrees */}
                    <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(120, 50, 50)" />
                    {/* Blade 3 - rotated 240 degrees */}
                    <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(240, 50, 50)" />
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
              <p className="text-xs text-muted-foreground">
                No curtailment events for {timeframeLabel}
              </p>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground mt-1">
              Wasted energy that could be utilized
            </p>
          )}
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
                : formatGBP(Number(summaryData.totalPayment))}
            </div>
          )}
          {Number(summaryData.totalPayment) === 0 ? (
            <div className="flex items-center mt-1 space-x-2">
              <div className="relative h-6 w-6 text-green-400">
                <svg
                  viewBox="0 0 100 100"
                  className="absolute inset-0"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  {/* Coin base */}
                  <circle
                    cx="50"
                    cy="50"
                    r="40"
                    fill="currentColor"
                    opacity="0.2"
                  />

                  {/* Pound symbol */}
                  <text
                    x="50"
                    y="57"
                    textAnchor="middle"
                    dominantBaseline="middle"
                    fill="currentColor"
                    fontSize="50"
                    fontWeight="bold"
                    style={{ animation: "pulse 2s ease-in-out infinite" }}
                  >
                    £
                  </text>

                  {/* Animation keyframes - added via style */}
                  <style>{`
                    @keyframes pulse {
                      0% { opacity: 0.6; transform: scale(0.95); }
                      50% { opacity: 1; transform: scale(1.05); }
                      100% { opacity: 0.6; transform: scale(0.95); }
                    }
                  `}</style>
                </svg>
              </div>
              <p className="text-xs text-muted-foreground">
                No curtailment payments for {timeframeLabel}
              </p>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground mt-1">
              Consumer cost for idle wind farms
            </p>
          )}
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
                  : formatBitcoin(Number(bitcoinData.bitcoinMined))}
              </div>
              <div className="text-sm text-muted-foreground mt-1">
                ≈ {formatGBP(Number(bitcoinData.valueAtCurrentPrice))}
              </div>
            </>
          )}
          {Number(summaryData.totalCurtailedEnergy) === 0 ? (
            <div className="flex items-center mt-1 space-x-2">
              <div className="relative h-6 w-6 text-[#F7931A]">
                <svg
                  viewBox="0 0 100 100"
                  className="absolute inset-0"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  {/* Bitcoin symbol with animation */}
                  <circle
                    cx="50"
                    cy="50"
                    r="40"
                    fill="currentColor"
                    opacity="0.2"
                  />
                  <g
                    style={{
                      transformOrigin: "center",
                      animation: "float 3s ease-in-out infinite",
                    }}
                  >
                    <path
                      d="M58 45c1-5.5-3.4-8.5-9.1-10.5l1.8-7.3-4.5-1.1-1.8 7.1c-1.1-0.3-2.3-0.5-3.5-0.8l1.8-7.2-4.5-1.1-1.8 7.3c-1-0.2-1.9-0.4-2.8-0.7l0 0-6.1-1.5-1.2 4.8c0 0 3.3 0.8 3.2 0.8 1.8 0.5 2.1 1.6 2.1 2.6l-2.1 8.3c0.1 0 0.3 0.1 0.4 0.2l-0.4-0.1-2.9 11.9c-0.2 0.6-0.9 1.5-2.3 1.1 0 0.1-3.2-0.8-3.2-0.8l-2.2 5.1 5.8 1.4c1.1 0.3 2.1 0.6 3.2 0.8l-1.9 7.5 4.5 1.1 1.8-7.3c1.2 0.3 2.4 0.6 3.5 0.9l-1.8 7.3 4.5 1.1 1.9-7.5c7.5 1.4 13.1 0.8 15.5-5.9 1.9-5.4 0-8.6-4-10.6C57.1 50.7 57.4 48.7 58 45zM47.7 57.5c-1.3 5.4-10.5 2.5-13.4 1.8l2-8.2c3 0.7 12.5 2.3 11.4 6.4zM49 45.2c-1.3 5-8.9 2.5-11.4 1.9l1.9-7.5C41.9 40.1 50.3 40 49 45.2z"
                      transform="translate(8, 5)"
                      fill="currentColor"
                    />
                  </g>

                  {/* Animation keyframes - added via style */}
                  <style>{`
                    @keyframes float {
                      0% { transform: translateY(0px); }
                      50% { transform: translateY(-5px); }
                      100% { transform: translateY(0px); }
                    }
                  `}</style>
                </svg>
              </div>
              <p className="text-xs text-muted-foreground">
                No Bitcoin mining potential without curtailment
              </p>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground mt-1">
              Using {minerModel.replace("_", " ")} miners
            </p>
          )}
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
          {Number(summaryData.totalCurtailedEnergy) === 0 ? (
            <div className="flex items-center mt-1 space-x-2">
              <div className="relative h-6 w-6 text-green-500">
                <svg
                  viewBox="0 0 100 100"
                  className="absolute inset-0"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  {/* Ratio icon with animation */}
                  <circle
                    cx="50"
                    cy="50"
                    r="40"
                    fill="currentColor"
                    opacity="0.2"
                  />
                  <g
                    style={{
                      transformOrigin: "center",
                      animation: "flip 3s ease-in-out infinite",
                    }}
                  >
                    {/* Bitcoin symbol on left */}
                    <text
                      x="30"
                      y="50"
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="currentColor"
                      fontSize="18"
                      fontWeight="bold"
                    >
                      ₿
                    </text>
                    
                    {/* Pound symbol on right */}
                    <text
                      x="70"
                      y="50"
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="currentColor"
                      fontSize="18"
                      fontWeight="bold"
                    >
                      £
                    </text>
                    
                    {/* Slash between symbols */}
                    <text
                      x="50"
                      y="50"
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="currentColor"
                      fontSize="24"
                      fontWeight="bold"
                    >
                      /
                    </text>
                  </g>

                  {/* Animation keyframes - added via style */}
                  <style>{`
                    @keyframes flip {
                      0% { transform: rotateY(0deg); }
                      50% { transform: rotateY(180deg); }
                      100% { transform: rotateY(360deg); }
                    }
                  `}</style>
                </svg>
              </div>
              <p className="text-xs text-muted-foreground">
                No comparison data available
              </p>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground mt-1">
              Mining value compared to subsidies paid
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
