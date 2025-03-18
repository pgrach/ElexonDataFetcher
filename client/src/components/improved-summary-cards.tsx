"use client";

import { format } from "date-fns";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Wind, Battery, Bitcoin, ArrowRightLeft } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

interface ImprovedSummaryCardsProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
  data?: {
    curtailedEnergy: number;
    subsidiesPaid: number;
    bitcoinMined: number;
    bitcoinValue: number;
  };
  isLoading?: boolean;
}

// Helper to format large numbers with appropriate units
function formatLargeNumber(num: number): string {
  if (num >= 1000000) {
    return `${(num / 1000000).toFixed(1)}M`;
  } else if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}k`;
  }
  return num.toFixed(0);
}

export default function ImprovedSummaryCards({
  timeframe,
  date,
  minerModel,
  farmId,
  data = {
    curtailedEnergy: 556006,
    subsidiesPaid: 11488822,
    bitcoinMined: 394.68,
    bitcoinValue: 25804237
  },
  isLoading = false
}: ImprovedSummaryCardsProps) {
  
  // Helper for displaying timeframe-specific text
  const timeframeLabel =
    timeframe === "yearly"
      ? format(date, "yyyy")
      : timeframe === "monthly"
        ? format(date, "MMMM yyyy")
        : format(date, "PP");
  
  // Calculate value ratio (Bitcoin value / Subsidies paid)
  const valueRatio = data.subsidiesPaid > 0 
    ? data.bitcoinValue / data.subsidiesPaid 
    : 0;

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
      {/* Energy Curtailed Card */}
      <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
        <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
          <div className="flex justify-between items-center">
            <CardTitle className="text-sm font-medium text-gray-700">
              Energy Curtailed
            </CardTitle>
            <Wind className="h-4 w-4 text-primary" />
          </div>
          <CardDescription className="text-xs text-gray-500">
            Total wasted wind energy
          </CardDescription>
        </CardHeader>
        <CardContent className="pt-4">
          {isLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="space-y-1">
              <div className="text-2xl font-bold text-gray-800">
                {formatLargeNumber(data.curtailedEnergy)} <span className="text-base font-medium">MWh</span>
              </div>
              {data.curtailedEnergy === 0 ? (
                <div className="flex items-center mt-1 space-x-2">
                  <div className="relative h-6 w-6 text-blue-500">
                    <svg
                      viewBox="0 0 100 100"
                      className="absolute inset-0"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      {/* Animated wind turbine */}
                      <rect x="47" y="55" width="6" height="35" fill="currentColor" rx="1" />
                      <rect x="40" y="90" width="20" height="5" rx="2" fill="currentColor" />
                      <rect x="42" y="48" width="16" height="4" rx="2" fill="currentColor" transform="rotate(5, 50, 50)" />
                      <circle cx="50" cy="50" r="3" fill="currentColor" />
                      <g style={{ transformOrigin: "50px 50px", animation: "windTurbineSpin 8s linear infinite" }}>
                        <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" />
                        <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(120, 50, 50)" />
                        <path d="M50 50 L85 40 Q90 35, 88 30 L52 45 Z" fill="currentColor" transform="rotate(240, 50, 50)" />
                      </g>
                      <style>{`
                        @keyframes windTurbineSpin {
                          0% { transform: rotate(0deg); }
                          100% { transform: rotate(360deg); }
                        }
                      `}</style>
                    </svg>
                  </div>
                  <p className="text-xs text-gray-500">
                    No curtailment for {timeframeLabel}
                  </p>
                </div>
              ) : (
                <div className="flex flex-col">
                  <div className="flex items-center">
                    <span className="inline-block w-2 h-2 rounded-full bg-primary mr-2"></span>
                    <p className="text-xs text-gray-500">
                      Untapped energy resource
                    </p>
                  </div>
                  <div className="mt-2 pt-2 border-t border-gray-100">
                    <div className="text-xs text-gray-500">
                      That's enough to power approximately{" "}
                      <span className="font-medium text-primary">
                        {formatLargeNumber(data.curtailedEnergy * 0.3)} homes
                      </span>{" "}
                      for a month
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Subsidies Paid Card */}
      <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
        <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
          <div className="flex justify-between items-center">
            <CardTitle className="text-sm font-medium text-gray-700">
              Subsidies Paid
            </CardTitle>
            <Battery className="h-4 w-4 text-red-500" />
          </div>
          <CardDescription className="text-xs text-gray-500">
            Consumer cost for curtailment
          </CardDescription>
        </CardHeader>
        <CardContent className="pt-4">
          {isLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="space-y-1">
              <div className="text-2xl font-bold text-red-500">
                £{formatLargeNumber(data.subsidiesPaid)}
              </div>
              {data.subsidiesPaid === 0 ? (
                <div className="flex items-center mt-1 space-x-2">
                  <div className="relative h-6 w-6 text-green-500">
                    <svg
                      viewBox="0 0 100 100"
                      className="absolute inset-0"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <circle cx="50" cy="50" r="40" fill="currentColor" opacity="0.2" />
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
                      <style>{`
                        @keyframes pulse {
                          0% { opacity: 0.6; transform: scale(0.95); }
                          50% { opacity: 1; transform: scale(1.05); }
                          100% { opacity: 0.6; transform: scale(0.95); }
                        }
                      `}</style>
                    </svg>
                  </div>
                  <p className="text-xs text-gray-500">
                    No payments for {timeframeLabel}
                  </p>
                </div>
              ) : (
                <div className="flex flex-col">
                  <div className="flex items-center">
                    <span className="inline-block w-2 h-2 rounded-full bg-red-500 mr-2"></span>
                    <p className="text-xs text-gray-500">
                      Paid to idle wind farms
                    </p>
                  </div>
                  <div className="mt-2 pt-2 border-t border-gray-100">
                    <div className="text-xs text-gray-500">
                      Approximately{" "}
                      <span className="font-medium text-red-500">
                        £{(data.subsidiesPaid / data.curtailedEnergy).toFixed(0)}
                      </span>{" "}
                      per MWh of curtailed energy
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Potential Bitcoin Card */}
      <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
        <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
          <div className="flex justify-between items-center">
            <CardTitle className="text-sm font-medium text-gray-700">
              Potential Bitcoin
            </CardTitle>
            <Bitcoin className="h-4 w-4 text-[#F7931A]" />
          </div>
          <CardDescription className="text-xs text-gray-500">
            Mining using {minerModel.replace("_", " ")}
          </CardDescription>
        </CardHeader>
        <CardContent className="pt-4">
          {isLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="space-y-1">
              <div>
                <div className="text-2xl font-bold text-[#F7931A]">
                  {data.bitcoinMined.toFixed(2)} <span className="text-base font-medium">₿</span>
                </div>
                <div className="text-sm text-gray-600 mt-0">
                  ≈ £{formatLargeNumber(data.bitcoinValue)}
                </div>
              </div>
              
              {data.curtailedEnergy === 0 ? (
                <div className="flex items-center mt-1 space-x-2">
                  <div className="relative h-6 w-6 text-[#F7931A]">
                    <svg
                      viewBox="0 0 100 100"
                      className="absolute inset-0"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <circle cx="50" cy="50" r="40" fill="currentColor" opacity="0.2" />
                      <g style={{ transformOrigin: "center", animation: "float 3s ease-in-out infinite" }}>
                        <path
                          d="M58 45c1-5.5-3.4-8.5-9.1-10.5l1.8-7.3-4.5-1.1-1.8 7.1c-1.1-0.3-2.3-0.5-3.5-0.8l1.8-7.2-4.5-1.1-1.8 7.3c-1-0.2-1.9-0.4-2.8-0.7l0 0-6.1-1.5-1.2 4.8c0 0 3.3 0.8 3.2 0.8 1.8 0.5 2.1 1.6 2.1 2.6l-2.1 8.3c0.1 0 0.3 0.1 0.4 0.2l-0.4-0.1-2.9 11.9c-0.2 0.6-0.9 1.5-2.3 1.1 0 0.1-3.2-0.8-3.2-0.8l-2.2 5.1 5.8 1.4c1.1 0.3 2.1 0.6 3.2 0.8l-1.9 7.5 4.5 1.1 1.8-7.3c1.2 0.3 2.4 0.6 3.5 0.9l-1.8 7.3 4.5 1.1 1.9-7.5c7.5 1.4 13.1 0.8 15.5-5.9 1.9-5.4 0-8.6-4-10.6C57.1 50.7 57.4 48.7 58 45zM47.7 57.5c-1.3 5.4-10.5 2.5-13.4 1.8l2-8.2c3 0.7 12.5 2.3 11.4 6.4zM49 45.2c-1.3 5-8.9 2.5-11.4 1.9l1.9-7.5C41.9 40.1 50.3 40 49 45.2z"
                          transform="translate(8, 5)"
                          fill="currentColor"
                        />
                      </g>
                      <style>{`
                        @keyframes float {
                          0% { transform: translateY(0px); }
                          50% { transform: translateY(-5px); }
                          100% { transform: translateY(0px); }
                        }
                      `}</style>
                    </svg>
                  </div>
                  <p className="text-xs text-gray-500">
                    No mining potential without curtailment
                  </p>
                </div>
              ) : (
                <div className="mt-2 pt-2 border-t border-gray-100">
                  <div className="text-xs text-gray-500">
                    <span className="font-medium text-[#F7931A]">
                      {(data.bitcoinMined / data.curtailedEnergy * 1000).toFixed(3)} ₿
                    </span>{" "}
                    per GWh of curtailed energy
                  </div>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Value Ratio Card */}
      <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
        <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
          <div className="flex justify-between items-center">
            <CardTitle className="text-sm font-medium text-gray-700">
              Value Ratio
            </CardTitle>
            <ArrowRightLeft className="h-4 w-4 text-green-500" />
          </div>
          <CardDescription className="text-xs text-gray-500">
            Bitcoin value vs. subsidy cost
          </CardDescription>
        </CardHeader>
        <CardContent className="pt-4">
          {isLoading ? (
            <Skeleton className="h-8 w-32 mb-1" />
          ) : (
            <div className="space-y-1">
              <div className="text-2xl font-bold text-green-500">
                {valueRatio === 0 ? "0.00" : valueRatio.toFixed(2)}×
              </div>
              
              {valueRatio === 0 ? (
                <div className="flex items-center mt-1 space-x-2">
                  <div className="relative h-6 w-6 text-green-500">
                    <svg
                      viewBox="0 0 100 100"
                      className="absolute inset-0"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <circle cx="50" cy="50" r="40" fill="currentColor" opacity="0.2" />
                      <g style={{ transformOrigin: "center", animation: "flip 3s ease-in-out infinite" }}>
                        <text x="30" y="50" textAnchor="middle" dominantBaseline="middle" fill="currentColor" fontSize="18" fontWeight="bold">₿</text>
                        <text x="70" y="50" textAnchor="middle" dominantBaseline="middle" fill="currentColor" fontSize="18" fontWeight="bold">£</text>
                        <text x="50" y="50" textAnchor="middle" dominantBaseline="middle" fill="currentColor" fontSize="24" fontWeight="bold">/</text>
                      </g>
                      <style>{`
                        @keyframes flip {
                          0% { transform: rotateY(0deg); }
                          50% { transform: rotateY(180deg); }
                          100% { transform: rotateY(360deg); }
                        }
                      `}</style>
                    </svg>
                  </div>
                  <p className="text-xs text-gray-500">
                    No ratio available for {timeframeLabel}
                  </p>
                </div>
              ) : (
                <div className="flex flex-col">
                  <div className="flex items-center gap-1">
                    {valueRatio >= 2.0 ? (
                      <>
                        <span className="inline-block w-2 h-2 rounded-full bg-green-500 mr-1"></span>
                        <p className="text-xs text-gray-500">
                          <span className="font-medium text-green-500">High value</span> from mining
                        </p>
                      </>
                    ) : valueRatio >= 1.0 ? (
                      <>
                        <span className="inline-block w-2 h-2 rounded-full bg-yellow-500 mr-1"></span>
                        <p className="text-xs text-gray-500">
                          <span className="font-medium text-yellow-500">Break-even</span> mining opportunity
                        </p>
                      </>
                    ) : (
                      <>
                        <span className="inline-block w-2 h-2 rounded-full bg-red-500 mr-1"></span>
                        <p className="text-xs text-gray-500">
                          <span className="font-medium text-red-500">Low value</span> mining opportunity
                        </p>
                      </>
                    )}
                  </div>
                  
                  <div className="mt-2 pt-2 border-t border-gray-100">
                    <div className="text-xs text-gray-500">
                      Bitcoin value is{" "}
                      <span className={`font-medium ${valueRatio >= 1.0 ? "text-green-500" : "text-red-500"}`}>
                        {valueRatio.toFixed(2)}×
                      </span>{" "}
                      the subsidy payment
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}