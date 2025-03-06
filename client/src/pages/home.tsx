import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, isValid, isToday } from "date-fns";
import { FilterBar } from "@/components/ui/filter-bar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Wind,
  Battery,
  Calendar as CalendarIcon,
  Building,
  Bitcoin,
} from "lucide-react";
import { ChartContainer, ChartTooltip } from "@/components/ui/chart";
import { DualAxisChart } from "@/components/ui/dual-axis-chart";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface BitcoinCalculation {
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  difficulty: number | null;
  price: number;
  currentPrice: number;
}

interface YearlyBitcoinCalculation {
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  curtailedEnergy: number;
  totalPayment: number;
  averageDifficulty: number;
  currentPrice: number;
  year: string;
}

interface DailySummary {
  totalCurtailedEnergy: number;
  totalPayment: number;
}

interface MonthlySummary {
  totalCurtailedEnergy: number;
  totalPayment: number;
}

interface YearlySummary {
  totalCurtailedEnergy: number;
  totalPayment: number;
}

interface HourlyData {
  hour: string;
  curtailedEnergy: number;
}

export default function Home() {
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2022-01-01");
    return today < startDate ? startDate : today;
  });
  const [selectedLeadParty, setSelectedLeadParty] = useState<string | null>(
    null,
  );
  const [selectedMinerModel, setSelectedMinerModel] = useState("S19J_PRO");

  const formattedDate = format(date, "yyyy-MM-dd");

  const { data: curtailedLeadParties = [] } = useQuery<string[]>({
    queryKey: [`/api/lead-parties/${formattedDate}`],
    enabled: !!formattedDate && isValid(date),
  });

  useEffect(() => {
    if (
      selectedLeadParty &&
      !curtailedLeadParties.includes(selectedLeadParty)
    ) {
      setSelectedLeadParty(null);
    }
  }, [formattedDate, curtailedLeadParties, selectedLeadParty]);

  const {
    data: dailyData,
    isLoading: isDailyLoading,
    error: dailyError,
  } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${formattedDate}`, selectedLeadParty],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error("Invalid date selected");
      }

      const url = new URL(
        `/api/summary/daily/${formattedDate}`,
        window.location.origin,
      );
      if (selectedLeadParty && selectedLeadParty !== "all") {
        url.searchParams.set("leadParty", selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        if (response.status === 404) {
          return {
            totalCurtailedEnergy: 0,
            totalPayment: 0
          };
        }
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!formattedDate && isValid(date),
  });

  const { data: bitcoinPotential } = useQuery<BitcoinCalculation>({
    queryKey: [
      `/api/curtailment/mining-potential`,
      selectedMinerModel,
      dailyData?.totalCurtailedEnergy,
      selectedLeadParty,
      formattedDate 
    ],
    queryFn: async () => {
      console.log("Bitcoin calculation parameters:", {
        date: formattedDate,
        minerModel: selectedMinerModel,
        curtailedEnergy: dailyData?.totalCurtailedEnergy,
        leadParty: selectedLeadParty,
      });

      if (!isValid(date) || !dailyData?.totalCurtailedEnergy) {
        console.log("Skipping Bitcoin calculation:", {
          isValidDate: isValid(date),
          hasCurtailedEnergy: !!dailyData?.totalCurtailedEnergy,
        });
        return {
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty: 0,
          price: 0,
          currentPrice: 0, 
        };
      }

      const url = new URL(
        "/api/curtailment/mining-potential",
        window.location.origin,
      );
      url.searchParams.set("date", formattedDate);
      url.searchParams.set("minerModel", selectedMinerModel);
      url.searchParams.set("energy", dailyData.totalCurtailedEnergy.toString());
      if (selectedLeadParty) {
        url.searchParams.set("leadParty", selectedLeadParty);
      }

      console.log("Fetching from URL:", url.toString());

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error("Failed to fetch mining potential");
      }

      const result = await response.json();
      console.log("Bitcoin calculation result:", result);
      return result;
    },
    enabled:
      !!formattedDate &&
      isValid(date) &&
      !!dailyData?.totalCurtailedEnergy,
  });

  const { data: monthlyBitcoinPotential } = useQuery<BitcoinCalculation>({
    queryKey: [
      `/api/curtailment/monthly-mining-potential/${format(date, 'yyyy-MM')}`,
      selectedMinerModel,
      selectedLeadParty
    ],
    queryFn: async () => {
      const url = new URL(
        `/api/curtailment/monthly-mining-potential/${format(date, 'yyyy-MM')}`,
        window.location.origin,
      );
      url.searchParams.set("minerModel", selectedMinerModel);
      if (selectedLeadParty) {
        url.searchParams.set("leadParty", selectedLeadParty);
      }

      console.log("Fetching monthly Bitcoin data from URL:", url.toString());

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error("Failed to fetch monthly mining potential");
      }

      const result = await response.json();
      console.log("Monthly Bitcoin calculation result:", result);
      return result;
    },
    enabled: !!date
  });
  
  const {
    data: yearlyBitcoinPotential,
    isLoading: isYearlyBitcoinLoading,
    error: yearlyBitcoinError
  } = useQuery<YearlyBitcoinCalculation>({
    queryKey: [
      `/api/mining-potential/yearly/${format(date, 'yyyy')}`,
      selectedMinerModel,
      selectedLeadParty
    ],
    queryFn: async () => {
      const url = new URL(
        `/api/mining-potential/yearly/${format(date, 'yyyy')}`,
        window.location.origin,
      );
      url.searchParams.set("minerModel", selectedMinerModel);
      if (selectedLeadParty) {
        url.searchParams.set("leadParty", selectedLeadParty);
      }

      console.log("Fetching yearly Bitcoin data from URL:", url.toString());

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error("Failed to fetch yearly mining potential");
      }

      const result = await response.json();
      console.log("Yearly Bitcoin calculation result:", result);
      return result;
    },
    enabled: !!date
  });

  const {
    data: monthlyData,
    isLoading: isMonthlyLoading,
    error: monthlyError,
  } = useQuery<MonthlySummary>({
    queryKey: [
      `/api/summary/monthly/${format(date, "yyyy-MM")}`,
      selectedLeadParty,
    ],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error("Invalid date selected");
      }

      const url = new URL(
        `/api/summary/monthly/${format(date, "yyyy-MM")}`,
        window.location.origin,
      );
      if (selectedLeadParty && selectedLeadParty !== "all") {
        url.searchParams.set("leadParty", selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        if (response.status === 404) {
          return {
            totalCurtailedEnergy: 0,
            totalPayment: 0
          };
        }
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!date && isValid(date),
  });

  const {
    data: yearlyData,
    isLoading: isYearlyLoading,
    error: yearlyError,
  } = useQuery<YearlySummary>({
    queryKey: [
      `/api/summary/yearly/${format(date, "yyyy")}`,
      selectedLeadParty,
    ],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error("Invalid date selected");
      }

      const url = new URL(
        `/api/summary/yearly/${format(date, "yyyy")}`,
        window.location.origin,
      );
      if (selectedLeadParty && selectedLeadParty !== "all") {
        url.searchParams.set("leadParty", selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!date && isValid(date),
  });

  const { data: hourlyData, isLoading: isHourlyLoading } = useQuery<
    HourlyData[]
  >({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, selectedLeadParty],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error("Invalid date selected");
      }

      const url = new URL(
        `/api/curtailment/hourly/${formattedDate}`,
        window.location.origin,
      );
      if (selectedLeadParty) {
        url.searchParams.set("leadParty", selectedLeadParty);
      }
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error("Failed to fetch hourly data");
      }
      return response.json();
    },
    enabled: !!formattedDate && isValid(date),
  });

  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(":").map(Number);
    const now = new Date();
    const selectedDate = new Date(date);

    if (format(now, "yyyy-MM-dd") === format(selectedDate, "yyyy-MM-dd")) {
      return hour > now.getHours();
    }
    return selectedDate > now;
  };

  return (
    <div className="min-h-screen">
      <FilterBar
        date={date}
        onDateChange={(newDate) => newDate && setDate(newDate)}
        selectedLeadParty={selectedLeadParty}
        onLeadPartyChange={(value) => setSelectedLeadParty(value || null)}
        curtailedLeadParties={curtailedLeadParties}
        selectedMinerModel={selectedMinerModel}
        onMinerModelChange={setSelectedMinerModel}
      />

      <div className="container mx-auto py-8">
        <h1 className="text-4xl font-bold mb-8">CurtailCoin</h1>

        <div className="grid md:grid-cols-2 gap-4 mb-8">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty ? "Farm Curtailed Energy" : "Curtailed MWh"}
              </CardTitle>
              <Wind className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div>
                  {isYearlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">
                      Loading...
                    </div>
                  ) : yearlyError ? (
                    <div className="text-sm text-red-500">
                      Failed to load yearly data
                    </div>
                  ) : yearlyData ? (
                    <div className="text-2xl font-bold">
                      {Number(yearlyData.totalCurtailedEnergy).toLocaleString()}{" "}
                      MWh
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">
                      No yearly data available
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {selectedLeadParty ? (
                      <>
                        Farm curtailed energy for {selectedLeadParty} in{" "}
                        {format(date, "yyyy")}
                      </>
                    ) : (
                      <>Total curtailed energy for {format(date, "yyyy")}</>
                    )}
                  </p>
                </div>

                <div>
                  <div className="text-sm font-medium">
                    Bitcoin could be mined
                  </div>
                  {isYearlyBitcoinLoading ? (
                    <div className="text-2xl font-bold animate-pulse">
                      Loading...
                    </div>
                  ) : yearlyBitcoinError ? (
                    <div className="text-sm text-red-500">
                      Failed to load yearly Bitcoin data
                    </div>
                  ) : yearlyBitcoinPotential ? (
                    <div className="text-2xl font-bold text-[#F7931A]">
                      ₿{yearlyBitcoinPotential.bitcoinMined.toFixed(8)}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">
                      No yearly mining data available
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    With {selectedMinerModel.replace("_", " ")} miners
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty
                  ? "Farm Payment & Value"
                  : "Yearly Payment & Value"}
              </CardTitle>
              <Battery className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div>
                  <div className="text-sm font-medium">
                    Paid for Curtailment
                  </div>
                  {isYearlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">
                      Loading...
                    </div>
                  ) : yearlyError ? (
                    <div className="text-sm text-red-500">
                      Failed to load yearly data
                    </div>
                  ) : yearlyData ? (
                    <div className="text-2xl font-bold">
                      £{Number(yearlyData.totalPayment).toLocaleString()}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">
                      No yearly data available
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {selectedLeadParty ? (
                      <>
                        Payment for {selectedLeadParty} in{" "}
                        {format(date, "yyyy")}
                      </>
                    ) : (
                      <>Total payment for {format(date, "yyyy")}</>
                    )}
                  </p>
                </div>

                <div>
                  <div className="text-sm font-medium">
                    Value if Bitcoin was mined
                  </div>
                  {isYearlyBitcoinLoading ? (
                    <div className="text-2xl font-bold animate-pulse">
                      Loading...
                    </div>
                  ) : yearlyBitcoinError ? (
                    <div className="text-sm text-red-500">
                      Failed to load yearly Bitcoin data
                    </div>
                  ) : yearlyBitcoinPotential ? (
                    <div className="text-2xl font-bold text-[#F7931A]">
                      £{yearlyBitcoinPotential.valueAtCurrentPrice.toLocaleString('en-GB', { maximumFractionDigits: 2 })}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">
                      No yearly mining data available
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    Estimated value at current BTC price
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="grid md:grid-cols-2 gap-4 mb-8">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty
                  ? "Farm Monthly Curtailed Energy"
                  : "Monthly Curtailed Energy"}
              </CardTitle>
              <Wind className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div>
                  {isMonthlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">
                      Loading...
                    </div>
                  ) : monthlyError ? (
                    <div className="text-sm text-red-500">
                      Failed to load monthly data
                    </div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold">
                      {Number(
                        monthlyData.totalCurtailedEnergy,
                      ).toLocaleString()}{" "}
                      MWh
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">
                      No monthly data available
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {selectedLeadParty ? (
                      <>
                        Farm curtailed energy for {selectedLeadParty} in{" "}
                        {format(date, "MMMM yyyy")}
                      </>
                    ) : (
                      <>
                        Total curtailed energy for {format(date, "MMMM yyyy")}
                      </>
                    )}
                  </p>
                </div>

                <div>
                  <div className="text-sm font-medium">
                    Bitcoin could be mined
                  </div>
                  {monthlyError ? (
                    <div className="text-sm text-red-500">
                      Failed to load monthly data
                    </div>
                  ) : monthlyData && monthlyBitcoinPotential ? (
                    <div className="text-2xl font-bold text-[#F7931A]">
                      ₿{monthlyBitcoinPotential.bitcoinMined.toFixed(8)}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">
                      No monthly data available
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    With {selectedMinerModel.replace("_", " ")} miners
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty
                  ? "Farm Monthly Payment & Value"
                  : "Monthly Payment & Value"}
              </CardTitle>
              <Battery className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div>
                  <div className="text-sm font-medium">
                    Paid for Curtailment
                  </div>
                  {isMonthlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">
                      Loading...
                    </div>
                  ) : monthlyError ? (
                    <div className="text-sm text-red-500">
                      Failed to load monthly data
                    </div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold">
                      £{Number(monthlyData.totalPayment).toLocaleString()}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">
                      No monthly data available
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {selectedLeadParty ? (
                      <>
                        Payment for {selectedLeadParty} in{" "}
                        {format(date, "MMMM yyyy")}
                      </>
                    ) : (
                      <>Total payment for {format(date, "MMMM yyyy")}</>
                    )}
                  </p>
                </div>

                <div>
                  <div className="text-sm font-medium">
                    Value if Bitcoin was mined
                  </div>
                  {monthlyError ? (
                    <div className="text-sm text-red-500">
                      Failed to load monthly data
                    </div>
                  ) : monthlyData && monthlyBitcoinPotential ? (
                    <div className="text-2xl font-bold text-[#F7931A]">
                      £{monthlyBitcoinPotential.valueAtCurrentPrice.toLocaleString('en-GB', { maximumFractionDigits: 2 })}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">
                      No monthly data available
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    Estimated value at current BTC price
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Hourly Curtailment</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col lg:flex-row gap-8">
              <div className="lg:w-1/4">
                <h3 className="text-lg font-semibold mb-4">Daily Stats</h3>
                <div className="space-y-6">
                  <div>
                    <div className="text-sm text-muted-foreground mb-1">
                      Curtailed Energy
                    </div>
                    {isDailyLoading ? (
                      <div className="text-3xl font-bold animate-pulse">
                        Loading...
                      </div>
                    ) : dailyError ? (
                      <div className="text-sm text-red-500">
                        {dailyError instanceof Error
                          ? dailyError.message
                          : "Failed to load daily data"}
                      </div>
                    ) : dailyData ? (
                      <div className="text-3xl font-bold">
                        {Number(
                          dailyData.totalCurtailedEnergy,
                        ).toLocaleString()}{" "}
                        MWh
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">
                        No daily data available
                      </div>
                    )}
                  </div>

                  <div>
                    <div className="text-sm text-muted-foreground mb-1">
                      Payment
                    </div>
                    {isDailyLoading ? (
                      <div className="text-3xl font-bold animate-pulse">
                        Loading...
                      </div>
                    ) : dailyError ? (
                      <div className="text-sm text-red-500">
                        {dailyError instanceof Error
                          ? dailyError.message
                          : "Failed to load daily data"}
                      </div>
                    ) : dailyData ? (
                      <div className="text-3xl font-bold">
                        £{Number(dailyData.totalPayment).toLocaleString()}
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">
                        No daily data available
                      </div>
                    )}
                  </div>

                  <div>
                    <div className="text-sm text-muted-foreground mb-1">
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger className="cursor-help">
                            Bitcoin Mining Potential
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>Network Difficulty: {bitcoinPotential?.difficulty ? bitcoinPotential.difficulty.toLocaleString() : 'Not available'}</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                    </div>
                    {isDailyLoading ? (
                      <div className="text-3xl font-bold animate-pulse">
                        Loading...
                      </div>
                    ) : dailyError ? (
                      <div className="text-sm text-red-500">
                        {dailyError instanceof Error
                          ? dailyError.message
                          : "Failed to load daily data"}
                      </div>
                    ) : dailyData ? (
                      <div className="text-3xl font-bold text-[#F7931A]">
                        ₿{(bitcoinPotential?.bitcoinMined ?? 0).toFixed(8)}
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">
                        No daily data available
                      </div>
                    )}
                    <p className="text-xs text-muted-foreground mt-1">
                      Potential mining with {selectedMinerModel.replace("_", " ")} miners
                    </p>
                  </div>

                  <div>
                    <div className="text-sm text-muted-foreground mb-1">
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger className="cursor-help">
                            Value if Bitcoin was mined
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>Current price: £{(bitcoinPotential?.currentPrice ?? 0).toLocaleString()}</p>
                            <p>USD/GBP Rate: 0.79</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                    </div>
                    {isDailyLoading ? (
                      <div className="text-3xl font-bold animate-pulse">
                        Loading...
                      </div>
                    ) : dailyError ? (
                      <div className="text-sm text-red-500">
                        {dailyError instanceof Error
                          ? dailyError.message
                          : "Failed to load daily data"}
                      </div>
                    ) : dailyData ? (
                      <div className="text-3xl font-bold text-[#F7931A]">
                        £{(bitcoinPotential?.valueAtCurrentPrice ?? 0).toLocaleString('en-GB', { maximumFractionDigits: 2 })}
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">
                        No daily data available
                      </div>
                    )}
                    <p className="text-xs text-muted-foreground mt-1">
                      Estimated value at current BTC price
                    </p>
                  </div>
                </div>
              </div>

              <div className="lg:w-3/4 h-[400px]">
                {isHourlyLoading ? (
                  <div className="h-full flex items-center justify-center">
                    <div className="animate-pulse">Loading chart data...</div>
                  </div>
                ) : hourlyData ? (
                  <DualAxisChart
                    data={hourlyData.map((hour) => ({
                      hour: `${hour.hour.split(":")[0].padStart(2, "0")}:00`,
                      curtailedEnergy: isHourInFuture(hour.hour)
                        ? 0
                        : hour.curtailedEnergy,
                      bitcoinMined: isHourInFuture(hour.hour)
                        ? 0
                        : (hour.curtailedEnergy *
                            (bitcoinPotential?.bitcoinMined ?? 0)) /
                          (dailyData?.totalCurtailedEnergy ?? 1),
                    }))}
                    chartConfig={{
                      leftAxis: {
                        label: "Curtailed Energy (MWh)",
                        dataKey: "curtailedEnergy",
                        color: "hsl(var(--primary))",
                      },
                      rightAxis: {
                        label: "Potential Bitcoin Mined (₿)",
                        dataKey: "bitcoinMined",
                        color: "#F7931A",
                      },
                    }}
                  />
                ) : (
                  <div className="h-full flex items-center justify-center text-muted-foreground">
                    No hourly data available
                  </div>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}