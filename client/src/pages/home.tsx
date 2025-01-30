import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, isValid, isToday } from "date-fns";
import { FilterBar } from "@/components/ui/filter-bar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, Battery, Calendar as CalendarIcon, Building, Bitcoin } from "lucide-react";
import { ChartContainer, ChartTooltip } from "@/components/ui/chart";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Scatter } from "recharts";

interface BitcoinCalculation {
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  difficulty: number;
  price: number;
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
  bitcoinMined?: number;
}

export default function Home() {
  const [date, setDate] = useState<Date>(() => {
    const today = new Date();
    const startDate = new Date("2023-01-01");
    return today < startDate ? startDate : today;
  });
  const [selectedLeadParty, setSelectedLeadParty] = useState<string | null>(null);
  const [selectedMinerModel, setSelectedMinerModel] = useState("S19J_PRO");

  const formattedDate = format(date, 'yyyy-MM-dd');

  // Fetch curtailed lead parties for the selected date
  const { data: curtailedLeadParties = [] } = useQuery<string[]>({
    queryKey: [`/api/lead-parties/${formattedDate}`],
    enabled: !!formattedDate && isValid(date)
  });

  // Reset selected lead party if it's not in the curtailed list for the new date
  useEffect(() => {
    if (selectedLeadParty && !curtailedLeadParties.includes(selectedLeadParty)) {
      setSelectedLeadParty(null);
    }
  }, [formattedDate, curtailedLeadParties, selectedLeadParty]);

  const { data: dailyData, isLoading: isDailyLoading, error: dailyError } = useQuery<DailySummary>({
    queryKey: [`/api/summary/daily/${formattedDate}`, selectedLeadParty],
    queryFn: async () => {
      // Validate date before making the request
      if (!isValid(date)) {
        throw new Error('Invalid date selected');
      }

      const url = new URL(`/api/summary/daily/${formattedDate}`, window.location.origin);
      if (selectedLeadParty && selectedLeadParty !== 'all') {
        url.searchParams.set('leadParty', selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!formattedDate && isValid(date)
  });

  const { data: bitcoinPotential } = useQuery<BitcoinCalculation>({
    queryKey: [`/api/curtailment/mining-potential`, selectedMinerModel, dailyData?.totalCurtailedEnergy],
    queryFn: async () => {
      console.log('Bitcoin calculation parameters:', {
        date: formattedDate,
        isToday: isToday(date),
        minerModel: selectedMinerModel,
        curtailedEnergy: dailyData?.totalCurtailedEnergy
      });

      if (!isValid(date) || !isToday(date) || !dailyData?.totalCurtailedEnergy) {
        console.log('Skipping Bitcoin calculation:', {
          isValidDate: isValid(date),
          isToday: isToday(date),
          hasCurtailedEnergy: !!dailyData?.totalCurtailedEnergy
        });
        return {
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty: 0,
          price: 0
        };
      }

      const url = new URL('/api/curtailment/mining-potential', window.location.origin);
      url.searchParams.set('date', formattedDate);
      url.searchParams.set('minerModel', selectedMinerModel);
      url.searchParams.set('energy', dailyData.totalCurtailedEnergy.toString());

      console.log('Fetching from URL:', url.toString());

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch mining potential');
      }

      const result = await response.json();
      console.log('Bitcoin calculation result:', result);
      return result;
    },
    enabled: !!formattedDate && isValid(date) && isToday(date) && !!dailyData?.totalCurtailedEnergy
  });

  const { data: monthlyData, isLoading: isMonthlyLoading, error: monthlyError } = useQuery<MonthlySummary>({
    queryKey: [`/api/summary/monthly/${format(date, 'yyyy-MM')}`, selectedLeadParty],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error('Invalid date selected');
      }

      const url = new URL(`/api/summary/monthly/${format(date, 'yyyy-MM')}`, window.location.origin);
      if (selectedLeadParty && selectedLeadParty !== 'all') {
        url.searchParams.set('leadParty', selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!date && isValid(date)
  });

  const { data: yearlyData, isLoading: isYearlyLoading, error: yearlyError } = useQuery<YearlySummary>({
    queryKey: [`/api/summary/yearly/${format(date, 'yyyy')}`, selectedLeadParty],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error('Invalid date selected');
      }

      const url = new URL(`/api/summary/yearly/${format(date, 'yyyy')}`, window.location.origin);
      if (selectedLeadParty && selectedLeadParty !== 'all') {
        url.searchParams.set('leadParty', selectedLeadParty);
      }

      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`API Error: ${response.status}`);
      }

      return response.json();
    },
    enabled: !!date && isValid(date)
  });

  // Fetch hourly data with improved error handling
  const { data: hourlyData, isLoading: isHourlyLoading } = useQuery<HourlyData[]>({
    queryKey: [`/api/curtailment/hourly/${formattedDate}`, selectedLeadParty],
    queryFn: async () => {
      if (!isValid(date)) {
        throw new Error('Invalid date selected');
      }

      const url = new URL(`/api/curtailment/hourly/${formattedDate}`, window.location.origin);
      if (selectedLeadParty) {
        url.searchParams.set('leadParty', selectedLeadParty);
      }
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error('Failed to fetch hourly data');
      }
      return response.json();
    },
    enabled: !!formattedDate && isValid(date)
  });

  // Function to check if a given hour is in the future
  const isHourInFuture = (hourStr: string) => {
    const [hour] = hourStr.split(':').map(Number);
    const now = new Date();
    const selectedDate = new Date(date);

    if (format(now, 'yyyy-MM-dd') === format(selectedDate, 'yyyy-MM-dd')) {
      return hour > now.getHours();
    }
    return selectedDate > now;
  };

  const calculateHourlyBitcoin = (hourlyData: HourlyData[] = [], bitcoinPotential?: BitcoinCalculation): HourlyData[] => {
    if (!hourlyData.length || !bitcoinPotential || !isToday(date)) {
      return hourlyData;
    }

    const totalEnergy = dailyData?.totalCurtailedEnergy || 0;
    if (totalEnergy <= 0) return hourlyData;

    // Calculate bitcoin per MWh ratio
    const bitcoinPerMWh = bitcoinPotential.bitcoinMined / totalEnergy;

    return hourlyData.map(hour => ({
      ...hour,
      bitcoinMined: hour.curtailedEnergy * bitcoinPerMWh
    }));
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
        <h1 className="text-4xl font-bold mb-8">Wind Farm Curtailment Data</h1>

        <div className="grid md:grid-cols-2 gap-4 mb-8">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty ? 'Farm Curtailed Energy' : 'Curtailed MWh'}
              </CardTitle>
              <Wind className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {/* Energy Section */}
                <div>
                  {isYearlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : yearlyError ? (
                    <div className="text-sm text-red-500">Failed to load yearly data</div>
                  ) : yearlyData ? (
                    <div className="text-2xl font-bold">
                      {Number(yearlyData.totalCurtailedEnergy).toLocaleString()} MWh
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No yearly data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {selectedLeadParty ? (
                      <>Farm curtailed energy for {selectedLeadParty} in {format(date, 'yyyy')}</>
                    ) : (
                      <>Total curtailed energy for {format(date, 'yyyy')}</>
                    )}
                  </p>
                </div>

                {/* Bitcoin Mining Potential */}
                <div>
                  <div className="text-sm font-medium">Bitcoin could be mined</div>
                  {isYearlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : yearlyError ? (
                    <div className="text-sm text-red-500">Failed to load yearly data</div>
                  ) : yearlyData ? (
                    <div className="text-2xl font-bold text-[#F7931A]">
                      ₿0.00
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No yearly data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    With {selectedMinerModel.replace('_', ' ')} miners
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty ? 'Farm Payment & Value' : 'Yearly Payment & Value'}
              </CardTitle>
              <Battery className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {/* Payment Section */}
                <div>
                  <div className="text-sm font-medium">Paid for Curtailment</div>
                  {isYearlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : yearlyError ? (
                    <div className="text-sm text-red-500">Failed to load yearly data</div>
                  ) : yearlyData ? (
                    <div className="text-2xl font-bold">
                      £{Number(yearlyData.totalPayment).toLocaleString()}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No yearly data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {selectedLeadParty ? (
                      <>Payment for {selectedLeadParty} in {format(date, 'yyyy')}</>
                    ) : (
                      <>Total payment for {format(date, 'yyyy')}</>
                    )}
                  </p>
                </div>

                {/* Bitcoin Value Section */}
                <div>
                  <div className="text-sm font-medium">Value if Bitcoin was mined</div>
                  {isYearlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : yearlyError ? (
                    <div className="text-sm text-red-500">Failed to load yearly data</div>
                  ) : yearlyData ? (
                    <div className="text-2xl font-bold text-[#F7931A]">
                      £0.00
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No yearly data available</div>
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
                {selectedLeadParty ? 'Farm Monthly Curtailed Energy' : 'Monthly Curtailed Energy'}
              </CardTitle>
              <Wind className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {/* Energy Section */}
                <div>
                  {isMonthlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : monthlyError ? (
                    <div className="text-sm text-red-500">Failed to load monthly data</div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold">
                      {Number(monthlyData.totalCurtailedEnergy).toLocaleString()} MWh
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No monthly data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {selectedLeadParty ? (
                      <>Farm curtailed energy for {selectedLeadParty} in {format(date, 'MMMM yyyy')}</>
                    ) : (
                      <>Total curtailed energy for {format(date, 'MMMM yyyy')}</>
                    )}
                  </p>
                </div>

                {/* Bitcoin Mining Potential */}
                <div>
                  <div className="text-sm font-medium">Bitcoin could be mined</div>
                  {isMonthlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : monthlyError ? (
                    <div className="text-sm text-red-500">Failed to load monthly data</div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold text-[#F7931A]">
                      ₿0.00
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No monthly data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    With {selectedMinerModel.replace('_', ' ')} miners
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                {selectedLeadParty ? 'Farm Monthly Payment & Value' : 'Monthly Payment & Value'}
              </CardTitle>
              <Battery className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {/* Payment Section */}
                <div>
                  <div className="text-sm font-medium">Paid for Curtailment</div>
                  {isMonthlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : monthlyError ? (
                    <div className="text-sm text-red-500">Failed to load monthly data</div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold">
                      £{Number(monthlyData.totalPayment).toLocaleString()}
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No monthly data available</div>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    {selectedLeadParty ? (
                      <>Payment for {selectedLeadParty} in {format(date, 'MMMM yyyy')}</>
                    ) : (
                      <>Total payment for {format(date, 'MMMM yyyy')}</>
                    )}
                  </p>
                </div>

                {/* Bitcoin Value Section */}
                <div>
                  <div className="text-sm font-medium">Value if Bitcoin was mined</div>
                  {isMonthlyLoading ? (
                    <div className="text-2xl font-bold animate-pulse">Loading...</div>
                  ) : monthlyError ? (
                    <div className="text-sm text-red-500">Failed to load monthly data</div>
                  ) : monthlyData ? (
                    <div className="text-2xl font-bold text-[#F7931A]">
                      £0.00
                    </div>
                  ) : (
                    <div className="text-sm text-muted-foreground">No monthly data available</div>
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
            <CardTitle>
              Hourly Curtailment
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col lg:flex-row gap-8">
              {/* Daily Stats Section */}
              <div className="lg:w-1/4">
                <h3 className="text-lg font-semibold mb-4">Daily Stats</h3>
                <div className="space-y-6">
                  <div>
                    <div className="text-sm text-muted-foreground mb-1">Curtailed Energy</div>
                    {isDailyLoading ? (
                      <div className="text-3xl font-bold animate-pulse">Loading...</div>
                    ) : dailyError ? (
                      <div className="text-sm text-red-500">
                        {dailyError instanceof Error ? dailyError.message : 'Failed to load daily data'}
                      </div>
                    ) : dailyData ? (
                      <div className="text-3xl font-bold">
                        {Number(dailyData.totalCurtailedEnergy).toLocaleString()} MWh
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">No daily data available</div>
                    )}
                  </div>

                  <div>
                    <div className="text-sm text-muted-foreground mb-1">Payment</div>
                    {isDailyLoading ? (
                      <div className="text-3xl font-bold animate-pulse">Loading...</div>
                    ) : dailyError ? (
                      <div className="text-sm text-red-500">
                        {dailyError instanceof Error ? dailyError.message : 'Failed to load daily data'}
                      </div>
                    ) : dailyData ? (
                      <div className="text-3xl font-bold">
                        £{Number(dailyData.totalPayment).toLocaleString()}
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">No daily data available</div>
                    )}
                  </div>

                  <div>
                    <div className="text-sm text-muted-foreground mb-1">Mining Opportunity Loss</div>
                    {isDailyLoading ? (
                      <div className="text-3xl font-bold animate-pulse">Loading...</div>
                    ) : dailyError ? (
                      <div className="text-sm text-red-500">
                        {dailyError instanceof Error ? dailyError.message : 'Failed to load daily data'}
                      </div>
                    ) : dailyData ? (
                      <div className="text-3xl font-bold text-[#F7931A]">
                        ₿{bitcoinPotential?.bitcoinMined.toFixed(8) || '0.00'}
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">No daily data available</div>
                    )}
                    <p className="text-xs text-muted-foreground mt-1">
                      Potential mining with {selectedMinerModel.replace('_', ' ')} miners
                    </p>
                  </div>
                </div>
              </div>

              {/* Chart Section */}
              <div className="lg:w-3/4 h-[400px]">
                {isHourlyLoading ? (
                  <div className="h-full flex items-center justify-center">
                    <div className="animate-pulse">Loading chart data...</div>
                  </div>
                ) : hourlyData ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={calculateHourlyBitcoin(hourlyData, bitcoinPotential)}
                      margin={{ top: 20, right: 60, left: 60, bottom: 20 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis
                        dataKey="hour"
                        interval={2}
                        tick={{ fontSize: 12 }}
                      />
                      <YAxis
                        yAxisId="left"
                        label={{
                          value: 'Curtailed Energy (MWh)',
                          angle: -90,
                          position: 'insideLeft',
                          offset: -40,
                          style: { fontSize: 12 }
                        }}
                        tick={{ fontSize: 12 }}
                      />
                      <YAxis
                        yAxisId="right"
                        orientation="right"
                        label={{
                          value: 'Bitcoin Mining Potential (₿)',
                          angle: 90,
                          position: 'insideRight',
                          offset: -40,
                          style: { fontSize: 12, fill: '#F7931A' }
                        }}
                        tick={{ fontSize: 12, fill: '#F7931A' }}
                      />
                      <ChartTooltip
                        content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const data = payload[0];
                          const hour = data.payload.hour;
                          const energyValue = Number(data.payload.curtailedEnergy);
                          const bitcoinValue = data.payload.bitcoinMined;

                          let message = "";
                          if (isHourInFuture(hour)) {
                            message = "Data not available yet";
                          } else if (energyValue === 0) {
                            message = "No curtailment detected";
                          } else {
                            message = `${energyValue.toFixed(2)} MWh${bitcoinValue ? ` / ₿${bitcoinValue.toFixed(8)}` : ''}`;
                          }

                          return (
                            <div className="rounded-lg border bg-background p-2 shadow-md">
                              <div className="grid gap-2">
                                <div className="flex items-center gap-2">
                                  <div className="h-2 w-2 rounded-full bg-primary" />
                                  <span className="font-medium">{hour}</span>
                                </div>
                                <div className="text-sm text-muted-foreground">
                                  {message}
                                </div>
                              </div>
                            </div>
                          );
                        }}
                      />
                      <Bar
                        yAxisId="left"
                        dataKey="curtailedEnergy"
                        fill="hsl(var(--primary))"
                      />
                      <Scatter
                        yAxisId="right"
                        dataKey="bitcoinMined"
                        fill="#F7931A"
                        shape={(props: any) => {
                          const { cx, cy } = props;
                          return (
                            <g transform={`translate(${cx - 8},${cy - 8})`}>
                              <circle cx="8" cy="8" r="6" fill="#F7931A" />
                              <text x="8" y="8" textAnchor="middle" dy=".3em" fill="white" fontSize="10">
                                ₿
                              </text>
                            </g>
                          );
                        }}
                      />
                    </BarChart>
                  </ResponsiveContainer>
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