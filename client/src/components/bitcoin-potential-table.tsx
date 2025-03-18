"use client";

import { format } from "date-fns";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { formatEnergy, formatGBP, formatBitcoin } from "@/lib/utils";
import { ArrowUpDown, ChevronDown, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

// Interfaces for Farm Data Table
interface FarmDetail {
  farmId: string;
  curtailedEnergy: number;
  percentageOfTotal: number;
  potentialBtc: number;
  payment: number;
}

interface GroupedFarm {
  leadPartyName: string;
  totalCurtailedEnergy: number;
  totalPercentageOfTotal: number;
  totalPotentialBtc: number;
  totalPayment: number;
  farms: FarmDetail[];
}

interface FarmDataResponse {
  farms: GroupedFarm[];
  meta: {
    currentPrice: number;
    date: string;
    timeframe: string;
    minerModel: string;
  };
}

interface SortConfig {
  key: keyof GroupedFarm;
  direction: 'asc' | 'desc';
}

interface BitcoinPotentialTableProps {
  timeframe: string;
  date: Date;
  minerModel: string;
  farmId: string;
}

export default function BitcoinPotentialTable({
  timeframe,
  date,
  minerModel,
  farmId,
}: BitcoinPotentialTableProps) {
  const formattedDate = format(date, "yyyy-MM-dd");
  const yearMonth = format(date, "yyyy-MM");
  const year = format(date, "yyyy");
  
  // Determine which bitcoin potential endpoint to use based on timeframe
  const bitcoinEndpoint = 
    timeframe === "yearly" ? `/api/mining-potential/yearly/${year}` :
    timeframe === "monthly" ? `/api/curtailment/monthly-mining-potential/${yearMonth}` :
    `/api/curtailment/mining-potential`;
  
  // Determine which summary endpoint to use based on timeframe  
  const summaryEndpoint = 
    timeframe === "yearly" ? `/api/summary/yearly/${year}` :
    timeframe === "monthly" ? `/api/summary/monthly/${yearMonth}` :
    `/api/summary/daily/${formattedDate}`;
  
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
  
  // Get title based on timeframe and farmId
  const title = 
    farmId ? 
      (timeframe === "yearly" ? `Farm Bitcoin Data (${year})` : 
       timeframe === "monthly" ? `Farm Bitcoin Data (${format(date, "MMMM yyyy")})` : 
       `Farm Bitcoin Data (${format(date, "PP")})`) :
      (timeframe === "yearly" ? `Bitcoin Mining Potential (${year})` : 
       timeframe === "monthly" ? `Bitcoin Mining Potential (${format(date, "MMMM yyyy")})` : 
       `Bitcoin Mining Potential (${format(date, "PP")})`);
  
  // Format FarmData timeframe from app timeframe
  const farmTimeframe = timeframe === "daily" ? "day" : 
                       timeframe === "monthly" ? "month" : "year";
  
  // State for table sorting and expansion
  const [expandedFarms, setExpandedFarms] = useState<Set<string>>(new Set());
  const [sortConfig, setSortConfig] = useState<SortConfig>({
    key: 'totalCurtailedEnergy',
    direction: 'desc'
  });

  // Format date for API
  const apiFormattedDate = timeframe === 'daily' 
    ? formattedDate
    : timeframe === 'monthly'
      ? yearMonth
      : year;

  // Fetch farm data
  const { 
    data: farmData, 
    isLoading: isFarmDataLoading, 
    error: farmDataError 
  } = useQuery<FarmDataResponse>({
    queryKey: [`/api/farm-tables/grouped-data`, farmTimeframe, apiFormattedDate, minerModel],
    queryFn: async () => {
      const response = await fetch(
        `/api/farm-tables/grouped-data?timeframe=${farmTimeframe}&value=${apiFormattedDate}&minerModel=${minerModel}`
      );
      if (!response.ok) {
        throw new Error('Failed to fetch farm data');
      }
      return response.json();
    },
  });

  const toggleFarmExpansion = (leadPartyName: string) => {
    const newExpanded = new Set(expandedFarms);
    if (expandedFarms.has(leadPartyName)) {
      newExpanded.delete(leadPartyName);
    } else {
      newExpanded.add(leadPartyName);
    }
    setExpandedFarms(newExpanded);
  };

  const handleSort = (key: keyof GroupedFarm) => {
    setSortConfig({
      key,
      direction: 
        sortConfig.key === key && sortConfig.direction === 'asc' 
          ? 'desc' 
          : 'asc',
    });
  };

  const sortedFarms = farmData?.farms 
    ? [...farmData.farms].sort((a, b) => {
        // Handle string comparisons for lead party name
        if (sortConfig.key === 'leadPartyName') {
          const compareResult = a.leadPartyName.localeCompare(b.leadPartyName);
          return sortConfig.direction === 'asc' ? compareResult : -compareResult;
        }
        
        // Handle numeric comparisons for other fields
        const aValue = a[sortConfig.key] as number;
        const bValue = b[sortConfig.key] as number;
        const compare = aValue < bValue ? -1 : 1;
        return sortConfig.direction === 'asc' ? compare : -compare;
      }) 
    : [];

  const SortButton = ({ label, sortKey, tooltip }: { 
    label: string; 
    sortKey: keyof GroupedFarm;
    tooltip: string;
  }) => (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button 
            variant="ghost" 
            onClick={() => handleSort(sortKey)}
            className="hover:text-sky-500 text-xs sm:text-sm whitespace-nowrap"
          >
            {label}
            <ArrowUpDown className="ml-1 sm:ml-2 h-3 w-3 sm:h-4 sm:w-4" />
          </Button>
        </TooltipTrigger>
        <TooltipContent className="max-w-[200px] text-xs">
          {tooltip}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
  
  // Overall loading state
  const isLoading = isSummaryLoading || isBitcoinLoading || isFarmDataLoading;
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
          </div>
        ) : (
          <div>
            {/* Bitcoin summary section at the top */}
            <div className="grid grid-cols-2 gap-4 mb-8">
              <div className="p-4 rounded-lg bg-muted/50">
                <div className="text-sm font-medium mb-1">Curtailed Energy</div>
                <div className="text-xl font-bold">{formatEnergy(Number(summaryData.totalCurtailedEnergy))}</div>
              </div>
              <div className="p-4 rounded-lg bg-muted/50">
                <div className="text-sm font-medium mb-1">Bitcoin Potential</div>
                <div className="text-xl font-bold">{formatBitcoin(Number(bitcoinData.bitcoinMined))}</div>
              </div>
              <div className="p-4 rounded-lg bg-muted/50">
                <div className="text-sm font-medium mb-1">Bitcoin Value</div>
                <div className="text-xl font-bold text-green-600">{formatGBP(Number(bitcoinData.valueAtCurrentPrice))}</div>
              </div>
              <div className="p-4 rounded-lg bg-muted/50">
                <div className="text-sm font-medium mb-1">Curtailment Payment</div>
                <div className="text-xl font-bold text-orange-500">{formatGBP(Number(summaryData.totalPayment))}</div>
              </div>
            </div>
            
            {/* Wind Farm Data table */}
            <div className="text-xs text-muted-foreground text-center mb-4">
              Bitcoin mining data using {minerModel.replace("_", " ")} miners at £{formatGBP(Number(bitcoinData.currentPrice))}
            </div>
            
            {farmDataError ? (
              <div className="p-4 rounded-lg bg-destructive/10 text-destructive">
                <p className="text-sm font-medium">Unable to fetch farm data</p>
                <p className="text-xs mt-1">
                  Please try again later or contact support if the problem persists.
                </p>
              </div>
            ) : !sortedFarms || sortedFarms.length === 0 ? (
              <p className="text-center text-muted-foreground py-4">No farm data available for the selected period.</p>
            ) : (
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-8 px-1 sm:px-2" />
                      <TableHead className="w-[30%] min-w-[120px]">
                        <SortButton 
                          label="Wind Farm" 
                          sortKey="leadPartyName" 
                          tooltip="The name of the wind farm operator. Click to expand and see individual turbine units."
                        />
                      </TableHead>
                      <TableHead className="w-[15%] text-right min-w-[60px]">
                        <SortButton 
                          label="% Total" 
                          sortKey="totalPercentageOfTotal" 
                          tooltip="Percentage of total curtailed energy from all wind farms for the selected period"
                        />
                      </TableHead>
                      <TableHead className="w-[20%] text-right min-w-[80px]">
                        <SortButton 
                          label="MWh" 
                          sortKey="totalCurtailedEnergy" 
                          tooltip="Total megawatt hours of curtailed energy (energy that could have been generated but was restricted)"
                        />
                      </TableHead>
                      <TableHead className="w-[20%] text-right min-w-[80px]">
                        <SortButton 
                          label="Cost" 
                          sortKey="totalPayment" 
                          tooltip="Total payment received by the wind farm for curtailing their energy generation"
                        />
                      </TableHead>
                      <TableHead className="w-[15%] text-right min-w-[70px]">
                        <SortButton 
                          label="BTC" 
                          sortKey="totalPotentialBtc" 
                          tooltip="Potential Bitcoin that could have been mined using the curtailed energy (shown with GBP value below)"
                        />
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {sortedFarms.map((farm) => (
                      <>
                        <TableRow 
                          key={farm.leadPartyName}
                          className="cursor-pointer hover:bg-muted/50"
                          onClick={() => toggleFarmExpansion(farm.leadPartyName)}
                        >
                          <TableCell className="px-1 sm:px-2">
                            {expandedFarms.has(farm.leadPartyName) ? (
                              <ChevronDown className="h-3 w-3 sm:h-4 sm:w-4" />
                            ) : (
                              <ChevronRight className="h-3 w-3 sm:h-4 sm:w-4" />
                            )}
                          </TableCell>
                          <TableCell className="font-medium text-xs sm:text-sm">{farm.leadPartyName}</TableCell>
                          <TableCell className="text-right text-xs sm:text-sm">
                            {farm.totalPercentageOfTotal.toFixed(1)}%
                          </TableCell>
                          <TableCell className="text-right text-xs sm:text-sm">
                            {formatEnergy(farm.totalCurtailedEnergy)}
                          </TableCell>
                          <TableCell className="text-right text-orange-500 text-xs sm:text-sm">
                            {formatGBP(farm.totalPayment)}
                          </TableCell>
                          <TableCell className="text-right text-xs sm:text-sm">
                            <div>{formatBitcoin(farm.totalPotentialBtc)}</div>
                            {farmData?.meta?.currentPrice && (
                              <div className="text-[10px] sm:text-xs text-green-500">
                                {formatGBP(farm.totalPotentialBtc * farmData.meta.currentPrice)}
                              </div>
                            )}
                          </TableCell>
                        </TableRow>
                        {expandedFarms.has(farm.leadPartyName) && farm.farms.map((subFarm) => (
                          <TableRow key={`${farm.leadPartyName}-${subFarm.farmId}`} className="bg-muted/30">
                            <TableCell className="px-1 sm:px-2" />
                            <TableCell className="text-xs sm:text-sm text-muted-foreground pl-4 sm:pl-8">
                              {subFarm.farmId}
                            </TableCell>
                            <TableCell className="text-xs sm:text-sm text-muted-foreground text-right">
                              {subFarm.percentageOfTotal.toFixed(1)}%
                            </TableCell>
                            <TableCell className="text-xs sm:text-sm text-muted-foreground text-right">
                              {formatEnergy(subFarm.curtailedEnergy)}
                            </TableCell>
                            <TableCell className="text-xs sm:text-sm text-orange-500/70 text-right">
                              {formatGBP(subFarm.payment)}
                            </TableCell>
                            <TableCell className="text-xs sm:text-sm text-muted-foreground text-right">
                              <div>{formatBitcoin(subFarm.potentialBtc)}</div>
                              {farmData?.meta?.currentPrice && (
                                <div className="text-[10px] sm:text-xs text-green-500">
                                  {formatGBP(subFarm.potentialBtc * farmData.meta.currentPrice)}
                                </div>
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                      </>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}