import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Wind, ArrowUpDown, ChevronDown, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { formatEnergy, formatGBP, formatBitcoin } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

// Types
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

export function CurtailedFarmsTable({ 
  date, 
  minerModel,
  timeframe = 'month' 
}: { 
  date: Date; 
  minerModel: string;
  timeframe?: 'day' | 'month' | 'year';
}) {
  const [expandedFarms, setExpandedFarms] = useState<Set<string>>(new Set());
  const [sortConfig, setSortConfig] = useState<SortConfig>({
    key: 'totalCurtailedEnergy',
    direction: 'desc'
  });

  // Format date based on timeframe
  const formattedDate = timeframe === 'day' 
    ? new Date(date).toISOString().slice(0, 10)
    : timeframe === 'month'
      ? new Date(date).toISOString().slice(0, 7)
      : new Date(date).getFullYear().toString();

  const { data, isLoading, error } = useQuery<FarmDataResponse>({
    queryKey: [`/api/farm-tables/grouped-data`, timeframe, formattedDate, minerModel],
    queryFn: async () => {
      const response = await fetch(
        `/api/farm-tables/grouped-data?timeframe=${timeframe}&value=${formattedDate}&minerModel=${minerModel}`
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

  const sortedFarms = data?.farms 
    ? [...data.farms].sort((a, b) => {
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

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-xl font-bold flex items-center gap-2">
          <Wind className="h-5 w-5 text-sky-500" />
          Curtailed Wind Farms
        </CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-4">
            <div className="flex items-center justify-center py-4">
              <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
            </div>
            <p className="text-sm text-center text-muted-foreground">Fetching data...</p>
          </div>
        ) : error ? (
          <div className="p-4 rounded-lg bg-destructive/10 text-destructive">
            <p className="text-sm font-medium">Unable to fetch farm data</p>
            <p className="text-xs mt-1">
              Please try again later or contact support if the problem persists.
            </p>
          </div>
        ) : sortedFarms.length > 0 ? (
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
                        {data?.meta?.currentPrice && (
                          <div className="text-[10px] sm:text-xs text-green-500">
                            {formatGBP(farm.totalPotentialBtc * data.meta.currentPrice)}
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
                          {data?.meta?.currentPrice && (
                            <div className="text-[10px] sm:text-xs text-green-500">
                              {formatGBP(subFarm.potentialBtc * data.meta.currentPrice)}
                            </div>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </>
                ))}
              </TableBody>
            </Table>
            <div className="text-xs text-muted-foreground text-center mt-3">
              Bitcoin mining data using {minerModel} miners.
            </div>
          </div>
        ) : (
          <p className="text-center text-muted-foreground py-8">No farm data available for the selected period.</p>
        )}
      </CardContent>
    </Card>
  );
}