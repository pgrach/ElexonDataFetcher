import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Wind, ArrowUpDown, ChevronDown, ChevronRight } from "lucide-react";
import { formatEnergy, formatGBP, formatBitcoin } from "@/lib/utils";
import { format } from 'date-fns';
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

// Farm Table Header Component
function FarmTableHeader({ onSort, sortConfig }: { 
  onSort: (key: keyof GroupedFarm) => void;
  sortConfig: SortConfig | null;
}) {
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
            onClick={() => onSort(sortKey)}
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
            tooltip="Percentage of total curtailed energy from all wind farms for the selected date"
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
  );
}

// Farm Table Row Component
function FarmTableRow({ 
  farm, 
  isExpanded, 
  onToggle,
  btcPrice
}: {
  farm: GroupedFarm;
  isExpanded: boolean;
  onToggle: () => void;
  btcPrice: number;
}) {
  return (
    <>
      <TableRow 
        className="cursor-pointer hover:bg-muted/50"
        onClick={onToggle}
      >
        <TableCell className="px-1 sm:px-2">
          {isExpanded ? (
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
          <div className="text-[10px] sm:text-xs text-green-500">
            {formatGBP(farm.totalPotentialBtc * btcPrice)}
          </div>
        </TableCell>
      </TableRow>
      {isExpanded && farm.farms.map((subFarm) => (
        <TableRow key={subFarm.farmId} className="bg-muted/30">
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
            <div className="text-[10px] sm:text-xs text-green-500">
              {formatGBP(subFarm.potentialBtc * btcPrice)}
            </div>
          </TableCell>
        </TableRow>
      ))}
    </>
  );
}

// Main Dialog Component
export function FarmDataTable({ 
  date, 
  minerModel,
  timeframe = 'day' 
}: { 
  date: Date; 
  minerModel: string;
  timeframe?: 'day' | 'month' | 'year';
}) {
  const [open, setOpen] = useState(false);
  const [expandedFarms, setExpandedFarms] = useState<Set<string>>(new Set());
  const [sortConfig, setSortConfig] = useState<SortConfig | null>({
    key: 'totalCurtailedEnergy',
    direction: 'desc'
  });

  // Format date based on timeframe
  const formattedDate = timeframe === 'day' 
    ? format(date, 'yyyy-MM-dd')
    : timeframe === 'month'
      ? format(date, 'yyyy-MM')
      : format(date, 'yyyy');

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
    enabled: open,
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
    setSortConfig((currentSort) => ({
      key,
      direction: 
        currentSort?.key === key && currentSort.direction === 'asc' 
          ? 'desc' 
          : 'asc',
    }));
  };

  const sortedFarms = data?.farms 
    ? [...data.farms].sort((a, b) => {
        if (!sortConfig) return 0;
        
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

  // Format date for display
  const displayDate = timeframe === 'day'
    ? format(date, 'MMMM d, yyyy')
    : timeframe === 'month'
      ? format(date, 'MMMM yyyy')
      : format(date, 'yyyy');

  const periodLabel = timeframe === 'day'
    ? 'on'
    : timeframe === 'month'
      ? 'during'
      : 'in';

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button
          variant="outline"
          className="w-full sm:w-auto gap-2 bg-sky-500 hover:bg-sky-600 text-white whitespace-nowrap"
        >
          <Wind className="h-4 w-4" />
          <span className="hidden sm:inline">View All Curtailed Farms</span>
          <span className="sm:hidden">All Farms</span>
        </Button>
      </DialogTrigger>

      <DialogContent className="w-[98vw] sm:w-[90vw] max-w-[900px] max-h-[90vh] overflow-y-auto p-2 sm:p-6">
        <DialogHeader>
          <DialogTitle className="text-xl sm:text-2xl font-bold text-sky-900 dark:text-sky-100 flex items-center gap-2">
            <Wind className="h-5 w-5 sm:h-6 sm:w-6 text-sky-500" />
            Curtailed Wind Farms
          </DialogTitle>
          <p className="text-sm text-muted-foreground mt-1">
            Data {periodLabel} {displayDate} • {minerModel} miner model
          </p>
        </DialogHeader>

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
              The server returned an error. Please try again later.
            </p>
          </div>
        ) : sortedFarms.length > 0 ? (
          <div className="mt-2 sm:mt-4 -mx-2 sm:mx-0">
            <div className="overflow-x-auto">
              <Table>
                <FarmTableHeader onSort={handleSort} sortConfig={sortConfig} />
                <TableBody>
                  {sortedFarms.map((farm) => (
                    <FarmTableRow
                      key={farm.leadPartyName}
                      farm={farm}
                      isExpanded={expandedFarms.has(farm.leadPartyName)}
                      onToggle={() => toggleFarmExpansion(farm.leadPartyName)}
                      btcPrice={data?.meta.currentPrice || 0}
                    />
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        ) : (
          <p className="text-center text-muted-foreground py-8">No farm data available for the selected date.</p>
        )}
      </DialogContent>
    </Dialog>
  );
}