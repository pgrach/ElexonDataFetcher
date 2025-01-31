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
import { formatNumber } from "@/lib/utils";
import { format } from 'date-fns';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { GroupedFarm, SortConfig } from "@/types/farm";

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
        <TableHead className="w-[35%] min-w-[120px]">
          <SortButton 
            label="Wind Farm" 
            sortKey="leadPartyName" 
            tooltip="The name of the wind farm operator. Click to expand and see individual turbine units."
          />
        </TableHead>
        <TableHead className="w-[20%] text-right min-w-[60px]">
          <SortButton 
            label="% Total" 
            sortKey="totalPercentageOfTotal" 
            tooltip="Percentage of total curtailed energy from all wind farms for the selected date"
          />
        </TableHead>
        <TableHead className="w-[25%] text-right min-w-[80px]">
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
      </TableRow>
    </TableHeader>
  );
}

// Farm Table Row Component
function FarmTableRow({ 
  farm, 
  isExpanded, 
  onToggle,
}: {
  farm: GroupedFarm;
  isExpanded: boolean;
  onToggle: () => void;
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
          {formatNumber(farm.totalPercentageOfTotal, 1)}%
        </TableCell>
        <TableCell className="text-right text-xs sm:text-sm">
          {formatNumber(farm.totalCurtailedEnergy)}
        </TableCell>
        <TableCell className="text-right text-orange-500 text-xs sm:text-sm">
          £{formatNumber(farm.totalPayment, 2)}
        </TableCell>
      </TableRow>
      {isExpanded && farm.farms.map((subFarm) => (
        <TableRow key={subFarm.farmId} className="bg-muted/30">
          <TableCell className="px-1 sm:px-2" />
          <TableCell className="text-xs sm:text-sm text-muted-foreground pl-4 sm:pl-8">
            {subFarm.farmId}
          </TableCell>
          <TableCell className="text-xs sm:text-sm text-muted-foreground text-right">
            {formatNumber(subFarm.percentageOfTotal, 1)}%
          </TableCell>
          <TableCell className="text-xs sm:text-sm text-muted-foreground text-right">
            {formatNumber(subFarm.curtailedEnergy)}
          </TableCell>
          <TableCell className="text-xs sm:text-sm text-orange-500/70 text-right">
            £{formatNumber(subFarm.payment, 2)}
          </TableCell>
        </TableRow>
      ))}
    </>
  );
}

// Export the button component that wraps the dialog
export function ViewFarmsButton({ date }: { date: string }) {
  return (
    <div className="flex justify-center sm:justify-start">
      <FarmsDialog date={date} />
    </div>
  );
}

// Main Dialog Component
function FarmsDialog({ date }: { date: string }) {
  const [open, setOpen] = useState(false);
  const [expandedFarms, setExpandedFarms] = useState<Set<string>>(new Set());
  const [sortConfig, setSortConfig] = useState<SortConfig | null>(null);

  const { data: farmsData, isLoading: isLoadingFarms, error: farmsError } = useQuery({
    queryKey: [`/api/farms/${date}`],
    queryFn: async () => {
      const response = await fetch(`/api/farms/${date}`);
      if (!response.ok) {
        throw new Error('Failed to fetch farms data');
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

  const sortedFarms = farmsData?.farms ? [...farmsData.farms].sort((a, b) => {
    if (!sortConfig) return 0;
    const compare = (a[sortConfig.key] < b[sortConfig.key]) ? -1 : 1;
    return sortConfig.direction === 'asc' ? compare : -compare;
  }) : [];

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
            Data for {format(new Date(date), 'MMMM d, yyyy')}
          </p>
        </DialogHeader>

        {isLoadingFarms ? (
          <div className="space-y-4">
            <div className="flex items-center justify-center py-4">
              <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
            </div>
            <p className="text-sm text-center text-muted-foreground">Fetching data...</p>
          </div>
        ) : farmsError ? (
          <div className="p-4 rounded-lg bg-destructive/10 text-destructive">
            <p className="text-sm font-medium">Unable to fetch curtailment data</p>
            <p className="text-xs mt-1">
              There was an error loading the wind farm data. Please try again later.
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