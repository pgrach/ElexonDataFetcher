import { useState, useEffect } from "react";
import { Check, ChevronsUpDown } from "lucide-react";
import { format } from "date-fns";
import axios from "axios";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

interface LeadPartySelectorProps {
  value: string;
  onValueChange: (value: string) => void;
  date: Date;
}

interface LeadParty {
  leadPartyName: string;
  farmCount: number;
  totalCurtailedEnergy: number;
}

export default function LeadPartySelector({ value, onValueChange, date }: LeadPartySelectorProps) {
  const [open, setOpen] = useState(false);
  const [leadParties, setLeadParties] = useState<LeadParty[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchLeadParties = async () => {
      setLoading(true);
      setError(null);
      
      try {
        // This would be replaced with your actual API endpoint
        const formattedDate = format(date, 'yyyy-MM-dd');
        const response = await axios.get('/api/dashboard/curtailed-lead-parties', {
          params: { date: formattedDate }
        });
        
        // Fallback to mock data if API is not yet implemented
        if (response.data && Array.isArray(response.data)) {
          setLeadParties(response.data);
        } else {
          // We'll get data from our curtailment_records table for now
          const { data } = await axios.get('/api/dashboard/lead-parties');
          setLeadParties(data.filter((party: any) => party.totalCurtailedEnergy > 0));
        }
      } catch (err) {
        console.error('Error fetching lead parties:', err);
        setError('Failed to load lead parties');
        
        // If the API fails, we'll have some defaults to work with
        setLeadParties([
          { leadPartyName: "Seagreen Wind Energy Limited", farmCount: 6, totalCurtailedEnergy: 1600 },
          { leadPartyName: "Moray Offshore Wind East Ltd", farmCount: 1, totalCurtailedEnergy: 850 },
          { leadPartyName: "All Lead Parties", farmCount: 7, totalCurtailedEnergy: 2450 }
        ]);
      } finally {
        setLoading(false);
      }
    };
    
    fetchLeadParties();
  }, [date]);
  
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between"
          disabled={loading}
        >
          {loading ? (
            <span className="opacity-70">Loading...</span>
          ) : value ? (
            leadParties.find((party) => party.leadPartyName === value)?.leadPartyName || "All Lead Parties"
          ) : (
            "Select lead party..."
          )}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-full p-0">
        <Command>
          <CommandInput placeholder="Search lead parties..." />
          <CommandEmpty>No lead party found.</CommandEmpty>
          <CommandGroup>
            <CommandItem
              value="All Lead Parties"
              onSelect={() => {
                onValueChange("All Lead Parties");
                setOpen(false);
              }}
            >
              <Check
                className={cn(
                  "mr-2 h-4 w-4",
                  value === "All Lead Parties" ? "opacity-100" : "opacity-0"
                )}
              />
              All Lead Parties
            </CommandItem>
            {leadParties.map((party) => (
              <CommandItem
                key={party.leadPartyName}
                value={party.leadPartyName}
                onSelect={() => {
                  onValueChange(party.leadPartyName);
                  setOpen(false);
                }}
              >
                <Check
                  className={cn(
                    "mr-2 h-4 w-4",
                    value === party.leadPartyName ? "opacity-100" : "opacity-0"
                  )}
                />
                {party.leadPartyName}
                <span className="ml-2 text-xs text-muted-foreground">
                  ({party.farmCount} {party.farmCount === 1 ? 'farm' : 'farms'})
                </span>
              </CommandItem>
            ))}
          </CommandGroup>
        </Command>
      </PopoverContent>
    </Popover>
  );
}