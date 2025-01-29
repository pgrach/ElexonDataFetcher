import { useState } from "react";
import { Calendar as CalendarIcon, ChevronDown } from "lucide-react";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";

interface FilterBarProps {
  date: Date;
  onDateChange: (date: Date | undefined) => void;
  selectedFarm: string | null;
  onFarmChange: (value: string) => void;
  availableFarms: string[];
}

export function FilterBar({
  date,
  onDateChange,
  selectedFarm,
  onFarmChange,
  availableFarms,
}: FilterBarProps) {
  const [compareFarmsEnabled, setCompareFarmsEnabled] = useState(false);

  return (
    <div className="flex items-center gap-4 p-4 bg-background border rounded-lg shadow-sm">
      <Popover>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            className={cn(
              "justify-start text-left font-normal w-[240px]",
              !date && "text-muted-foreground"
            )}
          >
            <CalendarIcon className="mr-2 h-4 w-4" />
            {date ? format(date, "PPP") : <span>Pick a date</span>}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <Calendar
            mode="single"
            selected={date}
            onSelect={onDateChange}
            disabled={(date) => {
              const startDate = new Date("2023-01-01");
              startDate.setHours(0, 0, 0, 0);
              const currentDate = new Date();
              return date < startDate || date > currentDate;
            }}
            initialFocus
          />
        </PopoverContent>
      </Popover>

      <Select
        value={selectedFarm || 'all'}
        onValueChange={(value) => onFarmChange(value === 'all' ? '' : value)}
      >
        <SelectTrigger className="w-[240px]">
          <SelectValue placeholder="Select a farm" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All Farms</SelectItem>
          {availableFarms.map((farm) => (
            <SelectItem key={farm} value={farm}>
              {farm}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <div className="flex items-center space-x-2 ml-4">
        <Switch
          id="compare-farms"
          checked={compareFarmsEnabled}
          onCheckedChange={setCompareFarmsEnabled}
        />
        <Label htmlFor="compare-farms">Compare Farms</Label>
      </div>
    </div>
  );
}
