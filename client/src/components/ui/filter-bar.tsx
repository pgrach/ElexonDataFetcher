import * as React from "react"
import { format, isValid } from "date-fns"
import { Calendar as CalendarIcon } from "lucide-react"
import { Calendar } from "@/components/ui/calendar"
import { Button } from "@/components/ui/button"
import { MinerSelect } from "@/components/ui/miner-select"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"

interface FilterBarProps {
  date: Date
  onDateChange: (date: Date | undefined) => void
  selectedLeadParty: string | null
  onLeadPartyChange: (value: string) => void
  curtailedLeadParties: string[]
  selectedMinerModel: string
  onMinerModelChange: (value: string) => void
}

export function FilterBar({
  date,
  onDateChange,
  selectedLeadParty,
  onLeadPartyChange,
  curtailedLeadParties,
  selectedMinerModel,
  onMinerModelChange,
}: FilterBarProps) {
  return (
    <div className="flex flex-wrap gap-4 items-center p-4 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="flex items-center space-x-2">
        <Popover>
          <PopoverTrigger asChild>
            <Button
              variant="outline"
              className="w-[240px] justify-start text-left font-normal"
            >
              <CalendarIcon className="mr-2 h-4 w-4" />
              {format(date, "PPP")}
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
      </div>

      <div className="flex items-center space-x-2">
        <Select
          value={selectedLeadParty || 'all'}
          onValueChange={(value) => onLeadPartyChange(value === 'all' ? '' : value)}
        >
          <SelectTrigger className="w-[240px]">
            <SelectValue placeholder="Select Farm" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Farms</SelectItem>
            {curtailedLeadParties.map((party) => (
              <SelectItem key={party} value={party}>
                {party}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        {curtailedLeadParties.length === 0 && (
          <span className="text-sm text-muted-foreground">
            No farms curtailed on this date
          </span>
        )}
      </div>

      <div className="flex items-center space-x-2">
        <MinerSelect value={selectedMinerModel} onValueChange={onMinerModelChange} />
      </div>
    </div>
  )
}