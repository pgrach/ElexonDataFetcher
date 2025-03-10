import { 
  ToggleGroup, 
  ToggleGroupItem 
} from "@/components/ui/toggle-group"
import { 
  CalendarDays, 
  Calendar, 
  BarChart 
} from "lucide-react"

interface TimeframeSelectorProps {
  value: string
  onValueChange: (value: string) => void
}

export default function TimeframeSelector({ value, onValueChange }: TimeframeSelectorProps) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-sm font-medium">Timeframe</span>
      <ToggleGroup
        type="single"
        size="sm"
        value={value}
        onValueChange={(newValue) => {
          if (newValue) onValueChange(newValue)
        }}
      >
        <ToggleGroupItem value="daily" aria-label="Daily view">
          <CalendarDays className="h-4 w-4 mr-1" />
          Daily
        </ToggleGroupItem>
        <ToggleGroupItem value="monthly" aria-label="Monthly view">
          <Calendar className="h-4 w-4 mr-1" />
          Monthly
        </ToggleGroupItem>
        <ToggleGroupItem value="yearly" aria-label="Yearly view">
          <BarChart className="h-4 w-4 mr-1" />
          Yearly
        </ToggleGroupItem>
      </ToggleGroup>
    </div>
  )
}