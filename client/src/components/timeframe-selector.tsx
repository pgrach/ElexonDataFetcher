import { Button } from "../components/ui/button"
import { Calendar, ClipboardList, BarChart } from "lucide-react"

interface TimeframeSelectorProps {
  selected: "today" | "month" | "year"
  onChange: (timeframe: "today" | "month" | "year") => void
}

export function TimeframeSelector({ selected, onChange }: TimeframeSelectorProps) {
  return (
    <div className="flex space-x-1">
      <Button
        variant={selected === "today" ? "default" : "outline"}
        size="sm"
        onClick={() => onChange("today")}
        className="flex items-center gap-1"
      >
        <Calendar className="h-4 w-4" />
        <span>Daily</span>
      </Button>
      
      <Button
        variant={selected === "month" ? "default" : "outline"}
        size="sm"
        onClick={() => onChange("month")}
        className="flex items-center gap-1"
      >
        <ClipboardList className="h-4 w-4" />
        <span>Monthly</span>
      </Button>
      
      <Button
        variant={selected === "year" ? "default" : "outline"}
        size="sm"
        onClick={() => onChange("year")}
        className="flex items-center gap-1"
      >
        <BarChart className="h-4 w-4" />
        <span>Yearly</span>
      </Button>
    </div>
  )
}