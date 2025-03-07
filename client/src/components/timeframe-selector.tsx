import { Button } from "@/components/ui/button"

interface TimeframeSelectorProps {
  selected: "today" | "month" | "year"
  onChange: (timeframe: "today" | "month" | "year") => void
}

export function TimeframeSelector({ selected, onChange }: TimeframeSelectorProps) {
  return (
    <div className="flex space-x-1 rounded-lg border p-1">
      <Button
        variant={selected === "today" ? "default" : "ghost"}
        size="sm"
        onClick={() => onChange("today")}
        className="text-xs px-3"
      >
        Today
      </Button>
      <Button
        variant={selected === "month" ? "default" : "ghost"}
        size="sm"
        onClick={() => onChange("month")}
        className="text-xs px-3"
      >
        Month
      </Button>
      <Button
        variant={selected === "year" ? "default" : "ghost"}
        size="sm"
        onClick={() => onChange("year")}
        className="text-xs px-3"
      >
        Year
      </Button>
    </div>
  )
}