import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"

interface TimeframeSelectorProps {
  value: string
  onValueChange: (value: string) => void
}

export default function TimeframeSelector({ value, onValueChange }: TimeframeSelectorProps) {
  return (
    <ToggleGroup type="single" value={value} onValueChange={(val) => val && onValueChange(val)}>
      <ToggleGroupItem value="daily" aria-label="Toggle daily view">
        Today
      </ToggleGroupItem>
      <ToggleGroupItem value="monthly" aria-label="Toggle monthly view">
        Month
      </ToggleGroupItem>
      <ToggleGroupItem value="yearly" aria-label="Toggle yearly view">
        Year
      </ToggleGroupItem>
    </ToggleGroup>
  )
}