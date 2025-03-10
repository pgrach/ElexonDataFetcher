import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

interface MinerModelSelectorProps {
  value: string
  onValueChange: (value: string) => void
}

export default function MinerModelSelector({ value, onValueChange }: MinerModelSelectorProps) {
  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger className="w-[180px]">
        <SelectValue placeholder="Select Model" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="S19J_PRO">S19J Pro</SelectItem>
        <SelectItem value="S9">S9</SelectItem>
        <SelectItem value="M20S">M20S</SelectItem>
      </SelectContent>
    </Select>
  )
}