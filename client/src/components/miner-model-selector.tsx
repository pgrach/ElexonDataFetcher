import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

interface MinerModelSelectorProps {
  value: string
  onValueChange: (value: string) => void
}

export default function MinerModelSelector({ value, onValueChange }: MinerModelSelectorProps) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-sm font-medium">Miner Model</span>
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger className="w-[180px]">
          <SelectValue placeholder="Select Miner Model" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="S19J_PRO">Antminer S19J Pro</SelectItem>
          <SelectItem value="S9">Antminer S9</SelectItem>
          <SelectItem value="M20S">Whatsminer M20S</SelectItem>
        </SelectContent>
      </Select>
    </div>
  )
}