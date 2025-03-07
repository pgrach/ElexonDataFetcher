import { Cpu } from "lucide-react"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../components/ui/select"

interface MinerModelSelectorProps {
  value: string
  onValueChange: (value: string) => void
}

export function MinerModelSelector({ value, onValueChange }: MinerModelSelectorProps) {
  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger className="w-[180px]">
        <Cpu className="mr-2 h-4 w-4" />
        <SelectValue placeholder="Select miner model" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="S19J_PRO">Antminer S19J Pro</SelectItem>
        <SelectItem value="S9">Antminer S9</SelectItem>
        <SelectItem value="M20S">Whatsminer M20S</SelectItem>
      </SelectContent>
    </Select>
  )
}