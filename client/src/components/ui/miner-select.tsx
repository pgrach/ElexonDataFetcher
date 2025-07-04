import * as React from "react"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { SiAntdesign } from "react-icons/si"

export interface MinerModel {
  id: string
  name: string
  hashrate: number
  efficiency: number
}

const minerModels: MinerModel[] = [
  {
    id: "S19J_PRO",
    name: "S19J Pro",
    hashrate: 100,
    efficiency: 29.5,
  },
  {
    id: "S9",
    name: "S9",
    hashrate: 13.5,
    efficiency: 94,
  },
  {
    id: "M20S",
    name: "M20S",
    hashrate: 68,
    efficiency: 48,
  },
]

interface MinerSelectProps {
  value: string
  onValueChange: (value: string) => void
}

export const MinerSelect = React.forwardRef<HTMLButtonElement, MinerSelectProps>(
  ({ value, onValueChange }, ref) => {
    const selectedModel = minerModels.find((m) => m.id === value);

    return (
      <div className="flex items-center gap-2">
        <SiAntdesign className="h-4 w-4 text-muted-foreground" />
        <TooltipProvider>
          <Tooltip>
            <Select value={value} onValueChange={onValueChange}>
              <TooltipTrigger asChild>
                <SelectTrigger ref={ref} className="w-[180px]">
                  <SelectValue placeholder="Select Miner Model" />
                </SelectTrigger>
              </TooltipTrigger>
              <SelectContent>
                {minerModels.map((model) => (
                  <SelectItem key={model.id} value={model.id}>
                    {model.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <TooltipContent>
              {selectedModel && (
                <p>
                  {selectedModel.name}: {selectedModel.hashrate} TH/s @ {selectedModel.efficiency} J/TH
                </p>
              )}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </div>
    )
  }
)

MinerSelect.displayName = "MinerSelect"