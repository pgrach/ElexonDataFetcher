import { Check, ChevronsUpDown } from "lucide-react"

import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
} from "@/components/ui/command"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { useState } from "react"

// Define miner models data
const minerModels = [
  { value: "S19J_PRO", label: "S19J Pro", hashrate: "100 TH/s", efficiency: "29.5 J/TH" },
  { value: "S9", label: "S9", hashrate: "13.5 TH/s", efficiency: "94 J/TH" },
  { value: "M20S", label: "M20S", hashrate: "68 TH/s", efficiency: "48 J/TH" },
]

interface MinerModelSelectorProps {
  value: string
  onValueChange: (value: string) => void
}

export function MinerModelSelector({ value, onValueChange }: MinerModelSelectorProps) {
  const [open, setOpen] = useState(false)
  
  const selectedModel = minerModels.find(model => model.value === value)

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between"
        >
          {selectedModel?.label || "Select miner"}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[300px] p-0">
        <Command>
          <CommandInput placeholder="Search miner model..." />
          <CommandEmpty>No miner model found.</CommandEmpty>
          <CommandGroup>
            {minerModels.map((model) => (
              <CommandItem
                key={model.value}
                value={model.value}
                onSelect={(currentValue) => {
                  onValueChange(currentValue)
                  setOpen(false)
                }}
              >
                <Check
                  className={cn(
                    "mr-2 h-4 w-4",
                    value === model.value ? "opacity-100" : "opacity-0"
                  )}
                />
                <div className="flex flex-col">
                  <span>{model.label}</span>
                  <span className="text-xs text-muted-foreground">
                    {model.hashrate} · {model.efficiency}
                  </span>
                </div>
              </CommandItem>
            ))}
          </CommandGroup>
        </Command>
      </PopoverContent>
    </Popover>
  )
}