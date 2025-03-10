"use client";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Cpu } from "lucide-react";

interface MinerModelSelectorProps {
  value: string;
  onValueChange: (value: string) => void;
}

export default function MinerModelSelector({ value, onValueChange }: MinerModelSelectorProps) {
  // These are the mining hardware models with their hashrates and power efficiency
  const minerModels = [
    { id: "S19J_PRO", name: "Antminer S19J Pro", hashrate: 104, efficiency: 29.5 },
    { id: "S9", name: "Antminer S9", hashrate: 13.5, efficiency: 98 },
    { id: "M20S", name: "Whatsminer M20S", hashrate: 68, efficiency: 48 },
  ];

  return (
    <div className="flex items-center space-x-2">
      <Cpu className="h-5 w-5 text-muted-foreground" />
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger className="w-[180px]">
          <SelectValue placeholder="Select miner model" />
        </SelectTrigger>
        <SelectContent>
          {minerModels.map((model) => (
            <SelectItem key={model.id} value={model.id}>
              <div className="flex flex-col">
                <span>{model.name}</span>
                <span className="text-xs text-muted-foreground">
                  {model.hashrate} TH/s, {model.efficiency} J/TH
                </span>
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}