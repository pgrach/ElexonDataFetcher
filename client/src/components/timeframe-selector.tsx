"use client";

import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";

interface TimeframeSelectorProps {
  value: string;
  onValueChange: (value: string) => void;
}

export default function TimeframeSelector({ value, onValueChange }: TimeframeSelectorProps) {
  return (
    <ToggleGroup type="single" value={value} onValueChange={onValueChange} className="bg-muted rounded-md p-1">
      <ToggleGroupItem value="daily" className="rounded-md px-4 py-2">Today</ToggleGroupItem>
      <ToggleGroupItem value="monthly" className="rounded-md px-4 py-2">Month</ToggleGroupItem>
      <ToggleGroupItem value="yearly" className="rounded-md px-4 py-2">Year</ToggleGroupItem>
    </ToggleGroup>
  );
}