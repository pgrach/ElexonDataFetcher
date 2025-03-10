"use client";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { CalendarDays, Calendar, CalendarRange } from "lucide-react";

interface TimeframeSelectorProps {
  value: string;
  onValueChange: (value: string) => void;
}

export default function TimeframeSelector({ value, onValueChange }: TimeframeSelectorProps) {
  return (
    <div className="flex items-center space-x-2">
      <CalendarRange className="h-5 w-5 text-muted-foreground" />
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger className="w-[140px]">
          <SelectValue placeholder="Select timeframe" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="daily">
            <div className="flex items-center">
              <CalendarDays className="mr-2 h-4 w-4" />
              <span>Daily</span>
            </div>
          </SelectItem>
          <SelectItem value="monthly">
            <div className="flex items-center">
              <Calendar className="mr-2 h-4 w-4" />
              <span>Monthly</span>
            </div>
          </SelectItem>
          <SelectItem value="yearly">
            <div className="flex items-center">
              <CalendarRange className="mr-2 h-4 w-4" />
              <span>Yearly</span>
            </div>
          </SelectItem>
        </SelectContent>
      </Select>
    </div>
  );
}