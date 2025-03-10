"use client";

import * as React from "react";
import { format } from "date-fns";
import { Calendar as CalendarIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { cn } from "@/lib/utils";

interface DatePickerProps {
  date: Date;
  onDateChange: (date: Date | undefined) => void;
}

export function DatePicker({ date, onDateChange }: DatePickerProps) {
  const startDate = new Date("2022-01-01");
  const today = new Date();
  
  return (
    <div className="flex items-center space-x-2">
      <CalendarIcon className="h-5 w-5 text-muted-foreground" />
      <Popover>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            className={cn(
              "w-[180px] justify-start text-left font-normal",
              !date && "text-muted-foreground"
            )}
          >
            {date ? (
              format(date, "PPP")
            ) : (
              <span>Pick a date</span>
            )}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <Calendar
            mode="single"
            selected={date}
            onSelect={onDateChange}
            fromDate={startDate}
            toDate={today}
            initialFocus
          />
        </PopoverContent>
      </Popover>
    </div>
  );
}