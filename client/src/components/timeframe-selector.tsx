"use client";

import { useEffect, useState } from "react";
import { format } from "date-fns";

interface TimeframeSelectorProps {
  value: string;
  onValueChange: (value: string) => void;
  date?: Date;
}

export default function TimeframeSelector({ 
  value, 
  onValueChange, 
  date = new Date() 
}: TimeframeSelectorProps) {
  const [isToday, setIsToday] = useState(true);
  
  useEffect(() => {
    // Check if the selected date is today
    const today = new Date();
    const isCurrentDay = 
      format(date, 'yyyy-MM-dd') === format(today, 'yyyy-MM-dd');
    setIsToday(isCurrentDay);
  }, [date]);

  return (
    <div className="inline-flex rounded-md">
      <button 
        type="button"
        onClick={() => onValueChange("daily")}
        className={`px-4 py-2 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 ${
          value === "daily" 
            ? "bg-muted" 
            : "hover:bg-muted/50"
        } rounded-l-md`}
      >
        {isToday ? "Today" : "Day"}
      </button>
      <button 
        type="button"
        onClick={() => onValueChange("monthly")}
        className={`px-4 py-2 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 ${
          value === "monthly" 
            ? "bg-muted" 
            : "hover:bg-muted/50"
        }`}
      >
        Month
      </button>
      <button 
        type="button"
        onClick={() => onValueChange("yearly")}
        className={`px-4 py-2 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 ${
          value === "yearly" 
            ? "bg-muted" 
            : "hover:bg-muted/50"
        } rounded-r-md`}
      >
        Year
      </button>
    </div>
  );
}