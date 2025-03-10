import { Button } from "@/components/ui/button";

interface TimeframeSelectorProps {
  value: string;
  onValueChange: (value: string) => void;
}

export default function TimeframeSelector({ value, onValueChange }: TimeframeSelectorProps) {
  return (
    <div className="flex gap-2">
      <Button
        variant={value === "daily" ? "default" : "outline"}
        onClick={() => onValueChange("daily")}
        className="w-[80px]"
      >
        Today
      </Button>
      <Button
        variant={value === "monthly" ? "default" : "outline"}
        onClick={() => onValueChange("monthly")}
        className="w-[80px]"
      >
        Month
      </Button>
      <Button
        variant={value === "yearly" ? "default" : "outline"}
        onClick={() => onValueChange("yearly")}
        className="w-[80px]"
      >
        Year
      </Button>
    </div>
  );
}