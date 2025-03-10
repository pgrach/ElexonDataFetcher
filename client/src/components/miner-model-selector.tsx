import { MinerSelect } from "@/components/ui/miner-select";

interface MinerModelSelectorProps {
  value: string;
  onValueChange: (value: string) => void;
}

export default function MinerModelSelector({ value, onValueChange }: MinerModelSelectorProps) {
  return (
    <MinerSelect value={value} onValueChange={onValueChange} />
  );
}