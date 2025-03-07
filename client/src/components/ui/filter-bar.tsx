import { DatePicker } from "@/components/date-picker"
import { MinerModelSelector } from "@/components/miner-model-selector"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"

interface FilterBarProps {
  date: Date
  onDateChange: (date: Date | undefined) => void
  selectedLeadParty: string | null
  onLeadPartyChange: (value: string) => void
  curtailedLeadParties: string[]
  selectedMinerModel: string
  onMinerModelChange: (value: string) => void
}

export function FilterBar({
  date,
  onDateChange,
  selectedLeadParty,
  onLeadPartyChange,
  curtailedLeadParties,
  selectedMinerModel,
  onMinerModelChange
}: FilterBarProps) {
  return (
    <div className="flex flex-wrap gap-2">
      <DatePicker date={date} onDateChange={onDateChange} />
      
      <Select 
        value={selectedLeadParty || ""} 
        onValueChange={onLeadPartyChange}
      >
        <SelectTrigger className="w-[180px]">
          <SelectValue placeholder="All Lead Parties" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="">All Lead Parties</SelectItem>
          {curtailedLeadParties.map((party) => (
            <SelectItem key={party} value={party}>{party}</SelectItem>
          ))}
        </SelectContent>
      </Select>
      
      <MinerModelSelector 
        value={selectedMinerModel} 
        onValueChange={onMinerModelChange} 
      />
    </div>
  )
}