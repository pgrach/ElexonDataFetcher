'use client';

import * as React from 'react';
import { useQuery } from "@tanstack/react-query";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Loader2 } from 'lucide-react';

interface FarmSelectorProps {
  value: string;
  onValueChange: (value: string) => void;
}

interface FarmData {
  name: string;
  farmIds: string[];
  curtailedEnergy: number;
}

export default function FarmSelector({ value, onValueChange }: FarmSelectorProps) {
  // Get the current date to match the chart data's date
  const today = new Date();
  const formattedDate = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`;
  
  // Use React Query for data fetching instead of local state & useEffect
  const { data: farmData = [], isLoading, error } = useQuery<FarmData[]>({
    queryKey: ['/api/mining-potential/farms', formattedDate],
    queryFn: async () => {
      // Pass the current date as a query parameter to get farms sorted by today's curtailment
      const response = await fetch(`/api/mining-potential/farms?date=${formattedDate}`);
      if (!response.ok) {
        throw new Error('Failed to fetch farms');
      }
      return response.json();
    }
  });
  
  // The farms are already sorted by curtailment volume on the server side
  // No need to sort them again here
  const farms = farmData;

  // Determine if we need to show an error message
  const errorMessage = error instanceof Error ? error.message : null;

  return (
    <div>
      <Select value={value} onValueChange={onValueChange} disabled={isLoading}>
        <SelectTrigger className="w-[200px]">
          <div className="flex items-center justify-between w-full">
            <SelectValue placeholder={isLoading ? 'Loading farms...' : 'Select a farm'} />
            {isLoading && <Loader2 className="h-4 w-4 animate-spin ml-2" />}
          </div>
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All Farms</SelectItem>
          
          {farms.map((farmGroup) => (
            <SelectItem key={farmGroup.name} value={farmGroup.name}>
              {farmGroup.name}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {errorMessage && <p className="text-sm text-red-500 mt-1">{errorMessage}</p>}
    </div>
  );
}