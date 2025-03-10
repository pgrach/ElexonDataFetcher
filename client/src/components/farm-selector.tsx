'use client';

import React, { useState, useEffect } from 'react';
import axios from 'axios';
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
}

export default function FarmSelector({ value, onValueChange }: FarmSelectorProps) {
  const [farms, setFarms] = useState<FarmData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchFarms = async () => {
      try {
        setLoading(true);
        const response = await axios.get('/api/mining-potential/farms');
        setFarms(response.data);
        setError(null);
      } catch (err) {
        console.error('Error fetching farms:', err);
        setError('Failed to load farms. Please try again later.');
      } finally {
        setLoading(false);
      }
    };

    fetchFarms();
  }, []);

  return (
    <div>
      <Select value={value} onValueChange={onValueChange} disabled={loading}>
        <SelectTrigger className="w-[200px]">
          <SelectValue placeholder={loading ? 'Loading farms...' : 'Select a farm'} />
          {loading && <Loader2 className="ml-2 h-4 w-4 animate-spin" />}
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="all">All Farms</SelectItem>
          
          {farms.map((farmGroup) => (
            <SelectGroup key={farmGroup.name}>
              <SelectLabel>{farmGroup.name}</SelectLabel>
              {farmGroup.farmIds.map((farmId) => (
                <SelectItem key={farmId} value={farmId}>
                  {farmId}
                </SelectItem>
              ))}
            </SelectGroup>
          ))}
        </SelectContent>
      </Select>
      {error && <p className="text-sm text-red-500 mt-1">{error}</p>}
    </div>
  );
}