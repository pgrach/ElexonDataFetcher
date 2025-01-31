// Farm data types for wind farm curtailment data
export interface FarmDetail {
  farmId: string;
  curtailedEnergy: number;
  percentageOfTotal: number;
  payment: number;
}

export interface GroupedFarm {
  leadPartyName: string;
  totalCurtailedEnergy: number;
  totalPercentageOfTotal: number;
  totalPayment: number;
  farms: FarmDetail[];
}

export interface FarmsResponse {
  farms: GroupedFarm[];
}

export interface SortConfig {
  key: keyof GroupedFarm;
  direction: 'asc' | 'desc';
}
