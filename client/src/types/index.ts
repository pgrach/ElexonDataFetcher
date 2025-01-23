export interface ElexonData {
  settlementDate: string;
  settlementPeriod: number;
  volume: number;
  finalPrice: number;
  payment: number;
  farmId: string;
}

export interface AggregatedData {
  hour: number;
  curtailedEnergy: number;
  potentialBtc: number;
  totalPayment: number;
}

export interface DailySummary {
  date: string;
  totalCurtailedEnergy: number;
  totalPotentialBtc: number;
  totalPayment: number;
  hourlyData: AggregatedData[];
}

export interface MonthlySummary {
  yearMonth: string;
  totalCurtailedEnergy: number;
  totalPayment: number;
}

export interface FarmDetail {
  farmId: string;
  curtailedEnergy: number;
  percentageOfTotal: number;
  potentialBtc: number;
  payment: number;
}

export interface GroupedFarm {
  leadPartyName: string;
  totalCurtailedEnergy: number;
  totalPercentageOfTotal: number;
  totalPotentialBtc: number;
  totalPayment: number;
  farms: FarmDetail[];
}

export interface TopCurtailedFarm {
  farmId: string;
  curtailedEnergy: number;
  percentageOfTotal: number;
  potentialBtc: number;
  payment: number;
  leadPartyName: string;
}