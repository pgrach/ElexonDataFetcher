export interface ElexonBidOffer {
  settlementDate: string;
  settlementPeriod: number;
  id: string;
  bmUnit?: string;
  volume: number;
  soFlag: boolean;
  cadlFlag: boolean | null;
  originalPrice: number;
  finalPrice: number;
  leadPartyName?: string;
}

export interface ElexonPhysicalNotification {
  dataset: string;
  settlementDate: string;
  settlementPeriod: number;
  timeFrom: string;
  timeTo: string;
  levelFrom: number;
  levelTo: number;
  nationalGridBmUnit: string;
  bmUnit: string;
}

export interface ElexonResponse {
  data: ElexonBidOffer[];
  error?: string;
}

export interface ElexonPNResponse {
  error?: string;
  data?: ElexonPhysicalNotification[];
}