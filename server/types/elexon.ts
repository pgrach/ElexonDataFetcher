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

export interface ElexonResponse {
  data: ElexonBidOffer[];
  error?: string;
}