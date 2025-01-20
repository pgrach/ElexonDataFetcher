export interface ElexonBidOffer {
  settlementDate: string;
  settlementPeriod: number;
  id: string;
  bmUnit?: string;
  bidPrice?: number;
  offerPrice?: number;
  volume: number;
  soFlag: boolean;
  cadlFlag: boolean;
  originalPrice: number;
  finalPrice: number;
}

export interface ElexonResponse {
  data: ElexonBidOffer[];
  error?: string;
}