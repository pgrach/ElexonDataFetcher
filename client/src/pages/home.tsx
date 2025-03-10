import { Suspense } from "react";
import DashboardOverview from "@/components/dashboard-overview";
import DashboardSkeleton from "@/components/dashboard-skeleton";

// These interfaces are kept for reference and type consistency
interface BitcoinCalculation {
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  difficulty: number | null;
  price: number;
  currentPrice: number;
}

interface YearlyBitcoinCalculation {
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  curtailedEnergy: number;
  totalPayment: number;
  difficulty: number; // Changed from averageDifficulty
  currentPrice: number;
  year: string;
}

interface DailySummary {
  totalCurtailedEnergy: number;
  totalPayment: number;
}

interface MonthlySummary {
  totalCurtailedEnergy: number;
  totalPayment: number;
}

interface YearlySummary {
  totalCurtailedEnergy: number;
  totalPayment: number;
}

interface HourlyData {
  hour: string;
  curtailedEnergy: number;
}

export default function Home() {
  return (
    <div className="min-h-screen bg-background">
      <Suspense fallback={<DashboardSkeleton />}>
        <DashboardOverview />
      </Suspense>
    </div>
  );
}