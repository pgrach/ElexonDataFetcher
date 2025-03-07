import { Suspense } from "react"
import DashboardOverview from "../components/dashboard-overview"
import DashboardSkeleton from "../components/dashboard-skeleton"

export default function Home() {
  return (
    <div className="min-h-screen bg-background">
      <Suspense fallback={<DashboardSkeleton />}>
        <DashboardOverview />
      </Suspense>
    </div>
  )
}