import { Skeleton } from "../components/ui/skeleton"

export default function DashboardSkeleton() {
  return (
    <div className="container mx-auto p-6 space-y-8">
      <div className="space-y-2">
        <Skeleton className="h-8 w-1/3" />
        <Skeleton className="h-4 w-1/2" />
      </div>

      {/* TimeframeSelector and DatePicker skeleton */}
      <div className="flex flex-wrap gap-4 items-center justify-between">
        <div className="flex items-center gap-1">
          <Skeleton className="h-9 w-24" />
          <Skeleton className="h-9 w-24" />
          <Skeleton className="h-9 w-24" />
        </div>
        <div className="flex flex-wrap gap-2 items-center">
          <Skeleton className="h-9 w-40" />
          <Skeleton className="h-9 w-40" />
        </div>
      </div>

      {/* Summary cards skeleton */}
      <div className="grid gap-4 grid-cols-1 sm:grid-cols-2 lg:grid-cols-4">
        {Array(4).fill(0).map((_, i) => (
          <div key={i} className="rounded-lg border p-4 space-y-2">
            <Skeleton className="h-4 w-20" />
            <Skeleton className="h-8 w-28" />
            <Skeleton className="h-4 w-24" />
          </div>
        ))}
      </div>

      {/* Tab headers skeleton */}
      <div className="border-b space-y-4">
        <div className="flex gap-2">
          <Skeleton className="h-10 w-32" />
          <Skeleton className="h-10 w-32" />
          <Skeleton className="h-10 w-32" />
        </div>
      </div>

      {/* Content area skeleton */}
      <div className="rounded-md border">
        <div className="p-4 border-b">
          <Skeleton className="h-5 w-60 mb-2" />
          <Skeleton className="h-4 w-80" />
        </div>
        <div className="p-4">
          <Skeleton className="h-[300px] w-full" />
        </div>
      </div>
    </div>
  )
}