"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Wind, Battery, Calendar as CalendarIcon, Building, Bitcoin } from "lucide-react"
import { format } from "date-fns"
import { useQuery } from "@tanstack/react-query"

interface SummaryProps {
  date: Date;
  selectedLeadParty: string | null;
  selectedMinerModel: string;
}

export default function SummaryReports({ date, selectedLeadParty, selectedMinerModel }: SummaryProps) {
  const formattedDate = format(date, "yyyy-MM-dd")

  const { data: dailyData, isLoading: isDailyLoading } = useQuery<{
    totalCurtailedEnergy: number;
    totalPayment: number;
  }>({
    queryKey: [`/api/summary/daily/${formattedDate}`, selectedLeadParty],
    enabled: !!formattedDate,
  })

  const { data: bitcoinPotential } = useQuery({
    queryKey: [
      `/api/curtailment/mining-potential`,
      selectedMinerModel,
      dailyData?.totalCurtailedEnergy,
      selectedLeadParty,
      formattedDate 
    ],
    enabled: !!formattedDate && !!dailyData?.totalCurtailedEnergy,
  })

  return (
    <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">
            {selectedLeadParty ? "Farm Curtailed Energy" : "Curtailed Energy"}
          </CardTitle>
          <Wind className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {isDailyLoading ? (
              "Loading..."
            ) : (
              `${Number(dailyData?.totalCurtailedEnergy || 0).toLocaleString()} MWh`
            )}
          </div>
          <p className="text-xs text-muted-foreground mt-1">
            {selectedLeadParty ? (
              <>Farm curtailed energy for {selectedLeadParty}</>
            ) : (
              <>Total curtailed energy</>
            )}
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Bitcoin Mining Potential</CardTitle>
          <Bitcoin className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold text-[#F7931A]">
            ₿{(bitcoinPotential?.bitcoinMined || 0).toFixed(8)}
          </div>
          <p className="text-xs text-muted-foreground mt-1">
            With {selectedMinerModel.replace("_", " ")} miners
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Current Payment</CardTitle>
          <Building className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            £{Number(dailyData?.totalPayment || 0).toLocaleString()}
          </div>
          <p className="text-xs text-muted-foreground mt-1">
            Current curtailment payment
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Potential Value</CardTitle>
          <Battery className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold text-[#F7931A]">
            £{(bitcoinPotential?.valueAtCurrentPrice || 0).toLocaleString('en-GB', { maximumFractionDigits: 2 })}
          </div>
          <p className="text-xs text-muted-foreground mt-1">
            Value if Bitcoin was mined
          </p>
        </CardContent>
      </Card>
    </div>
  )
}
