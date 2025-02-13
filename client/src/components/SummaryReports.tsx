"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useQuery } from "@tanstack/react-query"
import { format } from "date-fns"
import { Wind, Battery, Bitcoin } from "lucide-react"

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

  const { data: monthlyData, isLoading: isMonthlyLoading } = useQuery<{
    totalCurtailedEnergy: number;
    totalPayment: number;
  }>({
    queryKey: [`/api/summary/monthly/${format(date, "yyyy-MM")}`, selectedLeadParty],
    enabled: !!date,
  })

  const { data: yearlyData, isLoading: isYearlyLoading } = useQuery<{
    totalCurtailedEnergy: number;
    totalPayment: number;
  }>({
    queryKey: [`/api/summary/yearly/${format(date, "yyyy")}`, selectedLeadParty],
    enabled: !!date,
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

  const SummaryCard = ({ data, isLoading, period }: { 
    data: { totalCurtailedEnergy: number; totalPayment: number; } | undefined;
    isLoading: boolean;
    period: 'daily' | 'monthly' | 'yearly';
  }) => {
    const periodText = {
      daily: format(date, "MMMM d, yyyy"),
      monthly: format(date, "MMMM yyyy"),
      yearly: format(date, "yyyy")
    }

    const btcValue = data?.totalCurtailedEnergy 
      ? (data.totalCurtailedEnergy * (bitcoinPotential?.bitcoinMined ?? 0)) / (dailyData?.totalCurtailedEnergy ?? 1)
      : 0

    const fiatValue = data?.totalCurtailedEnergy
      ? (data.totalCurtailedEnergy * (bitcoinPotential?.valueAtCurrentPrice ?? 0)) / (dailyData?.totalCurtailedEnergy ?? 1)
      : 0

    return (
      <div className="grid grid-cols-3 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Curtailed Energy</CardTitle>
            <Wind className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {isLoading ? "Loading..." : `${Number(data?.totalCurtailedEnergy || 0).toLocaleString()} MWh`}
            </div>
            <p className="text-xs text-muted-foreground mt-1">
              {selectedLeadParty 
                ? `Farm curtailed energy for ${selectedLeadParty} in ${periodText[period]}`
                : `Total curtailed energy for ${periodText[period]}`}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Bitcoin Mining</CardTitle>
            <Bitcoin className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-[#F7931A]">
              {isLoading ? "Loading..." : `₿${btcValue.toFixed(8)}`}
            </div>
            <p className="text-xs text-muted-foreground mt-1">
              With {selectedMinerModel.replace("_", " ")} miners
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Value</CardTitle>
            <Battery className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {isLoading ? "Loading..." : `£${Number(data?.totalPayment || 0).toLocaleString()}`}
            </div>
            <div className="text-lg font-bold text-[#F7931A]">
              {isLoading ? "" : `£${fiatValue.toLocaleString('en-GB', { maximumFractionDigits: 2 })}`}
            </div>
            <p className="text-xs text-muted-foreground mt-1">
              Current payment vs. Bitcoin value
            </p>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Energy & Mining Summary</CardTitle>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="daily" className="space-y-4">
          <TabsList>
            <TabsTrigger value="daily">Daily</TabsTrigger>
            <TabsTrigger value="monthly">Monthly</TabsTrigger>
            <TabsTrigger value="yearly">Yearly</TabsTrigger>
          </TabsList>
          <TabsContent value="daily">
            <SummaryCard data={dailyData} isLoading={isDailyLoading} period="daily" />
          </TabsContent>
          <TabsContent value="monthly">
            <SummaryCard data={monthlyData} isLoading={isMonthlyLoading} period="monthly" />
          </TabsContent>
          <TabsContent value="yearly">
            <SummaryCard data={yearlyData} isLoading={isYearlyLoading} period="yearly" />
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  )
}