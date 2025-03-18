"use client";

import { useState } from "react";
import { format } from "date-fns";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import ImprovedSummaryCards from "../components/improved-summary-cards";
import ImprovedCurtailmentChart from "../components/improved-curtailment-chart";
import FarmComparisonChart from "../components/farm-comparison-chart";
import StyleGuide from "../components/style-guide";

export default function ImprovementsDemo() {
  const [date] = useState<Date>(new Date(2025, 2, 15)); // March 15, 2025
  const [timeframe] = useState<string>("monthly");
  const [minerModel] = useState<string>("S19J_PRO");
  const [farmId] = useState<string>("");
  
  return (
    <div className="container mx-auto py-8">
      <Card className="mb-8 shadow-md">
        <CardHeader>
          <CardTitle className="text-2xl font-bold text-gray-800">Bitcoin Mining Analytics Improvements</CardTitle>
          <CardDescription className="text-gray-600">
            Enhanced readability and visual consistency demonstration
          </CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-gray-700">
            This page demonstrates the improved components with enhanced readability features:
          </p>
          <ul className="list-disc list-inside mt-2 space-y-1 text-gray-600">
            <li>Consistent typography and color scales</li>
            <li>Enhanced chart annotations and axis labels</li>
            <li>Improved tooltips and legends</li>
            <li>Better contrasts for data visibility</li>
            <li>Consistent spacing and layout across all components</li>
          </ul>
        </CardContent>
      </Card>
      
      <div className="space-y-8">
        <section aria-labelledby="summary-cards-heading">
          <h2 id="summary-cards-heading" className="text-xl font-semibold text-gray-800 mb-4">Improved Summary Cards</h2>
          <ImprovedSummaryCards />
        </section>
        
        <section aria-labelledby="curtailment-chart-heading">
          <h2 id="curtailment-chart-heading" className="text-xl font-semibold text-gray-800 mb-4">Improved Curtailment Chart</h2>
          <ImprovedCurtailmentChart 
            timeframe={timeframe}
            date={date}
            minerModel={minerModel}
            farmId={farmId}
          />
        </section>
        
        <section aria-labelledby="farm-comparison-heading">
          <h2 id="farm-comparison-heading" className="text-xl font-semibold text-gray-800 mb-4">Farm Comparison Chart</h2>
          <FarmComparisonChart 
            date={date}
            timeframe={timeframe}
            minerModel={minerModel}
            limit={5}
          />
        </section>
        
        <section aria-labelledby="style-guide-heading">
          <h2 id="style-guide-heading" className="text-xl font-semibold text-gray-800 mb-4">Style Guide</h2>
          <StyleGuide />
        </section>
      </div>
    </div>
  );
}