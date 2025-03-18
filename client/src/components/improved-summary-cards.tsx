"use client";

import React from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Wind, Battery, Bitcoin, ArrowRightLeft } from "lucide-react";

interface SummaryCardProps {
  title: string;
  description: string;
  value: string | number;
  unit?: string;
  secondaryValue?: string | number;
  secondaryUnit?: string;
  icon: React.ReactNode;
  primaryColor: string;
  context: string;
  note?: string;
}

/**
 * Enhanced summary card with improved typography, spacing, and contrasts for readability
 */
export const ImprovedSummaryCard: React.FC<SummaryCardProps> = ({
  title,
  description,
  value,
  unit,
  secondaryValue,
  secondaryUnit,
  icon,
  primaryColor,
  context,
  note
}) => {
  // Format value if it's a number
  const formattedValue = typeof value === 'number' 
    ? value < 1 
      ? value.toFixed(4) 
      : value >= 1000000 
        ? `${(value / 1000000).toFixed(1)}M`
        : value >= 1000 
          ? `${(value / 1000).toFixed(0)}k`
          : value.toFixed(2)
    : value;
    
  // Format secondary value if it exists and is a number
  const formattedSecondaryValue = secondaryValue !== undefined && typeof secondaryValue === 'number'
    ? secondaryValue.toLocaleString(undefined, {
        maximumFractionDigits: secondaryValue >= 1000 ? 0 : 2
      })
    : secondaryValue;
  
  return (
    <Card className="overflow-hidden border-gray-200 shadow-sm hover:shadow-md transition-shadow">
      <CardHeader className="bg-gray-50/50 border-b border-gray-100 pb-2 pt-4">
        <div className="flex justify-between items-center">
          <CardTitle className="text-sm font-medium text-gray-700">
            {title}
          </CardTitle>
          <div className="text-white p-1 rounded-full shadow-sm" style={{ backgroundColor: primaryColor }}>
            {icon}
          </div>
        </div>
        <CardDescription className="text-xs text-gray-500">
          {description}
        </CardDescription>
      </CardHeader>
      <CardContent className="pt-4">
        <div className="space-y-1">
          <div>
            <div className="text-2xl font-bold" style={{ color: primaryColor }}>
              {formattedValue}
              {unit && <span className="text-base font-medium ml-1">{unit}</span>}
            </div>
            
            {secondaryValue !== undefined && (
              <div className="text-sm text-gray-600 mt-0">
                {secondaryUnit && secondaryUnit}{formattedSecondaryValue}
              </div>
            )}
          </div>
          
          <div className="flex flex-col">
            <div className="flex items-center">
              <span 
                className="inline-block w-2 h-2 rounded-full mr-2" 
                style={{ backgroundColor: primaryColor }}
              />
              <p className="text-xs text-gray-500">
                {context}
              </p>
            </div>
            
            {note && (
              <div className="mt-2 pt-2 border-t border-gray-100">
                <div className="text-xs text-gray-500">
                  {note}
                </div>
              </div>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

/**
 * Energy Curtailment Card with improved readability
 */
export const EnergyCurtailedCard: React.FC<{
  value: number;
  equivalentHomes?: number;
}> = ({ value, equivalentHomes = Math.round(value * 0.3) }) => {
  return (
    <ImprovedSummaryCard
      title="Energy Curtailed"
      description="Total wasted wind energy"
      value={value}
      unit="MWh"
      icon={<Wind className="h-4 w-4" />}
      primaryColor="#3b82f6" // blue-500
      context="Untapped energy resource"
      note={`That's enough to power approximately ${equivalentHomes.toLocaleString()} homes for a month`}
    />
  );
};

/**
 * Subsidies Paid Card with improved readability
 */
export const SubsidiesPaidCard: React.FC<{
  value: number;
  perMwh?: number;
}> = ({ value, perMwh = Math.round(value / 556) }) => {
  return (
    <ImprovedSummaryCard
      title="Subsidies Paid"
      description="Consumer cost for curtailment"
      value={value}
      unit="M"
      secondaryUnit="£"
      icon={<Battery className="h-4 w-4" />}
      primaryColor="#ef4444" // red-500
      context="Paid to idle wind farms"
      note={`Approximately £${perMwh} per MWh of curtailed energy`}
    />
  );
};

/**
 * Bitcoin Potential Card with improved readability
 */
export const BitcoinPotentialCard: React.FC<{
  bitcoinValue: number;
  fiatValue: number;
  minerModel: string;
  perGwh?: number;
}> = ({ bitcoinValue, fiatValue, minerModel, perGwh = (bitcoinValue / 556) * 1000 }) => {
  return (
    <ImprovedSummaryCard
      title="Potential Bitcoin"
      description={`Mining using ${minerModel}`}
      value={bitcoinValue}
      unit="₿"
      secondaryValue={fiatValue}
      secondaryUnit="≈ £"
      icon={<Bitcoin className="h-4 w-4" />}
      primaryColor="#F7931A" // Bitcoin orange
      context="Potential value from wasted energy"
      note={`${perGwh.toFixed(3)} ₿ per GWh of curtailed energy`}
    />
  );
};

/**
 * Value Ratio Card with improved readability
 */
export const ValueRatioCard: React.FC<{
  ratio: number;
}> = ({ ratio }) => {
  return (
    <ImprovedSummaryCard
      title="Value Ratio"
      description="Bitcoin value vs. subsidy cost"
      value={`${ratio.toFixed(2)}×`}
      icon={<ArrowRightLeft className="h-4 w-4" />}
      primaryColor="#22c55e" // green-500
      context={ratio > 1 ? "High value from mining" : "Low value compared to subsidies"}
      note={`Bitcoin value is ${ratio.toFixed(2)}× the subsidy payment`}
    />
  );
};

/**
 * Improved Summary Cards Component Set with better readability
 */
export default function ImprovedSummaryCards() {
  // Sample data - would be replaced with actual data in implementation
  const energyCurtailed = 556006;  // MWh
  const subsidiesPaid = 11.5;      // Million GBP
  const bitcoinPotential = 394.68; // BTC
  const bitcoinValue = 25.8;       // Million GBP
  const valueRatio = bitcoinValue / subsidiesPaid;
  
  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      <EnergyCurtailedCard value={energyCurtailed} />
      <SubsidiesPaidCard value={subsidiesPaid} />
      <BitcoinPotentialCard 
        bitcoinValue={bitcoinPotential} 
        fiatValue={bitcoinValue} 
        minerModel="S19J PRO" 
      />
      <ValueRatioCard ratio={valueRatio} />
    </div>
  );
}