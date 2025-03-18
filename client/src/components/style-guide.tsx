"use client";

import React from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Bitcoin, Wind, Battery, ArrowRightLeft, Info } from "lucide-react";

// Typography style guide to ensure consistency
export const Typography = () => {
  return (
    <Card className="overflow-hidden border-gray-200 shadow-sm">
      <CardHeader className="bg-gray-50/50 border-b border-gray-100">
        <CardTitle className="text-lg font-medium text-gray-800">Typography Guidelines</CardTitle>
        <CardDescription>Consistent text styles for improved readability</CardDescription>
      </CardHeader>
      <CardContent className="p-6 space-y-6">
        <div className="space-y-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Card Titles</h2>
            <div className="bg-gray-50 p-3 rounded-md">
              <p className="text-sm font-medium text-gray-700">Energy Curtailed</p>
              <p className="text-xs text-gray-500 mt-1">Font: 14px (text-sm), Medium weight, Gray-700</p>
            </div>
          </div>
          
          <div>
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Card Descriptions</h2>
            <div className="bg-gray-50 p-3 rounded-md">
              <p className="text-xs text-gray-500">Total wasted wind energy</p>
              <p className="text-xs text-gray-500 mt-1">Font: 12px (text-xs), Regular weight, Gray-500</p>
            </div>
          </div>
          
          <div>
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Primary Values</h2>
            <div className="bg-gray-50 p-3 rounded-md">
              <p className="text-2xl font-bold text-gray-800">556k <span className="text-base font-medium">MWh</span></p>
              <p className="text-xs text-gray-500 mt-1">Font: 24px (text-2xl), Bold weight, Gray-800</p>
            </div>
          </div>
          
          <div>
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Secondary Text</h2>
            <div className="bg-gray-50 p-3 rounded-md">
              <p className="text-xs text-gray-500">Approximately <span className="font-medium text-primary">£20</span> per MWh of curtailed energy</p>
              <p className="text-xs text-gray-500 mt-1">Font: 12px (text-xs), Regular weight, Gray-500 with Medium weight for highlights</p>
            </div>
          </div>
          
          <div>
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Chart Axis Labels</h2>
            <div className="bg-gray-50 p-3 rounded-md">
              <p className="text-xs font-medium text-gray-600">Curtailed Energy (MWh)</p>
              <p className="text-xs text-gray-500 mt-1">Font: 12px (text-xs), Medium weight, Gray-600</p>
            </div>
          </div>
          
          <div>
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Chart Legends</h2>
            <div className="bg-gray-50 p-3 rounded-md flex items-center">
              <span className="w-3 h-3 rounded bg-primary mr-2"></span>
              <p className="text-sm font-medium text-gray-700">Curtailed Energy (MWh)</p>
            </div>
            <p className="text-xs text-gray-500 mt-1">Font: 13px (text-sm), Medium weight, Gray-700</p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

// Color style guide to ensure consistency
export const ColorPalette = () => {
  const colorGroups = [
    {
      title: "Primary Colors",
      colors: [
        { name: "Primary", class: "bg-primary", textClass: "text-white", value: "var(--primary)" },
        { name: "Primary Hover", class: "bg-primary/90", textClass: "text-white", value: "var(--primary) at 90%" },
        { name: "Primary Light", class: "bg-primary/20", textClass: "text-primary", value: "var(--primary) at 20%" }
      ]
    },
    {
      title: "Semantic Colors",
      colors: [
        { name: "Bitcoin", class: "bg-[#F7931A]", textClass: "text-white", value: "#F7931A" },
        { name: "Energy", class: "bg-blue-500", textClass: "text-white", value: "#3B82F6" },
        { name: "Subsidies", class: "bg-red-500", textClass: "text-white", value: "#EF4444" },
        { name: "Value", class: "bg-green-500", textClass: "text-white", value: "#22C55E" }
      ]
    },
    {
      title: "Chart Colors",
      colors: [
        { name: "Energy Bars", class: "bg-blue-500", textClass: "text-white", value: "#3B82F6" },
        { name: "Bitcoin Bars", class: "bg-[#F7931A]", textClass: "text-white", value: "#F7931A" },
        { name: "Reference Line", class: "bg-gray-400", textClass: "text-white", value: "#9CA3AF" },
        { name: "Reference Area", class: "bg-blue-100", textClass: "text-blue-800", value: "#DBEAFE" }
      ]
    },
    {
      title: "Gray Scale",
      colors: [
        { name: "Gray 900", class: "bg-gray-900", textClass: "text-white", value: "#111827" },
        { name: "Gray 800", class: "bg-gray-800", textClass: "text-white", value: "#1F2937" },
        { name: "Gray 700", class: "bg-gray-700", textClass: "text-white", value: "#374151" },
        { name: "Gray 600", class: "bg-gray-600", textClass: "text-white", value: "#4B5563" },
        { name: "Gray 500", class: "bg-gray-500", textClass: "text-white", value: "#6B7280" },
        { name: "Gray 400", class: "bg-gray-400", textClass: "text-gray-900", value: "#9CA3AF" },
        { name: "Gray 300", class: "bg-gray-300", textClass: "text-gray-900", value: "#D1D5DB" },
        { name: "Gray 200", class: "bg-gray-200", textClass: "text-gray-900", value: "#E5E7EB" },
        { name: "Gray 100", class: "bg-gray-100", textClass: "text-gray-900", value: "#F3F4F6" },
        { name: "Gray 50", class: "bg-gray-50", textClass: "text-gray-900", value: "#F9FAFB" }
      ]
    }
  ];

  return (
    <Card className="overflow-hidden border-gray-200 shadow-sm">
      <CardHeader className="bg-gray-50/50 border-b border-gray-100">
        <CardTitle className="text-lg font-medium text-gray-800">Color Consistency Guidelines</CardTitle>
        <CardDescription>Standard colors for improved readability and visual consistency</CardDescription>
      </CardHeader>
      <CardContent className="p-6 space-y-6">
        {colorGroups.map((group, i) => (
          <div key={i} className="space-y-2">
            <h2 className="text-lg font-semibold text-gray-800 mb-3">{group.title}</h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {group.colors.map((color, j) => (
                <div key={j} className="overflow-hidden rounded-md border border-gray-200">
                  <div className={`h-12 ${color.class} flex items-center justify-center ${color.textClass}`}>
                    {color.name}
                  </div>
                  <div className="p-2 bg-white text-xs text-center text-gray-600">
                    {color.value}
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
};

// Icon usage guide
export const IconGuide = () => {
  const icons = [
    { name: "Wind", component: <Wind className="h-5 w-5 text-primary" />, usage: "Energy Curtailed" },
    { name: "Battery", component: <Battery className="h-5 w-5 text-red-500" />, usage: "Subsidies Paid" },
    { name: "Bitcoin", component: <Bitcoin className="h-5 w-5 text-[#F7931A]" />, usage: "Bitcoin Mining" },
    { name: "ArrowRightLeft", component: <ArrowRightLeft className="h-5 w-5 text-green-500" />, usage: "Value Ratio" },
    { name: "Info", component: <Info className="h-5 w-5 text-blue-500" />, usage: "Information Notes" }
  ];

  return (
    <Card className="overflow-hidden border-gray-200 shadow-sm">
      <CardHeader className="bg-gray-50/50 border-b border-gray-100">
        <CardTitle className="text-lg font-medium text-gray-800">Icon Usage Guidelines</CardTitle>
        <CardDescription>Standardized icon usage for improved visual recognition</CardDescription>
      </CardHeader>
      <CardContent className="p-6">
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
          {icons.map((icon, i) => (
            <div key={i} className="p-4 bg-gray-50 rounded-md border border-gray-200">
              <div className="flex flex-col items-center text-center">
                <div className="mb-2">{icon.component}</div>
                <h3 className="text-sm font-medium text-gray-700">{icon.name}</h3>
                <p className="text-xs text-gray-500 mt-1">{icon.usage}</p>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
};

// Main style guide component that combines all guidelines
export default function StyleGuide() {
  return (
    <div className="space-y-8 pb-10">
      <Card className="overflow-hidden border-gray-200 shadow-sm">
        <CardHeader className="bg-gray-50/50 border-b border-gray-100">
          <CardTitle className="text-xl font-medium text-gray-800">Bitcoin Mining Analytics Style Guide</CardTitle>
          <CardDescription>Comprehensive visual guidelines for consistent and readable interfaces</CardDescription>
        </CardHeader>
        <CardContent className="p-6">
          <p className="text-gray-600">
            This style guide provides standardized typography, colors, spacing, and component styles to ensure consistency
            across the Bitcoin Mining Analytics platform. Following these guidelines improves readability, accessibility,
            and overall user experience.
          </p>
        </CardContent>
      </Card>
      
      <Typography />
      <ColorPalette />
      <IconGuide />
    </div>
  );
}