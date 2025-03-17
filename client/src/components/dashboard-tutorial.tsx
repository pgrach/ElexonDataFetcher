"use client";

import { 
  Calendar, 
  Wind, 
  Bitcoin, 
  Gauge, 
  DollarSign,
  Clock,
  Building2,
  X
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useState, useEffect } from "react";

export default function DashboardTutorial() {
  const [viewMode, setViewMode] = useState<'closed' | 'minimized' | 'full'>('full');
  
  // Check if this is the user's first visit
  useEffect(() => {
    const tutorialPreference = localStorage.getItem('dashboardTutorialPreference');
    if (tutorialPreference === 'hidden') {
      setViewMode('closed');
    } else if (tutorialPreference === 'minimized') {
      setViewMode('minimized');
    }
  }, []);
  
  const closeTutorial = () => {
    setViewMode('closed');
    localStorage.setItem('dashboardTutorialPreference', 'hidden');
  };
  
  const minimizeTutorial = () => {
    setViewMode('minimized');
    localStorage.setItem('dashboardTutorialPreference', 'minimized');
  };
  
  const expandTutorial = () => {
    setViewMode('full');
    localStorage.setItem('dashboardTutorialPreference', 'full');
  };
  
  // Return null if the tutorial is closed
  if (viewMode === 'closed') {
    // Show a small button to bring the tutorial back
    return (
      <div className="mb-6 flex justify-end">
        <Button 
          variant="outline" 
          size="sm" 
          onClick={expandTutorial}
          className="flex items-center gap-2 border-primary/40 bg-primary/5 hover:bg-primary/10"
        >
          <Gauge className="h-4 w-4 text-primary" />
          <span>Show Guide</span>
        </Button>
      </div>
    );
  }
  
  // Return minimized version if selected
  if (viewMode === 'minimized') {
    return (
      <div className="mb-6 flex justify-center">
        <Button 
          variant="outline" 
          size="sm" 
          onClick={expandTutorial}
          className="flex items-center gap-2 border-primary/40 bg-primary/5 hover:bg-primary/10"
        >
          <Gauge className="h-4 w-4 text-primary" />
          <span>Show Dashboard Guide</span>
        </Button>
      </div>
    );
  }
  
  // Full tutorial
  return (
    <Card className="mb-8 border-primary/20 bg-gradient-to-r from-primary/5 to-transparent relative overflow-hidden">
      <div className="absolute top-3 right-3 z-10 flex items-center gap-2">
        <Button 
          variant="ghost" 
          size="sm" 
          onClick={minimizeTutorial}
          className="h-8 px-2 text-xs"
        >
          Minimize
        </Button>
        <Button 
          variant="ghost" 
          size="icon" 
          onClick={closeTutorial}
          className="h-8 w-8"
        >
          <X className="h-4 w-4" />
        </Button>
      </div>
      
      <CardHeader className="pb-2">
        <CardTitle className="text-lg flex items-center gap-2">
          <Gauge className="h-5 w-5 text-primary" />
          Quick Start Guide
        </CardTitle>
      </CardHeader>
      
      <CardContent>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          {/* Step 1 */}
          <div className="flex flex-col gap-3 p-4 pt-8 mt-4 rounded-md bg-background/95 shadow-sm border border-primary/10 relative">
            <div className="bg-primary text-primary-foreground rounded-full w-8 h-8 flex items-center justify-center absolute -top-4 left-4 shadow-md font-semibold text-lg">1</div>
            
            {/* Icon and title with visual emphasis */}
            <div className="flex items-center gap-2 mb-1">
              <div className="bg-primary/10 p-2 rounded-full">
                <Calendar className="h-5 w-5 text-primary" />
              </div>
              <h3 className="font-semibold">Select Time Period</h3>
            </div>
            
            {/* Bullet points with improved visual design */}
            <ul className="text-sm space-y-2 pl-0 list-none">
              <li className="flex items-center gap-2">
                <Clock className="h-4 w-4 text-primary flex-shrink-0" />
                <span>Choose daily, monthly, or yearly view</span>
              </li>
              <li className="flex items-center gap-2">
                <Calendar className="h-4 w-4 text-primary flex-shrink-0" />
                <span>Select specific dates to explore</span>
              </li>
            </ul>
          </div>
          
          {/* Step 2 */}
          <div className="flex flex-col gap-3 p-4 pt-8 mt-4 rounded-md bg-background/95 shadow-sm border border-primary/10 relative">
            <div className="bg-primary text-primary-foreground rounded-full w-8 h-8 flex items-center justify-center absolute -top-4 left-4 shadow-md font-semibold text-lg">2</div>
            
            <div className="flex items-center gap-2 mb-1">
              <div className="bg-primary/10 p-2 rounded-full">
                <Bitcoin className="h-5 w-5 text-primary" />
              </div>
              <h3 className="font-semibold">Choose Miner Model</h3>
            </div>
            
            <ul className="text-sm space-y-2 pl-0 list-none">
              <li className="flex items-center gap-2">
                <Gauge className="h-4 w-4 text-primary flex-shrink-0" />
                <span>Compare mining hardware efficiency</span>
              </li>
              <li className="flex items-center gap-2">
                <DollarSign className="h-4 w-4 text-primary flex-shrink-0" />
                <span>See value differences between models</span>
              </li>
            </ul>
          </div>
          
          {/* Step 3 */}
          <div className="flex flex-col gap-3 p-4 pt-8 mt-4 rounded-md bg-background/95 shadow-sm border border-primary/10 relative">
            <div className="bg-primary text-primary-foreground rounded-full w-8 h-8 flex items-center justify-center absolute -top-4 left-4 shadow-md font-semibold text-lg">3</div>
            
            <div className="flex items-center gap-2 mb-1">
              <div className="bg-primary/10 p-2 rounded-full">
                <Building2 className="h-5 w-5 text-primary" />
              </div>
              <h3 className="font-semibold">Filter Wind Farms</h3>
            </div>
            
            <ul className="text-sm space-y-2 pl-0 list-none">
              <li className="flex items-center gap-2">
                <Wind className="h-4 w-4 text-primary flex-shrink-0" />
                <span>View all farms or individual sites</span>
              </li>
              <li className="flex items-center gap-2">
                <Building2 className="h-4 w-4 text-primary flex-shrink-0" />
                <span>Filter by lead party companies</span>
              </li>
            </ul>
          </div>
        </div>
        
        {/* Dashboard explanation with more visual styling */}
        <div className="p-4 rounded-md bg-gradient-to-r from-primary/10 to-primary/5 border border-primary/20 shadow-sm">
          <h3 className="font-semibold flex items-center gap-2 mb-3 text-primary/90">
            <DollarSign className="h-5 w-5" />
            <span>Compare Energy Alternatives</span>
          </h3>
          
          <div className="flex flex-col sm:flex-row gap-4 sm:items-center">
            <div className="flex items-center gap-2 p-2 bg-white/80 rounded-md">
              <span className="bg-destructive/20 text-destructive font-bold rounded-full w-8 h-8 flex items-center justify-center flex-shrink-0">£</span>
              <div>
                <div className="font-medium text-sm">Current System</div>
                <div className="text-xs text-muted-foreground">Subsidies for curtailment</div>
              </div>
            </div>
            
            <div className="hidden sm:block text-lg font-bold text-primary/70">vs</div>
            
            <div className="flex items-center gap-2 p-2 bg-white/80 rounded-md">
              <span className="bg-amber-100 text-amber-800 font-bold rounded-full w-8 h-8 flex items-center justify-center flex-shrink-0">₿</span>
              <div>
                <div className="font-medium text-sm">Alternative</div>
                <div className="text-xs text-muted-foreground">Bitcoin from wasted energy</div>
              </div>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}