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
  const [showTutorial, setShowTutorial] = useState(false);
  
  // Check if this is the user's first visit
  useEffect(() => {
    const tutorialSeen = localStorage.getItem('dashboardTutorialSeen');
    if (!tutorialSeen) {
      setShowTutorial(true);
    }
  }, []);
  
  const closeTutorial = () => {
    setShowTutorial(false);
    localStorage.setItem('dashboardTutorialSeen', 'true');
  };
  
  if (!showTutorial) return null;
  
  return (
    <Card className="mb-8 border-primary/20 bg-muted/30 relative">
      <Button 
        variant="ghost" 
        size="icon" 
        onClick={closeTutorial}
        className="absolute top-2 right-2 z-10"
      >
        <X className="h-4 w-4" />
      </Button>
      
      <CardHeader>
        <CardTitle className="text-lg flex items-center gap-2">
          <Gauge className="h-5 w-5 text-primary" />
          How to Use This Dashboard
        </CardTitle>
      </CardHeader>
      
      <CardContent>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="flex flex-col gap-2 p-4 rounded-md bg-background shadow-sm border border-primary/10 relative">
            <div className="bg-primary text-primary-foreground rounded-full w-7 h-7 flex items-center justify-center absolute -top-3 -left-3">1</div>
            <div className="flex items-center gap-2 mb-1">
              <Calendar className="h-5 w-5 text-primary" />
              <h3 className="font-semibold">Select Time Period</h3>
            </div>
            <div className="text-sm text-muted-foreground flex flex-col gap-2">
              <div className="flex items-center gap-1">
                <span className="w-2 h-2 rounded-full bg-primary"></span>
                <span>Choose daily, monthly, or yearly</span>
              </div>
              <div className="flex items-center gap-1">
                <span className="w-2 h-2 rounded-full bg-primary"></span>
                <span>Select a specific date</span>
              </div>
            </div>
          </div>
          
          <div className="flex flex-col gap-2 p-4 rounded-md bg-background shadow-sm border border-primary/10 relative">
            <div className="bg-primary text-primary-foreground rounded-full w-7 h-7 flex items-center justify-center absolute -top-3 -left-3">2</div>
            <div className="flex items-center gap-2 mb-1">
              <Bitcoin className="h-5 w-5 text-primary" />
              <h3 className="font-semibold">Choose Miner Model</h3>
            </div>
            <div className="text-sm text-muted-foreground flex flex-col gap-2">
              <div className="flex items-center gap-1">
                <span className="w-2 h-2 rounded-full bg-primary"></span>
                <span>Each model has different efficiency</span>
              </div>
              <div className="flex items-center gap-1">
                <span className="w-2 h-2 rounded-full bg-primary"></span>
                <span>Compare hardware performance</span>
              </div>
            </div>
          </div>
          
          <div className="flex flex-col gap-2 p-4 rounded-md bg-background shadow-sm border border-primary/10 relative">
            <div className="bg-primary text-primary-foreground rounded-full w-7 h-7 flex items-center justify-center absolute -top-3 -left-3">3</div>
            <div className="flex items-center gap-2 mb-1">
              <Building2 className="h-5 w-5 text-primary" />
              <h3 className="font-semibold">Filter by Wind Farm</h3>
            </div>
            <div className="text-sm text-muted-foreground flex flex-col gap-2">
              <div className="flex items-center gap-1">
                <span className="w-2 h-2 rounded-full bg-primary"></span>
                <span>View all or specific farms</span>
              </div>
              <div className="flex items-center gap-1">
                <span className="w-2 h-2 rounded-full bg-primary"></span>
                <span>Group by lead party companies</span>
              </div>
            </div>
          </div>
        </div>
        
        <div className="mt-6 p-4 rounded-md bg-primary/10 border border-primary/20 shadow-sm">
          <h3 className="font-semibold flex items-center gap-2 mb-3">
            <DollarSign className="h-5 w-5 text-primary" />
            <span>What This Dashboard Shows</span>
          </h3>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <span className="bg-destructive/20 text-destructive font-bold rounded-full w-6 h-6 flex items-center justify-center">£</span>
              <span className="text-sm">Subsidies paid</span>
            </div>
            <div className="text-muted-foreground">vs</div>
            <div className="flex items-center gap-2">
              <span className="bg-amber-100 text-amber-800 font-bold rounded-full w-6 h-6 flex items-center justify-center">₿</span>
              <span className="text-sm">Potential Bitcoin value</span>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}