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
          <div className="flex flex-col gap-2 p-3 rounded-md bg-background/80">
            <div className="flex items-center gap-2">
              <Calendar className="h-4 w-4 text-primary" />
              <h3 className="font-semibold">1. Select Time Period</h3>
            </div>
            <p className="text-sm text-muted-foreground">
              Choose daily, monthly, or yearly view using the timeframe selector, then select a specific date.
            </p>
          </div>
          
          <div className="flex flex-col gap-2 p-3 rounded-md bg-background/80">
            <div className="flex items-center gap-2">
              <Bitcoin className="h-4 w-4 text-primary" />
              <h3 className="font-semibold">2. Choose Miner Model</h3>
            </div>
            <p className="text-sm text-muted-foreground">
              Select a Bitcoin miner model to see how different hardware affects potential mining returns.
            </p>
          </div>
          
          <div className="flex flex-col gap-2 p-3 rounded-md bg-background/80">
            <div className="flex items-center gap-2">
              <Building2 className="h-4 w-4 text-primary" />
              <h3 className="font-semibold">3. Filter by Wind Farm</h3>
            </div>
            <p className="text-sm text-muted-foreground">
              View all farms or filter by a specific lead party to focus on particular wind farm data.
            </p>
          </div>
        </div>
        
        <div className="mt-4 p-3 rounded-md bg-primary/10 border border-primary/20">
          <h3 className="font-semibold flex items-center gap-2 mb-2">
            <DollarSign className="h-4 w-4 text-primary" />
            <span>Key Comparison: Curtailment Payments vs. Bitcoin Mining Value</span>
          </h3>
          <p className="text-sm">
            This dashboard helps you compare what wind farms are <strong>currently paid to not produce electricity</strong> (curtailment payments) 
            versus what they <strong>could earn by mining Bitcoin</strong> with that same unused energy.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}