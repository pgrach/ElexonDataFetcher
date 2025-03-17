"use client";

import { AlertTriangle, Info, X } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";

export default function CurtailmentInfoBanner() {
  // State to track if the banner should be shown
  const [showBanner, setShowBanner] = useState(true);
  
  // Retrieve from localStorage on mount
  useEffect(() => {
    const bannerHidden = localStorage.getItem('curtailmentBannerHidden');
    if (bannerHidden === 'true') {
      setShowBanner(false);
    }
  }, []);
  
  // Hide banner and save preference
  const hideBanner = () => {
    setShowBanner(false);
    localStorage.setItem('curtailmentBannerHidden', 'true');
  };
  
  if (!showBanner) return null;
  
  return (
    <div className="mb-8 relative">
      <Alert variant="destructive" className="bg-opacity-90 border-red-400">
        <AlertTriangle className="h-5 w-5" />
        <div className="flex-1">
          <AlertTitle className="text-lg font-bold flex items-center">
            Wind Farm Curtailment: Paying for Energy Never Produced
          </AlertTitle>
          <AlertDescription className="text-sm mt-1">
            <p className="mb-2">
              <strong>Curtailment payments</strong> are made to wind farms when they're asked to reduce output, even though 
              no electricity is actually delivered. These payments are ultimately funded by consumers through 
              their energy bills.
            </p>
            <p className="font-semibold">
              This dashboard explores whether mining Bitcoin with this wasted energy could be 
              more economically beneficial than the current system.
            </p>
          </AlertDescription>
        </div>
        <Button variant="ghost" size="icon" onClick={hideBanner} className="absolute top-2 right-2">
          <X className="h-4 w-4" />
        </Button>
      </Alert>
    </div>
  );
}