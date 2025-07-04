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
          <AlertTitle className="text-xl font-bold flex items-center">
            Wind Farm Curtailment: Paying for Energy Never Produced
          </AlertTitle>
          <AlertDescription className="text-lg mt-3">
            <div className="flex flex-col sm:flex-row gap-5">
              <div className="flex-1 flex items-start gap-3">
                <span className="bg-red-100 text-red-800 font-bold rounded-full w-7 h-7 flex items-center justify-center mt-0.5">1</span>
                <p>Wind farms get paid to <strong>stop producing</strong> during grid constraints</p>
              </div>
              <div className="flex-1 flex items-start gap-3">
                <span className="bg-red-100 text-red-800 font-bold rounded-full w-7 h-7 flex items-center justify-center mt-0.5">2</span>
                <p>Consumers pay for this unused energy through their bills</p>
              </div>
              <div className="flex-1 flex items-start gap-3">
                <span className="bg-red-100 text-red-800 font-bold rounded-full w-7 h-7 flex items-center justify-center mt-0.5">3</span>
                <p>Could Bitcoin mining be a more economical alternative?</p>
              </div>
            </div>
          </AlertDescription>
        </div>
        <Button variant="ghost" size="icon" onClick={hideBanner} className="absolute top-2 right-2">
          <X className="h-4 w-4" />
        </Button>
      </Alert>
    </div>
  );
}