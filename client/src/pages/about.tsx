import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { SiGithub, SiLinkedin } from "react-icons/si";

export default function About() {
  return (
    <div className="min-h-screen w-full p-4 md:p-8 bg-background">
      <div className="max-w-4xl mx-auto space-y-8">
        <h1 className="text-4xl font-bold tracking-tight">About CurtailCoin</h1>
        
        <Card>
          <CardHeader className="text-xl font-semibold">
            Our Mission
          </CardHeader>
          <CardContent className="prose dark:prose-invert">
            <p>
              CurtailCoin is a sophisticated real-time data processing platform for analyzing 
              Elexon BMRS wind farm curtailment data, providing comprehensive insights into 
              energy production and grid management dynamics.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="text-xl font-semibold">
            The Team
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="flex items-start gap-4">
              <div>
                <h3 className="text-lg font-medium">Sergei Ivanov</h3>
                <p className="text-sm text-muted-foreground mt-1">
                  Co-founder & Technical Advisor
                </p>
                <p className="mt-2 text-sm">
                  Sergei has been instrumental in shaping CurtailCoin through his enthusiastic support,
                  valuable insights, and extensive knowledge sharing. His contributions to data analysis
                  and technical guidance have been invaluable to the project.
                </p>
                <div className="mt-3 flex gap-2">
                  <a
                    href="https://www.linkedin.com/in/sergei-ivanov/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
                  >
                    <SiLinkedin className="h-4 w-4" />
                    <span>LinkedIn Profile</span>
                  </a>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
