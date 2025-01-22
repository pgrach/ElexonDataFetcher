import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import NotFound from "@/pages/not-found";
import Home from "@/pages/home";
import YearlyStats from "@/pages/yearly-stats";
import { Button } from "@/components/ui/button";
import { Link } from "wouter";

function Navigation() {
  return (
    <nav className="border-b mb-4">
      <div className="container mx-auto px-4 py-2 flex gap-2">
        <Link href="/">
          <Button variant="ghost">Home</Button>
        </Link>
        <Link href="/yearly-stats">
          <Button variant="ghost">Yearly Statistics</Button>
        </Link>
      </div>
    </nav>
  );
}

function Router() {
  return (
    <div>
      <Navigation />
      <Switch>
        <Route path="/" component={Home}/>
        <Route path="/yearly-stats" component={YearlyStats}/>
        <Route component={NotFound} />
      </Switch>
    </div>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Router />
      <Toaster />
    </QueryClientProvider>
  );
}

export default App;