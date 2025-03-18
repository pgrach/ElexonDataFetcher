import { Switch, Route, Link } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import NotFound from "@/pages/not-found";
import Home from "@/pages/home";
import ImprovementsDemo from "@/pages/improvements-demo";

// Navigation component for easy switching between pages
function Navigation() {
  return (
    <nav className="bg-gray-800 text-white p-4 mb-4">
      <div className="container mx-auto flex items-center justify-between">
        <div className="text-lg font-bold">Bitcoin Mining Analytics</div>
        <div className="flex space-x-6">
          <Link href="/">
            <div className="hover:text-blue-300 transition-colors cursor-pointer">Home</div>
          </Link>
          <Link href="/improvements">
            <div className="hover:text-blue-300 transition-colors cursor-pointer">Improvements Demo</div>
          </Link>
        </div>
      </div>
    </nav>
  );
}

function Router() {
  return (
    <>
      <Navigation />
      <Switch>
        {/* Add pages below */}
        <Route path="/" component={Home}/>
        <Route path="/improvements" component={ImprovementsDemo}/>
        {/* Fallback to 404 */}
        <Route component={NotFound} />
      </Switch>
    </>
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