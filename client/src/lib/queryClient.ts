import { QueryClient } from "@tanstack/react-query";

// Set up WebSocket connection for cache invalidation
const ws = new WebSocket(`ws://${window.location.host}`);

ws.onmessage = (event) => {
  try {
    const message = JSON.parse(event.data);
    if (message.type === 'CACHE_INVALIDATE') {
      const { yearMonth } = message.data;
      // Invalidate both monthly and daily queries for the affected month
      queryClient.invalidateQueries({
        queryKey: [`/api/summary/monthly/${yearMonth}`]
      });
      // Also invalidate any daily summaries for this month
      queryClient.invalidateQueries({
        predicate: (query) => 
          typeof query.queryKey[0] === 'string' &&
          query.queryKey[0].startsWith(`/api/summary/daily/${yearMonth}`)
      });
    }
  } catch (error) {
    console.error('Error processing WebSocket message:', error);
  }
};

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      queryFn: async ({ queryKey }) => {
        const res = await fetch(queryKey[0] as string, {
          credentials: "include",
        });

        if (!res.ok) {
          if (res.status >= 500) {
            throw new Error(`${res.status}: ${res.statusText}`);
          }

          throw new Error(`${res.status}: ${await res.text()}`);
        }

        return res.json();
      },
      refetchInterval: false,
      refetchOnWindowFocus: false,
      staleTime: Infinity,
      retry: false,
    },
    mutations: {
      retry: false,
    }
  },
});