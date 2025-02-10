import { QueryClient } from "@tanstack/react-query";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      queryFn: async ({ queryKey }) => {
        const response = await fetch(queryKey[0] as string, {
          credentials: "include",
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
          },
        });

        if (!response.ok) {
          if (response.status === 404) {
            return null;
          }

          if (response.status >= 500) {
            throw new Error(`Server Error: ${response.status}`);
          }

          const errorText = await response.text();
          throw new Error(`API Error: ${response.status} - ${errorText}`);
        }

        return response.json();
      },
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 30000, // 30 seconds
    },
    mutations: {
      retry: false,
    }
  },
});