import type { Express } from "express";
import { createServer, type Server } from "http";
import { getDailySummary, getMonthlySummary } from "./controllers/summary";
import { processDailyCurtailment } from "./services/curtailment";
import { WebSocketServer, WebSocket } from "ws";

export function registerRoutes(app: Express): Server {
  // Daily summary endpoint
  app.get("/api/summary/daily/:date", getDailySummary);

  // Monthly summary endpoint
  app.get("/api/summary/monthly/:yearMonth", getMonthlySummary);

  // Re-ingest data endpoint
  app.post("/api/ingest/:date", async (req, res) => {
    try {
      const { date } = req.params;
      console.log(`Starting re-ingestion for date: ${date}`);
      await processDailyCurtailment(date);
      res.json({ message: `Successfully re-ingested data for ${date}` });
    } catch (error) {
      console.error('Error during re-ingestion:', error);
      res.status(500).json({ error: 'Failed to re-ingest data' });
    }
  });

  // Cache invalidation endpoint
  app.post("/api/cache/invalidate/:yearMonth", (req, res) => {
    const { yearMonth } = req.params;
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({
          type: 'CACHE_INVALIDATE',
          data: { yearMonth }
        }));
      }
    });
    res.json({ message: 'Cache invalidation broadcast sent' });
  });

  const httpServer = createServer(app);

  // Setup WebSocket server
  const wss = new WebSocketServer({ 
    server: httpServer,
    verifyClient: (info: { req: { headers: { [key: string]: string | undefined } } }) => {
      // Ignore vite HMR websocket connections
      return info.req.headers['sec-websocket-protocol'] !== 'vite-hmr';
    }
  });

  wss.on('connection', (ws) => {
    console.log('Client connected to WebSocket');
    ws.on('close', () => console.log('Client disconnected from WebSocket'));
  });

  return httpServer;
}
