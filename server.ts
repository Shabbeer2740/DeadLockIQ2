/**
 * DeadLockIQ Full-Stack Concurrency Proxy & WebSocket Server
 *
 * Integrates:
 * - Express REST API
 * - WebSocket Server on /ws
 * - Real-time state broadcasting
 * - Prometheus metrics
 * - Vite middleware integration (on port 3000)
 */

import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer as createViteServer } from 'vite';
import { transactionManager } from './src/server/transactionManager.ts';
import { lockCoordinator, redisLockEngine } from './src/server/lockCoordinator.ts';
import { waitForGraph } from './src/server/waitForGraph.ts';
import { recoveryEngine } from './src/server/recoveryEngine.ts';
import { scenarioSimulator, ScenarioType } from './src/server/simulator.ts';
import { benchmarkEngine } from './src/server/benchmark.ts';
import { metricsManager } from './src/server/metricsManager.ts';
import { postgresLockMonitor } from './src/server/postgresMonitor.ts';
import { fencingManager } from './src/server/fencing.ts';
import { WebSocketEvent, WSEventType } from './src/types/index.ts';

const app = express();
const port = Number(process.env.PORT) || 3000;
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const supportedScenarios = new Set<ScenarioType>([
  'TWO_WAY_DEADLOCK', 'THREE_WAY_DEADLOCK', 'N_WAY_DEADLOCK', 'NORMAL_CONTENTION',
  'MULTIPLE_DEADLOCKS', 'STALE_WORKER', 'FENCING_VIOLATION', 'RETRY_STORM',
  'POSTGRES_DEADLOCK', 'REDIS_FAILURE', 'WORKER_CRASH', 'NETWORK_DELAY', 'CROSS_SYSTEM_DEADLOCK',
]);

// Active WebSocket clients set
const clients = new Set<WebSocket>();

function broadcast(type: WSEventType, payload: any) {
  const event: WebSocketEvent = {
    type,
    payload,
    timestamp: Date.now(),
  };
  const message = JSON.stringify(event);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

// Hook up state callbacks to WebSockets
transactionManager.setOnStateChange((tx, oldState, newState) => {
  broadcast('TRANSACTION_CREATED', { tx, oldState, newState });
  broadcast('METRICS_UPDATED', metricsManager.getSnapshot());
});

lockCoordinator.setOnLockEvent(evt => {
  broadcast(evt.type as WSEventType, evt);
  broadcast('GRAPH_EDGE_ADDED', waitForGraph.getSnapshot());
  broadcast('METRICS_UPDATED', metricsManager.getSnapshot());
});

recoveryEngine.setCallbacks(
  incident => {
    broadcast('DEADLOCK_DETECTED', incident);
    broadcast('METRICS_UPDATED', metricsManager.getSnapshot());
  },
  (txId, retryCount) => {
    broadcast('RETRY_STARTED', { txId, retryCount });
  }
);

scenarioSimulator.setLogEmitter((message, level = 'info') => {
  broadcast('LOG_EVENT', { message, level, timestamp: Date.now() });
});

wss.on('connection', ws => {
  clients.add(ws);
  metricsManager.setWsClientCount(clients.size);

  // Send initial snapshot on connect
  const initialData: WebSocketEvent = {
    type: 'INIT_STATE',
    payload: {
      transactions: transactionManager.getAllTransactions(),
      locks: redisLockEngine.getAllLocks(),
      graph: waitForGraph.getSnapshot(),
      incidents: recoveryEngine.getAllIncidents(),
      metrics: metricsManager.getSnapshot(),
      postgresLocks: postgresLockMonitor.getObservedLocks(),
      fencingHistory: fencingManager.getHistory(),
    },
    timestamp: Date.now(),
  };

  ws.send(JSON.stringify(initialData));

  ws.on('close', () => {
    clients.delete(ws);
    metricsManager.setWsClientCount(clients.size);
  });
});

// Periodic broadcast of graph snapshot & live metrics
setInterval(() => {
  if (clients.size > 0) {
    broadcast('GRAPH_EDGE_ADDED', waitForGraph.getSnapshot());
    broadcast('METRICS_UPDATED', metricsManager.getSnapshot());
  }
}, 1000);

// ==========================================
// REST API ROUTES
// ==========================================

// Health Check
app.get('/api/v1/health', (req, res) => {
  res.json({
    status: 'HEALTHY',
    service: 'DeadLockIQ Distributed Concurrency Engine',
    uptime: process.uptime(),
    timestamp: Date.now(),
    redis: redisLockEngine.isHealthy() ? 'OK' : 'DEGRADED',
    redisConnectionState: redisLockEngine.getConnectionState(),
    redisConnectionError: redisLockEngine.getLastConnectionError(),
    postgresObserver: postgresLockMonitor.getStatus(),
    activeWorkers: clients.size,
  });
});

// Prometheus OpenMetrics
app.get('/metrics', (req, res) => {
  res.setHeader('Content-Type', 'text/plain; version=0.0.4');
  res.send(metricsManager.toPrometheusFormat());
});

// Metrics JSON Snapshot
app.get('/api/v1/metrics', (req, res) => {
  res.json(metricsManager.getSnapshot());
});

// Transactions API
app.get('/api/v1/transactions', (req, res) => {
  res.json({
    transactions: transactionManager.getAllTransactions(),
    total: transactionManager.getAllTransactions().length,
  });
});

app.get('/api/v1/transactions/:id', (req, res) => {
  const tx = transactionManager.getTransaction(req.params.id);
  if (!tx) {
    return res.status(404).json({ error: 'Transaction not found' });
  }
  res.json(tx);
});

app.post('/api/v1/transactions', (req, res) => {
  const { id, workerId, priority, maxRetries, workDoneUnits, rollbackCost } = req.body;
  if (!id || !workerId) {
    return res.status(400).json({ error: 'id and workerId are required' });
  }
  const tx = transactionManager.createTransaction({
    id,
    workerId,
    priority,
    maxRetries,
    workDoneUnits,
    rollbackCost,
  });
  res.status(201).json(tx);
});

// Locks API
app.post('/api/v1/locks/acquire', async (req, res) => {
  const { resourceId, txId, workerId, ttlMs } = req.body;
  if (!resourceId || !txId || !workerId) {
    return res.status(400).json({ error: 'resourceId, txId, and workerId are required' });
  }
  const result = await lockCoordinator.acquireLock(resourceId, txId, workerId, ttlMs);
  res.json(result);
});

app.post('/api/v1/locks/release', async (req, res) => {
  const { resourceId, txId, fencingToken } = req.body;
  if (!resourceId || !txId) {
    return res.status(400).json({ error: 'resourceId and txId are required' });
  }
  const result = await lockCoordinator.releaseLock(resourceId, txId, fencingToken);
  res.json(result);
});

// Graph API
app.get('/api/v1/graph', (req, res) => {
  res.json(waitForGraph.getSnapshot());
});

// Deadlocks & Incidents API
app.get('/api/v1/deadlocks', (req, res) => {
  res.json({
    incidents: recoveryEngine.getAllIncidents(),
    total: recoveryEngine.getAllIncidents().length,
  });
});

app.get('/api/v1/deadlocks/:id', (req, res) => {
  const inc = recoveryEngine.getIncident(req.params.id);
  if (!inc) {
    return res.status(404).json({ error: 'Incident not found' });
  }
  res.json(inc);
});

// PostgreSQL Lock Observer
app.get('/api/v1/postgres/locks', (req, res) => {
  res.json({
    locks: postgresLockMonitor.getObservedLocks(),
    hasDeadlock: postgresLockMonitor.isDeadlockObserved(),
    status: postgresLockMonitor.getStatus(),
  });
});

// Simulator Control APIs
app.post('/api/v1/simulator/scenarios/:scenario', async (req, res) => {
  const scenario = req.params.scenario as ScenarioType;
  const timeoutMs = 30000;
  if (!supportedScenarios.has(scenario)) {
    return res.status(400).json({ success: false, message: `Unknown scenario: ${req.params.scenario}`, timedOut: false });
  }

  try {
    const result = await Promise.race([
      scenarioSimulator.runScenario(scenario),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`Scenario ${scenario} timed out after ${timeoutMs}ms`)), timeoutMs)),
    ]);
    if (!result.success) {
      const status = result.timedOut ? 504 : 500;
      return res.status(status).json(result);
    }
    res.json(result);
  } catch (error: any) {
    const message = error?.message || `Scenario ${scenario} failed unexpectedly`;
    const timedOut = /timed out|timeout/i.test(message);
    res.status(timedOut ? 504 : 500).json({
      success: false,
      message,
      timedOut,
    });
  }
});

app.post('/api/v1/simulator/reset', (req, res) => {
  scenarioSimulator.resetAll();
  broadcast('INIT_STATE', {
    transactions: [],
    locks: [],
    graph: waitForGraph.getSnapshot(),
    incidents: [],
    metrics: metricsManager.getSnapshot(),
    postgresLocks: postgresLockMonitor.getObservedLocks(),
    fencingHistory: [],
  });
  res.json({ success: true, message: 'All engine state, locks, and graphs reset successfully' });
});

// Benchmark API
app.post('/api/v1/benchmark/run', async (req, res) => {
  const workers = parseInt(req.body.workers || '500', 10);
  const result = await benchmarkEngine.runBenchmark(workers);
  res.json(result);
});

// ==========================================
// VITE SPA MIDDLEWARE / STATIC ASSETS
// ==========================================
async function startServer() {
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    app.use(express.static('dist'));
  }

  server.listen(port, '0.0.0.0', () => {
    console.log(`[DeadLockIQ] Server running at http://0.0.0.0:${port}`);
    console.log(`[DeadLockIQ] WebSocket server live at ws://0.0.0.0:${port}/ws`);
  });
}

startServer().catch(err => {
  console.error('[DeadLockIQ] Failed to start server:', err);
});
