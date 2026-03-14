/**
 * Express Application Setup
 * Professional backend structure with controllers, services, and routes
 * 
 * IMPORTANT: NO FALLBACKS - Configuration must be complete to start
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const http = require('http');
const config = require('./config');
const routes = require('./routes');
const errorHandler = require('./middleware/errorHandler');
// WebSocket RE-ENABLED — streaming read model pushes real-time updates to frontend
const { initializeWebSocketService } = require('./services/websocketService');

// Validate configuration on startup - FAIL FAST
console.log('');
console.log('🔧 Validating configuration...');

if (!config.validate()) {
  console.error('');
  console.error('❌ FATAL: Configuration validation failed. Cannot start server.');
  console.error('   Please check your .env file and ensure all required variables are set.');
  console.error('   See .env.example for reference.');
  console.error('');
  process.exit(1);
}

// Log configuration summary (masked)
console.log('');
console.log('📋 Configuration Summary:');
console.log(JSON.stringify(config.getSummary(), null, 2));
console.log('');

// ─────────────────────────────────────────────────────────────────────
// Lazy SDK initialization for Vercel serverless (cold starts)
// On Vercel, there's no persistent server — each request may cold-start.
// We initialize the SDK on first request and cache it for the lifetime
// of the serverless function instance.
// ─────────────────────────────────────────────────────────────────────
let _sdkInitPromise = null;
let _sdkInitDone = false;

async function ensureSDKInitialized() {
  if (_sdkInitDone) return;
  if (_sdkInitPromise) return _sdkInitPromise;

  _sdkInitPromise = (async () => {
    try {
      console.log('[Vercel] 🔄 Lazy-initializing Canton Wallet SDK...');
      const { getCantonSDKClient } = require('./services/canton-sdk-client');
      const sdkClient = getCantonSDKClient();
      await sdkClient.initialize();
      if (sdkClient.isReady()) {
        console.log('[Vercel] ✅ Canton Wallet SDK initialized and ready');
      } else {
        console.warn('[Vercel] ⚠️  Canton Wallet SDK initialized but not ready');
      }
    } catch (err) {
      console.error('[Vercel] ❌ Canton Wallet SDK init failed:', err.message);
    }

    // Also init Read Model (non-critical)
    try {
      await initializeReadModel();
    } catch (err) {
      console.warn('[Vercel] ⚠️  Read Model init failed:', err.message);
    }

    _sdkInitDone = true;
  })();

  return _sdkInitPromise;
}

/**
 * Create Express application
 */
function createApp() {
  const isServerless = process.env.VERCEL === '1' || !!process.env.VERCEL_ENV;
  const app = express();
  const server = http.createServer(app);

  // ── Vercel: Lazy SDK init middleware ──
  // Ensures the Canton SDK is initialized before any API request is processed.
  // On local server, this is skipped (SDK init happens in startServer).
  if (isServerless) {
    app.use(async (req, res, next) => {
      try {
        await ensureSDKInitialized();
      } catch (err) {
        // Non-fatal: continue even if SDK init fails
        console.warn('[Vercel] SDK init middleware error:', err.message);
      }
      next();
    });
  }

  // Middleware - CORS configuration
  app.use(cors({
    origin: function (origin, callback) {
      // Allow requests with no origin (like mobile apps, Postman, or server-to-server)
      if (!origin) return callback(null, true);
      
      const allowedOrigins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
        "https://clob-exchange-on-canton.vercel.app"
      ];
      
      // Normalize origin (remove trailing slash)
      const normalizedOrigin = origin.replace(/\/$/, '');
      
      // Check exact match
      if (allowedOrigins.includes(normalizedOrigin)) {
        return callback(null, true);
      }
      
      // Check if it's a Vercel preview deployment
      if (normalizedOrigin.includes('.vercel.app')) {
        return callback(null, true);
      }
      
      // Default: reject
      callback(new Error('Not allowed by CORS'));
    },
    credentials: true,
    allowedHeaders: [
      "Content-Type", 
      "Authorization", 
      "x-user-id", 
      "x-public-key", 
      "x-party-id", 
      "X-Requested-With",
      "Accept",
      "Origin"
    ],
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    optionsSuccessStatus: 204,
    preflightContinue: false,
    maxAge: 86400, // 24 hours
  }));
  app.use(express.json({ limit: '10mb' }));
  app.use(express.urlencoded({ extended: true, limit: '10mb' }));

  // Security headers (Milestone 4)
  try {
    const { securityHeadersMiddleware, auditLogMiddleware } = require('./middleware/security');
    app.use(securityHeadersMiddleware);
    app.use(auditLogMiddleware);
  } catch (err) {
    console.warn('⚠️  Security middleware not available:', err.message);
  }

  // Activity marker middleware (Milestone 4)
  const { activityMarkerMiddleware } = require('./middleware/activityMarker');
  app.use(activityMarkerMiddleware);

  // Request logging middleware (logs to console + files via winston)
  const logger = require('./utils/logger');
  app.use(logger.requestMiddleware);

  // Health check (before API routes)
  app.get('/health', (req, res) => {
    let sdkStatus = 'unknown';
    try {
      const { getCantonSDKClient } = require('./services/canton-sdk-client');
      const sdkClient = getCantonSDKClient();
      sdkStatus = sdkClient.isReady() ? 'ready' : (sdkClient.initialized ? 'initialized_not_ready' : `not_initialized: ${sdkClient.initError || 'pending'}`);
    } catch (e) {
      sdkStatus = `error: ${e.message}`;
    }

    res.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      sdkStatus,
      sdkInitDone: _sdkInitDone,
      isServerless: !!(process.env.VERCEL === '1' || process.env.VERCEL_ENV),
      config: {
        cantonConfigured: !!config.canton.jsonApiBase,
        operatorConfigured: !!config.canton.operatorPartyId,
        packageConfigured: !!config.canton.packageIds.clobExchange,
      }
    });
  });

  // API Routes
  app.use('/api', routes);

  // v1 Exchange API - Clean, stable API endpoints
  const v1ExchangeRoutes = require('./routes/v1/exchangeRoutes');
  app.use('/api/v1', v1ExchangeRoutes);

  // 404 handler - must be after all routes
  app.use((req, res) => {
    res.status(404).json({
      ok: false,
      error: {
        code: 'ROUTE_NOT_FOUND',
        message: `Route not found: ${req.method} ${req.path}`
      },
      meta: {
        path: req.path,
        method: req.method
      }
    });
  });

  // Global error handler (must be last)
  app.use(errorHandler);

  // WebSocket RE-ENABLED — streaming read model feeds real-time updates to frontend
  const wsService = initializeWebSocketService(server);
  console.log('[App] ✅ WebSocket service initialized (frontend receives real-time pushes)');

  // Milestone 4: Start stop-loss service (skip in serverless mode)
  if (!isServerless) {
    try {
      const { getStopLossService } = require('./services/stopLossService');
      const stopLossService = getStopLossService();
      stopLossService.start().catch(err => {
        console.warn('⚠️  Stop-loss service failed to start:', err.message);
      });
    } catch (err) {
      console.warn('⚠️  Stop-loss service not available:', err.message);
    }
  }

  return { app, server };
}

/**
 * Initialize the Read Model Service for real-time ledger updates
 * 
 * When the streaming read model is active, it emits events on every
 * contract create/archive. We wire these events to global.broadcastWebSocket
 * so the frontend receives instant updates.
 */
async function initializeReadModel() {
  try {
    const cantonService = require('./services/cantonService');
    const { initializeReadModelService, getReadModelService } = require('./services/readModelService');

    // Initialize the service first (this creates the instance)
    initializeReadModelService(cantonService);
    
    // Now get the initialized instance
    const readModel = getReadModelService();
    if (readModel) {
      await readModel.initialize();
      console.log('✅ Read Model initialized');
      
      // Wire streaming events → WebSocket broadcasts for real-time frontend updates
      // No polling on the frontend — ALL data flows through these WebSocket channels
      try {
        const { getStreamingReadModel } = require('./services/streamingReadModel');
        const streaming = getStreamingReadModel();
        if (streaming?.isReady()) {
          // Push order book changes to subscribed frontend clients
          streaming.on('orderCreated', (order) => {
            if (!global.broadcastWebSocket) return;
            if (order.tradingPair) {
              global.broadcastWebSocket(`orderbook:${order.tradingPair}`, {
                type: 'ORDER_CREATED',
                order,
              });
            }
            // Also push to user-specific order channel
            if (order.owner) {
              global.broadcastWebSocket(`orders:${order.owner}`, {
                type: 'ORDER_CREATED',
                order,
              });
            }
          });
          streaming.on('orderArchived', (order) => {
            if (!global.broadcastWebSocket) return;
            if (order.tradingPair) {
              global.broadcastWebSocket(`orderbook:${order.tradingPair}`, {
                type: 'ORDER_ARCHIVED',
                orderId: order.orderId,
                contractId: order.contractId,
              });
            }
            // Also push to user-specific order channel
            if (order.owner) {
              global.broadcastWebSocket(`orders:${order.owner}`, {
                type: 'ORDER_ARCHIVED',
                orderId: order.orderId,
                contractId: order.contractId,
              });
            }
          });
          // Push trade events — also push balance updates for buyer & seller
          streaming.on('tradeCreated', (trade) => {
            if (!global.broadcastWebSocket) return;
            if (trade.tradingPair) {
              global.broadcastWebSocket(`trades:${trade.tradingPair}`, {
                type: 'NEW_TRADE',
                ...trade,
              });
              global.broadcastWebSocket('trades:all', {
                type: 'NEW_TRADE',
                ...trade,
              });
            }
            // After a trade, both buyer and seller balances change
            // Fetch fresh balances via Canton SDK and push to their channels
            if (trade.buyer || trade.seller) {
              const refreshAndBroadcast = async (party) => {
                try {
                  if (!party) return;
                  const { getCantonSDKClient } = require('./services/canton-sdk-client');
                  const sdkClient = getCantonSDKClient();
                  const bal = await sdkClient.getAllBalances(party);
                  global.broadcastWebSocket(`balance:${party}`, {
                    type: 'BALANCE_UPDATE',
                    partyId: party,
                    balances: bal?.available || {},
                    lockedBalances: bal?.locked || {},
                    timestamp: Date.now(),
                  });
                } catch (_) { /* non-critical */ }
              };
              refreshAndBroadcast(trade.buyer);
              refreshAndBroadcast(trade.seller);
            }
          });
          // Push generic update events (offset changes)
          streaming.on('update', (info) => {
            if (global.broadcastWebSocket) {
              global.broadcastWebSocket('ledger:updates', {
                type: 'LEDGER_UPDATE',
                offset: info.offset,
              });
            }
          });
          // ═══ EVENT-DRIVEN MATCHING ═══
          // When Canton delivers a new OPEN order via WebSocket, the streaming
          // model emits 'orderCreated'.  We debounce per trading pair (3s) and
          // trigger the matching engine.  This is the single, canonical trigger
          // path — no setTimeout hacks, no polling.
          const { getMatchingEngine } = require('./services/matching-engine');
          const matchEngine = getMatchingEngine();
          const _matchDebounce = new Map();

          streaming.on('orderCreated', (order) => {
            if (order.status !== 'OPEN' || !order.tradingPair) return;
            const pair = order.tradingPair;
            if (_matchDebounce.has(pair)) clearTimeout(_matchDebounce.get(pair));
            _matchDebounce.set(pair, setTimeout(async () => {
              _matchDebounce.delete(pair);
              try {
                await matchEngine.triggerMatchingCycle(pair);
              } catch (err) {
                console.warn(`[EventMatch] ${pair}: ${err.message}`);
              }
            }, 3000));
          });

          console.log('✅ Streaming events wired to WebSocket broadcasts + event-driven matching');
        }
      } catch (_) {
        // Streaming not available — WebSocket still works but no auto-push
      }
    }
    return readModel;
  } catch (error) {
    console.error('⚠️  Read Model initialization failed:', error.message);
    console.error('   The exchange will work but order books may not update in real-time.');
    return null;
  }
}

// Canton Update Stream removed — all data comes from WebSocket streaming read model

/**
 * Start server
 */
async function startServer() {
  const { app, server } = createApp();
  const PORT = config.server.port;

  // Listen on all interfaces
  server.listen(PORT, '0.0.0.0', async () => {
    console.log('');
    console.log('╔═══════════════════════════════════════════════════════════════╗');
    console.log('║           CLOB Exchange Backend Server                         ║');
    console.log('║           Canton/DAML Powered - No Fallbacks                   ║');
    console.log('╚═══════════════════════════════════════════════════════════════╝');
    console.log('');
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`✅ Real-time updates via WebSocket streaming (polling as fallback)`);
    console.log(`✅ Environment: ${config.server.env}`);
    console.log('');

    // Initialize Canton Wallet SDK (non-blocking)
    console.log('🔄 Initializing Canton Wallet SDK...');
    try {
      const { getCantonSDKClient } = require('./services/canton-sdk-client');
      const sdkClient = getCantonSDKClient();
      await sdkClient.initialize();
      if (sdkClient.isReady()) {
        console.log('✅ Canton Wallet SDK initialized and ready');
      } else {
        console.warn('⚠️  Canton Wallet SDK initialized but not ready (check SDK package installation)');
      }
    } catch (sdkErr) {
      console.error('⚠️  Canton Wallet SDK initialization failed:', sdkErr.message);
      console.error('   Balance queries and token transfers will be unavailable.');
    }

    // Initialize Read Model (non-blocking)
    console.log('🔄 Initializing Read Model from Canton ledger...');
    await initializeReadModel();


    // Start matching engine if enabled
    if (config.matchingEngine.enabled) {
      console.log('');
      console.log('🤖 Starting Matching Engine...');
      const { getMatchingEngine } = require('./services/matching-engine');
      const matchingEngine = getMatchingEngine();

      try {
        await matchingEngine.start();
        console.log(`✅ Matching Engine started (interval: ${matchingEngine.pollingInterval}ms)`);
      } catch (error) {
        console.error('⚠️  Failed to start Matching Engine:', error.message);
      }
    } else {
      console.log('');
      console.log('⚠️  Matching Engine disabled (set MATCHING_ENGINE_ENABLED=true to enable)');
    }

    // Milestone 4: Start stop-loss service
    console.log('');
    console.log('🛡️  Starting Stop-Loss Service...');
    try {
      const { getStopLossService } = require('./services/stopLossService');
      const stopLossService = getStopLossService();
      await stopLossService.start();
      console.log('✅ Stop-Loss Service started');
    } catch (error) {
      console.warn('⚠️  Stop-loss service not available:', error.message);
    }

    // ACS Cleanup Service — archives completed contracts to keep ACS lean
    console.log('');
    console.log('🧹 Starting ACS Cleanup Service...');
    try {
      const { getACSCleanupService } = require('./services/acsCleanupService');
      const cleanupService = getACSCleanupService();
      await cleanupService.start();
      console.log('✅ ACS Cleanup Service started (archives FILLED/CANCELLED orders, old trades)');
    } catch (error) {
      console.warn('⚠️  ACS Cleanup service not available:', error.message);
    }

    // Auto-Accept Incoming Transfers Service
    // Like Binance/Coinbase: incoming token transfers are automatically accepted
    console.log('');
    console.log('📨 Starting Auto-Accept Incoming Transfers Service...');
    try {
      const { getAutoAcceptService } = require('./services/autoAcceptService');
      const autoAcceptService = getAutoAcceptService();
      await autoAcceptService.start();
      console.log('✅ Auto-Accept Service started (incoming transfers auto-accepted)');
    } catch (error) {
      console.warn('⚠️  Auto-Accept service not available:', error.message);
    }

    console.log('');
    console.log('📋 Exchange API Endpoints:');
    console.log('  POST /api/v1/orders        - Place order');
    console.log('  DELETE /api/v1/orders/:id  - Cancel order');
    console.log('  GET /api/v1/orderbooks/:p  - Get order book');
    console.log('  GET /api/v1/orders         - Get user orders');
    console.log('  GET /api/v1/trades         - Get recent trades');
    console.log('  GET /api/v1/balances       - Get user balances');
    console.log('');
    console.log('🚀 Server is ready to accept connections');
    console.log('');
  });

  // Error handler for listen
  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.error(`❌ Port ${PORT} is already in use!`);
    } else {
      console.error('❌ Server error:', err);
    }
    process.exit(1);
  });

  return { app, server };
}

module.exports = { createApp, startServer };
