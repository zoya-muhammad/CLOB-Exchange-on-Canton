/**
 * Matching Engine Bot — Allocation-Based Settlement
 * 
 * Core exchange functionality — matches buy/sell orders and settles trades
 * using the Allocation API (replaces TransferInstruction 2-step flow).
 * 
 * Settlement Flow:
 * 1. Poll Canton for OPEN Order contracts every N seconds
 * 2. Separate into buys/sells per trading pair
 * 3. Sort by price-time priority (FIFO)
 * 4. Find crossing orders (buy price >= sell price)
 * 5. For each match:
 *    a. FillOrder on both Canton contracts (prevents re-matching loop)
 *    b. Execute buyer's Allocation (exchange acts as executor — NO buyer key needed)
 *    c. Execute seller's Allocation (exchange acts as executor — NO seller key needed)
 *    d. Create Trade record on Canton for history
 *    e. Trigger stop-loss checks at the new trade price
 *    f. Broadcast via WebSocket
 * 
 * Settlement uses Allocation API:
 * - Allocations are created at ORDER PLACEMENT time (funds locked)
 * - Exchange is set as executor in every Allocation
 * - At match time, exchange calls Allocation_Execute with ITS OWN KEY
 * - No user keys needed at settlement → works for external parties
 * 
 * Why Allocations instead of TransferInstruction:
 * - TransferInstruction requires sender's private key at SETTLEMENT time
 * - With external parties, backend has no user keys → TransferInstruction breaks
 * - Allocation: User signs ONCE at order time, exchange settles as executor
 * 
 * @see https://docs.sync.global/app_dev/api/splice-api-token-allocation-v1/
 * @see https://docs.digitalasset.com/integrate/devnet/token-standard/index.html
 */

const Decimal = require('decimal.js');
const cantonService = require('./cantonService');
const config = require('../config');
const tokenProvider = require('./tokenProvider');
const { getCantonSDKClient } = require('./canton-sdk-client');
const { getTokenSystemType } = require('../config/canton-sdk.config');

// Configure decimal.js for financial precision
Decimal.set({ precision: 20, rounding: Decimal.ROUND_DOWN });

class MatchingEngine {
  constructor() {
    this.isRunning = false;
    this.basePollingInterval = parseInt(config.matchingEngine?.intervalMs) || 2000;
    this.pollingInterval = this.basePollingInterval;
    this.matchingInProgress = false;
    this.matchingStartTime = 0;
    this.adminToken = null;
    this.tokenExpiry = null;
    this.tradingPairs = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'CC/CBTC'];

    // ═══ Log throttling ═══
    this._lastLogState = {};
    this._cyclesSinceLastLog = 0;
    this._LOG_THROTTLE_CYCLES = 30;

    // ═══ CRITICAL: Recently matched orders guard ═══
    this.recentlyMatchedOrders = new Map();
    this.RECENTLY_MATCHED_TTL = 30000; // 30 seconds cooldown

    // ═══ Pending pairs queue ═══
    this.pendingPairs = new Set();
    this.invalidSettlementContracts = new Map();
    this.INVALID_SETTLEMENT_TTL = 7 * 24 * 60 * 60 * 1000; // permanent for practical purposes

    // ═══ Adaptive polling ═══
    this._consecutiveIdleCycles = 0;
    this._IDLE_THRESHOLD_MEDIUM = 5;
    this._IDLE_THRESHOLD_SLOW = 20;
    this._MEDIUM_INTERVAL = 10000;
    this._SLOW_INTERVAL = 30000;
    this._lastMatchTime = 0;

    // ═══ CIRCUIT BREAKER — prevents flooding participant node ═══
    // If we hit too many consecutive settlement failures, PAUSE the engine.
    // This protects the Canton participant from CONTRACT_NOT_FOUND spam.
    this._consecutiveSettlementFailures = 0;
    this._CIRCUIT_BREAKER_THRESHOLD = 5;  // After 5 consecutive failures → pause
    this._CIRCUIT_BREAKER_PAUSE_MS = 120000; // 2 minutes pause
    this._circuitBreakerUntil = 0;
    this._totalFailuresSinceStart = 0;

    // ═══ Global CONTRACT_NOT_FOUND blacklist ═══
    // Track allocation CIDs that returned CONTRACT_NOT_FOUND — never retry them.
    this._archivedAllocationCids = new Set();
  }

  async getAdminToken() {
    if (this.adminToken && this.tokenExpiry && Date.now() < this.tokenExpiry) {
      return this.adminToken;
    }
    console.log('[MatchingEngine] Refreshing admin token...');
    this.adminToken = await tokenProvider.getServiceToken();
    this.tokenExpiry = Date.now() + (25 * 60 * 1000);
    return this.adminToken;
  }

  invalidateToken() {
    this.adminToken = null;
    this.tokenExpiry = null;
  }

  async start() {
    if (this.isRunning) {
      console.log('[MatchingEngine] Already running');
      return;
    }
    this.isRunning = true;
    console.log(`[MatchingEngine] Started (interval: ${this.pollingInterval}ms, pairs: ${this.tradingPairs.join(', ')}) [Allocation-based settlement]`);
    this.matchLoop();
  }

  stop() {
    console.log('[MatchingEngine] Stopping...');
    this.isRunning = false;
  }

  async matchLoop() {
    while (this.isRunning) {
      try {
        await this.runMatchingCycle();
      } catch (error) {
        console.error('[MatchingEngine] Cycle error:', error.message);
        if (error.message?.includes('401') || error.message?.includes('security-sensitive')) {
          this.invalidateToken();
      }
    }

      // Process any pairs queued by order placement triggers
      if (this.pendingPairs.size > 0) {
        this._resetToFastPolling();
        const pending = [...this.pendingPairs];
        this.pendingPairs.clear();
        console.log(`[MatchingEngine] ⚡ Processing ${pending.length} queued pair(s): ${pending.join(', ')}`);
        try {
          this.matchingInProgress = true;
          this.matchingStartTime = Date.now();
          const token = await this.getAdminToken();
          for (const pair of pending) {
            try {
              await this.processOrdersForPair(pair, token);
            } catch (pairErr) {
              if (!pairErr.message?.includes('No contracts found')) {
                console.error(`[MatchingEngine] Queued pair ${pair} error:`, pairErr.message);
              }
            }
          }
        } catch (queueErr) {
          console.error('[MatchingEngine] Queued pairs error:', queueErr.message);
        } finally {
          this.matchingInProgress = false;
        }
      }

      await new Promise(r => setTimeout(r, this.pollingInterval));
    }
    console.log('[MatchingEngine] Stopped');
  }

  _onMatchExecuted() {
    this._consecutiveIdleCycles = 0;
    this._lastMatchTime = Date.now();
    if (this.pollingInterval !== this.basePollingInterval) {
      console.log(`[MatchingEngine] ⚡ Match found — resetting polling to ${this.basePollingInterval}ms`);
      this.pollingInterval = this.basePollingInterval;
    }
  }

  _onIdleCycle() {
    this._consecutiveIdleCycles++;
    const oldInterval = this.pollingInterval;

    if (this._consecutiveIdleCycles >= this._IDLE_THRESHOLD_SLOW) {
      this.pollingInterval = this._SLOW_INTERVAL;
    } else if (this._consecutiveIdleCycles >= this._IDLE_THRESHOLD_MEDIUM) {
      this.pollingInterval = this._MEDIUM_INTERVAL;
    }

    if (this.pollingInterval !== oldInterval) {
      console.log(`[MatchingEngine] 😴 No matches for ${this._consecutiveIdleCycles} cycles — polling interval now ${this.pollingInterval}ms`);
    }
  }

  _resetToFastPolling() {
    this._consecutiveIdleCycles = 0;
    if (this.pollingInterval !== this.basePollingInterval) {
      console.log(`[MatchingEngine] ⚡ New order detected — resetting polling to ${this.basePollingInterval}ms`);
      this.pollingInterval = this.basePollingInterval;
    }
  }

  _markInvalidSettlementOrder(order, reason) {
    if (!order?.contractId) return;
    this.invalidSettlementContracts.set(order.contractId, { at: Date.now(), reason: String(reason || 'unknown') });
    console.warn(`[MatchingEngine] 🚫 Quarantined order ${order.orderId || order.contractId.substring(0, 24)} for settlement: ${reason}`);

    // Evict from streaming read model so it never appears again this session
    const streaming = this._getStreamingModel();
    if (streaming) streaming.evictOrder(order.contractId);
  }

  async _tryCancelStaleOrder(order, token) {
    // ═══ DISABLED: Do NOT submit cancel commands to participant ═══
    // Huz reported that our backend is flooding the participant node with
    // CONTRACT_NOT_FOUND errors causing congestion. Cancel attempts on
    // stale orders just generate MORE failed commands.
    // Instead, just evict from local read model silently.
    if (!order?.contractId) return;
    const orderId = order.orderId || order.contractId.substring(0, 24);
    console.warn(`[MatchingEngine] 🚫 Skipping CancelOrder for ${orderId} — evicting from read model only (protecting participant from spam)`);
    const streaming = this._getStreamingModel();
    if (streaming) streaming.evictOrder(order.contractId);
    if (order.allocationContractId) this._archivedAllocationCids.add(order.allocationContractId);
    return;

    /* ORIGINAL CODE - DISABLED TO PREVENT PARTICIPANT SPAM
    const packageId = config.canton.packageIds?.clobExchange;
    const operatorPartyId = config.canton.operatorPartyId;
    if (!packageId || !operatorPartyId) return;

    const templateId = order.templateId || `${packageId}:Order:Order`;

    // CancelOrder usually requires both operator + owner authorizers.
    // Try that first, then fall back to operator-only for edge cases.
    const strategyActAs = [];
    const fullAuth = [operatorPartyId, order.owner].filter(Boolean);
    if (fullAuth.length > 0) strategyActAs.push(fullAuth);
    strategyActAs.push([operatorPartyId]);

    try {
      let cancelled = false;
      const synchronizerId = await cantonService.resolveSubmissionSynchronizerId(
        token,
        config.canton.synchronizerId
      );

      for (const actAsParties of strategyActAs) {
        try {
          await cantonService.exerciseChoice({
            token,
            actAsParty: actAsParties,
            templateId,
            contractId: order.contractId,
            choice: 'CancelOrder',
            choiceArgument: {},
            readAs: [operatorPartyId, order.owner].filter(Boolean),
            synchronizerId,
          });
          cancelled = true;
          break;
        } catch (attemptErr) {
          const msg = String(attemptErr?.message || attemptErr || '');
          if (msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found') ||
              msg.includes('Contract could not be found')) {
            throw attemptErr;
          }
          const isAuthorizerError =
            msg.includes('DAML_AUTHORIZATION_ERROR') || msg.includes('requires authorizers');
          const isNoSynchronizerError =
            msg.includes('NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT') ||
            msg.includes('Not connected to a synchronizer') ||
            msg.includes('cannot submit as the given submitter on any connected synchronizer');
          if (isAuthorizerError || isNoSynchronizerError) {
            continue;
          }
          throw attemptErr;
        }
      }

      if (!cancelled) {
        throw new Error('CancelOrder attempts exhausted');
      }
      console.warn(`[MatchingEngine] 🧹 Auto-cancelled stale order ${orderId}`);
    } catch (cancelErr) {
      const msg = cancelErr.message || '';
      if (msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found') || msg.includes('already')) {
        const streaming = this._getStreamingModel();
        if (streaming) streaming.evictOrder(order.contractId);
        return;
      }
      // Any other error: don't retry — the order is already quarantined/evicted
      if (!msg.includes('NO_SYNCHRONIZER') && !msg.includes('Not connected')) {
        console.warn(`[MatchingEngine] ⚠️ CancelOrder failed for ${orderId}: ${msg.substring(0, 120)}`);
      }
    }
    END OF DISABLED CODE */
  }

  async runMatchingCycle() {
    // ═══ CIRCUIT BREAKER — protect participant from spam ═══
    if (Date.now() < this._circuitBreakerUntil) {
      // Silently skip — don't even log (to avoid log spam too)
      return;
    }

    if (this.matchingInProgress) {
      if (Date.now() - this.matchingStartTime > 25000) {
        console.warn('[MatchingEngine] ⚠️ matchingInProgress stuck for >25s — force-resetting');
        this.matchingInProgress = false;
      } else {
        return;
      }
    }

    try {
      this.matchingInProgress = true;
      this.matchingStartTime = Date.now();
      const token = await this.getAdminToken();

      // ── PRE-FLIGHT: Verify synchronizer connectivity BEFORE doing anything ──
      // If the participant is disconnected, skip the ENTIRE cycle to avoid
      // flooding the Canton participant with commands that will all fail.
      // This is CRITICAL — Huz reported the participant logs are filled with
      // submit commands from our partyId, which may be causing instability.
      try {
        const syncResult = await cantonService.resolveSubmissionSynchronizerId(
          token, config.canton.synchronizerId
        );
        if (!syncResult) {
          // Synchronizer is down — increase polling interval to reduce load
          this._consecutiveSyncFailures = (this._consecutiveSyncFailures || 0) + 1;
          const backoffMs = Math.min(60000, 5000 * this._consecutiveSyncFailures);
          if (this._consecutiveSyncFailures <= 3 || this._consecutiveSyncFailures % 10 === 0) {
            console.warn(`[MatchingEngine] ⏳ Synchronizer disconnected (${this._consecutiveSyncFailures}x) — skipping entire cycle, next attempt in ${backoffMs / 1000}s`);
          }
          this.pollingInterval = backoffMs;
          return;
        }
        // Synchronizer is back — reset failure counter and polling
        if (this._consecutiveSyncFailures > 0) {
          console.log(`[MatchingEngine] ✅ Synchronizer reconnected after ${this._consecutiveSyncFailures} failures — resuming normal operation`);
          this._consecutiveSyncFailures = 0;
          this.pollingInterval = this.basePollingInterval;
        }
      } catch (syncErr) {
        this._consecutiveSyncFailures = (this._consecutiveSyncFailures || 0) + 1;
        const backoffMs = Math.min(60000, 5000 * this._consecutiveSyncFailures);
        if (this._consecutiveSyncFailures <= 3 || this._consecutiveSyncFailures % 10 === 0) {
          console.warn(`[MatchingEngine] ⏳ Synchronizer health check failed (${this._consecutiveSyncFailures}x): ${syncErr.message?.substring(0, 80)} — skipping cycle`);
        }
        this.pollingInterval = backoffMs;
        return;
      }

      let matchFoundThisCycle = false;

      for (const pair of this.tradingPairs) {
        const hadMatch = await this.processOrdersForPair(pair, token);
        if (hadMatch) matchFoundThisCycle = true;
      }

      if (matchFoundThisCycle) {
        this._onMatchExecuted();
      } else {
        this._onIdleCycle();
      }
    } catch (error) {
      if (error.message?.includes('401') || error.message?.includes('security-sensitive')) {
        this.invalidateToken();
      }
      throw error;
    } finally {
      this.matchingInProgress = false;
    }
  }
  
  /**
   * Get the streaming read model (lazy init)
   */
  _getStreamingModel() {
    if (!this._streamingModel) {
      try {
        const { getStreamingReadModel } = require('./streamingReadModel');
        this._streamingModel = getStreamingReadModel();
      } catch (_) { /* streaming not available */ }
    }
    return this._streamingModel?.isReady() ? this._streamingModel : null;
  }

  /**
   * @returns {boolean} true if a match was found and executed
   */
  async processOrdersForPair(tradingPair, token) {
      const packageId = config.canton.packageIds?.clobExchange;
      const operatorPartyId = config.canton.operatorPartyId;
      
    if (!packageId || !operatorPartyId) return false;

    try {
      // WebSocket streaming read model is the only source of order data.
      const streaming = this._getStreamingModel();
      let rawOrders = null;
      
      if (streaming) {
        // Instant lookup from WebSocket-synced read model.
        rawOrders = streaming.getOpenOrdersForPair(tradingPair);
      }

      // No fallback to REST/patched registries by client requirement.
      // If streaming is unavailable or not bootstrapped yet, skip this cycle.
      if (!streaming || !streaming.isReady()) return false;
      
      if (!rawOrders || rawOrders.length === 0) return false;
      
      const buyOrders = [];
      const sellOrders = [];
      
      const now = Date.now();
      const MAX_UTILITY_ALLOCATION_AGE_MS = 24 * 60 * 60 * 1000;
      const MAX_SPLICE_ALLOCATION_AGE_MS = 15 * 60 * 1000;

      for (const payload of rawOrders) {
        if (payload.status !== 'OPEN') continue;
        if (this.invalidSettlementContracts.has(payload.contractId)) continue;

        // Pre-filter: evict orders whose allocations are guaranteed to be expired.
        // BUY locks quote asset; SELL locks base asset.
        const [baseAsset, quoteAsset] = String(payload.tradingPair || tradingPair || '').split('/');
        const side = String(payload.orderType || '').toUpperCase();
        const lockedAsset = side === 'BUY' ? quoteAsset : baseAsset;
        const lockedAssetType = lockedAsset ? getTokenSystemType(lockedAsset) : null;
        const maxAllocationAgeMs = lockedAssetType === 'splice'
          ? MAX_SPLICE_ALLOCATION_AGE_MS
          : MAX_UTILITY_ALLOCATION_AGE_MS;

        const orderAge = payload.timestamp ? (now - new Date(payload.timestamp).getTime()) : Infinity;
        if (orderAge > maxAllocationAgeMs) {
          this._markInvalidSettlementOrder(
            { contractId: payload.contractId, orderId: payload.orderId, owner: payload.owner },
            `Allocation expired for ${lockedAsset || 'unknown'} (${Math.round(orderAge / 60000)}m)`
          );
          continue;
        }

        const rawPrice = payload.price;
        let parsedPrice = null;
        if (rawPrice !== null && rawPrice !== undefined && rawPrice !== '') {
          if (typeof rawPrice === 'object' && rawPrice.Some !== undefined) {
            parsedPrice = parseFloat(rawPrice.Some);
          } else {
            parsedPrice = parseFloat(rawPrice);
          }
          if (isNaN(parsedPrice)) parsedPrice = null;
        }

        const qty = parseFloat(payload.quantity) || 0;
        const filled = parseFloat(payload.filled) || 0;
        const remaining = new Decimal(qty).minus(new Decimal(filled));

        if (remaining.lte(0)) continue;

        const contractTemplateId = payload.templateId || `${packageId}:Order:Order`;
        const isNewPackage = contractTemplateId.startsWith(packageId);

        // Skip orders from old/incompatible packages.
        if (!isNewPackage) {
          continue;
        }

        // Extract allocationCid (Token Standard allocation contract ID)
        const rawAllocationCid = payload.allocationCid || '';
        let allocationCid = (rawAllocationCid && rawAllocationCid !== 'FILL_ONLY' && rawAllocationCid !== 'NONE' && rawAllocationCid.length >= 10)
          ? rawAllocationCid
          : null;
        if (!allocationCid && payload.orderId) {
          try {
            const { getAllocationContractIdForOrder } = require('./order-service');
            allocationCid = await getAllocationContractIdForOrder(payload.orderId);
          } catch (_) { /* best effort */ }
        }
        if (!allocationCid) continue;

        let allocationType = 'SpliceAllocation';
        if (payload.orderId) {
          try {
            const { getAllocationTypeForOrder } = require('./order-service');
            allocationType = await getAllocationTypeForOrder(payload.orderId);
          } catch (_) { /* default */ }
        }
        
        const order = {
          contractId: payload.contractId,
          orderId: payload.orderId,
          owner: payload.owner,
          orderType: payload.orderType,
          orderMode: payload.orderMode || 'LIMIT',
          price: parsedPrice,
          quantity: qty,
          filled: filled,
          remaining: remaining.toNumber(),
          remainingDecimal: remaining,
          timestamp: payload.timestamp,
          tradingPair: payload.tradingPair,
          allocationContractId: allocationCid,
          allocationType,
          templateId: contractTemplateId,
          isNewPackage: isNewPackage,
        };
        
        if (payload.orderType === 'BUY') {
          buyOrders.push(order);
        } else if (payload.orderType === 'SELL') {
          sellOrders.push(order);
        }
      }
      
      if (buyOrders.length === 0 || sellOrders.length === 0) return false;

      // Throttled logging
      const stateKey = `${tradingPair}:${buyOrders.length}b:${sellOrders.length}s`;
      this._cyclesSinceLastLog++;
      const shouldLogStatus = (this._lastLogState[tradingPair] !== stateKey) || (this._cyclesSinceLastLog >= this._LOG_THROTTLE_CYCLES);
      if (shouldLogStatus) {
        console.log(`[MatchingEngine] ${tradingPair}: ${buyOrders.length} buys, ${sellOrders.length} sells`);
        this._lastLogState[tradingPair] = stateKey;
        this._cyclesSinceLastLog = 0;
      }

      // Sort: buys by highest price first (MARKET first), sells by lowest price first (MARKET first)
      buyOrders.sort((a, b) => {
        if (a.price === null && b.price !== null) return -1;
        if (a.price !== null && b.price === null) return 1;
        if (a.price === null && b.price === null) return new Date(a.timestamp) - new Date(b.timestamp);
        if (b.price !== a.price) return b.price - a.price;
        return new Date(a.timestamp) - new Date(b.timestamp);
      });

      sellOrders.sort((a, b) => {
        if (a.price === null && b.price !== null) return -1;
        if (a.price !== null && b.price === null) return 1;
        if (a.price === null && b.price === null) return new Date(a.timestamp) - new Date(b.timestamp);
        if (a.price !== b.price) return a.price - b.price;
        return new Date(a.timestamp) - new Date(b.timestamp);
      });

      const matched = await this.findAndExecuteOneMatch(tradingPair, buyOrders, sellOrders, token);
      return matched;
      
          } catch (error) {
      if (error.message?.includes('401') || error.message?.includes('security-sensitive')) {
        this.invalidateToken();
          }
      if (!error.message?.includes('No contracts found')) {
        console.error(`[MatchingEngine] Error for ${tradingPair}:`, error.message);
        }
      return false;
    }
  }
  
  /**
   * Find ONE crossing match and execute it.
   * Only one per cycle because contract IDs change after exercise.
   * 
   * Settlement uses Allocation API:
   * - Execute buyer's Allocation (exchange is executor — sends quote to seller)
   * - Execute seller's Allocation (exchange is executor — sends base to buyer)
   * - Both legs settled atomically by the exchange's own key
   */
  async findAndExecuteOneMatch(tradingPair, buyOrders, sellOrders, token) {
    const now = Date.now();

    // Clear expired entries from recentlyMatchedOrders
    for (const [key, ts] of this.recentlyMatchedOrders) {
      if (now - ts > this.RECENTLY_MATCHED_TTL) {
        this.recentlyMatchedOrders.delete(key);
      }
    }
    
    for (const buyOrder of buyOrders) {
      for (const sellOrder of sellOrders) {
        if (buyOrder.remaining <= 0 || sellOrder.remaining <= 0) continue;

        // Self-trade: allowed per client requirement (Mohak: "market itself should take care of it")
        // No blocking — same-owner orders can match normally

        // Skip recently matched order pairs
        const matchKey = `${buyOrder.contractId}::${sellOrder.contractId}`;
        if (this.recentlyMatchedOrders.has(matchKey)) {
          continue;
        }
        
        // Check if orders cross
        const buyPrice = buyOrder.price;
        const sellPrice = sellOrder.price;
        let canMatch = false;
        let matchPrice = 0;

        if (buyPrice !== null && sellPrice !== null) {
          if (buyPrice >= sellPrice) {
            canMatch = true;
            matchPrice = sellPrice; // Maker-taker: use sell (maker) price
          }
        } else if (buyPrice === null && sellPrice !== null) {
          canMatch = true;
          matchPrice = sellPrice;
        } else if (buyPrice !== null && sellPrice === null) {
          canMatch = true;
          matchPrice = buyPrice;
        }

        if (!canMatch || matchPrice <= 0) continue;

        const matchQty = Decimal.min(buyOrder.remainingDecimal, sellOrder.remainingDecimal);
        const matchQtyStr = matchQty.toFixed(10);
        const matchQtyNum = matchQty.toNumber();

        console.log(`[MatchingEngine] ✅ MATCH FOUND: BUY ${buyPrice !== null ? buyPrice : 'MARKET'} x ${buyOrder.remaining} ↔ SELL ${sellPrice !== null ? sellPrice : 'MARKET'} x ${sellOrder.remaining}`);
        console.log(`[MatchingEngine]    Fill: ${matchQtyStr} @ ${matchPrice} | Settlement: Allocation API (exchange as executor)`);

        this.recentlyMatchedOrders.set(matchKey, Date.now());

        // ═══ Pre-flight: Skip if either order's allocation is already known-archived ═══
        if (buyOrder.allocationContractId && this._archivedAllocationCids.has(buyOrder.allocationContractId)) {
          this._markInvalidSettlementOrder(buyOrder, 'Allocation archived (blacklisted)');
          continue;
        }
        if (sellOrder.allocationContractId && this._archivedAllocationCids.has(sellOrder.allocationContractId)) {
          this._markInvalidSettlementOrder(sellOrder, 'Allocation archived (blacklisted)');
          continue;
        }

        try {
          await this.executeMatch(tradingPair, buyOrder, sellOrder, matchQty, matchPrice, token);
          console.log(`[MatchingEngine] ✅ Match executed successfully via Allocation API`);
          // Reset circuit breaker on success
          this._consecutiveSettlementFailures = 0;
          return true;
        } catch (error) {
          this._consecutiveSettlementFailures++;
          this._totalFailuresSinceStart++;
          console.error(`[MatchingEngine] ❌ Match execution failed (${this._consecutiveSettlementFailures}/${this._CIRCUIT_BREAKER_THRESHOLD}):`, error.message?.substring(0, 200));

          // ═══ CIRCUIT BREAKER: Too many consecutive failures → pause engine ═══
          if (this._consecutiveSettlementFailures >= this._CIRCUIT_BREAKER_THRESHOLD) {
            this._circuitBreakerUntil = Date.now() + this._CIRCUIT_BREAKER_PAUSE_MS;
            console.error(`[MatchingEngine] 🛑 CIRCUIT BREAKER TRIPPED — ${this._consecutiveSettlementFailures} consecutive failures. Pausing for ${this._CIRCUIT_BREAKER_PAUSE_MS / 1000}s to protect participant node.`);
            return false;
          }

          // If auth/token expired, refresh immediately and allow instant retry
          // instead of waiting for the 30s recentlyMatched cooldown.
          if (error.message?.includes('401') || error.message?.includes('security-sensitive')) {
            this.invalidateToken();
            this.recentlyMatchedOrders.delete(matchKey);
            console.warn('[MatchingEngine] 🔄 Invalidated admin token after auth failure; retrying on next cycle');
          }
          
          // ═══ IMPORTANT: Check SPECIFIC allocation failures FIRST ═══
          // Errors like BUYER_ALLOCATION_FAILED: STALE_ALLOCATION_LOCK_MISSING: CONTRACT_NOT_FOUND
          // contain "CONTRACT_NOT_FOUND" in the message but should ONLY quarantine
          // the affected order (buyer or seller), NOT both.
          // The general CONTRACT_NOT_FOUND handler below quarantines BOTH orders,
          // so these specific checks MUST come first.
          const isBuyerAllocFailed = error.message?.includes('BUYER_ALLOCATION_FAILED');
          const isSellerAllocFailed = error.message?.includes('SELLER_ALLOCATION_FAILED');
          const isSpecificAllocFailure = isBuyerAllocFailed || isSellerAllocFailed;
          const isStaleOrPermanent = error.message?.includes('STALE_ALLOCATION_LOCK_MISSING') ||
              error.message?.includes('STALE_ALLOCATION_EXPIRED') ||
              error.message?.includes('CANTON_EXTERNAL_PARTY_LIMITATION');

          if (isSpecificAllocFailure || isStaleOrPermanent) {
            if (isSellerAllocFailed) {
              console.warn(`[MatchingEngine] 🚫 Seller allocation failed — quarantining SELL order only`);
              this._markInvalidSettlementOrder(sellOrder, error.message?.substring(0, 120));
              if (sellOrder.allocationContractId) this._archivedAllocationCids.add(sellOrder.allocationContractId);
            } else if (isBuyerAllocFailed) {
              console.warn(`[MatchingEngine] 🚫 Buyer allocation failed — quarantining BUY order only`);
              this._markInvalidSettlementOrder(buyOrder, error.message?.substring(0, 120));
              if (buyOrder.allocationContractId) this._archivedAllocationCids.add(buyOrder.allocationContractId);
            } else {
              // Generic STALE error without BUYER/SELLER prefix — quarantine both
              console.warn(`[MatchingEngine] 🚫 Stale allocation — quarantining both orders`);
              this._markInvalidSettlementOrder(buyOrder, error.message?.substring(0, 120));
              this._markInvalidSettlementOrder(sellOrder, error.message?.substring(0, 120));
              if (buyOrder.allocationContractId) this._archivedAllocationCids.add(buyOrder.allocationContractId);
              if (sellOrder.allocationContractId) this._archivedAllocationCids.add(sellOrder.allocationContractId);
            }
            this.recentlyMatchedOrders.delete(matchKey);
            continue;
          }

          // ═══ General CONTRACT_NOT_FOUND / already filled ═══
          // This catches FillOrder failures and other contract-not-found errors
          // that are NOT wrapped in BUYER/SELLER_ALLOCATION_FAILED.
          if (error.message?.includes('already filled') || 
              error.message?.includes('could not be found') ||
              error.message?.includes('CONTRACT_NOT_FOUND')) {
            // Contract is archived on the ledger — evict from read model permanently
            // AND blacklist the allocation CID to prevent future commands
            const streaming = this._getStreamingModel();
            if (streaming) {
              streaming.evictOrder(buyOrder.contractId);
              streaming.evictOrder(sellOrder.contractId);
            }
            // Blacklist allocation CIDs permanently
            if (buyOrder.allocationContractId) this._archivedAllocationCids.add(buyOrder.allocationContractId);
            if (sellOrder.allocationContractId) this._archivedAllocationCids.add(sellOrder.allocationContractId);
            this._markInvalidSettlementOrder(buyOrder, 'CONTRACT_NOT_FOUND — allocation archived');
            this._markInvalidSettlementOrder(sellOrder, 'CONTRACT_NOT_FOUND — allocation archived');
            this.recentlyMatchedOrders.delete(matchKey);
            if (error.message?.includes('Buy FillOrder failed')) {
              break;
            }
            continue;
          }

          // ═══ Transient synchronizer errors ═══
          // Quarantine the SPECIFIC order whose allocation failed (if identifiable),
          // otherwise quarantine both to stop spam.
          const isTransient = error.message?.includes('TRANSIENT_SYNCHRONIZER_ERROR') ||
              error.message?.includes('NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT') ||
              error.message?.includes('Not connected to a synchronizer') ||
              error.message?.includes('cannot submit as the given submitter on any connected synchronizer');
          if (isTransient) {
            console.warn(`[MatchingEngine] ⏳ Transient synchronizer error — quarantining affected order(s)`);
            this.recentlyMatchedOrders.delete(matchKey);
            // Quarantine BOTH orders' allocations to prevent immediate re-submission
            this._markInvalidSettlementOrder(buyOrder, 'Synchronizer error — quarantined');
            this._markInvalidSettlementOrder(sellOrder, 'Synchronizer error — quarantined');
            continue;
          }

          // Signing key missing — the external party hasn't stored their key yet.
          // Don't quarantine — the key may arrive when they re-login or place an order.
          if (error.message?.includes('SIGNING_KEY_MISSING')) {
            console.warn(`[MatchingEngine] ⚠️ Signing key not available for external party — skipping match (will retry when key is stored)`);
            continue;
          }

          // FAILED_TO_EXECUTE_TRANSACTION — Canton limitation with external parties.
          // This is a PERMANENT protocol error (not transient). Quarantine both orders
          // and DO NOT retry — it will never succeed with current architecture.
          if (error.message?.includes('FAILED_TO_EXECUTE_TRANSACTION') ||
              error.message?.includes('did not provide an external signature')) {
            console.error(`[MatchingEngine] 🚫 Canton external party protocol limitation — quarantining orders (requires Propose-Accept redesign)`);
            this._markInvalidSettlementOrder(buyOrder, 'Canton external party limitation');
            this._markInvalidSettlementOrder(sellOrder, 'Canton external party limitation');
            this.recentlyMatchedOrders.delete(matchKey);
            continue;
          }

          // DAML_AUTHORIZATION_ERROR — permanent, quarantine
          if (error.message?.includes('DAML_AUTHORIZATION_ERROR') || error.message?.includes('requires authorizers')) {
            console.error(`[MatchingEngine] 🚫 DAML authorization error — quarantining orders`);
            this._markInvalidSettlementOrder(buyOrder, 'DAML authorization error');
            this._markInvalidSettlementOrder(sellOrder, 'DAML authorization error');
            this.recentlyMatchedOrders.delete(matchKey);
            continue;
          }
          
          return false;
        }
      }
    }

    // Diagnostic: Log why no match was found (throttled)
    const bestBuy = buyOrders.find(o => o.remaining > 0 && o.price !== null);
    const bestSell = sellOrders.find(o => o.remaining > 0 && o.price !== null);
    if (bestBuy && bestSell) {
      const spread = bestSell.price - bestBuy.price;
      if (spread <= 0) {
        const skippedByCache = [];
        for (const b of buyOrders) {
          for (const s of sellOrders) {
            if (b.remaining > 0 && s.remaining > 0 && b.price !== null && s.price !== null && b.price >= s.price) {
              const key = `${b.contractId}::${s.contractId}`;
              if (this.recentlyMatchedOrders.has(key)) skippedByCache.push(`B@${b.price}↔S@${s.price}`);
            }
          }
        }
        if (skippedByCache.length > 0) {
          console.warn(`[MatchingEngine] ⚠️ ${tradingPair}: Crossing orders blocked by recentlyMatched cache: ${skippedByCache.join(', ')}`);
        }
      } else {
        const spreadKey = `spread:${tradingPair}:${bestBuy.price}:${bestSell.price}`;
        if (this._lastLogState[`spread:${tradingPair}`] !== spreadKey) {
          console.log(`[MatchingEngine] ${tradingPair}: No crossing (bid=${bestBuy.price} < ask=${bestSell.price}, spread=${spread.toFixed(4)})`);
          this._lastLogState[`spread:${tradingPair}`] = spreadKey;
        }
      }
    }
    return false;
  }
  
  /**
   * Execute a match using Allocation API for settlement.
   * 
   * Settlement flow (Allocation-based):
   * 1. FillOrder on BOTH Canton order contracts FIRST (prevents re-matching loop)
   * 2. Execute seller's Allocation: base asset (e.g., CC) → buyer
   *    → Exchange calls Allocation_Execute as executor — NO seller key needed
   * 3. Execute buyer's Allocation: quote asset (e.g., CBTC) → seller
   *    → Exchange calls Allocation_Execute as executor — NO buyer key needed
   * 4. Create Trade record on Canton for history
   * 5. Trigger stop-loss checks at the new trade price
   * 6. Broadcast via WebSocket
   * 
   * Both transfers are REAL Canton token movements visible on Canton Explorer.
   * The exchange settles with its OWN key — users sign only at order placement.
   */
  async executeMatch(tradingPair, buyOrder, sellOrder, matchQty, matchPrice, token) {
        const packageId = config.canton.packageIds?.clobExchange;
        const operatorPartyId = config.canton.operatorPartyId;
    const synchronizerId = config.canton.synchronizerId;
    const [baseSymbol, quoteSymbol] = tradingPair.split('/');

    // ── Pre-flight: verify synchronizer connectivity ──────────────
    // Fail fast if the participant is disconnected — saves time and log noise.
    try {
      const syncResult = await cantonService.resolveSubmissionSynchronizerId(token, synchronizerId);
      if (!syncResult) {
        throw new Error('TRANSIENT_SYNCHRONIZER_ERROR: Participant has no connected synchronizer — settlement postponed');
      }
    } catch (syncCheckErr) {
      if (syncCheckErr.message?.includes('TRANSIENT_SYNCHRONIZER_ERROR')) throw syncCheckErr;
      console.warn(`[MatchingEngine] ⚠️ Synchronizer health check failed: ${syncCheckErr.message} — proceeding anyway`);
    }

    const quoteAmount = matchQty.times(new Decimal(matchPrice));
    const matchQtyStr = matchQty.toFixed(10);
    const quoteAmountStr = quoteAmount.toFixed(10);
    
    let tradeContractId = null;
    const buyIsPartial = new Decimal(buyOrder.remaining).gt(matchQty);
    const sellIsPartial = new Decimal(sellOrder.remaining).gt(matchQty);
    
    const sdkClient = getCantonSDKClient();

    // ═══════════════════════════════════════════════════════════════════
    // STEP 1: Execute Allocation_ExecuteTransfer for both sides
    //
    // Uses the real Splice Token Standard: Allocation_ExecuteTransfer
    // transfers the locked tokens on-chain to the counterparty.
    // The operator (executor) submits alone — user consent was captured
    // at allocation creation time (order placement).
    // ═══════════════════════════════════════════════════════════════════
    console.log(`[MatchingEngine] Settlement: ${matchQtyStr} ${baseSymbol} @ ${matchPrice} ${quoteSymbol}`);
    console.log(`[MatchingEngine] Step 1: Executing Allocation_ExecuteTransfer...`);
    console.log(`[MatchingEngine]    Seller alloc: ${sellOrder.allocationContractId ? sellOrder.allocationContractId.substring(0, 24) + '...' : 'missing'} (${baseSymbol}, type: ${sellOrder.allocationType})`);
    console.log(`[MatchingEngine]    Buyer alloc:  ${buyOrder.allocationContractId ? buyOrder.allocationContractId.substring(0, 24) + '...' : 'missing'} (${quoteSymbol}, type: ${buyOrder.allocationType})`);

    if (!sellOrder.allocationContractId || !buyOrder.allocationContractId) {
      throw new Error('Settlement aborted: missing allocation on one or both orders');
    }

    const tradeId = `trade-${Date.now()}-${Math.random().toString(36).substring(2, 10)}`;

    let replacementSellAllocationCid = null;
    let replacementBuyAllocationCid = null;

    // ── Execute SELLER allocation (Allocation_ExecuteTransfer) ────
    try {
      console.log(`[MatchingEngine]    Seller: Allocation_ExecuteTransfer (${sellOrder.allocationType})...`);
      const sellerResult = await sdkClient.tryRealAllocationExecution(
        sellOrder.allocationContractId,
        operatorPartyId,
        baseSymbol,
        sellOrder.owner,
        buyOrder.owner
      );
      if (!sellerResult) {
        throw new Error(`${baseSymbol} seller Allocation_ExecuteTransfer returned null`);
      }
      console.log(`[MatchingEngine]    Seller: Allocation_ExecuteTransfer succeeded — tokens transferred on-chain`);

      if (sellIsPartial && sellerResult?.transaction?.events) {
        for (const event of sellerResult.transaction.events) {
          const created = event.created || event.CreatedEvent;
          if (created?.contractId) {
            const payload = created.createArgument || created.payload || {};
            if (payload.status === 'PENDING') {
              replacementSellAllocationCid = created.contractId;
              break;
            }
          }
        }
      }
    } catch (sellAllocErr) {
      const msg = sellAllocErr.message || String(sellAllocErr);
      throw new Error(`SELLER_ALLOCATION_FAILED: ${msg}`);
    }

    // ── Execute BUYER allocation (Allocation_ExecuteTransfer) ─────
    try {
      console.log(`[MatchingEngine]    Buyer: Allocation_ExecuteTransfer (${buyOrder.allocationType})...`);
      const buyerResult = await sdkClient.tryRealAllocationExecution(
        buyOrder.allocationContractId,
        operatorPartyId,
        quoteSymbol,
        buyOrder.owner,
        sellOrder.owner
      );
      if (!buyerResult) {
        throw new Error(`${quoteSymbol} buyer Allocation_ExecuteTransfer returned null`);
      }
      console.log(`[MatchingEngine]    Buyer: Allocation_ExecuteTransfer succeeded — tokens transferred on-chain`);

      if (buyIsPartial && buyerResult?.transaction?.events) {
        for (const event of buyerResult.transaction.events) {
          const created = event.created || event.CreatedEvent;
          if (created?.contractId) {
            const payload = created.createArgument || created.payload || {};
            if (payload.status === 'PENDING') {
              replacementBuyAllocationCid = created.contractId;
              break;
            }
          }
        }
      }
    } catch (buyAllocErr) {
      const msg = buyAllocErr.message || String(buyAllocErr);
      throw new Error(`BUYER_ALLOCATION_FAILED: ${msg}`);
    }

    console.log(`[MatchingEngine]    Settlement COMPLETE via Allocation_ExecuteTransfer (both sides), operator-only`);

    // ═══════════════════════════════════════════════════════════════════
    // STEP 2: FillOrder on BOTH Canton order contracts AFTER settlement
    //
    // FillOrder controller = operator. Owner's auth carries from signatory
    // of the consumed contract (DAML authorization rule).
    // → OPERATOR-ONLY actAs — no external party co-auth needed.
    //
    // Previously used actAsParty: [operatorPartyId, buyOrder.owner] which
    // caused NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT for external
    // parties. Fixed: operator submits alone.
    // ═══════════════════════════════════════════════════════════════════
    console.log(`[MatchingEngine] 📝 Step 2: Filling orders on-chain (operator-only submission)...`);

    // Keep a reference to the streaming model for immediate eviction after FillOrder
    const streaming = this._getStreamingModel();

    try {
      const buyFillArg = {
        fillQuantity: matchQtyStr,
        newAllocationCid: buyIsPartial && replacementBuyAllocationCid
          ? replacementBuyAllocationCid
          : null,
      };
      await cantonService.exerciseChoice({
        token,
        actAsParty: [operatorPartyId],  // OPERATOR ONLY — owner auth from signatory
        templateId: buyOrder.templateId || `${packageId}:Order:Order`,
        contractId: buyOrder.contractId,
        choice: 'FillOrder',
        choiceArgument: buyFillArg,
        readAs: [operatorPartyId, buyOrder.owner],
      });
      console.log(`[MatchingEngine] ✅ Buy order filled: ${buyOrder.orderId}${buyIsPartial ? ' (partial, remainder alloc: ' + (replacementBuyAllocationCid?.substring(0,20) || 'none') + '...)' : ' (complete)'}`);

      // Immediately evict old contract from streaming model.
      // FillOrder is a consuming choice — the old contract ID is now archived
      // on the ledger. The WebSocket ArchivedEvent will also remove it, but
      // this ensures the order book is updated instantly (belt-and-suspenders).
      if (streaming) {
        streaming.evictOrder(buyOrder.contractId);
      }
    } catch (fillError) {
      console.error(`[MatchingEngine] ❌ Buy FillOrder FAILED after settlement: ${fillError.message}`);
      if (!fillError.message?.includes('already filled') && !fillError.message?.includes('CONTRACT_NOT_FOUND')) {
        throw new Error(`Post-settlement Buy FillOrder failed: ${fillError.message}`);
      }
      // Even on "already filled" / "CONTRACT_NOT_FOUND", evict — it's stale
      if (streaming) streaming.evictOrder(buyOrder.contractId);
    }

    try {
      const sellFillArg = {
        fillQuantity: matchQtyStr,
        newAllocationCid: sellIsPartial && replacementSellAllocationCid
          ? replacementSellAllocationCid
          : null,
      };
      await cantonService.exerciseChoice({
        token,
        actAsParty: [operatorPartyId],  // OPERATOR ONLY — owner auth from signatory
        templateId: sellOrder.templateId || `${packageId}:Order:Order`,
        contractId: sellOrder.contractId,
        choice: 'FillOrder',
        choiceArgument: sellFillArg,
        readAs: [operatorPartyId, sellOrder.owner],
      });
      console.log(`[MatchingEngine] ✅ Sell order filled: ${sellOrder.orderId}${sellIsPartial ? ' (partial, remainder alloc: ' + (replacementSellAllocationCid?.substring(0,20) || 'none') + '...)' : ' (complete)'}`);

      // Immediately evict old contract from streaming model (same as buy above)
      if (streaming) {
        streaming.evictOrder(sellOrder.contractId);
      }
    } catch (fillError) {
      console.error(`[MatchingEngine] ❌ Sell FillOrder FAILED after settlement: ${fillError.message}`);
      if (!fillError.message?.includes('already filled') && !fillError.message?.includes('CONTRACT_NOT_FOUND')) {
        throw new Error(`Post-settlement Sell FillOrder failed: ${fillError.message}`);
      }
      // Even on "already filled" / "CONTRACT_NOT_FOUND", evict — it's stale
      if (streaming) streaming.evictOrder(sellOrder.contractId);
    }

    // ═══════════════════════════════════════════════════════════════════
    // STEP 2b: Record trade balance effects in PostgreSQL
    //
    // Allocation_ExecuteTransfer moved real tokens on-chain.
    // We also record credits/debits so the balance API can compute:
    //   available = Splice holdings + trade credits - trade debits - reservations
    // ═══════════════════════════════════════════════════════════════════
    try {
      const { recordTradeSettlement, isTradeRecorded } = require('./tradeSettlementService');
      const alreadyRecorded = await isTradeRecorded(tradeId);
      if (!alreadyRecorded) {
        await recordTradeSettlement({
          tradeId,
          buyer: buyOrder.owner,
          seller: sellOrder.owner,
          baseSymbol,
          quoteSymbol,
          baseAmount: matchQtyStr,
          quoteAmount: quoteAmountStr,
          price: matchPrice,
          tradingPair,
          buyOrderId: buyOrder.orderId,
          sellOrderId: sellOrder.orderId,
          sellerUsedRealTransfer,
          buyerUsedRealTransfer,
        });
      }
    } catch (tsErr) {
      console.warn(`[MatchingEngine] ⚠️ TradeSettlement recording failed (non-critical): ${tsErr.message}`);
    }

    // Release balance reservations for filled quantities
    try {
      const { releasePartialReservation } = require('./order-service');
      await releasePartialReservation(sellOrder.orderId, matchQtyStr);
      await releasePartialReservation(buyOrder.orderId, quoteAmountStr);
    } catch (_) { /* non-critical */ }

    // ═══════════════════════════════════════════════════════════════════
    // STEP 3: Create Trade record on Canton for history
    // ═══════════════════════════════════════════════════════════════════
    try {
      const tradeTemplateId = `${packageId}:Settlement:Trade`;
      // Use the tradeId generated for this settlement
      const tradeResult = await cantonService.createContractWithTransaction({
          token,
        actAsParty: operatorPartyId,
        templateId: tradeTemplateId,
          createArguments: {
            tradeId: tradeId,
            operator: operatorPartyId,
            buyer: buyOrder.owner,
            seller: sellOrder.owner,
            baseInstrumentId: {
              issuer: operatorPartyId,
              symbol: baseSymbol,
              version: '1.0',
            },
            quoteInstrumentId: {
              issuer: operatorPartyId,
              symbol: quoteSymbol,
              version: '1.0',
            },
          baseAmount: matchQtyStr,
          quoteAmount: quoteAmountStr,
            price: matchPrice.toString(),
            buyOrderId: buyOrder.orderId,
            sellOrderId: sellOrder.orderId,
            timestamp: new Date().toISOString(),
          },
          readAs: [operatorPartyId, buyOrder.owner, sellOrder.owner],
        synchronizerId,
      });
      const events = tradeResult?.transaction?.events || [];
            for (const event of events) {
              const created = event.created || event.CreatedEvent;
        if (created?.contractId) {
          tradeContractId = created.contractId;
                break;
              }
            }
      console.log(`[MatchingEngine] ✅ Trade record created: ${tradeContractId?.substring(0, 25)}...`);
    } catch (tradeErr) {
      console.warn(`[MatchingEngine] ⚠️ Trade record creation failed (non-critical): ${tradeErr.message}`);
      tradeContractId = `trade-${Date.now()}`;
    }

    // ═══════════════════════════════════════════════════════════════════
    // STEP 4: Trigger stop-loss checks at the new trade price
    // After every successful trade, check if any stop-loss orders
    // should be triggered by the new price.
    // ═══════════════════════════════════════════════════════════════════
    try {
      const { getStopLossService } = require('./stopLossService');
      const stopLossService = getStopLossService();
      await stopLossService.checkTriggers(tradingPair, matchPrice);
    } catch (slErr) {
      console.warn(`[MatchingEngine] ⚠️ Stop-loss trigger check failed (non-critical): ${slErr.message}`);
    }

    // ═══════════════════════════════════════════════════
    // FINAL: Record trade and broadcast via WebSocket
    // ═══════════════════════════════════════════════════
    const tradeRecord = {
      tradeId: tradeContractId || `trade-${Date.now()}`,
      tradingPair,
      buyer: buyOrder.owner,
      seller: sellOrder.owner,
      price: matchPrice.toString(),
      quantity: matchQtyStr,
      buyOrderId: buyOrder.orderId,
      sellOrderId: sellOrder.orderId,
      timestamp: new Date().toISOString(),
      settlementType: 'Allocation_ExecuteTransfer',
      instrumentAllocationId: sellOrder.allocationContractId || null,
      paymentAllocationId: buyOrder.allocationContractId || null,
    };

    // Broadcast via WebSocket
    if (global.broadcastWebSocket) {
      global.broadcastWebSocket(`trades:${tradingPair}`, { type: 'NEW_TRADE', ...tradeRecord });
      global.broadcastWebSocket('trades:all', { type: 'NEW_TRADE', ...tradeRecord });
      global.broadcastWebSocket(`orderbook:${tradingPair}`, {
        type: 'TRADE_EXECUTED',
        buyOrderId: buyOrder.orderId,
        sellOrderId: sellOrder.orderId,
        fillQuantity: matchQtyStr,
        fillPrice: matchPrice,
      });

      global.broadcastWebSocket(`balance:${buyOrder.owner}`, {
        type: 'BALANCE_UPDATE',
        partyId: buyOrder.owner,
        timestamp: Date.now()
      });
      global.broadcastWebSocket(`balance:${sellOrder.owner}`, {
        type: 'BALANCE_UPDATE',
        partyId: sellOrder.owner,
        timestamp: Date.now()
      });
    }

    console.log(`[MatchingEngine] ═══ Match complete: ${matchQtyStr} ${baseSymbol} @ ${matchPrice} ${quoteSymbol} (Allocation) ═══`);
  }

  setPollingInterval(ms) {
    this.pollingInterval = ms;
    console.log(`[MatchingEngine] Polling interval: ${ms}ms`);
  }

  /**
   * Run a single matching cycle on-demand (for serverless / API trigger).
   */
  async triggerMatchingCycle(targetPair = null) {
    this._resetToFastPolling();

    if (this.matchingInProgress) {
      const elapsed = Date.now() - this.matchingStartTime;
      if (elapsed > 25000) {
        console.warn(`[MatchingEngine] ⚠️ matchingInProgress stuck for ${elapsed}ms — force-resetting`);
        this.matchingInProgress = false;
      } else {
        if (targetPair) {
          this.pendingPairs.add(targetPair);
          console.log(`[MatchingEngine] ⏳ Queued ${targetPair} for matching after current cycle (${elapsed}ms in progress)`);
          return { success: false, reason: 'queued_for_next_cycle' };
        }
        console.log(`[MatchingEngine] ⚡ Skipping trigger — matching already in progress (${elapsed}ms)`);
        return { success: false, reason: 'matching_in_progress' };
      }
    }

    const now = Date.now();
    if (this._lastTriggerTime && (now - this._lastTriggerTime) < 2000) {
      if (targetPair) {
        this.pendingPairs.add(targetPair);
        return { success: false, reason: 'rate_limited_queued' };
      }
      return { success: false, reason: 'rate_limited' };
    }
    this._lastTriggerTime = now;

    const pairsToProcess = targetPair ? [targetPair] : this.tradingPairs;
    console.log(`[MatchingEngine] ⚡ On-demand cycle triggered for: ${pairsToProcess.join(', ')}`);
    const startTime = Date.now();

    try {
      this.matchingInProgress = true;
      this.matchingStartTime = startTime;

      const token = await this.getAdminToken();

      for (const tradingPair of pairsToProcess) {
        try {
          await this.processOrdersForPair(tradingPair, token);
        } catch (error) {
          if (!error.message?.includes('No contracts found')) {
            console.error(`[MatchingEngine] Trigger error for ${tradingPair}:`, error.message);
          }
        }
      }

      const elapsed = Date.now() - startTime;
      console.log(`[MatchingEngine] ⚡ On-demand cycle complete in ${elapsed}ms`);

      return { success: true, elapsed, tradingPairs: pairsToProcess };
    } catch (error) {
      console.error(`[MatchingEngine] ⚡ On-demand cycle failed:`, error.message);
      if (error.message?.includes('401') || error.message?.includes('security-sensitive')) {
        this.invalidateToken();
      }
      return { success: false, error: error.message };
    } finally {
      this.matchingInProgress = false;
    }
  }
}

// Singleton
let matchingEngineInstance = null;

function getMatchingEngine() {
  if (!matchingEngineInstance) {
    matchingEngineInstance = new MatchingEngine();
  }
  return matchingEngineInstance;
}

module.exports = {
  MatchingEngine,
  getMatchingEngine,
};
