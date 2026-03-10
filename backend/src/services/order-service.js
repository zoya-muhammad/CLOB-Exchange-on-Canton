/**
 * Order Service — Canton JSON Ledger API v2 + Allocation-Based Settlement
 * 
 * Uses the correct Canton APIs:
 * - POST /v2/commands/submit-and-wait-for-transaction — Place/Cancel orders
 * - POST /v2/state/active-contracts — Query orders
 * 
 * Balance checks use the Canton Wallet SDK (listHoldingUtxos).
 * 
 * Settlement is Allocation-based:
 * - At ORDER PLACEMENT: creates an Allocation (exchange = executor, funds locked)
 * - At MATCH TIME: exchange executes Allocation with its OWN key (no user key needed)
 * - At CANCEL: Allocation_Cancel releases locked funds back to sender
 * 
 * Why Allocations (not TransferInstruction):
 * - TransferInstruction requires user's private key at SETTLEMENT time
 * - With external parties, backend has no user keys → TransferInstruction breaks
 * - Allocation: User signs ONCE at order time, exchange settles with its own key
 * 
 * @see https://docs.sync.global/app_dev/api/splice-api-token-allocation-v1/
 * @see https://docs.digitalasset.com/integrate/devnet/token-standard/index.html
 */

const Decimal = require('decimal.js');
const config = require('../config');
const cantonService = require('./cantonService');
const tokenProvider = require('./tokenProvider');
const { ValidationError, NotFoundError } = require('../utils/errors');
const { v4: uuidv4 } = require('uuid');
const { getReadModelService } = require('./readModelService');
const { getCantonSDKClient } = require('./canton-sdk-client');

// Configure Decimal for financial precision
Decimal.set({ precision: 20, rounding: Decimal.ROUND_DOWN });

// ═══════════════════════════════════════════════════════════════════════════
// BALANCE RESERVATION TRACKER — PostgreSQL via Prisma (Neon)
// ALL reads/writes go directly to PostgreSQL. No in-memory cache.
// ═══════════════════════════════════════════════════════════════════════════
const { getDb } = require('./db');

// ═══════════════════════════════════════════════════════════════════════════
// GLOBAL OPEN ORDER REGISTRY (in-memory — rebuilt from Canton on each query)
// This is OK: it's a Canton data cache, not application state.
// ═══════════════════════════════════════════════════════════════════════════
const _globalOpenOrders = new Map();

function registerOpenOrders(orders) {
  if (!Array.isArray(orders)) return;
  for (const order of orders) {
    if (order.status === 'OPEN' && order.contractId) {
      _globalOpenOrders.set(order.contractId, {
        contractId: order.contractId,
        orderId: order.orderId,
        owner: order.owner,
        tradingPair: order.tradingPair,
        orderType: order.orderType,
        orderMode: order.orderMode,
        price: order.price,
        quantity: order.quantity,
        filled: order.filled || '0',
        remaining: parseFloat(order.quantity || 0) - parseFloat(order.filled || 0),
        status: 'OPEN',
        timestamp: order.timestamp,
      });
    }
  }
  for (const order of orders) {
    if (order.status !== 'OPEN' && order.contractId && _globalOpenOrders.has(order.contractId)) {
      _globalOpenOrders.delete(order.contractId);
    }
  }
}

function getGlobalOpenOrders() {
  return [..._globalOpenOrders.values()];
}

async function getReservedBalance(partyId, asset) {
  const db = getDb();
  // OrderReservation.amount is String (Decimal). Prisma aggregate _sum does not support String.
  // Use findMany + manual sum.
  const rows = await db.orderReservation.findMany({
    where: { partyId, asset },
    select: { amount: true },
  });
  let total = new Decimal(0);
  for (const r of rows) {
    total = total.plus(new Decimal(r.amount || '0'));
  }
  return total;
}

async function addReservation(orderId, partyId, asset, amount, allocationContractId = null, allocationType = 'EXCHANGE') {
  const db = getDb();
  const amountStr = new Decimal(amount).toString();
  await db.orderReservation.upsert({
    where: { orderId },
    create: { orderId, partyId, asset, amount: amountStr, allocationContractId, allocationType },
    update: { partyId, asset, amount: amountStr, allocationContractId, allocationType },
  });
  console.log(`[BalanceReservation] ➕ Reserved ${amount} ${asset} for ${orderId} (allocation: ${allocationContractId ? allocationContractId.substring(0, 30) + '...' : 'none'}, type: ${allocationType})`);
}

async function releaseReservation(orderId) {
  const db = getDb();
  try {
    const reservation = await db.orderReservation.findUnique({ where: { orderId } });
  if (!reservation) return;
    await db.orderReservation.delete({ where: { orderId } });
    console.log(`[BalanceReservation] ➖ Released ${reservation.amount} ${reservation.asset} for ${orderId}`);
  } catch (err) {
    console.warn(`[BalanceReservation] releaseReservation failed for ${orderId}: ${err.message}`);
  }
}

async function releasePartialReservation(orderId, filledAmount) {
  const db = getDb();
  try {
    const reservation = await db.orderReservation.findUnique({ where: { orderId } });
  if (!reservation) return;

  const releaseAmt = Decimal.min(new Decimal(filledAmount), new Decimal(reservation.amount));
  const remaining = Decimal.max(new Decimal(reservation.amount).minus(releaseAmt), new Decimal(0));

  if (remaining.lte(0)) {
      await db.orderReservation.delete({ where: { orderId } });
  } else {
      await db.orderReservation.update({
        where: { orderId },
        data: { amount: remaining.toString() },
      });
  }
    console.log(`[BalanceReservation] ➖ Partially released ${filledAmount} ${reservation.asset} for ${orderId} (remaining: ${remaining.toString()})`);
  } catch (err) {
    console.warn(`[BalanceReservation] releasePartialReservation failed for ${orderId}: ${err.message}`);
  }
}

/**
 * Get the allocation contract ID stored for an order's reservation.
 */
async function getAllocationContractIdForOrder(orderId) {
  const db = getDb();
  const reservation = await db.orderReservation.findUnique({
    where: { orderId },
    select: { allocationContractId: true },
  });
  return reservation?.allocationContractId || null;
}

async function setAllocationContractIdForOrder(orderId, allocationContractId, allocationType = null) {
  const db = getDb();
  try {
    const data = { allocationContractId: allocationContractId || null };
    if (allocationType) data.allocationType = allocationType;
    await db.orderReservation.update({
      where: { orderId },
      data,
    });
  } catch (err) {
    console.warn(`[BalanceReservation] setAllocationContractId failed for ${orderId}: ${err.message}`);
  }
}

async function getAllocationTypeForOrder(orderId) {
  const db = getDb();
  const reservation = await db.orderReservation.findUnique({
    where: { orderId },
    select: { allocationType: true },
  });
  return reservation?.allocationType || 'EXCHANGE';
}

class OrderService {
  constructor() {
    console.log('[OrderService] Initialized with Canton JSON API v2 + Allocation-based settlement');
  }

  _templateIdToString(templateId) {
    if (typeof templateId === 'string') return templateId;
    if (templateId && typeof templateId === 'object' && templateId.packageId && templateId.moduleName && templateId.entityName) {
      return `${templateId.packageId}:${templateId.moduleName}:${templateId.entityName}`;
    }
    return '';
  }

  _extractAllocationCidFromExecuteResult(result) {
    const isCidLike = (value) => typeof value === 'string' && value.length > 20;
    const visited = new Set();
    const stack = [result];

    while (stack.length > 0) {
      const current = stack.pop();
      if (!current || typeof current !== 'object') continue;
      if (visited.has(current)) continue;
      visited.add(current);

      if (isCidLike(current.allocationCid)) return current.allocationCid;
      if (isCidLike(current.allocationContractId)) return current.allocationContractId;

      if (Array.isArray(current)) {
        for (const item of current) stack.push(item);
      } else {
        for (const key of Object.keys(current)) {
          stack.push(current[key]);
        }
      }
    }
    return null;
  }

  _extractOrderRefFromAllocationPayload(payload) {
    if (!payload || typeof payload !== 'object') return null;
    return (
      payload?.orderId ||
      payload?.settlement?.settlementRef?.id ||
      payload?.allocation?.settlement?.settlementRef?.id ||
      payload?.settlementRef?.id ||
      payload?.allocation?.settlementRef?.id ||
      payload?.transferLegId ||
      payload?.allocation?.transferLegId ||
      payload?.output?.allocation?.settlement?.settlementRef?.id ||
      payload?.output?.allocation?.transferLegId ||
      null
    );
  }

  async _findAllocationCidForOrder(orderId, partyId, token) {
    if (!orderId) return null;
    const operatorPartyId = config.canton.operatorPartyId;
    const parties = [...new Set([partyId, operatorPartyId].filter(Boolean))];

    // 1) First attempt: use SDK pending-allocation view (narrow, deterministic, and fast).
    try {
      const sdkClient = getCantonSDKClient();
      if (sdkClient?.isReady?.()) {
        for (const party of parties) {
          const pending = await sdkClient.fetchPendingAllocations(party);
          for (const row of Array.isArray(pending) ? pending : []) {
            const contractId = row?.contractId || row?.activeContract?.createdEvent?.contractId || null;
            const payload =
              row?.activeContract?.createdEvent?.createArgument ||
              row?.payload ||
              row?.createArgument ||
              {};
            const ref = this._extractOrderRefFromAllocationPayload(payload);
            if (ref === orderId && contractId) {
              console.log(`[OrderService] ✅ Found allocation via SDK pending view for ${orderId}: ${contractId.substring(0, 30)}...`);
              return contractId;
            }
          }
        }
      }
    } catch (sdkErr) {
      console.warn(`[OrderService] SDK pending-allocation lookup failed: ${sdkErr.message}`);
    }

    // 2) Fallback: raw ACS query. Retry briefly for eventual-consistency lag after execute.
    const attempts = 4;
    for (let attempt = 1; attempt <= attempts; attempt++) {
    for (const party of parties) {
      try {
        const contracts = await cantonService.queryActiveContracts({
          party,
          templateIds: [],
          verbose: true,
        }, token);

        for (const contract of Array.isArray(contracts) ? contracts : []) {
          const templateId = this._templateIdToString(contract.templateId || contract.identifier);
          if (!templateId.includes('Allocation')) continue;

          const payload = contract.payload || contract.createArgument || {};
          const settlementRefId = this._extractOrderRefFromAllocationPayload(payload);

          if (settlementRefId === orderId && contract.contractId) {
            console.log(`[OrderService] ✅ Found allocation for order ${orderId}: ${contract.contractId.substring(0, 30)}...`);
            return contract.contractId;
          }

          // Defensive fallback: token-standard payloads can vary across package versions.
          // If the payload references the orderId anywhere, treat this as the matching allocation.
          if (contract.contractId && JSON.stringify(payload).includes(orderId)) {
            console.log(`[OrderService] ✅ Found allocation via payload scan for order ${orderId}: ${contract.contractId.substring(0, 30)}...`);
            return contract.contractId;
          }
        }
      } catch (err) {
        console.warn(`[OrderService] Allocation lookup failed for party ${party.substring(0, 20)}...: ${err.message}`);
      }
    }
      if (attempt < attempts) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    return null;
  }

  /**
   * Calculate amount to lock for an order (uses Decimal for precision).
   * BUY order: lock quote currency (e.g., CBTC for CC/CBTC pair)
   * SELL order: lock base currency (e.g., CC for CC/CBTC pair)
   * 
   * For MARKET orders, use estimatedPrice (from order book) 
   * with a 5% slippage buffer to ensure sufficient funds.
   */
  calculateLockAmount(tradingPair, orderType, price, quantity, orderMode = 'LIMIT', estimatedPrice = null) {
    const [baseAsset, quoteAsset] = tradingPair.split('/');
    const qty = new Decimal(quantity);

    if (orderType.toUpperCase() === 'BUY') {
      let prc;
      if (orderMode.toUpperCase() === 'MARKET') {
        prc = new Decimal(estimatedPrice || '0').times('1.05'); // 5% slippage buffer
      } else {
        prc = new Decimal(price || '0');
      }
      
      return {
        asset: quoteAsset,
        amount: prc.times(qty).toNumber()
      };
    } else {
      return {
        asset: baseAsset,
        amount: qty.toNumber()
      };
    }
  }

  /**
   * Check available balance via Canton Wallet SDK before order placement.
   * 
   * @returns {Object} { verified: true, availableBalance, asset }
   */
  async checkBalanceForOrder(token, partyId, operatorPartyId, asset, amount, orderId) {
    console.log(`[OrderService] SDK: Checking ${amount} ${asset} balance for order ${orderId}`);
    
    const sdkClient = getCantonSDKClient();
    
    if (!sdkClient.isReady()) {
      console.warn(`[OrderService] ⚠️ Canton SDK not ready — skipping balance check (order will proceed)`);
      return { verified: false, availableBalance: 0, asset };
    }

    try {
      const balance = await sdkClient.getBalance(partyId, asset);
      const availableBalance = parseFloat(balance.available || '0');

      // Deduct balance already reserved by other open orders (prevents overselling)
      const reserved = await getReservedBalance(partyId, asset);
      const effectiveAvailable = new Decimal(availableBalance).minus(reserved);
      
      if (effectiveAvailable.lt(new Decimal(amount))) {
        throw new ValidationError(
          `Insufficient ${asset} balance. On-chain available: ${availableBalance}, ` +
          `Reserved by open orders: ${reserved.toString()}, ` +
          `Effective available: ${effectiveAvailable.toString()}, ` +
          `Required: ${amount}`
        );
      }
      console.log(`[OrderService] ✅ Balance check passed: ${availableBalance} ${asset} on-chain, ${reserved.toString()} reserved, ${effectiveAvailable.toString()} effective (need ${amount})`);
      
      return {
        verified: true,
        availableBalance: effectiveAvailable.toNumber(),
        asset,
      };
    } catch (err) {
      if (err instanceof ValidationError) throw err;
      console.warn(`[OrderService] ⚠️ Balance check failed (proceeding anyway): ${err.message}`);
      return { verified: false, availableBalance: 0, asset };
    }
  }

  /**
   * Cancel the Allocation associated with an order being cancelled.
   * 
   * With Allocation-based settlement, each order has an Allocation contract
   * that locks the user's holdings. Cancelling the Allocation releases
   * the locked funds back to the user.
   * 
   * @param {string} orderId - Order ID (for looking up the allocation)
   * @param {string} allocationContractId - The Allocation contract ID (from order creation)
   * @param {string} partyId - The order owner (sender in the allocation)
   */
  async cancelAllocationForOrder(orderId, allocationContractId, partyId) {
    console.log(`[OrderService] 🔓 Cancelling Allocation for order ${orderId}`);

    // Find the allocationContractId from the reservation if not provided
    if (!allocationContractId) {
      allocationContractId = await getAllocationContractIdForOrder(orderId);
    }

    if (!allocationContractId) {
      console.log(`[OrderService] No allocationContractId for order ${orderId} — nothing to cancel`);
      return;
    }
    
    const executorPartyId = config.canton.operatorPartyId;
    const sdkClient = getCantonSDKClient();

    try {
      const cancelResult = await sdkClient.cancelAllocation(allocationContractId, partyId, executorPartyId);
      if (cancelResult?.cancelled) {
        console.log(`[OrderService] Allocation cancelled for order ${orderId} — holdings unlocked`);
      } else if (cancelResult?.skipped) {
        console.log(`[OrderService] Allocation cancel skipped for order ${orderId}: ${cancelResult.reason}`);
      } else {
        console.warn(`[OrderService] Allocation cancel not confirmed for order ${orderId}`);
      }
      return cancelResult;
    } catch (cancelErr) {
      console.warn(`[OrderService] ⚠️ Could not cancel Allocation: ${cancelErr.message}`);
      // Don't throw — order cancellation should still proceed
      return { cancelled: false, skipped: false, reason: cancelErr.message };
    }
  }

  /**
   * Place order using Canton JSON Ledger API v2.
   * 
   * Flow:
   * 1. Check balance via Canton SDK
   * 2. Create Allocation (exchange = executor, funds locked)
   * 3. Create Order contract on Canton
   * 4. Trigger matching engine
   * 
   * The Allocation ensures funds are locked at order time.
   * The exchange can settle at match time with its own key.
   */
  async placeOrder(orderData) {
    const {
      partyId,
      tradingPair,
      orderType, // BUY | SELL
      orderMode, // LIMIT | MARKET | STOP_LOSS
      price,
      quantity,
      timeInForce = 'GTC',
      stopPrice = null, // For STOP_LOSS orders
    } = orderData;

    // Validation
    if (!partyId || !tradingPair || !orderType || !orderMode || !quantity) {
      throw new ValidationError('Missing required fields: partyId, tradingPair, orderType, orderMode, quantity');
    }

    // Validate standard order types
    if (orderMode === 'LIMIT' && !price) {
      throw new ValidationError('Price is required for LIMIT orders');
    }

    // Validate stop-loss orders
    if (orderMode === 'STOP_LOSS') {
      if (!stopPrice) {
        throw new ValidationError('stopPrice is required for STOP_LOSS orders');
      }
      const sp = parseFloat(stopPrice);
      if (isNaN(sp) || sp <= 0) {
        throw new ValidationError('stopPrice must be a positive number');
      }

      // Validate stop-loss price direction
      try {
        const { getOrderBookService } = require('./orderBookService');
        const orderBookService = getOrderBookService();
        const orderBook = await orderBookService.getOrderBook(tradingPair);
        
        // Get current market price for validation
        const buys = orderBook.buyOrders || [];
        const sells = orderBook.sellOrders || [];
        let currentPrice = null;
        
        if (buys.length > 0 && sells.length > 0) {
          const bestBid = parseFloat(buys.sort((a, b) => parseFloat(b.price) - parseFloat(a.price))[0].price);
          const bestAsk = parseFloat(sells.sort((a, b) => parseFloat(a.price) - parseFloat(b.price))[0].price);
          currentPrice = (bestBid + bestAsk) / 2;
        } else if (buys.length > 0) {
          currentPrice = parseFloat(buys[0].price);
        } else if (sells.length > 0) {
          currentPrice = parseFloat(sells[0].price);
        }

        if (currentPrice) {
          // SELL stop loss must be below current price
          if (orderType.toUpperCase() === 'SELL' && new Decimal(stopPrice).gte(new Decimal(currentPrice))) {
            throw new ValidationError(
              `SELL stop loss stopPrice (${stopPrice}) must be below current market price (${currentPrice.toFixed(4)})`
            );
          }
          // BUY stop loss must be above current price
          if (orderType.toUpperCase() === 'BUY' && new Decimal(stopPrice).lte(new Decimal(currentPrice))) {
            throw new ValidationError(
              `BUY stop loss stopPrice (${stopPrice}) must be above current market price (${currentPrice.toFixed(4)})`
            );
          }
        }
      } catch (err) {
        if (err instanceof ValidationError) throw err;
        console.warn('[OrderService] Could not validate stop price against market:', err.message);
        // Continue — validation is best-effort
      }
    }

    // Validate quantity is positive
    const qty = parseFloat(quantity);
    if (isNaN(qty) || qty <= 0) {
      throw new ValidationError('Quantity must be a positive number');
    }

    // For limit orders, validate price
    if (orderMode === 'LIMIT') {
      const prc = parseFloat(price);
      if (isNaN(prc) || prc <= 0) {
        throw new ValidationError('Price must be a positive number for limit orders');
      }
    }

    console.log('[OrderService] Placing order via Canton:', {
      partyId,
      tradingPair,
      orderType,
      orderMode,
      price,
      quantity,
      stopPrice: stopPrice || 'N/A',
    });

    // Get service token
    const token = await tokenProvider.getServiceToken();
    const packageId = config.canton.packageIds.clobExchange;
    const operatorPartyId = config.canton.operatorPartyId;

    if (!packageId) {
      throw new Error('CLOB_EXCHANGE_PACKAGE_ID is not configured');
    }

    // Generate unique order ID
    const orderId = `order-${Date.now()}-${uuidv4().substring(0, 8)}`;

    // For MARKET orders, get estimated price from order book (query Canton directly)
    let estimatedPrice = null;
    if (orderMode.toUpperCase() === 'MARKET') {
      try {
        const { getOrderBookService } = require('./orderBookService');
        const orderBookService = getOrderBookService();
        const orderBook = await orderBookService.getOrderBook(tradingPair);
        
        if (orderType.toUpperCase() === 'BUY') {
          const sells = orderBook.sellOrders || [];
          if (sells.length > 0) {
            const sortedSells = sells.sort((a, b) => parseFloat(a.price) - parseFloat(b.price));
            estimatedPrice = parseFloat(sortedSells[0].price);
            console.log(`[OrderService] MARKET BUY estimated price: ${estimatedPrice} (best ask from ${sells.length} sell orders)`);
          }
        } else {
          const buys = orderBook.buyOrders || [];
          if (buys.length > 0) {
            const sortedBuys = buys.sort((a, b) => parseFloat(b.price) - parseFloat(a.price));
            estimatedPrice = parseFloat(sortedBuys[0].price);
            console.log(`[OrderService] MARKET SELL estimated price: ${estimatedPrice} (best bid from ${buys.length} buy orders)`);
          }
        }
      } catch (err) {
        console.warn('[OrderService] Could not get order book for price estimation:', err.message);
      }
      
      if (orderType.toUpperCase() === 'BUY' && !estimatedPrice) {
        throw new ValidationError('No sell orders available in the market. Please use LIMIT order or wait for sellers.');
      }
      if (orderType.toUpperCase() === 'SELL' && !estimatedPrice) {
        throw new ValidationError('No buy orders available in the market. Please use LIMIT order or wait for buyers.');
      }
    }

    // For STOP_LOSS orders, use stopPrice for lock amount calculation
    // (funds must be locked NOW, even though the order triggers later)
    let effectivePrice = price;
    let effectiveOrderMode = orderMode;
    if (orderMode === 'STOP_LOSS') {
      // Use stop price for balance calculation
      effectivePrice = stopPrice;
      effectiveOrderMode = 'LIMIT'; // Lock based on stop price
    }

    // Calculate what needs to be locked
    const lockInfo = this.calculateLockAmount(tradingPair, orderType, effectivePrice, quantity, effectiveOrderMode, estimatedPrice);
    console.log(`[OrderService] Order will lock ${lockInfo.amount} ${lockInfo.asset}`);

    // ========= CHECK BALANCE VIA CANTON SDK =========
    let balanceCheck = null;
    try {
      balanceCheck = await this.checkBalanceForOrder(
        token, 
        partyId, 
        operatorPartyId,
        lockInfo.asset, 
        lockInfo.amount,
        orderId
      );
      if (balanceCheck.verified) {
        console.log(`[OrderService] ✅ Balance verified: ${balanceCheck.availableBalance} ${lockInfo.asset} available`);
      } else {
        console.warn(`[OrderService] ⚠️ Balance check skipped (SDK not ready) — order will proceed`);
      }
    } catch (balanceError) {
      console.error(`[OrderService] Balance check failed:`, balanceError.message);
      throw new ValidationError(`Insufficient ${lockInfo.asset} balance. Required: ${lockInfo.amount}`);
    }

    // ═══════════════════════════════════════════════════════════════════
    // STEP A: Balance verified — reserve locked amount in memory
    // For external parties, the actual Allocation authorization is done
    // in the interactive order-placement transaction prepared below.
    // ═══════════════════════════════════════════════════════════════════
    let allocationContractId = null;

    // ═══ RESERVE BALANCE to prevent overselling ═══
    await addReservation(orderId, partyId, lockInfo.asset, lockInfo.amount, allocationContractId);

    // Determine initial order status
    // STOP_LOSS orders start as 'PENDING_TRIGGER' — NOT added to active order book
    const initialStatus = orderMode === 'STOP_LOSS' ? 'PENDING_TRIGGER' : 'OPEN';

    // Create Order contract on Canton
    const timestamp = new Date().toISOString();

    // ═══════════════════════════════════════════════════════════════════
    // TEMPLE PATTERN — Transaction 1: Self-transfer + Self-allocation
    //
    // Step 1: Self-transfer to create exact-amount holding
    // Step 2: Self-allocation (sender = receiver = user, NOT executed)
    // Purpose: Lock/reserve tokens for future settlement
    //
    // Settlement (Transaction 2) will:
    //   - Withdraw allocations
    //   - Create new multi-leg allocation (2 transfer legs)
    //   - Execute the new allocation
    // ═══════════════════════════════════════════════════════════════════

    const sdkClient = getCantonSDKClient();
    let allocationCommand = null;
    let readAs = null;
    let disclosedContracts = [];
    let synchronizerId = null;
    let allocationType = null;
    let exactAmountHoldingCid = null;

    // ═══ TEMPLE PATTERN STEP 1: Self-transfer for exact-amount holding ═══
    // NOTE: Self-transfer must be interactive for external parties.
    // However, Canton doesn't support multiple commands in one interactive submission.
    // So we skip self-transfer for now and use existing holdings.
    // The exact-amount requirement is an optimization; existing holdings work fine.
    console.log(`[OrderService] 🔄 Temple Pattern: Using existing holdings (self-transfer skipped due to Canton limitation)`);
    console.log(`[OrderService]    Note: Self-transfer requires interactive signature but cannot be combined with allocation`);
    console.log(`[OrderService]    Using existing holdings for allocation (exact-amount optimization skipped)`);

    // ═══ TEMPLE PATTERN STEP 2: Self-allocation (sender = receiver = user) ═══
    console.log(`[OrderService] 🔄 Temple Pattern Step 2: Creating self-allocation (NOT executed)...`);
    const realAlloc = await sdkClient.tryBuildRealAllocationCommand(
      partyId,
      operatorPartyId,
      String(lockInfo.amount),
      lockInfo.asset,
      orderId,
      null  // Use existing holdings (self-transfer skipped)
    );

    if (!realAlloc) {
      throw new Error(`Cannot place order: failed to create Token Standard allocation for ${lockInfo.amount} ${lockInfo.asset}. Tokens must be locked on-chain before an order can be placed.`);
    }

    allocationCommand = realAlloc.command;
    readAs = [...new Set([operatorPartyId, partyId, ...(realAlloc.readAs || [])])];
    disclosedContracts = realAlloc.disclosedContracts || [];
    synchronizerId = realAlloc.synchronizerId || config.canton.synchronizerId;
    allocationType = realAlloc.allocationType;
    console.log(`[OrderService] ✅ Temple Pattern: Self-allocation prepared (sender = receiver = user, NOT executed) for ${orderId}`);

    // Store the allocation type with the reservation
    await setAllocationContractIdForOrder(orderId, null, allocationType);

    // ═══════════════════════════════════════════════════════════════════
    // Canton does NOT support preparing multiple commands in one
    // interactive submission. So we prepare the allocation command alone.
    // After the user signs and executes this, executeOrderPlacement()
    // prepares the Order Create as step 2. The FRONTEND auto-signs step 2
    // using the same private key (kept in memory) so the user only enters
    // their password ONCE.
    // ═══════════════════════════════════════════════════════════════════

    const prepareResult = await cantonService.prepareInteractiveSubmission({
      token,
      actAsParty: [partyId],
      commands: [allocationCommand],
      readAs,
      synchronizerId,
      disclosedContracts,
    });
    
    if (!prepareResult.preparedTransaction || !prepareResult.preparedTransactionHash) {
      throw new Error('Prepare returned incomplete result: missing preparedTransaction or preparedTransactionHash');
    }
    
    console.log(`[OrderService] ✅ Allocation prepared (type: ${allocationType}). Hash to sign: ${prepareResult.preparedTransactionHash.substring(0, 40)}...`);
    
    return {
      requiresSignature: true,
      step: 'ALLOCATION_PREPARED',
      orderId,
      tradingPair,
      orderType: orderType.toUpperCase(),
      orderMode: orderMode.toUpperCase(),
      price: orderMode.toUpperCase() === 'LIMIT' && price ? price.toString() : (stopPrice ? stopPrice.toString() : null),
      quantity: quantity.toString(),
      stopPrice: stopPrice || null,
      preparedTransaction: prepareResult.preparedTransaction,
      preparedTransactionHash: prepareResult.preparedTransactionHash,
      hashingSchemeVersion: prepareResult.hashingSchemeVersion,
      partyId,
      lockInfo,
      stage: 'ALLOCATION_PREPARED',
      allocationType,
    };
  }

  /**
   * Place order with UTXO handling (wrapper for placeOrder)
   */
  async placeOrderWithUTXOHandling(
    partyId,
    tradingPair,
    orderType,
    orderMode,
    quantity,
    price,
    orderBookContractId = null,
    userAccountContractId = null
  ) {
    return this.placeOrder({
      partyId,
      tradingPair,
      orderType,
      orderMode,
      price,
      quantity
    });
  }

  /**
   * Place order with allocation (now default behavior)
   */
  async placeOrderWithAllocation(
    partyId,
    tradingPair,
    orderType,
    orderMode,
    quantity,
    price,
    orderBookContractId,
    allocationCid
  ) {
    return this.placeOrder({
      partyId,
      tradingPair,
      orderType,
      orderMode,
      price,
      quantity
    });
  }

  /**
   * STEP 2: Execute a prepared order placement with the user's signature
   * 
   * Called after the frontend signs the preparedTransactionHash from placeOrder()
   * 
   * @param {string} preparedTransaction - Opaque blob from prepare step
   * @param {string} partyId - The external party that signed
   * @param {string} signatureBase64 - User's Ed25519 signature of preparedTransactionHash
   * @param {string} signedBy - Public key fingerprint that signed
   * @param {string|number} hashingSchemeVersion - From prepare response
   * @param {object} orderMeta - { orderId, tradingPair, orderType, orderMode, price, quantity, stopPrice, lockInfo }
   * @returns {Object} Order result with contractId
   */
  async executeOrderPlacement(preparedTransaction, partyId, signatureBase64, signedBy, hashingSchemeVersion, orderMeta = {}) {
    const token = await tokenProvider.getServiceToken();
    const operatorPartyId = config.canton.operatorPartyId;
    
    try {
      console.log(`[OrderService] EXECUTE order placement for ${partyId.substring(0, 30)}...`);
      
      const partySignatures = {
        signatures: [
          {
            party: partyId,
            signatures: [{
              format: 'SIGNATURE_FORMAT_RAW',
              signature: signatureBase64,
              signedBy: signedBy,
              signingAlgorithmSpec: 'SIGNING_ALGORITHM_SPEC_ED25519'
            }]
          }
        ]
      };
      
      const result = await cantonService.executeInteractiveSubmission({
        preparedTransaction,
        partySignatures,
        hashingSchemeVersion,
      }, token);
      
      const stage = orderMeta?.stage || 'ALLOCATION_PREPARED';

      // Extract created contract IDs from executed transaction
      let contractId = null;
      let allocationContractId = null;
      if (result.transaction?.events) {
        for (const event of result.transaction.events) {
          const created = event.created || event.CreatedEvent;
          const templateId = this._templateIdToString(created?.templateId);
          if (!created?.contractId) continue;
          if (!contractId && templateId.includes(':Order:Order')) {
            contractId = created.contractId;
          }
          if (!allocationContractId && templateId.includes('Allocation')) {
            allocationContractId = created.contractId;
          }
        }
      }

      // Fallback: extract from nested payload structure
      if (!allocationContractId) {
        allocationContractId = this._extractAllocationCidFromExecuteResult(result);
      }
      
      if (orderMeta.orderId && allocationContractId) {
        const allocType = orderMeta.allocationType || null;
        await setAllocationContractIdForOrder(orderMeta.orderId, allocationContractId, allocType);
        console.log(`[OrderService] ✅ Allocation linked to order ${orderMeta.orderId}: ${allocationContractId.substring(0, 30)}... (type: ${allocType || 'auto'})`);

        try {
          const sdkClient = getCantonSDKClient();
          const holdingState = await sdkClient.verifyHoldingState(partyId, orderMeta.lockInfo?.asset);
          console.log(`[OrderService] Holding verification after allocation: ${holdingState.totalAvailable} available, ${holdingState.totalLocked} locked (${orderMeta.lockInfo?.asset})`);
        } catch (verifyErr) {
          console.warn(`[OrderService] Post-allocation holding verification skipped: ${verifyErr.message}`);
        }
      }

      // SINGLE-SIGN: Allocation + Order in one tx; skip second prepare.
      if (stage === 'ALLOCATION_AND_ORDER_PREPARED') {
        // If Canton execute response didn't include allocation CID in events,
        // actively search for it so the matching engine can find it later.
        if (!allocationContractId && orderMeta.orderId) {
          console.log(`[OrderService] Single-sign: allocation CID not in events, searching...`);
          try {
            allocationContractId = await this._findAllocationCidForOrder(orderMeta.orderId, partyId, token);
          } catch (searchErr) {
            console.warn(`[OrderService] Allocation CID search failed: ${searchErr.message}`);
          }
        }
        if (allocationContractId && orderMeta.orderId) {
          const allocType = orderMeta.allocationType || null;
          await setAllocationContractIdForOrder(orderMeta.orderId, allocationContractId, allocType);
          console.log(`[OrderService] ✅ Single-sign: allocation CID stored for matching: ${allocationContractId.substring(0, 30)}...`);
        } else {
          console.warn(`[OrderService] ⚠️ Single-sign: could not resolve real allocation CID for ${orderMeta.orderId} — matching engine will retry via DB lookup`);
        }
        // Fall through to success path
      } else if (stage === 'ALLOCATION_PREPARED') {
        // LEGACY TWO-STEP: Allocation executed; prepare order create for second signature.
        const packageId = config.canton.packageIds.clobExchange;
        let effectiveAllocationCid = orderMeta.allocationContractId || allocationContractId;
        if (!effectiveAllocationCid) {
          effectiveAllocationCid = await this._findAllocationCidForOrder(orderMeta.orderId, partyId, token);
        }
        if (!packageId) {
          throw new Error('CLOB_EXCHANGE_PACKAGE_ID is not configured');
        }
        if (!effectiveAllocationCid) {
          throw new Error('Allocation execute succeeded but allocationContractId is missing for order create step');
        }

        const orderStatus = orderMeta.orderMode === 'STOP_LOSS' ? 'PENDING_TRIGGER' : 'OPEN';
        const orderCreateArgs = {
          orderId: orderMeta.orderId,
          owner: partyId,
          orderType: orderMeta.orderType,
          orderMode: orderMeta.orderMode,
          tradingPair: orderMeta.tradingPair,
          price: orderMeta.price ? String(orderMeta.price) : null,
          quantity: String(orderMeta.quantity),
          filled: '0.0',
          status: orderStatus,
          timestamp: new Date().toISOString(),
          operator: operatorPartyId,
          allocationCid: effectiveAllocationCid,
          stopPrice: orderMeta.stopPrice || null,
        };

        const orderPrepareResult = await cantonService.prepareInteractiveSubmission({
          token,
          actAsParty: [partyId],
          commands: [{
            CreateCommand: {
              templateId: `${packageId}:Order:Order`,
              createArguments: orderCreateArgs,
            },
          }],
          readAs: [operatorPartyId, partyId],
          synchronizerId: config.canton.synchronizerId,
        });

        if (!orderPrepareResult.preparedTransaction || !orderPrepareResult.preparedTransactionHash) {
          throw new Error('Order create prepare returned incomplete result');
        }

        console.log(`[OrderService] ✅ Order create prepared (step 2/2) for ${orderMeta.orderId}`);
        return {
          requiresSignature: true,
          step: 'ORDER_CREATE_PREPARED',
          orderId: orderMeta.orderId,
          preparedTransaction: orderPrepareResult.preparedTransaction,
          preparedTransactionHash: orderPrepareResult.preparedTransactionHash,
          hashingSchemeVersion: orderPrepareResult.hashingSchemeVersion,
          partyId,
          orderMeta: {
            ...orderMeta,
            stage: 'ORDER_CREATE_PREPARED',
            allocationContractId: effectiveAllocationCid,
          },
        };
      }

      // Canton interactive execute may not return created contract IDs.
      // Instead of blocking with a polling loop, we return immediately.
      // The WebSocket streaming model will receive the real contract from
      // Canton and emit 'orderCreated', which triggers matching via the
      // event-driven wiring in app.js.
      if (!contractId) {
        const txUpdateId = result.transaction?.updateId || result.updateId;
        if (txUpdateId && /^[0-9a-f]{40,}$/i.test(txUpdateId)) {
          contractId = txUpdateId;
        }
      }

      const hasRealCid = !!contractId;
      if (!contractId) {
        contractId = `${orderMeta.orderId}-pending`;
      }
      
      console.log(`[OrderService] ✅ Order placed via interactive submission: ${orderMeta.orderId} (cid: ${hasRealCid ? contractId.substring(0, 30) + '...' : 'pending — WebSocket will deliver'})`);
      
      const finalAllocationCid = orderMeta.allocationContractId || allocationContractId || null;
      const orderRecord = {
        contractId,
        orderId: orderMeta.orderId,
        owner: partyId,
        tradingPair: orderMeta.tradingPair,
        orderType: orderMeta.orderType,
        orderMode: orderMeta.orderMode,
        price: orderMeta.price,
        stopPrice: orderMeta.stopPrice || null,
        quantity: orderMeta.quantity,
        filled: '0',
        status: orderMeta.orderMode === 'STOP_LOSS' ? 'PENDING_TRIGGER' : 'OPEN',
        timestamp: new Date().toISOString(),
        lockId: null,
        lockedAmount: orderMeta.lockInfo?.amount || '0',
        lockedAsset: orderMeta.lockInfo?.asset || '',
        allocationContractId: finalAllocationCid,
      };
      
      if (orderRecord.status === 'OPEN') {
        registerOpenOrders([orderRecord]);
      }
      
      // Do NOT eagerly inject the order into the streaming read model or
      // trigger matching here. The Canton WebSocket stream will deliver the
      // confirmed Order contract, which the StreamingReadModel picks up and
      // emits 'orderCreated'. That event (debounced 3s in app.js) triggers
      // matching ONLY after Canton has fully committed the Order contract.
      //
      // Triggering matching before the Order contract is fully committed
      // causes LOCKED_CONTRACTS errors because FillOrder races against the
      // still-propagating CREATE transaction.

      if (global.broadcastWebSocket && orderRecord.status === 'OPEN') {
        global.broadcastWebSocket(`orderbook:${orderMeta.tradingPair}`, {
          type: 'NEW_ORDER',
          orderId: orderMeta.orderId,
          contractId: contractId,
          owner: partyId,
          orderType: orderMeta.orderType,
          orderMode: orderMeta.orderMode,
          price: orderMeta.price,
          quantity: orderMeta.quantity,
          remaining: orderMeta.quantity,
          tradingPair: orderMeta.tradingPair,
          timestamp: new Date().toISOString()
        });
      }
      
      return {
        success: true,
        usedInteractiveSubmission: true,
        orderId: orderMeta.orderId,
        contractId,
        status: orderRecord.status,
        tradingPair: orderMeta.tradingPair,
        orderType: orderMeta.orderType,
        orderMode: orderMeta.orderMode,
        price: orderMeta.price,
        stopPrice: orderMeta.stopPrice || null,
        quantity: orderMeta.quantity,
        filled: '0',
        remaining: orderMeta.quantity,
        allocationContractId: finalAllocationCid,
        timestamp: new Date().toISOString()
      };
      
    } catch (error) {
      console.error('[OrderService] Failed to execute order placement:', error.message);
      
      // Release reservation on failure
      if (orderMeta.orderId && orderMeta.stage !== 'ORDER_CREATE_PREPARED') {
        await releaseReservation(orderMeta.orderId);
      }
      throw error;
    }
  }

  /**
   * Cancel order: cancels the Allocation (releases locked funds),
   * then exercises CancelOrder on Canton to archive the Order contract.
   */
  async cancelOrder(orderContractId, partyId, tradingPair = null) {
    if (!orderContractId || !partyId) {
      throw new ValidationError('Order contract ID and party ID are required');
    }

    console.log(`[OrderService] Cancelling order: ${orderContractId} for party: ${partyId}`);

    const token = await tokenProvider.getServiceToken();
    const packageId = config.canton.packageIds.clobExchange;
    const operatorPartyId = config.canton.operatorPartyId;

    if (!packageId) {
      throw new Error('CLOB_EXCHANGE_PACKAGE_ID is not configured');
    }

    // First, get the order details to know what was locked
    let orderDetails = null;
    try {
      const templateIdsToQuery = [`${packageId}:Order:Order`];
      const contracts = await cantonService.queryActiveContracts({
        party: partyId,
        templateIds: templateIdsToQuery,
        pageSize: 200
      }, token);
      
      const matchingContract = (Array.isArray(contracts) ? contracts : [])
        .find(c => c.contractId === orderContractId);
      
      if (matchingContract) {
        const payload = matchingContract.payload || matchingContract.createArgument || {};
        orderDetails = {
          contractId: orderContractId,
          orderId: payload.orderId,
          owner: payload.owner,
          tradingPair: payload.tradingPair,
          orderType: payload.orderType,
          orderMode: payload.orderMode,
          price: payload.price?.Some || payload.price,
          quantity: payload.quantity,
          filled: payload.filled || '0',
          status: payload.status,
          timestamp: payload.timestamp,
          allocationCid: payload.allocationCid || null
        };
        console.log(`[OrderService] Found order details: ${orderDetails.orderId}, allocationCid: ${orderDetails.allocationCid?.substring(0, 30) || 'none'}...`);
      } else {
        console.warn(`[OrderService] Order ${orderContractId.substring(0, 30)}... not found in active contracts`);
      }
    } catch (e) {
      console.warn('[OrderService] Could not fetch order details before cancel:', e.message);
    }

    // ═══ Cancel the Allocation — release locked funds via Allocation_Cancel ═══
    const orderId_cancel = orderDetails?.orderId;
    if (orderId_cancel) {
      const payloadAllocationCid = orderDetails?.allocationCid;
      const isRealCid = typeof payloadAllocationCid === 'string' && payloadAllocationCid.length > 20 && !payloadAllocationCid.startsWith('#');
      const allocationCid = isRealCid
        ? payloadAllocationCid
        : await getAllocationContractIdForOrder(orderId_cancel);
      if (allocationCid) {
        try {
          const allocationCancelResult = await this.cancelAllocationForOrder(orderId_cancel, allocationCid, partyId);
          if (allocationCancelResult?.cancelled) {
            console.log(`[OrderService] ✅ Allocation cancelled — funds released`);
          } else if (allocationCancelResult?.skipped) {
            console.log(`[OrderService] ⏭️ Allocation cancel skipped: ${allocationCancelResult.reason}`);
          } else {
            console.warn('[OrderService] ⚠️ Allocation cancel not confirmed; continuing with interactive order cancel');
          }
        } catch (allocCancelErr) {
          console.warn('[OrderService] Could not cancel Allocation:', allocCancelErr.message);
          // Continue with cancellation even if allocation cancel fails
      }
    } else {
        console.log(`[OrderService] No allocationCid for order — skipping Allocation cancel`);
      }
    }

    // Unregister stop-loss if this was a stop-loss order
    if (orderDetails?.orderMode === 'STOP_LOSS' || orderDetails?.status === 'PENDING_TRIGGER') {
      try {
        const { getStopLossService } = require('./stopLossService');
        const stopLossService = getStopLossService();
        await stopLossService.unregisterStopLoss(orderContractId);
        console.log(`[OrderService] ✅ Stop-loss unregistered for cancelled order`);
      } catch (slErr) {
        console.warn(`[OrderService] ⚠️ Could not unregister stop-loss: ${slErr.message}`);
      }
    }

    // CancelOrder is controlled by "owner" — external parties need interactive submission.
    console.log(`[OrderService] Preparing CancelOrder for interactive signing`);
    
    let prepareResult;
    try {
      prepareResult = await cantonService.prepareInteractiveSubmission({
      token,
      actAsParty: [partyId],
      templateId: `${packageId}:Order:Order`,
      contractId: orderContractId,
      choice: 'CancelOrder',
      choiceArgument: {},
      readAs: [operatorPartyId, partyId],
    });
    } catch (prepErr) {
      if (prepErr.message?.includes('CONTRACT_NOT_FOUND') || prepErr.message?.includes('could not be found')) {
        console.warn(`[OrderService] Order contract already consumed/archived — treating as cancelled`);
        const readModel = getReadModelService();
        if (readModel) readModel.removeOrder(orderContractId);
        _globalOpenOrders.delete(orderContractId);
        if (orderDetails?.orderId) await releaseReservation(orderDetails.orderId);
        return {
          cancelled: true,
          alreadyArchived: true,
          orderContractId,
          orderId: orderDetails?.orderId,
          message: 'Order contract was already consumed (filled or archived). No action needed.',
        };
      }
      throw prepErr;
    }
    
    if (!prepareResult.preparedTransaction || !prepareResult.preparedTransactionHash) {
      throw new Error('Prepare returned incomplete result for CancelOrder');
    }
    
    console.log(`[OrderService] ✅ CancelOrder prepared. Hash to sign: ${prepareResult.preparedTransactionHash.substring(0, 40)}...`);
    
    return {
      requiresSignature: true,
      step: 'PREPARED',
      action: 'CANCEL',
      orderContractId,
      orderId: orderDetails?.orderId,
      tradingPair: orderDetails?.tradingPair || tradingPair,
      preparedTransaction: prepareResult.preparedTransaction,
      preparedTransactionHash: prepareResult.preparedTransactionHash,
      hashingSchemeVersion: prepareResult.hashingSchemeVersion,
      partyId,
      orderDetails,
    };
  }

  /**
   * Cancel order with UTXO handling (wrapper)
   */
  async cancelOrderWithUTXOHandling(
    partyId,
    tradingPair,
    orderType,
    orderContractId,
    orderBookContractId = null,
    userAccountContractId = null
  ) {
    return this.cancelOrder(orderContractId, partyId);
  }

  /**
   * STEP 2: Execute a prepared order cancellation with the user's signature
   * 
   * Called after the frontend signs the preparedTransactionHash from cancelOrder()
   * 
   * @param {string} preparedTransaction - Opaque blob from prepare step
   * @param {string} partyId - The external party that signed
   * @param {string} signatureBase64 - User's Ed25519 signature
   * @param {string} signedBy - Public key fingerprint
   * @param {string|number} hashingSchemeVersion - From prepare response
   * @param {object} cancelMeta - { orderContractId, orderId, tradingPair, orderDetails }
   * @returns {Object} Cancellation result
   */
  async executeOrderCancel(preparedTransaction, partyId, signatureBase64, signedBy, hashingSchemeVersion, cancelMeta = {}) {
    const token = await tokenProvider.getServiceToken();
    const operatorPartyId = config.canton.operatorPartyId;
    
    try {
      console.log(`[OrderService] EXECUTE order cancel for ${partyId.substring(0, 30)}...`);
      
      const partySignatures = {
        signatures: [
          {
            party: partyId,
            signatures: [{
              format: 'SIGNATURE_FORMAT_RAW',
              signature: signatureBase64,
              signedBy: signedBy,
              signingAlgorithmSpec: 'SIGNING_ALGORITHM_SPEC_ED25519'
            }]
          }
        ]
      };
      
      const result = await cantonService.executeInteractiveSubmission({
        preparedTransaction,
        partySignatures,
        hashingSchemeVersion,
      }, token);
      
      console.log(`[OrderService] ✅ Order cancelled via interactive submission: ${cancelMeta.orderContractId?.substring(0, 20)}...`);
      
      // Release balance reservation
      if (cancelMeta.orderId) {
        await releaseReservation(cancelMeta.orderId);
      }
      
      // Remove from tracking
      const readModel = getReadModelService();
      if (readModel) {
        readModel.removeOrder(cancelMeta.orderContractId);
      }
      
      // Unregister from global registry
      if (cancelMeta.orderContractId) {
        _globalOpenOrders.delete(cancelMeta.orderContractId);
      }
      
      // Broadcast via WebSocket
      if (global.broadcastWebSocket && cancelMeta.tradingPair) {
        global.broadcastWebSocket(`orderbook:${cancelMeta.tradingPair}`, {
          type: 'ORDER_CANCELLED',
          contractId: cancelMeta.orderContractId,
          orderId: cancelMeta.orderId,
          tradingPair: cancelMeta.tradingPair,
          timestamp: new Date().toISOString()
        });
      }
      
      return {
        success: true,
        usedInteractiveSubmission: true,
        cancelled: true,
        orderContractId: cancelMeta.orderContractId,
        orderId: cancelMeta.orderId,
        tradingPair: cancelMeta.tradingPair,
      };
      
    } catch (error) {
      console.error('[OrderService] Failed to execute order cancel:', error.message);
      throw error;
    }
  }

  /**
   * Get user's orders DIRECTLY from Canton API
   * NO CACHE - always queries Canton
   */
  async getUserOrders(partyId, status = 'OPEN', limit = 100) {
    if (!partyId) {
      throw new ValidationError('Party ID is required');
    }

    console.log(`[OrderService] Querying Canton DIRECTLY for party: ${partyId.substring(0, 30)}...`);
    
    const token = await tokenProvider.getServiceToken();
    const packageId = config.canton.packageIds.clobExchange;

    if (!packageId) {
      throw new Error('CLOB_EXCHANGE_PACKAGE_ID is not configured');
    }

    try {
      const templateIdsToQuery = [`${packageId}:Order:Order`];
      const contracts = await cantonService.queryActiveContracts({
        party: partyId,
        templateIds: templateIdsToQuery,
        pageSize: limit
      }, token);

      const orders = (Array.isArray(contracts) ? contracts : [])
        .filter(c => {
          const templateId = c.templateId;
          if (!templateId?.includes(':Order:Order')) {
            return false;
          }
          const payload = c.payload || c.createArgument || {};
          if (payload.owner !== partyId) return false;
          return status === 'ALL' || payload.status === status;
        })
        .map(c => {
          const payload = c.payload || c.createArgument || {};
          const contractId = c.contractId;
          
          let extractedPrice = null;
          if (payload.price) {
            if (payload.price.Some !== undefined) {
              extractedPrice = payload.price.Some;
            } else if (typeof payload.price === 'string' || typeof payload.price === 'number') {
              extractedPrice = payload.price;
            } else if (payload.price === null) {
              extractedPrice = null;
            }
          }
          
          return {
            contractId: contractId,
            orderId: payload.orderId,
            owner: payload.owner,
            tradingPair: payload.tradingPair,
            orderType: payload.orderType,
            orderMode: payload.orderMode,
            price: extractedPrice,
            quantity: payload.quantity,
            filled: payload.filled || '0',
            status: payload.status,
            timestamp: payload.timestamp,
            allocationCid: payload.allocationCid || null
          };
        });

      console.log(`[OrderService] Found ${orders.length} orders from Canton for ${partyId.substring(0, 30)}...`);
      
      // Register OPEN orders in the global registry so the OrderBookService can see them
      // (handles orders placed through other backend instances where operator is not a stakeholder)
      registerOpenOrders(orders);
      
      return orders;
    } catch (error) {
      if (error.message?.includes('200') || error.message?.includes('MAXIMUM_LIST')) {
        console.log('[OrderService] 200+ contracts, using operator party query');
        try {
          const operatorPartyId = config.canton.operatorPartyId;
          const contracts = await cantonService.queryActiveContracts({
            party: operatorPartyId,
            templateIds: [`${packageId}:Order:Order`],
            pageSize: 50
          }, token);
          
          const orders = (Array.isArray(contracts) ? contracts : [])
            .filter(c => {
              const payload = c.payload || c.createArgument || {};
              return payload.owner === partyId && 
                     (status === 'ALL' || payload.status === status);
            })
            .map(c => {
              const payload = c.payload || c.createArgument || {};
              return {
                contractId: c.contractId,
                orderId: payload.orderId,
                owner: payload.owner,
                tradingPair: payload.tradingPair,
                orderType: payload.orderType,
                orderMode: payload.orderMode,
                price: payload.price?.Some || payload.price,
                quantity: payload.quantity,
                filled: payload.filled || '0',
                status: payload.status,
                timestamp: payload.timestamp
              };
            });
          return orders;
        } catch (fallbackError) {
          console.error('[OrderService] Fallback query also failed:', fallbackError.message);
          return [];
        }
      }
      console.error('[OrderService] Error getting user orders from Canton:', error.message);
      return [];
    }
  }

  /**
   * Get all open orders for a trading pair (Global Order Book)
   */
  async getOrdersForPair(tradingPair, limit = 200) {
    console.log(`[OrderService] Getting all orders for pair: ${tradingPair}`);

    const token = await tokenProvider.getServiceToken();
    const packageId = config.canton.packageIds.clobExchange;
    const operatorPartyId = config.canton.operatorPartyId;

    if (!packageId) {
      throw new Error('CLOB_EXCHANGE_PACKAGE_ID is not configured');
    }

    try {
      const contracts = await cantonService.queryActiveContracts({
        party: operatorPartyId,
        templateIds: [`${packageId}:Order:Order`],
        pageSize: limit
      }, token);

      // Filter by trading pair and OPEN status (exclude PENDING_TRIGGER stop-loss orders)
      const orders = (Array.isArray(contracts) ? contracts : [])
        .filter(c => {
          const payload = c.payload || c.createArgument || {};
          return payload.tradingPair === tradingPair && payload.status === 'OPEN';
        })
        .map(c => {
          const payload = c.payload || c.createArgument || {};
          return {
            contractId: c.contractId,
            orderId: payload.orderId,
            owner: payload.owner,
            tradingPair: payload.tradingPair,
            orderType: payload.orderType,
            orderMode: payload.orderMode,
            price: payload.price?.Some || payload.price,
            quantity: payload.quantity,
            filled: payload.filled || '0',
            remaining: (parseFloat(payload.quantity) - parseFloat(payload.filled || 0)).toString(),
            status: payload.status,
            timestamp: payload.timestamp,
            allocationCid: payload.allocationCid || null,
          };
        });

      const buyOrders = orders
        .filter(o => o.orderType === 'BUY')
        .sort((a, b) => {
          const priceA = parseFloat(a.price) || Infinity;
          const priceB = parseFloat(b.price) || Infinity;
          if (priceA !== priceB) return priceB - priceA;
          return new Date(a.timestamp) - new Date(b.timestamp);
        });

      const sellOrders = orders
        .filter(o => o.orderType === 'SELL')
        .sort((a, b) => {
          const priceA = parseFloat(a.price) || 0;
          const priceB = parseFloat(b.price) || 0;
          if (priceA !== priceB) return priceA - priceB;
          return new Date(a.timestamp) - new Date(b.timestamp);
        });

      console.log(`[OrderService] Found ${buyOrders.length} buys, ${sellOrders.length} sells for ${tradingPair}`);

      return {
        tradingPair,
        buyOrders,
        sellOrders,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error('[OrderService] Error getting orders for pair:', error.message);
      return {
        tradingPair,
        buyOrders: [],
        sellOrders: [],
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Get order by contract ID.
   * Uses streaming read model first (avoids Canton lookup which may 404 on some deployments).
   */
  async getOrder(orderContractId) {
    if (!orderContractId) {
      throw new ValidationError('Order contract ID is required');
    }

    try {
      const readModel = getReadModelService();
      const fromCache = await readModel.getOrderByContractId(orderContractId);
      if (fromCache) {
        return {
          contractId: fromCache.contractId,
          orderId: fromCache.orderId,
          owner: fromCache.owner,
          tradingPair: fromCache.tradingPair,
          orderType: fromCache.orderType,
          orderMode: fromCache.orderMode || 'LIMIT',
          price: fromCache.price?.Some ?? fromCache.price,
          quantity: fromCache.quantity,
          filled: fromCache.filled || '0',
          status: fromCache.status,
          timestamp: fromCache.timestamp,
          allocationCid: fromCache.allocationCid || null
        };
      }

      const token = await tokenProvider.getServiceToken();
      const contract = await cantonService.lookupContract(orderContractId, token);
      if (!contract) {
        throw new NotFoundError(`Order not found: ${orderContractId}`);
      }

      const payload = contract.payload || contract.createArgument || {};
      return {
        contractId: orderContractId,
        orderId: payload.orderId,
        owner: payload.owner,
        tradingPair: payload.tradingPair,
        orderType: payload.orderType,
        orderMode: payload.orderMode,
        price: payload.price?.Some || payload.price,
        quantity: payload.quantity,
        filled: payload.filled || '0',
        status: payload.status,
        timestamp: payload.timestamp,
        allocationCid: payload.allocationCid || null
      };
    } catch (error) {
      if (error instanceof NotFoundError || error instanceof ValidationError) throw error;
      console.error('[OrderService] Error getting order:', error.message);
      throw error;
    }
  }
}

module.exports = OrderService;

// Export reservation helpers for use by the matching engine
module.exports.releaseReservation = releaseReservation;
module.exports.releasePartialReservation = releasePartialReservation;
module.exports.getReservedBalance = getReservedBalance;
module.exports.getAllocationContractIdForOrder = getAllocationContractIdForOrder;
module.exports.setAllocationContractIdForOrder = setAllocationContractIdForOrder;
module.exports.getAllocationTypeForOrder = getAllocationTypeForOrder;
module.exports.getGlobalOpenOrders = getGlobalOpenOrders;
