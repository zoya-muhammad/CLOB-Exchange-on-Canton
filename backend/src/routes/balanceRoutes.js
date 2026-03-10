/**
 * Balance Routes — Canton Wallet SDK
 * 
 * ALL balance operations go through the Canton Wallet SDK
 * which queries real holdings from the Canton ledger (UTXO-based).
 * 
 * Supported instruments: CC (Amulet), CBTC (case-sensitive).
 * 
 * @see https://docs.digitalasset.com/integrate/devnet/token-standard/index.html
 */

const express = require('express');
const router = express.Router();
const Decimal = require('decimal.js');
const { success, error } = require('../utils/response');
const asyncHandler = require('../middleware/asyncHandler');
const { ValidationError } = require('../utils/errors');
const { getCantonSDKClient } = require('../services/canton-sdk-client');
const { getAllNetTradeBalances } = require('../services/tradeSettlementService');
const { getDb } = require('../services/db');

Decimal.set({ precision: 20, rounding: Decimal.ROUND_DOWN });

// ─────────────────────────────────────────────────────────
// GET /api/balance/:partyId
// Primary balance endpoint — Canton SDK (UTXO-based)
// ─────────────────────────────────────────────────────────
router.get('/:partyId', asyncHandler(async (req, res) => {
  const { partyId } = req.params;
  
  if (!partyId) {
    throw new ValidationError('Party ID is required');
  }

  console.log(`[Balance] Getting balance for party: ${partyId.substring(0, 30)}...`);
  
  const sdkClient = getCantonSDKClient();
  
  try {
    // ─── 1. Splice holdings from Canton SDK (UTXO-based) ───
    const balances = await sdkClient.getAllBalances(partyId);
    
    const available = {};
    const locked = {};
    const total = {};
    
    for (const [sym, amt] of Object.entries(balances.available)) {
      available[sym] = parseFloat(amt) || 0;
    }
    for (const [sym, amt] of Object.entries(balances.locked)) {
      locked[sym] = parseFloat(amt) || 0;
    }
    for (const [sym, amt] of Object.entries(balances.total)) {
      total[sym] = parseFloat(amt) || 0;
    }

    // ─── 2. Trade credits/debits from PostgreSQL ───
    // We add trade credits and subtract trade debits for balance tracking.
    try {
      const tradeAdjustments = await getAllNetTradeBalances(partyId);
      for (const [sym, adj] of Object.entries(tradeAdjustments)) {
        const adjNum = parseFloat(adj.toString()) || 0;
        if (adjNum !== 0) {
          available[sym] = (available[sym] || 0) + adjNum;
          total[sym] = (total[sym] || 0) + adjNum;
        }
      }
    } catch (tradeErr) {
      console.warn(`[Balance] Trade adjustments lookup failed (non-critical): ${tradeErr.message}`);
    }

    // ─── 3. Subtract open order reservations ───
    // Funds reserved for open orders reduce available balance.
    try {
      const db = getDb();
      const reservations = await db.orderReservation.findMany({
        where: { partyId },
        select: { asset: true, amount: true },
      });
      for (const r of reservations) {
        const resAmt = parseFloat(r.amount || '0');
        if (resAmt > 0 && r.asset) {
          available[r.asset] = (available[r.asset] || 0) - resAmt;
        }
      }
    } catch (resErr) {
      console.warn(`[Balance] Reservation lookup failed (non-critical): ${resErr.message}`);
    }

    // Clamp negatives to zero (rounding can cause tiny negatives)
    for (const sym of Object.keys(available)) {
      if (available[sym] < 0) available[sym] = 0;
    }

    console.log(`[Balance] Balances (hybrid):`, available);

    return success(res, {
      partyId,
      balance: available,
      available,
      locked,
      total,
      holdings: [],
      tokenStandard: true,
      source: 'canton-sdk-hybrid',
    }, 'Balances retrieved (hybrid: Splice holdings + trade adjustments)');
  } catch (err) {
    console.error(`[Balance] Canton SDK query failed:`, err.message);
    return success(res, {
      partyId,
      balance: {},
      available: {},
      locked: {},
      total: {},
      holdings: [],
      tokenStandard: true,
      source: 'canton-sdk',
    }, 'No balances found');
  }
}));

// ─────────────────────────────────────────────────────────
// POST /api/balance/mint
// Mint tokens by transferring from faucet via Canton SDK
// ─────────────────────────────────────────────────────────
router.post('/mint', asyncHandler(async (req, res) => {
  const { partyId, tokens } = req.body;
  
  if (!partyId) {
    throw new ValidationError('Party ID is required');
  }

  if (!tokens || !Array.isArray(tokens)) {
    throw new ValidationError('Tokens array is required');
  }

  console.log(`[Balance] Minting for party: ${partyId.substring(0, 30)}...`, tokens);

  const sdkClient = getCantonSDKClient();
  const FAUCET_PARTY = process.env.FAUCET_PARTY_ID || 'faucet::1220faucet';
  
  const results = [];
  
  for (const tokenInfo of tokens) {
    try {
      if (!sdkClient.isReady()) {
        throw new Error('Canton SDK not initialized — cannot mint');
      }
      const result = await sdkClient.executeFullTransfer(
        FAUCET_PARTY,
        partyId,
        String(tokenInfo.amount),
        tokenInfo.symbol,
        `faucet_mint_${Date.now()}`
      );
      
      // If Accept was skipped (faucet transfer), auto-accept service will handle it
      if (result.skippedAccept && result.transferInstructionCid) {
        console.log(`[Balance] TransferInstruction created: ${result.transferInstructionCid.substring(0, 30)}... — auto-accept will complete within seconds`);
        
        // Ensure auto-accept is aware of this party (if not already subscribed)
        try {
          const { getAutoAcceptService } = require('../services/autoAcceptService');
          const autoAcceptService = getAutoAcceptService();
          if (autoAcceptService.isRunning) {
            await autoAcceptService.onNewPartyRegistered(partyId);
          }
        } catch (autoAcceptErr) {
          console.warn(`[Balance] Auto-accept notification skipped: ${autoAcceptErr.message}`);
        }
      }
      
      results.push({
        symbol: tokenInfo.symbol,
        amount: tokenInfo.amount,
        status: 'minted',
        updateId: result.updateId || null,
        transferInstructionCid: result.transferInstructionCid || null,
        autoAcceptPending: result.skippedAccept || false,
      });
      console.log(`[Balance] Transferred ${tokenInfo.amount} ${tokenInfo.symbol} from faucet to ${partyId.substring(0, 30)}...`);
    } catch (err) {
      console.error(`[Balance] Failed to mint ${tokenInfo.symbol}:`, err.message);
      results.push({
        symbol: tokenInfo.symbol,
        amount: tokenInfo.amount,
        status: 'failed',
        error: err.message,
      });
    }
  }

  const successful = results.filter(r => r.status === 'minted');
  const failed = results.filter(r => r.status === 'failed');

  return success(res, {
    partyId,
    minted: successful,
    failed,
    tokenStandard: true,
    source: 'canton-sdk',
  }, `Minted ${successful.length}/${tokens.length} tokens`, 201);
}));

// ─────────────────────────────────────────────────────────
// V2 ENDPOINTS (same behavior — Canton SDK only)
// ─────────────────────────────────────────────────────────

/**
 * GET /api/balance/v2/:partyId
 * Get balance — Canton SDK (UTXO-based).
 */
router.get('/v2/:partyId', asyncHandler(async (req, res) => {
  const { partyId } = req.params;
  
  if (!partyId) {
    throw new ValidationError('Party ID is required');
  }

  console.log(`[Balance V2] Getting balance for party: ${partyId.substring(0, 30)}...`);
  
  const sdkClient = getCantonSDKClient();
  
  try {
    // ─── 1. Splice holdings from Canton SDK ───
    const balances = await sdkClient.getAllBalances(partyId);
    
    const available = {};
    const locked = {};
    const total = {};
    
    for (const [sym, amt] of Object.entries(balances.available)) {
      available[sym] = parseFloat(amt) || 0;
    }
    for (const [sym, amt] of Object.entries(balances.locked)) {
      locked[sym] = parseFloat(amt) || 0;
    }
    for (const [sym, amt] of Object.entries(balances.total)) {
      total[sym] = parseFloat(amt) || 0;
    }

    // ─── 2. Trade credits/debits (hybrid model) ───
    try {
      const tradeAdjustments = await getAllNetTradeBalances(partyId);
      for (const [sym, adj] of Object.entries(tradeAdjustments)) {
        const adjNum = parseFloat(adj.toString()) || 0;
        if (adjNum !== 0) {
          available[sym] = (available[sym] || 0) + adjNum;
          total[sym] = (total[sym] || 0) + adjNum;
        }
      }
    } catch (tradeErr) {
      console.warn(`[Balance V2] Trade adjustments failed (non-critical): ${tradeErr.message}`);
    }

    // ─── 3. Subtract open order reservations (tokens locked at order placement) ───
    const reserved = {};
    try {
      const db = getDb();
      const reservations = await db.orderReservation.findMany({
        where: { partyId },
        select: { asset: true, amount: true },
      });
      for (const r of reservations) {
        const resAmt = parseFloat(r.amount || '0');
        if (resAmt > 0 && r.asset) {
          available[r.asset] = (available[r.asset] || 0) - resAmt;
          reserved[r.asset] = (reserved[r.asset] || 0) + resAmt;
        }
      }
    } catch (resErr) {
      console.warn(`[Balance V2] Reservation lookup failed (non-critical): ${resErr.message}`);
    }

    // Clamp negatives to zero
    for (const sym of Object.keys(available)) {
      if (available[sym] < 0) available[sym] = 0;
    }

    console.log(`[Balance V2] Balances (hybrid) for ${partyId.substring(0, 30)}...:`, available, 'reserved:', reserved);

    return success(res, {
      partyId,
      balance: available,
      available,
      locked,
      reserved, // Tokens locked in open orders (AllocationFactory_Allocate on-chain + DB reservation)
      total,
      holdings: [],
      source: 'canton-sdk-hybrid',
      tokenStandard: true,
    }, 'Balances retrieved (hybrid: Splice holdings + trade adjustments)');
  } catch (err) {
    console.error('[Balance V2] Canton SDK query failed:', err.message);
    return success(res, {
      partyId,
      balance: {},
      available: {},
      locked: {},
      total: {},
      holdings: [],
      source: 'canton-sdk',
      tokenStandard: true,
    }, 'No balances found');
  }
}));

/**
 * POST /api/balance/v2/mint
 * Mint tokens via Canton SDK faucet transfer.
 */
router.post('/v2/mint', asyncHandler(async (req, res) => {
  const { partyId, tokens } = req.body;
  
  if (!partyId) {
    throw new ValidationError('Party ID is required');
  }

  if (!tokens || !Array.isArray(tokens)) {
    throw new ValidationError('Tokens array is required (e.g., [{ symbol: "CC", amount: 10000 }])');
  }

  console.log(`[Balance V2] Minting for party: ${partyId.substring(0, 30)}...`, tokens);

  const sdkClient = getCantonSDKClient();
  const FAUCET_PARTY = process.env.FAUCET_PARTY_ID || 'faucet::1220faucet';

  const results = [];
  
  for (const tokenInfo of tokens) {
    try {
      if (!sdkClient.isReady()) {
        throw new Error('Canton SDK not initialized — cannot mint');
      }
      const result = await sdkClient.executeFullTransfer(
        FAUCET_PARTY,
        partyId,
        String(tokenInfo.amount),
        tokenInfo.symbol,
        `faucet_mint_v2_${Date.now()}`
      );
      
      // If Accept was skipped (faucet transfer), auto-accept service will handle it
      if (result.skippedAccept && result.transferInstructionCid) {
        console.log(`[Balance V2] TransferInstruction created: ${result.transferInstructionCid.substring(0, 30)}... — auto-accept will complete within seconds`);
        
        // Ensure auto-accept is aware of this party (if not already subscribed)
        try {
          const { getAutoAcceptService } = require('../services/autoAcceptService');
          const autoAcceptService = getAutoAcceptService();
          if (autoAcceptService.isRunning) {
            await autoAcceptService.onNewPartyRegistered(partyId);
          }
        } catch (autoAcceptErr) {
          console.warn(`[Balance V2] Auto-accept notification skipped: ${autoAcceptErr.message}`);
        }
      }
      
      results.push({
        symbol: tokenInfo.symbol,
        amount: tokenInfo.amount,
        status: 'minted',
        updateId: result.updateId || null,
        transferInstructionCid: result.transferInstructionCid || null,
        autoAcceptPending: result.skippedAccept || false,
      });
      console.log(`[Balance V2] Transferred ${tokenInfo.amount} ${tokenInfo.symbol} from faucet to ${partyId.substring(0, 30)}...`);
    } catch (err) {
      console.error(`[Balance V2] Failed to mint ${tokenInfo.symbol}:`, err.message);
      results.push({
        symbol: tokenInfo.symbol,
        amount: tokenInfo.amount,
        status: 'failed',
        error: err.message,
      });
    }
  }

  const successful = results.filter(r => r.status === 'minted');
  const failed = results.filter(r => r.status === 'failed');

  return success(res, {
    partyId,
    minted: successful,
    failed,
    tokenStandard: true,
    source: 'canton-sdk',
  }, `Minted ${successful.length}/${tokens.length} tokens`, 201);
}));

/**
 * GET /api/balance/v2/holdings/:partyId
 * Get holding UTXOs from Canton SDK.
 */
router.get('/v2/holdings/:partyId', asyncHandler(async (req, res) => {
  const { partyId } = req.params;
  const { symbol } = req.query;
  
  if (!partyId) {
    throw new ValidationError('Party ID is required');
  }

  console.log(`[Balance V2] Getting holdings for party: ${partyId.substring(0, 30)}...${symbol ? ` (${symbol})` : ''}`);

  const sdkClient = getCantonSDKClient();
  
  try {
    if (symbol) {
      // Single instrument query
      const balance = await sdkClient.getBalance(partyId, symbol);
      return success(res, {
        partyId,
        symbol,
        holdings: [{
          type: 'canton-sdk-utxo',
          instrument: symbol,
          available: balance.available || '0',
          locked: balance.locked || '0',
          total: balance.total || '0',
        }],
        count: 1,
        totalAmount: parseFloat(balance.total || '0'),
        source: 'canton-sdk',
      }, `${symbol} balance from Canton SDK`);
    }
    
    // All instruments
    const balances = await sdkClient.getAllBalances(partyId);
    const holdings = [];
    const summary = {};
    
    for (const sym of Object.keys(balances.available)) {
      const avail = balances.available[sym] || '0';
      const lock = balances.locked[sym] || '0';
      const tot = balances.total[sym] || '0';
      
      holdings.push({
        type: 'canton-sdk-utxo',
        instrument: sym,
        available: avail,
        locked: lock,
        total: tot,
      });
      summary[sym] = parseFloat(avail) || 0;
    }
    
    return success(res, {
      partyId,
      holdings,
      count: holdings.length,
      summary,
      source: 'canton-sdk',
    }, `Found ${holdings.length} instruments`);
  } catch (err) {
    console.error('[Balance V2] Failed to get holdings:', err.message);
    return success(res, {
      partyId,
      holdings: [],
      count: 0,
      summary: {},
      source: 'canton-sdk',
    }, 'No holdings found');
  }
}));

/**
 * POST /api/balance/v2/lock
 * Lock funds — NOT supported in SDK mode.
 * With the SDK approach, holdings are locked naturally when
 * a 2-step transfer instruction is created at settlement time.
 */
router.post('/v2/lock', asyncHandler(async (req, res) => {
  const { lockReason, lockAmount, ownerPartyId, instrument } = req.body;
  
  if (!lockReason || !lockAmount || !ownerPartyId || !instrument) {
    throw new ValidationError('Required fields: instrument, lockReason, lockAmount, ownerPartyId');
  }

  console.log(`[Balance V2] Lock request: ${lockAmount} ${instrument} for ${ownerPartyId.substring(0, 30)}...`);
  console.log(`[Balance V2] ⚠️ Explicit locking not supported in SDK mode — holdings are locked at transfer time`);
  
  // In SDK mode, there's no explicit lock API. Holdings are locked when
  // the matching engine creates a TransferInstruction (createTransfer).
  // Return a simulated lock response for backward compatibility.
  return success(res, {
    lockId: `sdk-no-lock-${Date.now()}`,
    instrument,
    lockReason,
    lockAmount,
    remainingBalance: null,
    source: 'canton-sdk',
    note: 'SDK mode: Holdings are locked at transfer time, not at order placement.',
  }, 'Lock acknowledged (SDK mode: actual lock occurs at settlement)');
}));

/**
 * POST /api/balance/v2/unlock
 * Unlock funds — NOT supported in SDK mode.
 * With the SDK approach, locked holdings are released when the
 * TransferInstruction is withdrawn (Withdraw choice).
 */
router.post('/v2/unlock', asyncHandler(async (req, res) => {
  const { lockId } = req.body;
  
  if (!lockId) {
    throw new ValidationError('lockId is required');
  }

  console.log(`[Balance V2] Unlock request: ${lockId.substring(0, 30)}...`);
  console.log(`[Balance V2] ⚠️ Explicit unlocking not supported in SDK mode — use transfer withdrawal`);
  
  // In SDK mode, there's no explicit unlock API. Locked holdings are released
  // when a TransferInstruction is withdrawn by the sender.
  return success(res, {
    lockId,
    amount: null,
    remainingBalance: null,
    source: 'canton-sdk',
    note: 'SDK mode: Locked holdings are released via TransferInstruction withdrawal.',
  }, 'Unlock acknowledged (SDK mode: use transfer withdrawal to release)');
}));

module.exports = router;
