/**
 * TradingApp Settlement Service
 *
 * When USE_TRADING_APP_PATTERN=true:
 * - Order placement: self-allocation (sender=receiver=user)
 * - Match: create PendingSettlement, both parties sign withdraw + multi-leg
 * - Tokens flow only between users (no operator custody)
 */

const { PrismaClient } = require('@prisma/client');
const config = require('../config');
const cantonService = require('./cantonService');
const tokenProvider = require('./tokenProvider');
const { getCantonSDKClient } = require('./canton-sdk-client');

const prisma = new PrismaClient();

/**
 * Create pending settlement when match occurs (TradingApp flow).
 */
async function createPendingSettlement(match) {
  const {
    tradeId,
    tradingPair,
    sellerPartyId,
    buyerPartyId,
    sellOrderId,
    buyOrderId,
    sellOrderContractId,
    buyOrderContractId,
    sellOrderTemplateId,
    buyOrderTemplateId,
    sellOrderRemaining,
    buyOrderRemaining,
    sellIsPartial,
    buyIsPartial,
    matchPrice,
    sellAllocCid,
    buyAllocCid,
    baseSymbol,
    quoteSymbol,
    matchQty,
    quoteAmount,
  } = match;

  const data = {
    id: tradeId,
    tradingPair,
    sellerPartyId,
    buyerPartyId,
    sellOrderId,
    buyOrderId,
    sellAllocCid,
    buyAllocCid,
    baseSymbol,
    quoteSymbol,
    matchQty: String(matchQty),
    quoteAmount: String(quoteAmount),
    status: 'PENDING_WITHDRAW',
    sellIsPartial: Boolean(sellIsPartial),
    buyIsPartial: Boolean(buyIsPartial),
  };
  if (sellOrderContractId != null) data.sellOrderContractId = sellOrderContractId;
  if (buyOrderContractId != null) data.buyOrderContractId = buyOrderContractId;
  if (sellOrderTemplateId != null) data.sellOrderTemplateId = sellOrderTemplateId;
  if (buyOrderTemplateId != null) data.buyOrderTemplateId = buyOrderTemplateId;
  if (sellOrderRemaining != null) data.sellOrderRemaining = String(sellOrderRemaining);
  if (buyOrderRemaining != null) data.buyOrderRemaining = String(buyOrderRemaining);
  if (matchPrice != null) data.matchPrice = String(matchPrice);

  try {
    await prisma.pendingSettlement.create({ data });
    return tradeId;
  } catch (err) {
    const meta = err.meta || {};
    console.error('[TradingAppSettlement] createPendingSettlement failed:', err.message);
    if (meta.target) console.error('[TradingAppSettlement] Constraint:', meta.target);
    if (meta.field_name) console.error('[TradingAppSettlement] Field:', meta.field_name);
    throw err;
  }
}

/**
 * Prepare withdraw for a party (seller or buyer). Returns prepared tx for frontend to sign.
 * If allocation is already gone (CONTRACT_NOT_FOUND), marks withdrawal as done and throws
 * so the UI can refresh and stop showing "Sign Withdraw" for that item.
 */
async function prepareWithdraw(matchId, partyId, token) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending) throw new Error('Pending settlement not found');

  const isSeller = pending.sellerPartyId === partyId;
  const isBuyer = pending.buyerPartyId === partyId;
  if (!isSeller && !isBuyer) throw new Error('Party not part of this settlement');

  const allocCid = isSeller ? pending.sellAllocCid : pending.buyAllocCid;
  const symbol = isSeller ? pending.baseSymbol : pending.quoteSymbol;

  try {
    const sdkClient = getCantonSDKClient();
    const prepareResult = await sdkClient.prepareWithdrawInteractive(allocCid, partyId, symbol, token);

    return {
      matchId,
      role: isSeller ? 'seller' : 'buyer',
      ...prepareResult,
    };
  } catch (err) {
    const msg = String(err?.message || err || '');
    const isContractNotFound = msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found');

    if (isContractNotFound) {
      // Allocation already consumed (withdrawn or executed) — sync DB so UI stops showing Sign Withdraw
      console.log(`[TradingAppSettlement] Allocation ${allocCid?.substring(0, 24)}... already gone — marking ${isSeller ? 'seller' : 'buyer'} withdrawn`);
      await prisma.pendingSettlement.update({
        where: { id: matchId },
        data: {
          sellerWithdrawn: isSeller ? true : pending.sellerWithdrawn,
          buyerWithdrawn: isBuyer ? true : pending.buyerWithdrawn,
          status: (isSeller ? pending.buyerWithdrawn : pending.sellerWithdrawn) ? 'PENDING_MULTILEG' : 'PENDING_WITHDRAW',
        },
      });
      throw new Error('ALREADY_WITHDRAWN: Allocation already withdrawn or settled. Refresh the list.');
    }
    throw err;
  }
}

/**
 * Submit signed withdraw. Called after user signs via frontend.
 */
async function submitWithdraw(matchId, partyId, partySignatures, token) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending) throw new Error('Pending settlement not found');

  const isSeller = pending.sellerPartyId === partyId;
  const isBuyer = pending.buyerPartyId === partyId;
  if (!isSeller && !isBuyer) throw new Error('Party not part of this settlement');

  // Need preparedTransaction from when we prepared - we don't store it. The frontend
  // must call prepareWithdraw, sign, then call submitWithdraw with the same preparedTransaction.
  // So we need submitWithdraw to accept preparedTransaction + partySignatures.
  throw new Error('submitWithdraw requires preparedTransaction - use submitSignedWithdraw');
}

/**
 * Submit signed withdraw. Frontend calls prepareWithdraw, user signs, then calls this.
 */
async function submitSignedWithdraw(matchId, partyId, preparedTransaction, partySignatures, hashingSchemeVersion, token) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending) throw new Error('Pending settlement not found');

  const isSeller = pending.sellerPartyId === partyId;
  const isBuyer = pending.buyerPartyId === partyId;
  if (!isSeller && !isBuyer) throw new Error('Party not part of this settlement');

  const adminToken = await tokenProvider.getServiceToken();
  await cantonService.executeInteractiveSubmission(
    {
      preparedTransaction,
      partySignatures,
      hashingSchemeVersion: hashingSchemeVersion || 'HASHING_SCHEME_VERSION_V2',
      submissionId: `withdraw-${matchId}-${partyId.substring(0, 12)}`,
    },
    adminToken
  );

  await prisma.pendingSettlement.update({
    where: { id: matchId },
    data: {
      sellerWithdrawn: isSeller ? true : pending.sellerWithdrawn,
      buyerWithdrawn: isBuyer ? true : pending.buyerWithdrawn,
      status: (isSeller ? pending.buyerWithdrawn : pending.sellerWithdrawn) ? 'PENDING_MULTILEG' : 'PENDING_WITHDRAW',
    },
  });

  // Verify holding state after withdraw — Allocation_Withdraw should unlock tokens
  const symbol = isSeller ? pending.baseSymbol : pending.quoteSymbol;
  let holdingState = null;
  try {
    const sdkClient = getCantonSDKClient();
    holdingState = await sdkClient.verifyHoldingState(partyId, symbol);
    console.log(`[TradingAppSettlement] ✅ Withdraw executed for ${partyId.substring(0, 30)}... — holdings: available=${holdingState.totalAvailable} ${symbol}, locked=${holdingState.totalLocked}`);
    if (holdingState.totalLocked !== '0' && holdingState.locked?.length > 0) {
      console.warn(`[TradingAppSettlement] ⚠️ Party still has locked holdings: ${holdingState.locked.map(h => `${h.amount} ${h.exchangeSymbol}`).join(', ')}`);
    }
  } catch (verifyErr) {
    console.warn(`[TradingAppSettlement] Holding verification skipped: ${verifyErr.message}`);
  }

  return {
    success: true,
    holdingState: holdingState
      ? {
          totalAvailable: holdingState.totalAvailable,
          totalLocked: holdingState.totalLocked,
          symbol,
          unlocked: holdingState.totalLocked === '0',
        }
      : null,
  };
}

/**
 * Prepare multi-leg allocation. Both parties must sign. Call after both withdraws done.
 */
async function prepareMultiLeg(matchId, token) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending) throw new Error('Pending settlement not found');
  if (!pending.sellerWithdrawn || !pending.buyerWithdrawn) {
    throw new Error('Both parties must withdraw first');
  }

  // Idempotent: if already prepared, return stored result (both parties must sign same tx)
  if (pending.preparedMultiLeg && typeof pending.preparedMultiLeg === 'object') {
    return pending.preparedMultiLeg;
  }

  const sdkClient = getCantonSDKClient();
  const prepareResult = await sdkClient.prepareMultiLegAllocationInteractive({
    sellerPartyId: pending.sellerPartyId,
    buyerPartyId: pending.buyerPartyId,
    baseSymbol: pending.baseSymbol,
    quoteSymbol: pending.quoteSymbol,
    matchQty: pending.matchQty,
    quoteAmount: pending.quoteAmount,
    tradeId: matchId,
    token,
  });

  await prisma.pendingSettlement.update({
    where: { id: matchId },
    data: {
      status: 'PENDING_MULTILEG',
      preparedMultiLeg: prepareResult,
    },
  });

  return prepareResult;
}

/**
 * Add party signature for multi-leg. When both have signed, auto-execute.
 * partySignaturesFromFrontend: { party: partyId, signatures: [...] } - one party's sig
 */
async function addMultiLegSignature(matchId, partyId, partySignaturesFromFrontend, token) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending) throw new Error('Pending settlement not found');
  if (!pending.preparedMultiLeg) throw new Error('Multi-leg not prepared yet');

  const isSeller = pending.sellerPartyId === partyId;
  const isBuyer = pending.buyerPartyId === partyId;
  if (!isSeller && !isBuyer) throw new Error('Party not part of this settlement');

  const sigObj = typeof partySignaturesFromFrontend === 'object' && partySignaturesFromFrontend.party
    ? partySignaturesFromFrontend
    : { party: partyId, signatures: partySignaturesFromFrontend?.signatures || partySignaturesFromFrontend || [] };

  const updateData = {};
  if (isSeller) updateData.sellerMultiLegSig = sigObj;
  else updateData.buyerMultiLegSig = sigObj;

  await prisma.pendingSettlement.update({
    where: { id: matchId },
    data: updateData,
  });

  const updated = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (updated.sellerMultiLegSig?.signatures?.length && updated.buyerMultiLegSig?.signatures?.length) {
    await executeMultiLegCreation(matchId, token);
  }

  return { success: true };
}

/**
 * Submit signed multi-leg allocation (both parties signed). Then operator executes.
 */
async function submitSignedMultiLeg(matchId, preparedTransaction, partySignatures, hashingSchemeVersion, token) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending) throw new Error('Pending settlement not found');

  const result = await cantonService.executeInteractiveSubmission(
    {
      preparedTransaction,
      partySignatures,
      hashingSchemeVersion: hashingSchemeVersion || 'HASHING_SCHEME_VERSION_V2',
      submissionId: `multileg-${matchId}`,
    },
    token
  );

  const allocCid = extractAllocationCidFromTransaction(result);
  if (!allocCid) throw new Error('Could not extract allocation CID from multi-leg creation result');

  await prisma.pendingSettlement.update({
    where: { id: matchId },
    data: { multiLegAllocCid: allocCid },
  });

  await executeMultiLegAllocations(matchId);
}

function extractAllocationCidFromTransaction(txResult) {
  const events = txResult?.transaction?.events || txResult?.events || [];
  for (const event of events) {
    const created = event.created || event.CreatedEvent?.value || event.CreatedEvent;
    if (!created?.contractId) continue;
    const tpl = created.templateId || '';
    if (tpl.includes('Allocation') && !tpl.includes('AllocationFactory')) {
      return created.contractId;
    }
  }
  return null;
}

async function executeMultiLegCreation(matchId, token) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending || !pending.preparedMultiLeg || !pending.sellerMultiLegSig || !pending.buyerMultiLegSig) {
    throw new Error('Cannot execute: missing signatures');
  }

  const prep = pending.preparedMultiLeg;
  const sellerSigs = pending.sellerMultiLegSig;
  const buyerSigs = pending.buyerMultiLegSig;
  const partySignatures = {
    signatures: [
      { party: pending.sellerPartyId, signatures: Array.isArray(sellerSigs?.signatures) ? sellerSigs.signatures : (sellerSigs?.signatures ? [sellerSigs.signatures] : []) },
      { party: pending.buyerPartyId, signatures: Array.isArray(buyerSigs?.signatures) ? buyerSigs.signatures : (buyerSigs?.signatures ? [buyerSigs.signatures] : []) },
    ].filter((s) => s.signatures.length > 0),
  };
  if (partySignatures.signatures.length < 2) {
    throw new Error('Both parties must provide signatures');
  }

  const adminToken = await tokenProvider.getServiceToken();
  await submitSignedMultiLeg(
    matchId,
    prep.preparedTransaction,
    partySignatures,
    prep.hashingSchemeVersion,
    adminToken
  );
}

async function executeMultiLegAllocations(matchId) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending) throw new Error('Pending settlement not found');

  const sdkClient = getCantonSDKClient();
  const operatorPartyId = config.canton.operatorPartyId;

  const multiLegAllocCid = pending.multiLegAllocCid;
  if (!multiLegAllocCid) {
    throw new Error('Multi-leg allocation CID not found - creation may have failed');
  }

  // Execute multi-leg allocation (both legs run atomically)
  await sdkClient.tryRealAllocationExecution(
    multiLegAllocCid,
    operatorPartyId,
    pending.baseSymbol,
    pending.sellerPartyId,
    pending.buyerPartyId
  );

  await prisma.pendingSettlement.update({
    where: { id: matchId },
    data: { status: 'COMPLETED' },
  });

  await finalizeSettlement(matchId);
  return { success: true };
}

async function finalizeSettlement(matchId) {
  const pending = await prisma.pendingSettlement.findUnique({ where: { id: matchId } });
  if (!pending) return;

  const packageId = (config.canton?.packageIds && config.canton.packageIds.clobExchange) || config.canton?.packageId;
  const operatorPartyId = config.canton.operatorPartyId;
  const token = await tokenProvider.getServiceToken();

  try {
    if (pending.sellOrderContractId && pending.buyOrderContractId) {
      await cantonService.exerciseChoice({
        token,
        actAsParty: operatorPartyId,
        templateId: pending.buyOrderTemplateId || `${packageId}:Order:Order`,
        contractId: pending.buyOrderContractId,
        choice: 'FillOrder',
        choiceArgument: {
          fillQuantity: pending.matchQty,
          newAllocationCid: pending.buyIsPartial ? null : null,
        },
        readAs: [operatorPartyId, pending.buyerPartyId],
      });
      await cantonService.exerciseChoice({
        token,
        actAsParty: operatorPartyId,
        templateId: pending.sellOrderTemplateId || `${packageId}:Order:Order`,
        contractId: pending.sellOrderContractId,
        choice: 'FillOrder',
        choiceArgument: {
          fillQuantity: pending.matchQty,
          newAllocationCid: pending.sellIsPartial ? null : null,
        },
        readAs: [operatorPartyId, pending.sellerPartyId],
      });
    }
  } catch (fillErr) {
    console.warn(`[TradingAppSettlement] FillOrder failed (non-critical): ${fillErr.message}`);
  }

  try {
    const { recordTradeSettlement, isTradeRecorded } = require('./tradeSettlementService');
    const alreadyRecorded = await isTradeRecorded(matchId);
    if (!alreadyRecorded) {
      await recordTradeSettlement({
        tradeId: matchId,
        buyer: pending.buyerPartyId,
        seller: pending.sellerPartyId,
        baseSymbol: pending.baseSymbol,
        quoteSymbol: pending.quoteSymbol,
        baseAmount: pending.matchQty,
        quoteAmount: pending.quoteAmount,
        price: pending.matchPrice,
        tradingPair: pending.tradingPair,
        buyOrderId: pending.buyOrderId,
        sellOrderId: pending.sellOrderId,
        sellerUsedRealTransfer: true,
        buyerUsedRealTransfer: true,
      });
    }
  } catch (tsErr) {
    console.warn(`[TradingAppSettlement] recordTradeSettlement failed: ${tsErr.message}`);
  }

  try {
    const { releasePartialReservation } = require('./order-service');
    await releasePartialReservation(pending.sellOrderId, pending.matchQty);
    await releasePartialReservation(pending.buyOrderId, pending.quoteAmount);
  } catch (_) { /* non-critical */ }

  if (global.broadcastWebSocket) {
    global.broadcastWebSocket(`trades:${pending.tradingPair}`, {
      type: 'NEW_TRADE',
      tradeId: matchId,
      tradingPair: pending.tradingPair,
      buyer: pending.buyerPartyId,
      seller: pending.sellerPartyId,
      price: pending.matchPrice,
      quantity: pending.matchQty,
      settlementType: 'TradingApp',
    });
    global.broadcastWebSocket(`balance:${pending.sellerPartyId}`, { type: 'BALANCE_UPDATE', partyId: pending.sellerPartyId });
    global.broadcastWebSocket(`balance:${pending.buyerPartyId}`, { type: 'BALANCE_UPDATE', partyId: pending.buyerPartyId });
  }
}

/**
 * Get pending settlements for a party.
 */
async function getPendingForParty(partyId) {
  return prisma.pendingSettlement.findMany({
    where: {
      OR: [{ sellerPartyId: partyId }, { buyerPartyId: partyId }],
      status: { in: ['PENDING_WITHDRAW', 'PENDING_MULTILEG'] },
    },
    orderBy: { createdAt: 'desc' },
  });
}

module.exports = {
  createPendingSettlement,
  prepareWithdraw,
  submitSignedWithdraw,
  prepareMultiLeg,
  addMultiLegSignature,
  submitSignedMultiLeg,
  getPendingForParty,
};
