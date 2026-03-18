/**
 * Settlement routes — TradingApp pattern (USE_TRADING_APP_PATTERN=true)
 * Both parties sign withdraw + multi-leg allocation at match time.
 */

const express = require('express');
const asyncHandler = require('../middleware/asyncHandler');
const tradingAppSettlement = require('../services/tradingAppSettlementService');
const config = require('../config');

const router = express.Router();

if (!config.useTradingAppPattern) {
  router.use((req, res) => {
    res.status(503).json({ error: 'TradingApp pattern disabled. Set USE_TRADING_APP_PATTERN=true' });
  });
  module.exports = router;
  return;
}

/** GET /api/settlement/pending — Pending settlements for party (from X-Party-Id or query) */
router.get(
  '/pending',
  asyncHandler(async (req, res) => {
    const partyId = req.headers['x-party-id'] || req.query.partyId;
    if (!partyId) {
      return res.status(400).json({ error: 'partyId required (X-Party-Id header or query)' });
    }
    const list = await tradingAppSettlement.getPendingForParty(partyId);
    res.json({ pending: list });
  })
);

/** POST /api/settlement/:matchId/prepare-withdraw — Prepare withdraw for party to sign */
router.post(
  '/:matchId/prepare-withdraw',
  asyncHandler(async (req, res) => {
    const { matchId } = req.params;
    const partyId = req.headers['x-party-id'] || req.body.partyId;
    const token = req.headers.authorization?.replace('Bearer ', '') || req.body.token;
    if (!partyId || !token) {
      return res.status(400).json({ error: 'partyId and token required' });
    }
    const result = await tradingAppSettlement.prepareWithdraw(matchId, partyId, token);
    if (result.alreadyWithdrawn) {
      return res.json({ success: true, alreadyWithdrawn: true });
    }
    res.json(result.data);
  })
);

/** POST /api/settlement/:matchId/submit-withdraw — Submit signed withdraw */
router.post(
  '/:matchId/submit-withdraw',
  asyncHandler(async (req, res) => {
    const { matchId } = req.params;
    const { partyId, preparedTransaction, partySignatures, hashingSchemeVersion } = req.body;
    const token = req.headers.authorization?.replace('Bearer ', '') || req.body.token;
    if (!partyId || !preparedTransaction || !partySignatures || !token) {
      return res.status(400).json({ error: 'partyId, preparedTransaction, partySignatures, token required' });
    }
    const result = await tradingAppSettlement.submitSignedWithdraw(
      matchId,
      partyId,
      preparedTransaction,
      partySignatures,
      hashingSchemeVersion,
      token
    );
    res.json(result);
  })
);

/** GET /api/settlement/verify-holdings/:partyId — Verify holding state (available vs locked) after withdraw */
router.get(
  '/verify-holdings/:partyId',
  asyncHandler(async (req, res) => {
    const { partyId } = req.params;
    const symbol = req.query.symbol || null;
    const { getCantonSDKClient } = require('../services/canton-sdk-client');
    const sdkClient = getCantonSDKClient();
    const state = await sdkClient.verifyHoldingState(partyId, symbol);
    res.json({
      partyId,
      totalAvailable: state.totalAvailable,
      totalLocked: state.totalLocked,
      holdings: state.holdings?.length ?? 0,
      locked: state.locked?.length ?? 0,
      unlocked: state.totalLocked === '0',
      lockedDetails: state.locked || [],
    });
  })
);

/** POST /api/settlement/:matchId/prepare-multileg — Prepare multi-leg (both parties sign). Call after both withdraws. */
router.post(
  '/:matchId/prepare-multileg',
  asyncHandler(async (req, res) => {
    const { matchId } = req.params;
    const token = req.headers.authorization?.replace('Bearer ', '') || req.body.token;
    const tokenProvider = require('../services/tokenProvider');
    const effectiveToken = token || (await tokenProvider.getServiceToken());
    const result = await tradingAppSettlement.prepareMultiLeg(matchId, effectiveToken);
    res.json(result);
  })
);

/** POST /api/settlement/:matchId/submit-multileg-signature — Add one party's signature. When both present, auto-execute. */
router.post(
  '/:matchId/submit-multileg-signature',
  asyncHandler(async (req, res) => {
    const { matchId } = req.params;
    const { partyId, partySignatures } = req.body;
    const token = req.headers.authorization?.replace('Bearer ', '') || req.body.token;
    if (!partyId || !partySignatures || !token) {
      return res.status(400).json({ error: 'partyId, partySignatures, token required' });
    }
    await tradingAppSettlement.addMultiLegSignature(matchId, partyId, partySignatures, token);
    res.json({ success: true });
  })
);

module.exports = router;
