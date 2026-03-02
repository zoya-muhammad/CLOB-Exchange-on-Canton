/**
 * Onboarding Controller
 * Handles external party onboarding HTTP requests
 */

const OnboardingService = require('../services/onboarding-service');
const { success, error } = require('../utils/response');
const asyncHandler = require('../middleware/asyncHandler');
const quota = require('../state/quota');
const userRegistry = require('../state/userRegistry');

class OnboardingController {
  constructor() {
    this.onboardingService = new OnboardingService();
  }

  /**
   * 2-step allocate party endpoint
   *
   * STEP 1: Generate topology
   * Request: { publicKeyBase64, partyHint? }
   * Response: { step: "TOPOLOGY", multiHash, topologyTransactions, onboardingTransactions, ... }
   *
   * STEP 2: Allocate party
   * Request: { publicKeyBase64, signatureBase64, topologyTransactions }
   * Response: { step: "ALLOCATED", partyId, synchronizerId }
   */
  allocateParty = asyncHandler(async (req, res) => {
    const userId = req.userId;
    // Accept both publicKey and publicKeyBase64 for compatibility
    let publicKeyBase64 = req.body.publicKeyBase64 || req.body.publicKey;
    // Accept both signature and signatureBase64 for compatibility
    const signatureBase64 = req.body.signatureBase64 || req.body.signature;
    const { topologyTransactions, onboardingTransactions, partyHint, publicKeyFingerprint } = req.body;

    // Validate publicKeyBase64 is always required and must be a string
    if (!publicKeyBase64) {
      return error(res, 'publicKeyBase64 or publicKey is required (base64 string)', 400);
    }

    // If publicKeyBase64 is an object, reject it (backend must construct the publicKey object)
    if (typeof publicKeyBase64 !== 'string') {
      return error(res, 'publicKeyBase64 must be a base64 string, not an object. Backend constructs the publicKey object from this string.', 400);
    }

    // Ensure it's not empty after trimming
    if (publicKeyBase64.trim() === '') {
      return error(res, 'publicKeyBase64 cannot be empty', 400);
    }

    // Determine which step based on presence of signature
    const isStep2 = !!signatureBase64;

    if (isStep2) {
      // STEP 2: Allocate party
      console.log('[OnboardingController] Step 2: Allocate party');

      // Accept either topologyTransactions or onboardingTransactions
      const txs = topologyTransactions || onboardingTransactions;

      if (!txs || !Array.isArray(txs) || txs.length === 0) {
        return error(res, 'topologyTransactions or onboardingTransactions required for step 2', 400);
      }

      // Validate signature
      if (!signatureBase64) {
        return error(res, 'signatureBase64 or signature is required for step 2', 400);
      }
      if (typeof signatureBase64 !== 'string') {
        return error(res, 'signatureBase64 must be a base64 string', 400);
      }
      if (signatureBase64.trim() === '') {
        return error(res, 'signatureBase64 cannot be empty', 400);
      }

      // Validate publicKeyFingerprint
      if (!publicKeyFingerprint) {
        return error(res, 'publicKeyFingerprint is required for step 2 (from generate-topology response)', 400);
      }
      if (typeof publicKeyFingerprint !== 'string' || publicKeyFingerprint.trim() === '') {
        return error(res, 'publicKeyFingerprint must be a non-empty string', 400);
      }

      // Debug log the parameters being passed to the service
      console.log('[OnboardingController] Step 2 parameters:', {
        publicKeyBase64: publicKeyBase64 ? `${publicKeyBase64.substring(0, 20)}...` : 'missing',
        signatureBase64: signatureBase64 ? `${signatureBase64.substring(0, 20)}...` : 'missing',
        publicKeyFingerprint: publicKeyFingerprint ? `${publicKeyFingerprint.substring(0, 20)}...` : 'missing',
        transactionsCount: txs ? txs.length : 0,
      });

      try {
        // Quota enforcement BEFORE completing allocation
        await quota.assertAvailable();

        // Extract partyHint from topology transaction if available (for deduplication)
        // The partyHint helps prevent duplicate allocations
        const partyHint = req.body.partyHint || null;

        const result = await this.onboardingService.completeOnboarding(
          publicKeyBase64,
          signatureBase64,
          txs,
          publicKeyFingerprint,
          partyHint
        );

        // Increment quota only after successful allocation+onboarding
        const quotaStatus = await quota.increment();

        // Store mapping userId -> { partyId, publicKeyBase64, publicKeyFingerprint }
        await userRegistry.upsertUser(userId, {
          partyId: result.partyId,
          publicKeyBase64,
          ...(publicKeyFingerprint ? { publicKeyFingerprint } : {}),
        });

        // ── Store signing key for server-side interactive settlement ──
        // The frontend sends the Ed25519 private key (base64) during onboarding
        // so the backend can sign allocation operations at settlement time.
        const signingKeyBase64 = req.body.signingKeyBase64;
        if (signingKeyBase64 && typeof signingKeyBase64 === 'string' && signingKeyBase64.trim()) {
          await userRegistry.storeSigningKey(result.partyId, signingKeyBase64.trim(), publicKeyFingerprint || '');
          console.log(`[OnboardingController] 🔑 Signing key stored for party ${result.partyId.substring(0, 30)}...`);
        } else {
          console.warn(`[OnboardingController] ⚠️ No signingKeyBase64 provided — interactive settlement will not work until key is stored`);
        }

        return success(res, { ...result, quotaStatus }, 'Party onboarded successfully', 200);
      } catch (err) {
        const statusCode = err.statusCode || 500;
        return error(res, err.message, statusCode, err.cause);
      }
    } else {
      // STEP 1: Generate topology
      console.log('[OnboardingController] Step 1: Generate topology');

      try {
        const result = await this.onboardingService.generateTopology(
          publicKeyBase64,
          partyHint
        );

        // Store public key early for signature verification later
        await userRegistry.upsertUser(userId, { publicKeyBase64 });

        return success(res, result, 'Topology generated successfully', 200);
      } catch (err) {
        const statusCode = err.statusCode || 500;
        return error(res, err.message, statusCode, err.cause);
      }
    }
  });

  /**
   * Rehydrate user mapping after refresh/restart
   * Request: { partyId, publicKeyBase64? }
   * Response: { partyId }
   */
  rehydrate = asyncHandler(async (req, res) => {
    const userId = req.userId;
    const { partyId, publicKeyBase64, signingKeyBase64, publicKeyFingerprint } = req.body || {};

    if (!partyId || typeof partyId !== 'string' || partyId.trim() === '') {
      return error(res, 'partyId is required', 400);
    }

    if (publicKeyBase64 && (typeof publicKeyBase64 !== 'string' || publicKeyBase64.trim() === '')) {
      return error(res, 'publicKeyBase64 must be a non-empty string if provided', 400);
    }

    await userRegistry.upsertUser(userId, {
      partyId: partyId.trim(),
      ...(publicKeyBase64 ? { publicKeyBase64: publicKeyBase64.trim() } : {}),
    });

    // Also store signing key if provided (for interactive settlement)
    if (signingKeyBase64 && typeof signingKeyBase64 === 'string' && signingKeyBase64.trim()) {
      const fingerprint = publicKeyFingerprint || '';
      await userRegistry.storeSigningKey(partyId.trim(), signingKeyBase64.trim(), fingerprint);
      console.log(`[OnboardingController] 🔑 Signing key restored during rehydrate for ${partyId.substring(0, 30)}...`);
    }

    return success(res, { partyId: partyId.trim() }, 'User mapping restored', 200);
  });

  /**
   * Ensure rights endpoint (NO-OP for external party flow)
   * Validator token already has actAs rights
   */
  ensureRights = asyncHandler(async (req, res) => {
    const { partyId } = req.body;

    if (!partyId) {
      return error(res, 'partyId is required', 400);
    }

    const result = await this.onboardingService.ensureRights(partyId);
    return success(res, result, 'Rights verification successful', 200);
  });

  /**
   * Create preapproval endpoint (optional/not required)
   * Returns success without blocking
   */
  createPreapproval = asyncHandler(async (req, res) => {
    const { partyId } = req.body;

    if (!partyId) {
      return error(res, 'partyId is required', 400);
    }

    const result = await this.onboardingService.createPreapproval(partyId);
    return success(res, result, 'Preapproval successful', 200);
  });

  /**
   * Discover synchronizerId
   * Useful for debugging and frontend to show which synchronizer is being used
   */
  discoverSynchronizer = asyncHandler(async (req, res) => {
    const synchronizerId = await this.onboardingService.discoverSynchronizerId();
    return success(res, { synchronizerId }, 'Synchronizer discovered successfully', 200);
  });

  /**
   * Store signing key for interactive settlement
   * 
   * This allows the backend to sign allocation operations on behalf
   * of the external party during settlement (interactive submission).
   * 
   * Request: { partyId, signingKeyBase64, publicKeyFingerprint }
   * Response: { stored: true }
   */
  storeSigningKey = asyncHandler(async (req, res) => {
    const { partyId, signingKeyBase64, publicKeyFingerprint } = req.body || {};

    if (!partyId || typeof partyId !== 'string' || partyId.trim() === '') {
      return error(res, 'partyId is required', 400);
    }
    if (!signingKeyBase64 || typeof signingKeyBase64 !== 'string' || signingKeyBase64.trim() === '') {
      return error(res, 'signingKeyBase64 is required (base64-encoded Ed25519 private key)', 400);
    }

    const fingerprint = publicKeyFingerprint || '';
    await userRegistry.storeSigningKey(partyId.trim(), signingKeyBase64.trim(), fingerprint);

    console.log(`[OnboardingController] 🔑 Signing key stored via /store-signing-key for ${partyId.substring(0, 30)}...`);
    return success(res, { stored: true, partyId: partyId.trim() }, 'Signing key stored successfully', 200);
  });
}

module.exports = new OnboardingController();
