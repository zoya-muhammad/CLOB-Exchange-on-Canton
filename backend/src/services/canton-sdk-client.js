/**
 * Canton Wallet SDK Client — Allocation-Based Settlement
 * 
 * Wraps the official @canton-network/wallet-sdk for the CLOB Exchange.
 * 
 * Provides:
 * - Balance queries via listHoldingUtxos (UTXO-based)
 * - Allocation-based settlement flow (replaces TransferInstruction)
 * - Allocation cancellation (for order cancellations)
 * 
 * Settlement is Allocation-based:
 * 1. At ORDER PLACEMENT: User creates Allocation (exchange = executor, funds locked)
 * 2. At MATCH TIME: Exchange executes Allocation with ITS OWN KEY (no user key needed)
 * 
 * Why Allocations instead of TransferInstructions:
 * - TransferInstruction requires the sender's private key at SETTLEMENT time
 * - With external parties, the backend has no user keys → TransferInstruction breaks
 * - Allocation: User signs ONCE at order time, exchange settles with its own key
 * - Works with external parties (users control their own keys, Confirmation permission)
 * 
 * @see https://docs.sync.global/app_dev/api/splice-api-token-allocation-v1/
 * @see https://docs.sync.global/app_dev/api/splice-api-token-allocation-instruction-v1/
 * @see https://docs.digitalasset.com/integrate/devnet/token-standard/index.html
 */

const { CANTON_SDK_CONFIG, UTILITIES_CONFIG, toCantonInstrument, toExchangeSymbol, extractInstrumentId, getTokenSystemType, getInstrumentAdmin } = require('../config/canton-sdk.config');
const cantonService = require('./cantonService');
const tokenProvider = require('./tokenProvider');
const Decimal = require('decimal.js');
const userRegistry = require('../state/userRegistry');
const { getRegistryApi } = require('../http/clients');

// ─── Ed25519 Signing for Interactive Settlement ──────────────────────────
// Used to sign Allocation_ExecuteTransfer for external parties at match time.
// External parties' keys are stored in userRegistry during onboarding.
let ed25519Module = null;
let sha512Module = null;

async function getEd25519() {
  if (!ed25519Module) {
    ed25519Module = require('@noble/ed25519');
    sha512Module = require('@noble/hashes/sha512');
    if (!ed25519Module.etc.sha512Sync) {
      ed25519Module.etc.sha512Sync = (...m) => sha512Module.sha512(ed25519Module.etc.concatBytes(...m));
    }
  }
  return ed25519Module;
}

/**
 * Sign a base64-encoded hash with a stored Ed25519 private key.
 * Used by interactive settlement for external parties.
 *
 * @param {string} privateKeyBase64 - Base64-encoded 32-byte Ed25519 private key
 * @param {string} hashBase64 - Base64-encoded hash to sign (preparedTransactionHash)
 * @returns {Promise<string>} Base64-encoded signature
 */
async function signHashWithKey(privateKeyBase64, hashBase64) {
  const ed = await getEd25519();
  const privateKey = Buffer.from(privateKeyBase64, 'base64');
  const hashBytes = Buffer.from(hashBase64, 'base64');
  const signature = await ed.sign(hashBytes, privateKey);
  return Buffer.from(signature).toString('base64');
}

// Configure decimal precision
Decimal.set({ precision: 20, rounding: Decimal.ROUND_DOWN });

/**
 * Canton JSON API expects Numeric values as plain decimal strings
 * (scientific notation like "1e-7" is rejected).
 */
function toDamlNumericString(value) {
  try {
    return new Decimal(value).toFixed();
  } catch (error) {
    throw new Error(`Invalid numeric value for DAML payload: ${value}`);
  }
}

// ─── Lazy-load SDK — loaded via dynamic import() in _doInitialize ───────────
// The SDK's CJS bundle require()s `jose` which is ESM-only (v5+).
// Static require() fails on Vercel's Node runtime with ERR_REQUIRE_ESM.
// Dynamic import() works universally for ESM modules.
let WalletSDKImpl = null;
let ClientCredentialOAuthController = null;
let LedgerController = null;
let TokenStandardController = null;
let ValidatorController = null;
let sdkLoadError = null;
let sdkLoaded = false;

// ─── SDK Client ────────────────────────────────────────────────────────────

class CantonSDKClient {
  constructor() {
    this.sdk = null;
    this.initialized = false;
    this.initError = null; // Set after initialize() if SDK fails to load
    this.instrumentAdminPartyId = null;
    this.currentPartyId = null;
    this._initPromise = null; // guards against concurrent initialize() calls

    // Simple sequential executor to prevent party-context races
    // SDK is stateful (setPartyId) so concurrent calls for different parties would conflict
    this._operationQueue = Promise.resolve();
  }

  /**
   * Initialize the SDK and connect to Canton
   * Call this once at server startup.
   * Safe to call concurrently — only the first call runs, others await the same promise.
   * Automatically retries with exponential backoff if initialization fails.
   */
  async initialize() {
    if (this.initialized) {
      return;
    }
    // If initialization is already in progress, await the same promise
    if (this._initPromise) {
      return this._initPromise;
    }
    this._initPromise = this._doInitializeWithRetry();
    return this._initPromise;
  }

  /**
   * Retry wrapper around _doInitialize with exponential backoff.
   * Retries up to MAX_RETRIES times with delays of 5s, 10s, 20s, 40s, 60s.
   * If all retries fail, starts a background retry loop (every 60s).
   */
  async _doInitializeWithRetry() {
    const MAX_RETRIES = 5;
    const BASE_DELAY_MS = 5000;
    const MAX_DELAY_MS = 60000;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      await this._doInitialize();
      if (this.initialized) {
        return; // Success
      }

      if (attempt < MAX_RETRIES) {
        const delay = Math.min(BASE_DELAY_MS * Math.pow(2, attempt - 1), MAX_DELAY_MS);
        console.warn(`[CantonSDK] ⏳ Retry ${attempt}/${MAX_RETRIES} in ${delay / 1000}s... (error: ${this.initError})`);
        await new Promise(resolve => setTimeout(resolve, delay));
        // Reset state for retry
        this.initError = null;
        this.sdk = null;
      }
    }

    // All retries exhausted — start background retry loop
    if (!this.initialized) {
      console.error(`[CantonSDK] ❌ All ${MAX_RETRIES} initialization attempts failed. Starting background retry (every 60s)...`);
      this._startBackgroundRetry();
    }
  }

  /**
   * Background retry: Periodically attempts SDK initialization.
   * Stops once initialized successfully.
   */
  _startBackgroundRetry() {
    if (this._backgroundRetryTimer) return; // Already running

    this._backgroundRetryTimer = setInterval(async () => {
      if (this.initialized) {
        clearInterval(this._backgroundRetryTimer);
        this._backgroundRetryTimer = null;
        return;
      }
      console.log('[CantonSDK] 🔄 Background retry: attempting SDK initialization...');
      this.initError = null;
      this.sdk = null;
      await this._doInitialize();
      if (this.initialized) {
        console.log('[CantonSDK] ✅ Background retry SUCCEEDED — SDK is now ready!');
        clearInterval(this._backgroundRetryTimer);
        this._backgroundRetryTimer = null;
      } else {
        console.warn(`[CantonSDK] ⚠️ Background retry failed: ${this.initError}. Will retry in 60s...`);
      }
    }, 60000);
  }

  async _doInitialize() {

    // ── Step 0: Load the SDK package via dynamic import() ─────────────
    // Must use import() not require() because the SDK's CJS bundle
    // transitively require()s jose v5+ which is ESM-only.
    // Dynamic import() handles ESM modules correctly on all Node versions.
    if (!sdkLoaded) {
      try {
        console.log('[CantonSDK] Loading SDK via dynamic import()...');
        const walletSdk = await import('@canton-network/wallet-sdk');
        WalletSDKImpl = walletSdk.WalletSDKImpl;
        ClientCredentialOAuthController = walletSdk.ClientCredentialOAuthController;
        LedgerController = walletSdk.LedgerController;
        TokenStandardController = walletSdk.TokenStandardController;
        ValidatorController = walletSdk.ValidatorController;
        sdkLoaded = true;
        console.log('[CantonSDK] ✅ @canton-network/wallet-sdk loaded via import()');
      } catch (e) {
        sdkLoadError = `${e.code || 'UNKNOWN'}: ${e.message}`;
        console.error('[CantonSDK] ❌ SDK import() failed:', e.code, e.message);
        console.error('[CantonSDK] ❌ Stack:', (e.stack || '').split('\n').slice(0, 5).join('\n'));
      }
    }

    if (!WalletSDKImpl) {
      this.initError = `SDK package not loaded: ${sdkLoadError || 'import() failed'}`;
      console.error(`[CantonSDK] ❌ ${this.initError}`);
      return;
    }

    try {
      console.log('[CantonSDK] ═══════════════════════════════════════');
      console.log('[CantonSDK] Initializing Canton Wallet SDK...');
      console.log(`[CantonSDK]   Ledger API:   ${CANTON_SDK_CONFIG.LEDGER_API_URL}`);
      console.log(`[CantonSDK]   Validator:    ${CANTON_SDK_CONFIG.VALIDATOR_API_URL}`);
      console.log(`[CantonSDK]   Registry:     ${CANTON_SDK_CONFIG.REGISTRY_API_URL}`);
      console.log(`[CantonSDK]   Scan API:     ${CANTON_SDK_CONFIG.SCAN_API_URL}`);
      console.log('[CantonSDK] ═══════════════════════════════════════');

      const jsonApiUrl = CANTON_SDK_CONFIG.LEDGER_API_URL;
      const validatorUrl = CANTON_SDK_CONFIG.VALIDATOR_API_URL;
      const registryUrl = CANTON_SDK_CONFIG.REGISTRY_API_URL;
      const scanApiUrl = CANTON_SDK_CONFIG.SCAN_API_URL;

      // Keycloak client credentials
      const keycloakBaseUrl = process.env.KEYCLOAK_BASE_URL || 'https://keycloak.wolfedgelabs.com:8443';
      const keycloakRealm = process.env.KEYCLOAK_REALM || 'canton-devnet';
      const keycloakConfigUrl = `${keycloakBaseUrl}/realms/${keycloakRealm}/.well-known/openid-configuration`;
      const clientId = (process.env.OAUTH_CLIENT_ID || '').trim();
      const clientSecret = (process.env.OAUTH_CLIENT_SECRET || '').trim();
      const oauthScope = process.env.OAUTH_SCOPE || 'openid profile email daml_ledger_api';
      const audience = jsonApiUrl;

      console.log(`[CantonSDK]   Keycloak:     ${keycloakConfigUrl}`);
      console.log(`[CantonSDK]   Client ID:    ${clientId.substring(0, 8)}...`);

      // Configure SDK with proper controller factories
      this.sdk = new WalletSDKImpl().configure({
        logger: console,

        // Auth factory — OAuth2 client credentials via Keycloak
        authFactory: () => {
          return new ClientCredentialOAuthController(
            keycloakConfigUrl,
            console,
            clientId,
            clientSecret,
            clientId,
            clientSecret,
            oauthScope,
            audience
          );
        },

        // Ledger factory — returns LedgerController connected to JSON Ledger API
        ledgerFactory: (userId, accessTokenProvider, isAdmin, accessToken = '') => {
          return new LedgerController(
            userId,
            new URL(jsonApiUrl),
            accessToken,
            isAdmin,
            accessTokenProvider
          );
        },

        // Token Standard factory — returns TokenStandardController
        tokenStandardFactory: (userId, accessTokenProvider, isAdmin, accessToken = '') => {
          return new TokenStandardController(
            userId,
            new URL(jsonApiUrl),
            new URL(validatorUrl),
            accessToken,
            accessTokenProvider,
            isAdmin,
            undefined,
            scanApiUrl
          );
        },

        // Validator factory — returns ValidatorController
        validatorFactory: (userId, accessTokenProvider, isAdmin = false, accessToken = '') => {
          return new ValidatorController(
            userId,
            new URL(validatorUrl),
            accessTokenProvider,
            isAdmin,
            accessToken
          );
        },
      });

      // Connect to Canton ledger
      await this.sdk.connect();
      console.log('[CantonSDK] ✅ Connected to Canton Ledger');

      // Set Transfer/Allocation Factory Registry URL
      this.sdk.tokenStandard?.setTransferFactoryRegistryUrl(new URL(registryUrl));
      console.log(`[CantonSDK] ✅ Factory Registry configured: ${registryUrl}`);

      // Discover instrument admin party (needed for allocations)
      try {
        this.instrumentAdminPartyId = await this.sdk.tokenStandard?.getInstrumentAdmin();
        console.log(`[CantonSDK] ✅ Instrument admin: ${this.instrumentAdminPartyId?.substring(0, 40)}...`);
      } catch (e) {
        console.warn(`[CantonSDK] ⚠️ Could not discover instrument admin via SDK: ${e.message}`);
        this.instrumentAdminPartyId = CANTON_SDK_CONFIG.INSTRUMENT_ADMIN_PARTY;
        if (!this.instrumentAdminPartyId) {
          try {
            const scanUrl = `${CANTON_SDK_CONFIG.SCAN_PROXY_URL}/api/scan/v0/amulet-rules`;
            const { data: scanData } = await getRegistryApi().get(scanUrl);
            const dso = scanData?.amulet_rules_update?.contract?.payload?.dso;
            if (dso) {
              this.instrumentAdminPartyId = dso;
              console.log(`[CantonSDK] ✅ Discovered DSO party from Scan API: ${dso.substring(0, 40)}...`);
            }
          } catch (scanErr) {
            console.warn(`[CantonSDK] ⚠️ Scan API fallback failed: ${scanErr.message}`);
          }
        }
        if (this.instrumentAdminPartyId) {
          console.log(`[CantonSDK] Using instrument admin: ${this.instrumentAdminPartyId.substring(0, 40)}...`);
        } else {
          console.warn('[CantonSDK] ⚠️ No instrument admin configured — allocations will need it');
        }
      }

      this.initialized = true;
      console.log('[CantonSDK] ✅ SDK fully initialized and ready (Allocation-based settlement)');
    } catch (error) {
      console.error('[CantonSDK] ❌ Initialization failed:', error.message);
      if (error.stack) {
        console.error('[CantonSDK]   Stack:', error.stack.split('\n').slice(0, 3).join('\n'));
      }
      this.initError = error.message;
    }
  }

  /**
   * Check if SDK is ready for operations
   */
  isReady() {
    return this.initialized && this.sdk && this.sdk.tokenStandard;
  }

  /**
   * Get instrument admin for a given symbol
   */
  getInstrumentAdminForSymbol(symbol) {
    return getInstrumentAdmin(symbol, this.instrumentAdminPartyId);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // PARTY CONTEXT — thread-safe sequential execution
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Execute an operation with a specific party context.
   * Queues operations to prevent concurrent party-context conflicts.
   */
  async _withPartyContext(partyId, operation) {
    return new Promise((resolve, reject) => {
      this._operationQueue = this._operationQueue
        .catch(() => {})
        .then(async () => {
          try {
            if (this.currentPartyId !== partyId) {
              await this.sdk.setPartyId(partyId);
              this.currentPartyId = partyId;
              const registryUrl = CANTON_SDK_CONFIG.REGISTRY_API_URL;
              if (registryUrl && this.sdk.tokenStandard) {
                this.sdk.tokenStandard.setTransferFactoryRegistryUrl(new URL(registryUrl));
              }
            }
            const result = await operation();
            resolve(result);
          } catch (err) {
            reject(err);
          }
        });
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // HOLDINGS & BALANCE — Query UTXOs from Canton Ledger
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Get all holdings (UTXOs) for a party
   */
  async getHoldings(partyId, includeLocked = false) {
    if (!this.isReady()) {
      throw new Error('Canton SDK not initialized');
    }

    return this._withPartyContext(partyId, async () => {
      try {
        const utxos = await this.sdk.tokenStandard?.listHoldingUtxos(includeLocked);
        return utxos || [];
      } catch (error) {
        console.error(`[CantonSDK] Failed to get holdings for ${partyId.substring(0, 30)}...:`, error.message);
        throw error;
      }
    });
  }

  /**
   * Get balance for a party and instrument
   * Calculates from UTXOs: total (all), available (unlocked), locked (in-allocation)
   */
  async getBalance(partyId, symbol) {
    if (!this.isReady()) {
      console.warn(`[CantonSDK] SDK not ready — returning zero balance for ${symbol}`);
      return { total: '0', available: '0', locked: '0' };
    }

    const instrumentId = toCantonInstrument(symbol);

    return this._withPartyContext(partyId, async () => {
      try {
        const allHoldings = await this.sdk.tokenStandard?.listHoldingUtxos(true) || [];
        const availableHoldings = await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];

        const filterAndSum = (holdings) => {
          return holdings
            .filter(h => {
              const holdingInstrument = extractInstrumentId(h.interfaceViewValue?.instrumentId);
              return holdingInstrument === instrumentId;
            })
            .reduce((sum, h) => {
              const amount = h.interfaceViewValue?.amount || '0';
              return sum.plus(new Decimal(amount));
            }, new Decimal(0));
        };

        const totalAmount = filterAndSum(allHoldings);
        const availableAmount = filterAndSum(availableHoldings);
        const lockedAmount = totalAmount.minus(availableAmount);

        return {
          total: totalAmount.toString(),
          available: availableAmount.toString(),
          locked: lockedAmount.toString(),
        };
      } catch (error) {
        console.error(`[CantonSDK] Balance query failed for ${partyId.substring(0, 30)}... ${symbol}:`, error.message);
        return { total: '0', available: '0', locked: '0' };
      }
    });
  }

  /**
   * Get all balances for a party (all instruments)
   */
  async getAllBalances(partyId) {
    if (!this.isReady()) {
      console.warn('[CantonSDK] SDK not ready — returning zero balances');
      return {
        available: { CC: '0', CBTC: '0' },
        locked: { CC: '0', CBTC: '0' },
        total: { CC: '0', CBTC: '0' },
      };
    }

    return this._withPartyContext(partyId, async () => {
      try {
        const allHoldings = await this.sdk.tokenStandard?.listHoldingUtxos(true) || [];
        const availableHoldings = await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];

        const available = {};
        const locked = {};
        const total = {};

        for (const h of allHoldings) {
          const rawInstr = h.interfaceViewValue?.instrumentId;
          if (!rawInstr) continue;
          const instrId = extractInstrumentId(rawInstr);
          const sym = toExchangeSymbol(instrId);
          const amount = new Decimal(h.interfaceViewValue?.amount || '0');
          total[sym] = (total[sym] ? new Decimal(total[sym]).plus(amount) : amount).toString();
        }

        for (const h of availableHoldings) {
          const rawInstr = h.interfaceViewValue?.instrumentId;
          if (!rawInstr) continue;
          const instrId = extractInstrumentId(rawInstr);
          const sym = toExchangeSymbol(instrId);
          const amount = new Decimal(h.interfaceViewValue?.amount || '0');
          available[sym] = (available[sym] ? new Decimal(available[sym]).plus(amount) : amount).toString();
        }

        for (const sym of Object.keys(total)) {
          const t = new Decimal(total[sym] || '0');
          const a = new Decimal(available[sym] || '0');
          locked[sym] = t.minus(a).toString();
          if (!available[sym]) available[sym] = '0';
        }

        return { available, locked, total };
      } catch (error) {
        console.error(`[CantonSDK] getAllBalances failed for ${partyId.substring(0, 30)}...:`, error.message);
        return {
          available: { CC: '0', CBTC: '0' },
          locked: { CC: '0', CBTC: '0' },
          total: { CC: '0', CBTC: '0' },
        };
      }
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // REAL TOKEN TRANSFERS — Uses Splice Transfer Factory API
  //
  // This is the ACTUAL token transfer mechanism that moves real Splice Holdings.
  // Used at MATCH TIME to settle trades between buyer and seller.
  //
  // Flow:
  // 1. SDK's createTransfer → exercises TransferFactory_Transfer → creates TransferInstruction
  // 2. SDK's exerciseTransferInstructionChoice('Accept') → moves the holding to receiver
  //
  // Token routing:
  // - CC (Amulet): Registry at Scan Proxy (http://65.108.40.104:8088)
  // - CBTC (Utilities): Registry at Utilities Backend (api/utilities)
  //
  // NOTE: When Splice Allocation Factory becomes available on this network,
  // the allocation path (below) will automatically be preferred.
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Perform a REAL token transfer between two parties.
   * 
   * Uses the Splice Transfer Factory API to create a TransferInstruction,
   * then immediately accepts it. This actually moves real Splice Holdings
   * (CC/CBTC) between wallets — visible on Canton Explorer.
   * 
   * @param {string} senderPartyId - Party sending the tokens
   * @param {string} receiverPartyId - Party receiving the tokens
   * @param {string} amount - Amount to transfer (as string)
   * @param {string} symbol - Exchange symbol (e.g., 'CC', 'CBTC')
   * @returns {Object} Result of the transfer acceptance
   */
  /**
   * Perform a real token transfer using the Transfer Factory Registry API.
   * 
   * This is the FALLBACK path — only used if Allocation-based settlement
   * is not available (e.g., no allocation CID on order). The PREFERRED
   * path is always Allocation API (createAllocation → executeAllocation).
   * 
   * Uses the Transfer Factory registry endpoint directly (not the SDK's
   * createTransfer, which generates incorrect payloads).
   * 
   * Correct endpoint: /registry/transfer-instruction/v1/transfer-factory
   * Confirmed payload structure via live API probing.
   */
  async performTransfer(senderPartyId, receiverPartyId, amount, symbol, skipAccept = false) {
    if (!this.isReady()) {
      throw new Error('Canton SDK not initialized');
    }

    const instrumentId = toCantonInstrument(symbol);
    const adminParty = this.getInstrumentAdminForSymbol(symbol);
    const tokenType = getTokenSystemType(symbol);

    // Detect faucet transfers (mint operations) — let auto-accept handle them
    const FAUCET_PARTY = process.env.FAUCET_PARTY_ID || 'faucet::1220faucet';
    const isFaucetTransfer = senderPartyId === FAUCET_PARTY || senderPartyId.startsWith('faucet::');
    if (isFaucetTransfer && !skipAccept) {
      skipAccept = true;
      console.log(`[CantonSDK]    🔵 Faucet transfer detected — skipping immediate Accept, auto-accept will handle`);
    }

    console.log(`[CantonSDK] Transfer: ${amount} ${symbol} (${instrumentId})`);
    console.log(`[CantonSDK]    From: ${senderPartyId.substring(0, 30)}...`);
    console.log(`[CantonSDK]    To:   ${receiverPartyId.substring(0, 30)}...`);

    const adminToken = await tokenProvider.getServiceToken();
    const configModule = require('../config');
    const synchronizerId = configModule.canton.synchronizerId;
    const MAX_ATTEMPTS = 3;

    let result;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      // Canton UTXO model: holding contract IDs change on every fee/merge/transfer.
      // Fetching inside the loop guarantees fresh IDs on each attempt.
      const holdings = await this._withPartyContext(senderPartyId, async () => {
        return await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];
      });

      const holdingCids = holdings
        .filter(h => extractInstrumentId(h.interfaceViewValue?.instrumentId) === instrumentId)
        .map(h => h.contractId);

      if (holdingCids.length === 0) {
        throw new Error(`No ${symbol} holdings found for sender ${senderPartyId.substring(0, 30)}...`);
      }

      console.log(`[CantonSDK]    Found ${holdingCids.length} ${symbol} UTXOs (attempt ${attempt}/${MAX_ATTEMPTS})`);

      const registryUrl = this._getTransferFactoryUrl(tokenType);
      const now = new Date().toISOString();
      const executeBefore = new Date(Date.now() + 86400000).toISOString();

      try {
        const { data: factory } = await getRegistryApi().post(registryUrl, {
          choiceArguments: {
            expectedAdmin: adminParty,
            transfer: {
              sender: senderPartyId,
              receiver: receiverPartyId,
              amount: toDamlNumericString(amount),
              instrumentId: { id: instrumentId, admin: adminParty },
              requestedAt: now,
              executeBefore,
              inputHoldingCids: holdingCids,
              meta: { values: {} },
            },
            extraArgs: { context: { values: {} }, meta: { values: {} } },
          },
          excludeDebugFields: true,
        }, {
          headers: { Authorization: `Bearer ${adminToken}`, Accept: 'application/json' },
        });

        console.log(`[CantonSDK]    Transfer Factory returned — factoryId: ${factory.factoryId?.substring(0, 30)}...`);

        const TRANSFER_FACTORY_INTERFACE = UTILITIES_CONFIG.TRANSFER_FACTORY_INTERFACE
          || '#splice-api-token-transfer-instruction-v1:Splice.Api.Token.TransferInstructionV1:TransferFactory';

        result = await cantonService.exerciseChoice({
          token: adminToken,
          actAsParty: [senderPartyId],
          templateId: TRANSFER_FACTORY_INTERFACE,
          contractId: factory.factoryId,
          choice: 'TransferFactory_Transfer',
          choiceArgument: {
            expectedAdmin: adminParty,
            transfer: {
              sender: senderPartyId,
              receiver: receiverPartyId,
              amount: toDamlNumericString(amount),
              instrumentId: { id: instrumentId, admin: adminParty },
              requestedAt: now,
              executeBefore,
              inputHoldingCids: holdingCids,
              meta: { values: {} },
            },
            extraArgs: {
              context: factory.choiceContext?.choiceContextData || { values: {} },
              meta: { values: {} },
            },
          },
          readAs: [senderPartyId, receiverPartyId],
          synchronizerId,
          disclosedContracts: (factory.choiceContext?.disclosedContracts || []).map(dc => ({
            templateId: dc.templateId,
            contractId: dc.contractId,
            createdEventBlob: dc.createdEventBlob,
            synchronizerId: dc.synchronizerId || synchronizerId,
          })),
        });
        break; // success — exit loop
      } catch (err) {
        const msg = err.message || '';
        const isStaleUtxo = msg.includes('CONTRACT_NOT_FOUND') ||
          msg.includes('could not be found') ||
          msg.includes('INACTIVE_CONTRACTS');

        if (!isStaleUtxo || attempt === MAX_ATTEMPTS) throw err;

        console.warn(`[CantonSDK]    Stale UTXO on attempt ${attempt} — refreshing holdings...`);
        await new Promise(r => setTimeout(r, 1500 * attempt));
      }
    }

    // Self-transfer: skip Accept step; extract created holding CIDs for caller
    const isSelfTransfer = senderPartyId === receiverPartyId;
    if (isSelfTransfer) {
      console.log(`[CantonSDK]    Self-transfer completed — no Accept needed`);
      const createdHoldings = this._extractCreatedHoldingCids(result);
      console.log(`[CantonSDK]    Created ${createdHoldings.length} new holding(s) from self-transfer`);
      return { ...result, createdHoldingCids: createdHoldings, isSelfTransfer: true };
    }

    // Find TransferInstruction CID from the result
    let tiCid = null;
    for (const event of (result?.transaction?.events || [])) {
      const created = event.created || event.CreatedEvent;
      if (created?.contractId) {
        const tpl = typeof created.templateId === 'string' ? created.templateId : '';
        if (tpl.includes('TransferInstruction') || tpl.includes('Transfer')) {
          tiCid = created.contractId;
          break;
        }
      }
    }

    if (!tiCid) {
      console.log(`[CantonSDK]    ℹ️ No TransferInstruction found — transfer may have auto-completed`);
      return result;
    }

    // ── Step 3: Accept the TransferInstruction as receiver ─────────────
    // For faucet transfers (mint), skip immediate Accept and let auto-accept service handle it.
    // This avoids authorization issues when faucet is external or on a different participant.
    if (skipAccept) {
      console.log(`[CantonSDK]    📨 TransferInstruction created: ${tiCid.substring(0, 30)}...`);
      console.log(`[CantonSDK]    ⏭️ Skipping immediate Accept — auto-accept service will handle this transfer`);
      return { ...result, transferInstructionCid: tiCid, skippedAccept: true };
    }

    // CRITICAL: AmuletTransferInstruction has signatories: admin (instrumentId.admin) + sender.
    // When receiver exercises Accept, Canton needs ALL parties on the same participant in actAs.
    // Since sender + receiver are both external wallet users on the SAME participant,
    // BOTH must be in actAs to satisfy the authorization check.
    console.log(`[CantonSDK]    📨 TransferInstruction created: ${tiCid.substring(0, 30)}...`);
    console.log(`[CantonSDK]    ✅ Accepting as receiver (with sender co-auth)...`);

    const acceptActAs = [receiverPartyId, senderPartyId];
    const acceptReadAs = [receiverPartyId, senderPartyId];

    const acceptResult = await this._withPartyContext(receiverPartyId, async () => {
      try {
        const [acceptCmd, acceptDisclosed] = await this.sdk.tokenStandard.exerciseTransferInstructionChoice(
          tiCid,
          'Accept'
        );

        const commands = Array.isArray(acceptCmd) ? acceptCmd : [acceptCmd];
        let res = null;
        for (const rawCmd of commands) {
          const cmd = rawCmd.ExerciseCommand || rawCmd;
          res = await cantonService.exerciseChoice({
            token: adminToken,
            actAsParty: acceptActAs,
            templateId: cmd.templateId,
            contractId: cmd.contractId,
            choice: cmd.choice,
            choiceArgument: cmd.choiceArgument,
            readAs: acceptReadAs,
            synchronizerId,
            disclosedContracts: (acceptDisclosed || []).map(dc => ({
              templateId: dc.templateId,
              contractId: dc.contractId,
              createdEventBlob: dc.createdEventBlob,
              synchronizerId: dc.synchronizerId || synchronizerId,
            })),
          });
        }
        console.log(`[CantonSDK]    ✅ Transfer ACCEPTED — real ${symbol} tokens moved!`);
        return res;
      } catch (sdkAcceptErr) {
        // SDK accept failed — try direct registry accept
        console.warn(`[CantonSDK]    SDK accept failed: ${sdkAcceptErr.message} — trying registry API`);
        const acceptUrl = `${this._getRegistryBaseUrl(tokenType)}/registry/transfer-instructions/v1/${encodeURIComponent(tiCid)}/choice-contexts/accept`;
        const { data: acceptCtx } = await getRegistryApi().post(acceptUrl, { excludeDebugFields: true }, {
          headers: { Authorization: `Bearer ${adminToken}`, Accept: 'application/json' },
        });
        const TRANSFER_INSTRUCTION_INTERFACE = '#splice-api-token-transfer-instruction-v1:Splice.Api.Token.TransferInstructionV1:TransferInstruction';
        const res = await cantonService.exerciseChoice({
          token: adminToken,
          actAsParty: acceptActAs,
          templateId: TRANSFER_INSTRUCTION_INTERFACE,
          contractId: tiCid,
          choice: 'TransferInstruction_Accept',
          choiceArgument: {
            extraArgs: {
              context: acceptCtx.choiceContextData || { values: {} },
              meta: { values: {} },
            },
          },
          readAs: acceptReadAs,
          synchronizerId,
          disclosedContracts: (acceptCtx.disclosedContracts || []).map(dc => ({
            templateId: dc.templateId,
            contractId: dc.contractId,
            createdEventBlob: dc.createdEventBlob,
            synchronizerId: dc.synchronizerId || synchronizerId,
          })),
        });
        console.log(`[CantonSDK]    ✅ Transfer ACCEPTED via registry API — real ${symbol} tokens moved!`);
        return res;
      }
    });

    return acceptResult;
  }

  /**
   * Backward-compatible alias used by balance routes.
   * Executes a full token transfer sender -> receiver.
   */
  async executeFullTransfer(senderPartyId, receiverPartyId, amount, symbol, transferId = '') {
    if (transferId) {
      console.log(`[CantonSDK] executeFullTransfer transferId=${String(transferId).substring(0, 40)}...`);
    }
    return this.performTransfer(senderPartyId, receiverPartyId, amount, symbol);
  }

  /**
   * Accept a TransferInstruction (transfer offer) — used by TransferOfferService.
   * 
   * This exercises `TransferInstruction_Accept` on the Canton ledger directly.
   * The receiver exercises the Accept choice. Since all parties are on the same
   * participant, both receiver AND the instruction's sender must be in actAs.
   * 
   * @param {string} transferInstructionCid - The TransferInstruction contract ID
   * @param {string} receiverPartyId - The party accepting (receiver of tokens)
   * @param {string} symbol - Token symbol (CC, CBTC) for routing to correct registry
   * @returns {Object} Exercise result
   */
  async acceptTransfer(transferInstructionCid, receiverPartyId, symbol = null) {
    if (!this.isReady()) {
      throw new Error('Canton SDK not initialized');
    }

    console.log(`[CantonSDK] 📨 Accepting transfer: ${transferInstructionCid.substring(0, 30)}... for ${receiverPartyId.substring(0, 30)}...`);

    const tokenType = symbol ? getTokenSystemType(symbol) : null;
    const adminToken = await tokenProvider.getServiceToken();
    const configModule = require('../config');
    const synchronizerId = configModule.canton.synchronizerId;
    const operatorPartyId = configModule.canton.operatorPartyId;

    const TRANSFER_INSTRUCTION_INTERFACE = '#splice-api-token-transfer-instruction-v1:Splice.Api.Token.TransferInstructionV1:TransferInstruction';

    // actAs needs BOTH receiver + operator (operator hosts all external parties)
    const actAsParties = [receiverPartyId];
    if (operatorPartyId && operatorPartyId !== receiverPartyId) {
      actAsParties.push(operatorPartyId);
    }

    // Try SDK method first
    try {
      if (this.sdk.tokenStandard && typeof this.sdk.tokenStandard.exerciseTransferInstructionChoice === 'function') {
        return await this._withPartyContext(receiverPartyId, async () => {
          const [acceptCmd, acceptDisclosed] = await this.sdk.tokenStandard.exerciseTransferInstructionChoice(
            transferInstructionCid,
            'Accept'
          );

          const commands = Array.isArray(acceptCmd) ? acceptCmd : [acceptCmd];
          let result = null;
          for (const rawCmd of commands) {
            const cmd = rawCmd.ExerciseCommand || rawCmd;
            result = await cantonService.exerciseChoice({
              token: adminToken,
              actAsParty: actAsParties,
              templateId: cmd.templateId,
              contractId: cmd.contractId,
              choice: cmd.choice,
              choiceArgument: cmd.choiceArgument,
              readAs: actAsParties,
              synchronizerId,
              disclosedContracts: (acceptDisclosed || []).map(dc => ({
                templateId: dc.templateId,
                contractId: dc.contractId,
                createdEventBlob: dc.createdEventBlob,
                synchronizerId: dc.synchronizerId || synchronizerId,
              })),
            });
          }
          console.log(`[CantonSDK]    ✅ Transfer accepted via SDK`);
          return result;
        });
      }
    } catch (sdkErr) {
      console.warn(`[CantonSDK]    SDK exerciseTransferInstructionChoice failed: ${sdkErr.message} — trying registry API`);
    }

    const registryBase = this._getRegistryBaseUrl(tokenType);
    const acceptContextUrl = `${registryBase}/registry/transfer-instruction/v1/${encodeURIComponent(transferInstructionCid)}/choice-contexts/accept`;

    console.log(`[CantonSDK]    Trying registry accept context: ${acceptContextUrl.substring(0, 100)}...`);

    let acceptCtx;
    try {
      const { data } = await getRegistryApi().post(acceptContextUrl, { excludeDebugFields: true });
      acceptCtx = data;
    } catch (ctxErr) {
      const status = ctxErr.response?.status;
      const errText = typeof ctxErr.response?.data === 'string'
        ? ctxErr.response.data
        : JSON.stringify(ctxErr.response?.data ?? ctxErr.message);
      if (status === 404 || errText.includes('CONTRACT_NOT_FOUND') || errText.includes('not found')) {
        const gone = new Error(`Transfer instruction ${transferInstructionCid.substring(0, 20)}... already archived/expired`);
        gone.code = 'CONTRACT_NOT_FOUND';
        throw gone;
      }
      throw new Error(`Accept context API failed (${status}): ${errText.substring(0, 200)}`);
    }
    console.log(`[CantonSDK]    ✅ Got accept context (${acceptCtx.disclosedContracts?.length || 0} disclosed contracts)`);

    const result = await cantonService.exerciseChoice({
      token: adminToken,
      actAsParty: actAsParties,
      templateId: TRANSFER_INSTRUCTION_INTERFACE,
      contractId: transferInstructionCid,
      choice: 'TransferInstruction_Accept',
      choiceArgument: {
        extraArgs: {
          context: acceptCtx.choiceContextData || { values: {} },
          meta: { values: {} },
        },
      },
      readAs: actAsParties,
      synchronizerId,
      disclosedContracts: (acceptCtx.disclosedContracts || []).map(dc => ({
        templateId: dc.templateId,
        contractId: dc.contractId,
        createdEventBlob: dc.createdEventBlob,
        synchronizerId: dc.synchronizerId || synchronizerId,
      })),
    });

    console.log(`[CantonSDK]    ✅ Transfer accepted via registry API`);
    return result;
  }

  /**
   * Get the Transfer Factory URL for a token type.
   * - CC (splice):     /registry/transfer-instruction/v1/transfer-factory
   * - CBTC (utilities): {token-standard}/v0/registrars/{admin}/registry/transfer-instruction/v1/transfer-factory
   */
  _getTransferFactoryUrl(tokenType) {
    if (tokenType === 'utilities') {
      const adminParty = getInstrumentAdmin('CBTC');
      return `${UTILITIES_CONFIG.TOKEN_STANDARD_URL}/v0/registrars/${encodeURIComponent(adminParty)}/registry/transfer-instruction/v1/transfer-factory`;
    }
    return `${CANTON_SDK_CONFIG.REGISTRY_API_URL}/registry/transfer-instruction/v1/transfer-factory`;
  }

  /**
   * Get the base registry URL for a token type (for building sub-paths).
   */
  _getRegistryBaseUrl(tokenType) {
    if (tokenType === 'utilities') {
      const adminParty = getInstrumentAdmin('CBTC');
      return `${UTILITIES_CONFIG.TOKEN_STANDARD_URL}/v0/registrars/${encodeURIComponent(adminParty)}`;
    }
    return CANTON_SDK_CONFIG.REGISTRY_API_URL;
  }

  /**
   * Get the correct Transfer Factory Registry URL for a token type (legacy).
   */
  _getRegistryUrlForToken(tokenType) {
    if (tokenType === 'utilities') {
      const adminParty = getInstrumentAdmin('CBTC');
      return `${UTILITIES_CONFIG.TOKEN_STANDARD_URL}/v0/registrars/${encodeURIComponent(adminParty)}`;
    }
    return CANTON_SDK_CONFIG.REGISTRY_API_URL;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // ALLOCATIONS — Settlement via Allocation API
  //
  // CC (Splice/Amulet): Allocation Factory IS available on Scan Proxy
  //   POST /registry/allocation-instruction/v1/allocation-factory → 200
  //   POST /registry/allocations/v1/{id}/choice-contexts/execute-transfer → available
  //
  // CBTC (Utilities): Allocation Factory NOT yet available (404)
  //   Falls back to Transfer Factory API for CBTC settlement
  //
  // Architecture:
  // 1. At ORDER PLACEMENT: User creates Allocation (exchange = executor)
  // 2. At MATCH TIME: Exchange executes Allocation with its OWN key
  // 3. At CANCEL: Exchange cancels Allocation, funds returned to user
  //
  // @see https://docs.sync.global/app_dev/api/splice-api-token-allocation-v1/
  // @see https://docs.sync.global/app_dev/api/splice-api-token-allocation-instruction-v1/
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Create an Allocation for an order.
   * 
   * Called at ORDER PLACEMENT time. Locks the user's holdings and sets the
   * exchange as executor. The exchange can later execute this allocation
   * at match time without needing the user's key.
   * 
   * @param {string} senderPartyId - The order placer (funds locked from this party)
   * @param {string} receiverPartyId - The counterparty (null if unknown at order time)
   * @param {string} amount - Amount to allocate (as string)
   * @param {string} symbol - Exchange symbol (e.g., 'CC', 'CBTC')
   * @param {string} executorPartyId - The exchange party (settles at match time)
   * @param {string} orderId - Order ID for tracking (used in memo)
   * @returns {Object} { allocationContractId, result }
   */
  async createAllocation(senderPartyId, receiverPartyId, amount, symbol, executorPartyId, orderId = '') {
    if (!this.isReady()) {
      throw new Error('Canton SDK not initialized');
    }

    // AllocationFactory_Allocate is controlled by the transfer-leg sender.
    // External parties cannot be backend submitters on this participant.
    // Attempting match-time creation for ext-* sender causes:
    // NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT.
    const isExternalSender = typeof senderPartyId === 'string' && senderPartyId.startsWith('ext-');
    if (isExternalSender) {
      throw new Error(
        'Allocation creation requires sender authorization. External sender cannot be submitted by backend at match-time; create/authorize allocations at order placement via interactive signing.'
      );
    }

    const instrumentId = toCantonInstrument(symbol);
    const tokenSystemType = getTokenSystemType(symbol);
    const adminParty = this.getInstrumentAdminForSymbol(symbol);

    console.log(`[CantonSDK] 📋 Creating Allocation: ${amount} ${symbol} (${instrumentId})`);
    console.log(`[CantonSDK]    Sender: ${senderPartyId.substring(0, 30)}...`);
    console.log(`[CantonSDK]    Receiver: ${receiverPartyId ? receiverPartyId.substring(0, 30) + '...' : 'TBD (set at match)'}`);
    console.log(`[CantonSDK]    Executor: ${executorPartyId.substring(0, 30)}...`);

    // Route to correct API based on token system type
    if (tokenSystemType === 'utilities') {
      return this._createUtilitiesAllocation(senderPartyId, receiverPartyId, amount, symbol, executorPartyId, orderId);
    }
    return this._createSpliceAllocation(senderPartyId, receiverPartyId, amount, symbol, executorPartyId, orderId);
  }

  /**
   * Build interactive self-transfer command for Temple pattern order placement.
   * 
   * Self-transfer must be interactive for external parties (backend cannot submit on their behalf).
   * This prepares the TransferFactory_Transfer command for user signature.
   * 
   * @param {string} partyId - Party performing self-transfer
   * @param {string} amount - Exact amount needed
   * @param {string} symbol - Token symbol
   * @returns {Promise<{command: Object, readAs: Array, disclosedContracts: Array, synchronizerId: string, holdingCids: Array}>}
   */
  async buildSelfTransferInteractiveCommand(partyId, amount, symbol) {
    if (!this.isReady()) {
      throw new Error('Canton SDK not initialized');
    }

    const instrumentId = toCantonInstrument(symbol);
    const adminParty = this.getInstrumentAdminForSymbol(symbol);
    const tokenType = getTokenSystemType(symbol);
    const adminToken = await tokenProvider.getServiceToken();
    const configModule = require('../config');
    const synchronizerId = configModule.canton.synchronizerId;

    console.log(`[CantonSDK] 🔄 Temple Pattern: Building interactive self-transfer command`);
    console.log(`[CantonSDK]    Party: ${partyId.substring(0, 30)}...`);
    console.log(`[CantonSDK]    Amount: ${amount} ${symbol}`);

    // Query holdings for the sender
    const holdings = await this._withPartyContext(partyId, async () => {
      return await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];
    });

    const holdingCids = holdings
      .filter(h => extractInstrumentId(h.interfaceViewValue?.instrumentId) === instrumentId)
      .map(h => h.contractId);

    if (holdingCids.length === 0) {
      throw new Error(`No ${symbol} holdings found for self-transfer`);
    }

    console.log(`[CantonSDK]    Found ${holdingCids.length} ${symbol} UTXOs for self-transfer`);

    // Get transfer factory from registry
    const registryUrl = this._getTransferFactoryUrl(tokenType);
    const now = new Date().toISOString();
    const executeBefore = new Date(Date.now() + 86400000).toISOString();

    const { data: factory } = await getRegistryApi().post(registryUrl, {
      choiceArguments: {
        expectedAdmin: adminParty,
        transfer: {
          sender: partyId,
          receiver: partyId, // Self-transfer
          amount: toDamlNumericString(amount),
          instrumentId: { id: instrumentId, admin: adminParty },
          requestedAt: now,
          executeBefore,
          inputHoldingCids: holdingCids,
          meta: { values: {} },
        },
        extraArgs: { context: { values: {} }, meta: { values: {} } },
      },
      excludeDebugFields: true,
    }, {
      headers: { Authorization: `Bearer ${adminToken}`, Accept: 'application/json' },
    });

    const TRANSFER_FACTORY_INTERFACE = UTILITIES_CONFIG.TRANSFER_FACTORY_INTERFACE
      || '#splice-api-token-transfer-instruction-v1:Splice.Api.Token.TransferInstructionV1:TransferFactory';

    return {
      command: {
        ExerciseCommand: {
          templateId: TRANSFER_FACTORY_INTERFACE,
          contractId: factory.factoryId,
          choice: 'TransferFactory_Transfer',
          choiceArgument: {
            expectedAdmin: adminParty,
            transfer: {
              sender: partyId,
              receiver: partyId,
              amount: toDamlNumericString(amount),
              instrumentId: { id: instrumentId, admin: adminParty },
              requestedAt: now,
              executeBefore,
              inputHoldingCids: holdingCids,
              meta: { values: {} },
            },
            extraArgs: {
              context: factory.choiceContext?.choiceContextData || { values: {} },
              meta: { values: {} },
            },
          },
        },
      },
      readAs: [partyId],
      disclosedContracts: (factory.choiceContext?.disclosedContracts || []).map(dc => ({
        templateId: dc.templateId,
        contractId: dc.contractId,
        createdEventBlob: dc.createdEventBlob,
        synchronizerId: dc.synchronizerId || synchronizerId,
      })),
      synchronizerId,
      holdingCids,
    };
  }

  /**
   * Extract exact-amount holding CID from self-transfer execution result.
   * 
   * @param {Object} transferResult - Result from executing self-transfer
   * @param {string} expectedAmount - Expected amount
   * @returns {string|null} Exact-amount holding CID or null
   */
  _extractExactAmountHoldingFromResult(transferResult, expectedAmount) {
    if (!transferResult?.transaction?.events) {
      return null;
    }

    const createdHoldings = this._extractCreatedHoldingCids(transferResult);
    
    // Find holding with exact amount
    const exactAmountHolding = createdHoldings.find(h => {
      const holdingAmount = h.amount || '0';
      return new Decimal(holdingAmount).eq(new Decimal(expectedAmount));
    });

    return exactAmountHolding?.contractId || (createdHoldings.length > 0 ? createdHoldings[0].contractId : null);
  }

  /**
   * Build multi-leg allocation command for Temple pattern settlement.
   * Creates allocation with 2 transfer legs: buy leg + sell leg.
   * 
   * @param {string} executorPartyId - Exchange operator
   * @param {Array} transferLegs - [{sender, receiver, amount, symbol}, ...]
   * @param {string} settlementId - Settlement reference ID
   * @param {Array|null} holdingCids - Input holding CIDs (from withdrawn allocations), or null to query fresh
   * @returns {Promise<{command, readAs, disclosedContracts, synchronizerId}>}
   */
  async buildMultiLegAllocationCommand(executorPartyId, transferLegs, settlementId, holdingCids = null) {
    if (!this.isReady()) {
      throw new Error('Canton SDK not initialized');
    }

    if (!transferLegs || transferLegs.length !== 2) {
      throw new Error('Multi-leg allocation requires exactly 2 transfer legs');
    }

    const [buyLeg, sellLeg] = transferLegs;
    const buyTokenType = getTokenSystemType(buyLeg.symbol);
    const sellTokenType = getTokenSystemType(sellLeg.symbol);
    
    // Use the token system type of the first leg (both should be same system)
    const tokenSystemType = buyTokenType !== 'unknown' ? buyTokenType : sellTokenType;
    if (tokenSystemType === 'unknown') {
      throw new Error('Cannot determine token system type for multi-leg allocation');
    }

    const buyInstrumentId = toCantonInstrument(buyLeg.symbol);
    const sellInstrumentId = toCantonInstrument(sellLeg.symbol);
    const buyAdminParty = this.getInstrumentAdminForSymbol(buyLeg.symbol);
    const sellAdminParty = this.getInstrumentAdminForSymbol(sellLeg.symbol);

    console.log(`[CantonSDK] 🔄 Temple Pattern: Building multi-leg allocation for settlement ${settlementId}`);
    console.log(`[CantonSDK]    Buy leg: ${buyLeg.sender.substring(0, 20)}... → ${buyLeg.receiver.substring(0, 20)}... (${buyLeg.amount} ${buyLeg.symbol})`);
    console.log(`[CantonSDK]    Sell leg: ${sellLeg.sender.substring(0, 20)}... → ${sellLeg.receiver.substring(0, 20)}... (${sellLeg.amount} ${sellLeg.symbol})`);

    // Query fresh holdings if not provided
    let finalHoldingCids = holdingCids;
    if (!finalHoldingCids || finalHoldingCids.length === 0) {
      console.log(`[CantonSDK]    Querying fresh holdings for both parties...`);
      const buyLegHoldings = await this._withPartyContext(buyLeg.sender, async () => {
        return await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];
      });
      const sellLegHoldings = await this._withPartyContext(sellLeg.sender, async () => {
        return await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];
      });
      
      const buyLegCids = buyLegHoldings
        .filter(h => extractInstrumentId(h.interfaceViewValue?.instrumentId) === buyInstrumentId)
        .map(h => h.contractId);
      const sellLegCids = sellLegHoldings
        .filter(h => extractInstrumentId(h.interfaceViewValue?.instrumentId) === sellInstrumentId)
        .map(h => h.contractId);
      
      finalHoldingCids = [...buyLegCids, ...sellLegCids];
      console.log(`[CantonSDK]    Found ${buyLegCids.length} ${buyLeg.symbol} holdings + ${sellLegCids.length} ${sellLeg.symbol} holdings`);
    }

    // Build choice args with multiple transfer legs
    // Note: Splice API may require creating two separate allocations if multi-leg not supported
    const choiceArgs = this._buildMultiLegAllocationChoiceArgs({
      adminParty: buyAdminParty, // Use buy leg admin (both should be same system)
      executorPartyId,
      transferLegs: [
        {
          sender: buyLeg.sender,
          receiver: buyLeg.receiver,
          amount: buyLeg.amount,
          instrumentId: buyInstrumentId,
        },
        {
          sender: sellLeg.sender,
          receiver: sellLeg.receiver,
          amount: sellLeg.amount,
          instrumentId: sellInstrumentId,
        },
      ],
      orderId: settlementId,
      holdingCids: finalHoldingCids,
      choiceContextData: null,
      tokenSystemType,
    });

    const allocationFactoryUrl = tokenSystemType === 'utilities'
      ? `${UTILITIES_CONFIG.TOKEN_STANDARD_URL}/v0/registrars/${encodeURIComponent(buyAdminParty)}/registry/allocation-instruction/v1/allocation-factory`
      : `${CANTON_SDK_CONFIG.REGISTRY_API_URL}/registry/allocation-instruction/v1/allocation-factory`;

    try {
      const { data: factory } = await getRegistryApi().post(allocationFactoryUrl, {
        choiceArguments: choiceArgs,
        excludeDebugFields: true,
      }, {
        headers: { Authorization: `Bearer ${await tokenProvider.getServiceToken()}`, Accept: 'application/json' },
      });

      const exerciseArgs = this._buildMultiLegAllocationChoiceArgs({
        adminParty: buyAdminParty,
        executorPartyId,
        transferLegs: [
          {
            sender: buyLeg.sender,
            receiver: buyLeg.receiver,
            amount: buyLeg.amount,
            instrumentId: buyInstrumentId,
          },
          {
            sender: sellLeg.sender,
            receiver: sellLeg.receiver,
            amount: sellLeg.amount,
            instrumentId: sellInstrumentId,
          },
        ],
        orderId: settlementId,
        holdingCids,
        choiceContextData: factory.choiceContext?.choiceContextData,
        tokenSystemType,
      });

      const configModule = require('../config');
      const synchronizerId = this._pickSynchronizerIdFromDisclosed(
        factory.choiceContext?.disclosedContracts || [],
        configModule.canton.synchronizerId
      );

      const readAsParties = [
        executorPartyId,
        buyLeg.sender,
        buyLeg.receiver,
        sellLeg.sender,
        sellLeg.receiver,
      ].filter(Boolean);

      return {
        command: {
          ExerciseCommand: {
            templateId: UTILITIES_CONFIG.ALLOCATION_INSTRUCTION_FACTORY_INTERFACE,
            contractId: factory.factoryId,
            choice: 'AllocationFactory_Allocate',
            choiceArgument: exerciseArgs,
          },
        },
        readAs: [...new Set(readAsParties)],
        disclosedContracts: (factory.choiceContext?.disclosedContracts || []).map(dc => ({
          templateId: dc.templateId,
          contractId: dc.contractId,
          createdEventBlob: dc.createdEventBlob,
          synchronizerId: dc.synchronizerId || synchronizerId,
        })),
        synchronizerId,
      };
    } catch (err) {
      // If multi-leg allocation fails, fall back to creating two separate allocations
      console.warn(`[CantonSDK] Multi-leg allocation failed: ${err.message} — will create two separate allocations`);
      throw new Error(`Multi-leg allocation not supported, need to create two allocations: ${err.message}`);
    }
  }

  /**
   * Build an AllocationFactory_Allocate interactive command payload for an external signer.
   * This does NOT submit the command; caller includes the returned command in
   * /v2/interactive-submission/prepare so the external party signs it.
   * 
   * For Temple pattern: receiverPartyId can be same as senderPartyId (self-allocation).
   */
  async buildAllocationInteractiveCommand(senderPartyId, receiverPartyId, amount, symbol, executorPartyId, orderId = '', overrideHoldingCids = null) {
    if (!this.isReady()) {
      throw new Error('Canton SDK not initialized');
    }

    const tokenSystemType = getTokenSystemType(symbol);
    const instrumentId = toCantonInstrument(symbol);
    const adminParty = this.getInstrumentAdminForSymbol(symbol);

    let holdingCids;
    if (overrideHoldingCids && overrideHoldingCids.length > 0) {
      holdingCids = overrideHoldingCids;
      console.log(`[CantonSDK]    Using ${holdingCids.length} pre-specified holding CID(s) for allocation`);
    } else {
      const holdings = await this._withPartyContext(senderPartyId, async () => {
        return await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];
      });

      holdingCids = holdings
        .filter(h => extractInstrumentId(h.interfaceViewValue?.instrumentId) === instrumentId)
        .map(h => h.contractId);
    }

    if (holdingCids.length === 0) {
      throw new Error(`No ${symbol} holdings found for allocation sender ${senderPartyId.substring(0, 30)}...`);
    }

    // Temple Pattern: Self-allocation (sender = receiver = user) for order placement
    // Settlement will withdraw and create new multi-leg allocation
    const isSelfAllocation = receiverPartyId === senderPartyId;
    const effectiveReceiver = receiverPartyId || executorPartyId;
    
    if (isSelfAllocation) {
      console.log(`[CantonSDK]    🔄 Temple Pattern: Self-allocation (sender = receiver = user, NOT executed)`);
    }
    
    const choiceArgs = this._buildAllocationChoiceArgs({
      adminParty,
      senderPartyId,
      receiverPartyId: effectiveReceiver,
      executorPartyId,
      amount,
      instrumentId,
      orderId,
      holdingCids,
      choiceContextData: null,
      tokenSystemType,
    });

    const allocationFactoryUrl = tokenSystemType === 'utilities'
      ? `${UTILITIES_CONFIG.TOKEN_STANDARD_URL}/v0/registrars/${encodeURIComponent(adminParty)}/registry/allocation-instruction/v1/allocation-factory`
      : `${CANTON_SDK_CONFIG.REGISTRY_API_URL}/registry/allocation-instruction/v1/allocation-factory`;

    const adminToken = await tokenProvider.getServiceToken();
    const { data: factory } = await getRegistryApi().post(allocationFactoryUrl, {
      choiceArguments: choiceArgs,
      excludeDebugFields: true,
    }, {
      headers: { Authorization: `Bearer ${adminToken}`, Accept: 'application/json' },
    });

    const exerciseArgs = this._buildAllocationChoiceArgs({
      adminParty,
      senderPartyId,
      receiverPartyId: effectiveReceiver,
      executorPartyId,
      amount,
      instrumentId,
      orderId,
      holdingCids,
      choiceContextData: factory.choiceContext?.choiceContextData,
      tokenSystemType,
    });

    const configModule = require('../config');
    const synchronizerId = this._pickSynchronizerIdFromDisclosed(
      factory.choiceContext?.disclosedContracts || [],
      configModule.canton.synchronizerId
    );

    return {
      command: {
        ExerciseCommand: {
          templateId: UTILITIES_CONFIG.ALLOCATION_INSTRUCTION_FACTORY_INTERFACE,
          contractId: factory.factoryId,
          choice: 'AllocationFactory_Allocate',
          choiceArgument: exerciseArgs,
        },
      },
      readAs: [senderPartyId, effectiveReceiver, executorPartyId],
      disclosedContracts: (factory.choiceContext?.disclosedContracts || []).map(dc => ({
        templateId: dc.templateId,
        contractId: dc.contractId,
        createdEventBlob: dc.createdEventBlob,
        synchronizerId: dc.synchronizerId || synchronizerId,
      })),
      synchronizerId,
    };
  }

  /**
   * Build multi-leg allocation choiceArguments for Temple pattern settlement.
   * Creates allocation with 2 transfer legs: buy leg + sell leg.
   * 
   * @param {Object} params - { adminParty, executorPartyId, transferLegs: [{sender, receiver, amount, instrumentId}], orderId, holdingCids, choiceContextData, tokenSystemType }
   * @returns {Object} Allocation choice arguments with multiple transfer legs
   */
  _buildMultiLegAllocationChoiceArgs(params) {
    const {
      adminParty, executorPartyId, transferLegs, orderId, holdingCids,
      choiceContextData, tokenSystemType = 'splice',
    } = params;

    if (!transferLegs || transferLegs.length !== 2) {
      throw new Error('Multi-leg allocation requires exactly 2 transfer legs (buy + sell)');
    }

    const nowMs = Date.now();
    const now = new Date(nowMs).toISOString();
    const defaultSettleWindowMs =
      tokenSystemType === 'utilities'
        ? 24 * 60 * 60 * 1000   // 24h
        : 15 * 60 * 1000;       // 15m
    const configuredSettleWindowMs = Number(process.env.ALLOCATION_SETTLE_WINDOW_MS || defaultSettleWindowMs);
    const settleWindowMs = Number.isFinite(configuredSettleWindowMs) && configuredSettleWindowMs > 60_000
      ? configuredSettleWindowMs
      : defaultSettleWindowMs;
    const allocateWindowMs = Math.max(60_000, Math.min(5 * 60 * 1000, Math.floor(settleWindowMs / 2)));
    const allocateBefore = new Date(nowMs + allocateWindowMs).toISOString();
    const settleBefore = new Date(nowMs + settleWindowMs).toISOString();

    // Build allocation with multiple transfer legs
    // Note: Splice API may require separate allocations per leg, but we structure it for multi-leg support
    const [buyLeg, sellLeg] = transferLegs;

    return {
      expectedAdmin: adminParty,
      allocation: {
        settlement: {
          executor: executorPartyId,
          settleBefore,
          allocateBefore,
          settlementRef: { id: orderId || `settlement-${Date.now()}` },
          requestedAt: now,
          meta: { values: {} },
        },
        // Primary transfer leg (buy leg: buyer receives base tokens)
        transferLegId: `${orderId}-buy-leg`,
        transferLeg: {
          sender: buyLeg.sender,
          receiver: buyLeg.receiver,
          amount: toDamlNumericString(buyLeg.amount),
          instrumentId: { id: buyLeg.instrumentId, admin: adminParty },
          meta: { values: {} },
        },
        // Secondary transfer leg (sell leg: seller receives quote tokens)
        // Note: If API doesn't support multiple legs in one allocation, we'll create two allocations
        transferLegs: [
          {
            transferLegId: `${orderId}-buy-leg`,
            transferLeg: {
              sender: buyLeg.sender,
              receiver: buyLeg.receiver,
              amount: toDamlNumericString(buyLeg.amount),
              instrumentId: { id: buyLeg.instrumentId, admin: adminParty },
              meta: { values: {} },
            },
          },
          {
            transferLegId: `${orderId}-sell-leg`,
            transferLeg: {
              sender: sellLeg.sender,
              receiver: sellLeg.receiver,
              amount: toDamlNumericString(sellLeg.amount),
              instrumentId: { id: sellLeg.instrumentId, admin: adminParty },
              meta: { values: {} },
            },
          },
        ],
      },
      requestedAt: now,
      inputHoldingCids: holdingCids,
      extraArgs: {
        context: choiceContextData || { values: {} },
        meta: { values: {} },
      },
    };
  }

  /**
   * Build the correct allocation choiceArguments structure.
   * 
   * VERIFIED against live Splice & Utilities endpoints (2026-02-17):
   * 
   * choiceArguments = {
   *   expectedAdmin,
   *   allocation: {
   *     settlement: { executor, settleBefore, allocateBefore, settlementRef: {id}, requestedAt, meta },
   *     transferLegId,
   *     transferLeg: { sender, receiver, amount, instrumentId: {id, admin}, meta }
   *   },
   *   requestedAt,         // top-level, separate from settlement.requestedAt
   *   inputHoldingCids,    // outside allocation, at choiceArguments level
   *   extraArgs: { context, meta }
   * }
   */
  _buildAllocationChoiceArgs(params) {
    const {
      adminParty, senderPartyId, receiverPartyId, executorPartyId,
      amount, instrumentId, orderId, holdingCids,
      choiceContextData, tokenSystemType = 'splice',
    } = params;

      const nowMs = Date.now();
      const now = new Date(nowMs).toISOString();
      // Splice (Amulet/CC) enforces lock-expiry invariants during allocation prepare.
      // The settle window must be shorter than the LockedAmulet's lock expiry
      // (tied to the current Splice round, typically 10-60 min on devnet).
      // 5 min was too aggressive for a CLOB exchange — orders may wait several
      // minutes for a counterparty.  15 min balances safety with usability.
      // Utilities tokens (CBTC) are not round-bound, so 24h is safe.
      const defaultSettleWindowMs =
        tokenSystemType === 'utilities'
          ? 24 * 60 * 60 * 1000   // 24h
          : 15 * 60 * 1000;       // 15m — safe within Splice round bounds
      const configuredSettleWindowMs = Number(process.env.ALLOCATION_SETTLE_WINDOW_MS || defaultSettleWindowMs);
      const settleWindowMs = Number.isFinite(configuredSettleWindowMs) && configuredSettleWindowMs > 60_000
        ? configuredSettleWindowMs
        : defaultSettleWindowMs;
      const allocateWindowMs = Math.max(60_000, Math.min(5 * 60 * 1000, Math.floor(settleWindowMs / 2)));
      const allocateBefore = new Date(nowMs + allocateWindowMs).toISOString();
      const settleBefore = new Date(nowMs + settleWindowMs).toISOString();

    return {
      expectedAdmin: adminParty,
      allocation: {
        settlement: {
          executor: executorPartyId,
          settleBefore,
          allocateBefore,
          settlementRef: { id: orderId || `settlement-${Date.now()}` },
          requestedAt: now,
          meta: { values: {} },
        },
        transferLegId: orderId || `leg-${Date.now()}`,
        transferLeg: {
          sender: senderPartyId,
          receiver: receiverPartyId || executorPartyId,
          amount: toDamlNumericString(amount),
          instrumentId: { id: instrumentId, admin: adminParty },
          meta: { values: {} },
          },
        },
      requestedAt: now,
        inputHoldingCids: holdingCids,
      extraArgs: {
        context: choiceContextData || { values: {} },
        meta: { values: {} },
      },
    };
  }

  /**
   * Extract created Amulet/Holding contract IDs from transaction events.
   * Used after self-transfers to find the newly created exact-amount holding.
   */
  _extractCreatedHoldingCids(result) {
    const created = [];
    for (const event of (result?.transaction?.events || [])) {
      const ev = event.created || event.CreatedEvent;
      if (ev?.contractId) {
        const tpl = typeof ev.templateId === 'string' ? ev.templateId : '';
        if ((tpl.includes('Amulet') || tpl.includes('Holding')) &&
            !tpl.includes('Locked') && !tpl.includes('Allocation') &&
            !tpl.includes('Reward') && !tpl.includes('Marker') &&
            !tpl.includes('FeaturedApp')) {
          const payload = ev.createArgument || ev.payload || {};
          const amt = payload?.amount?.initialAmount || payload?.amount || null;
          created.push({ contractId: ev.contractId, amount: amt });
        }
      }
    }
    return created;
  }

  _pickSynchronizerIdFromDisclosed(disclosedContracts, fallbackSynchronizerId) {
    if (Array.isArray(disclosedContracts)) {
      const found = disclosedContracts.find(dc => typeof dc?.synchronizerId === 'string' && dc.synchronizerId.length > 0);
      if (found?.synchronizerId) return found.synchronizerId;
    }
    return fallbackSynchronizerId;
  }

  /**
   * Create allocation for Splice tokens (CC/Amulet) via registry API.
   */
  async _createSpliceAllocation(senderPartyId, receiverPartyId, amount, symbol, executorPartyId, orderId) {
    const instrumentId = toCantonInstrument(symbol);
    const adminParty = this.instrumentAdminPartyId;

    return this._withPartyContext(senderPartyId, async () => {
      // Get sender's available holdings for auto-selection
      const holdings = await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];
      const holdingCids = holdings
        .filter(h => extractInstrumentId(h.interfaceViewValue?.instrumentId) === instrumentId)
        .map(h => h.contractId);

      if (holdingCids.length === 0) {
        throw new Error(`No ${symbol} holdings found for sender ${senderPartyId.substring(0, 30)}...`);
      }

      console.log(`[CantonSDK]    Found ${holdingCids.length} ${symbol} holding UTXOs`);

      // ── Call registry API to get the allocation factory context ─────
        const registryUrl = CANTON_SDK_CONFIG.REGISTRY_API_URL;
      const allocationFactoryUrl = `${registryUrl}/registry/allocation-instruction/v1/allocation-factory`;

      const choiceArgs = this._buildAllocationChoiceArgs({
        adminParty, senderPartyId, receiverPartyId, executorPartyId,
        amount, instrumentId, orderId, holdingCids,
        choiceContextData: null,
        tokenSystemType: 'splice',
      });

      console.log(`[CantonSDK]    Calling Splice allocation-factory: ${allocationFactoryUrl}`);

      const adminToken = await tokenProvider.getServiceToken();
      const { data: factory } = await getRegistryApi().post(allocationFactoryUrl, {
        choiceArguments: choiceArgs,
        excludeDebugFields: true,
      }, {
        headers: { Authorization: `Bearer ${adminToken}`, Accept: 'application/json' },
      });
      console.log(`[CantonSDK]    ✅ Splice allocation factory returned — factoryId: ${factory.factoryId?.substring(0, 30)}...`);

      // ── Build exercise command with real choice context from factory ─
      const exerciseArgs = this._buildAllocationChoiceArgs({
        adminParty, senderPartyId, receiverPartyId, executorPartyId,
        amount, instrumentId, orderId, holdingCids,
        choiceContextData: factory.choiceContext?.choiceContextData,
        tokenSystemType: 'splice',
      });
      const configModule = require('../config');
      const synchronizerId = this._pickSynchronizerIdFromDisclosed(
        factory.choiceContext?.disclosedContracts || [],
        configModule.canton.synchronizerId
      );

        const result = await cantonService.exerciseChoice({
          token: adminToken,
          // AllocationFactory_Allocate authorizes sender; submit as sender.
          actAsParty: [senderPartyId],
        templateId: UTILITIES_CONFIG.ALLOCATION_INSTRUCTION_FACTORY_INTERFACE,
        contractId: factory.factoryId,
        choice: 'AllocationFactory_Allocate',
        choiceArgument: exerciseArgs,
          readAs: [senderPartyId, receiverPartyId || executorPartyId, executorPartyId],
        synchronizerId,
        disclosedContracts: (factory.choiceContext?.disclosedContracts || []).map(dc => ({
            templateId: dc.templateId,
            contractId: dc.contractId,
            createdEventBlob: dc.createdEventBlob,
            synchronizerId: dc.synchronizerId || synchronizerId,
          })),
        });

        return this._extractAllocationResult(result, orderId);
    });
  }

  /**
   * Create allocation for Utilities tokens (CBTC) via Utilities Backend API.
   */
  async _createUtilitiesAllocation(senderPartyId, receiverPartyId, amount, symbol, executorPartyId, orderId) {
    const instrumentId = toCantonInstrument(symbol);
    const adminParty = getInstrumentAdmin(symbol);

    if (!adminParty) {
      throw new Error(`No admin party configured for ${symbol} (${instrumentId})`);
    }

    console.log(`[CantonSDK]    Using Utilities Backend API for ${symbol} allocation`);

    // Get sender's holdings via SDK
    const holdings = await this._withPartyContext(senderPartyId, async () => {
      return await this.sdk.tokenStandard?.listHoldingUtxos(false) || [];
    });

    const holdingCids = holdings
      .filter(h => extractInstrumentId(h.interfaceViewValue?.instrumentId) === instrumentId)
      .map(h => h.contractId);

    if (holdingCids.length === 0) {
      throw new Error(`No ${symbol} holdings found for sender ${senderPartyId.substring(0, 30)}...`);
    }

    console.log(`[CantonSDK]    Found ${holdingCids.length} ${symbol} holding UTXOs`);

    const tokenStandardUrl = UTILITIES_CONFIG.TOKEN_STANDARD_URL;
    const allocationFactoryUrl = `${tokenStandardUrl}/v0/registrars/${encodeURIComponent(adminParty)}/registry/allocation-instruction/v1/allocation-factory`;

    // Build choiceArguments with the correct nested structure
    const choiceArgs = this._buildAllocationChoiceArgs({
      adminParty, senderPartyId, receiverPartyId, executorPartyId,
      amount, instrumentId, orderId, holdingCids,
      choiceContextData: null,
      tokenSystemType: 'utilities',
    });

    console.log(`[CantonSDK]    Calling Utilities allocation-factory: ${allocationFactoryUrl}`);

    const adminToken = await tokenProvider.getServiceToken();
    const { data: factory } = await getRegistryApi().post(allocationFactoryUrl, {
      choiceArguments: choiceArgs,
      excludeDebugFields: true,
    }, {
      headers: { Authorization: `Bearer ${adminToken}`, Accept: 'application/json' },
    });
    console.log(`[CantonSDK]    ✅ Utilities allocation factory returned — factoryId: ${factory.factoryId?.substring(0, 30)}...`);

    // Build exercise command with real choice context from factory
    const exerciseArgs = this._buildAllocationChoiceArgs({
      adminParty, senderPartyId, receiverPartyId, executorPartyId,
      amount, instrumentId, orderId, holdingCids,
      choiceContextData: factory.choiceContext?.choiceContextData,
      tokenSystemType: 'utilities',
    });

    const configModule = require('../config');
    const synchronizerId = this._pickSynchronizerIdFromDisclosed(
      factory.choiceContext?.disclosedContracts || [],
      configModule.canton.synchronizerId
    );

    const result = await cantonService.exerciseChoice({
      token: adminToken,
      // AllocationFactory_Allocate authorizes sender; submit as sender.
      actAsParty: [senderPartyId],
      templateId: UTILITIES_CONFIG.ALLOCATION_INSTRUCTION_FACTORY_INTERFACE,
      contractId: factory.factoryId,
        choice: 'AllocationFactory_Allocate',
      choiceArgument: exerciseArgs,
        readAs: [senderPartyId, receiverPartyId || executorPartyId, executorPartyId],
      synchronizerId,
      disclosedContracts: (factory.choiceContext?.disclosedContracts || []).map(dc => ({
        templateId: dc.templateId,
        contractId: dc.contractId,
        createdEventBlob: dc.createdEventBlob,
        synchronizerId: dc.synchronizerId || synchronizerId,
      })),
    });

      return this._extractAllocationResult(result, orderId);
  }

  // NOTE: _createDirectAllocation (AllocationRecord fallback) has been REMOVED.
  // CC allocations use the real Splice Allocation Factory API.
  // CBTC falls back to Transfer API at match time if allocation not available.

  /**
   * Extract Allocation contract ID from exercise result.
   */
  _extractAllocationResult(result, orderId) {
    let allocationContractId = null;
    const events = result?.transaction?.events || [];
    for (const event of events) {
      const created = event.created || event.CreatedEvent;
      if (created?.contractId) {
        const templateId = created.templateId || '';
        if (typeof templateId === 'string' && 
            (templateId.includes('Allocation') || templateId.includes('AllocationRecord'))) {
          allocationContractId = created.contractId;
          break;
        }
      }
    }

    if (!allocationContractId) {
      // Fallback: take the first created contract
      for (const event of events) {
        const created = event.created || event.CreatedEvent;
        if (created?.contractId) {
          allocationContractId = created.contractId;
          break;
        }
      }
    }

    if (allocationContractId) {
      console.log(`[CantonSDK]    ✅ Allocation created: ${allocationContractId.substring(0, 30)}...`);
    } else {
      console.log(`[CantonSDK]    ℹ️ No Allocation contract found in result — may have auto-completed`);
    }

    return {
      allocationContractId,
      result,
      orderId,
      updateId: result?.transaction?.updateId,
    };
  }

  /**
   * Fetch pending Allocation requests for a party.
   * Used to check if allocations exist before settlement.
   * 
   * @param {string} partyId - Party to query
   * @returns {Array} Pending allocation request views
   */
  async fetchPendingAllocationRequests(partyId) {
    if (!this.isReady()) return [];

    return this._withPartyContext(partyId, async () => {
      try {
        if (typeof this.sdk.tokenStandard?.fetchPendingAllocationRequestView === 'function') {
          const requests = await this.sdk.tokenStandard.fetchPendingAllocationRequestView();
          console.log(`[CantonSDK] Found ${requests?.length || 0} pending allocation requests for ${partyId.substring(0, 30)}...`);
          return requests || [];
        }
        return [];
      } catch (error) {
        console.warn(`[CantonSDK] fetchPendingAllocationRequests failed: ${error.message}`);
        return [];
      }
    });
  }

  /**
   * Fetch pending Allocations (ready for execution).
   * Exchange checks these at match time before executing.
   * 
   * @param {string} partyId - Party to query (executor or sender)
   * @returns {Array} Pending allocation views
   */
  async fetchPendingAllocations(partyId) {
    if (!this.isReady()) return [];

    return this._withPartyContext(partyId, async () => {
      try {
        if (typeof this.sdk.tokenStandard?.fetchPendingAllocationView === 'function') {
          const allocations = await this.sdk.tokenStandard.fetchPendingAllocationView();
          console.log(`[CantonSDK] Found ${allocations?.length || 0} pending allocations for ${partyId.substring(0, 30)}...`);
          return allocations || [];
        }
        return [];
      } catch (error) {
        console.warn(`[CantonSDK] fetchPendingAllocations failed: ${error.message}`);
        return [];
      }
    });
  }

  /**
   * Execute an Allocation — exchange acts as executor.
   * 
   * Called at MATCH TIME. The exchange (executor) settles the allocation,
   * transferring funds from sender to receiver. NO user key needed.
   * 
   * CRITICAL: The Splice AmuletAllocation contract has TWO signatories:
   *   1. The operator (executor)
   *   2. The allocation owner (the wallet user who locked the tokens)
   * BOTH must be in actAs, otherwise Canton returns DAML_AUTHORIZATION_ERROR.
   * 
   * @param {string} allocationContractId - The Allocation contract ID
   * @param {string} executorPartyId - The exchange party (executor)
   * @param {string} symbol - Symbol for routing (splice vs utilities)
   * @param {string} ownerPartyId - The allocation owner (user who locked tokens) — REQUIRED for authorization
   * @returns {Object} Exercise result
   */
  async executeAllocation(allocationContractId, executorPartyId, symbol = null, ownerPartyId = null, receiverPartyId = null) {
    if (!allocationContractId) {
      console.warn('[CantonSDK] No allocationContractId — skipping execution');
      return null;
    }

    const readAsParties = [...new Set([executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean))];

    console.log(`[CantonSDK] ✅ Executing Allocation: ${allocationContractId.substring(0, 30)}...`);
    console.log(`[CantonSDK]    Executor: ${executorPartyId.substring(0, 30)}...`);
    console.log(`[CantonSDK]    Owner/Sender: ${ownerPartyId ? ownerPartyId.substring(0, 30) + '...' : 'N/A'}`);
    console.log(`[CantonSDK]    Receiver: ${receiverPartyId ? receiverPartyId.substring(0, 30) + '...' : 'N/A'}`);
    const summarize = (parties) => `[${parties.map(p => p.substring(0, 20) + '...').join(', ')}]`;
    const isAuthError = (err) => {
      const msg = String(err?.message || err || '');
      return msg.includes('DAML_AUTHORIZATION_ERROR') || msg.includes('requires authorizers');
    };
    const isNoSynchronizerError = (err) => {
      const msg = String(err?.message || err || '');
      return msg.includes('NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT') ||
             msg.includes('Not connected to a synchronizer on which this participant can submit for all submitters') ||
             msg.includes('cannot submit as the given submitter on any connected synchronizer') ||
             msg.includes('TRANSIENT_SYNCHRONIZER_ERROR');
    };

    const tokenSystemType = symbol ? getTokenSystemType(symbol) : null;
    const adminToken = await tokenProvider.getServiceToken();
    const configModule = require('../config');
    const synchronizerId = await cantonService.resolveSubmissionSynchronizerId(
      adminToken,
      configModule.canton.synchronizerId
    );
    console.log(`[CantonSDK]    Using submission synchronizer: ${synchronizerId || 'none'}`);

    // ── Route Utilities tokens (CBTC) directly to Utilities API ──────────
    // The SDK's exerciseAllocationChoice looks for AmuletAllocation (Splice-specific).
    // Utilities tokens use a DIFFERENT allocation contract type that the SDK
    // will NEVER find, resulting in "AmuletAllocation '...' not found" every time.
    // Skip the SDK entirely for Utilities tokens — go straight to the API.
    if (tokenSystemType === 'utilities') {
      console.log(`[CantonSDK]    ${symbol} is a Utilities token — using Utilities API directly (not SDK)`);
      return this._executeUtilitiesAllocation(allocationContractId, executorPartyId, symbol, adminToken, ownerPartyId, receiverPartyId);
    }

    // ── SETTLEMENT STRATEGY for Allocation_ExecuteTransfer ─────────────
    // Per Canton team (Huz): Allocation_ExecuteTransfer can be run with
    // JUST the executor if all other parties already signed the allocation.
    // The operator/executor's key IS hosted on the participant, so Canton
    // auto-signs for it in non-interactive submit-and-wait — no external
    // signature needed.
    //
    // STRATEGY ORDER:
    //   1. Non-interactive executor-only (fastest, no external signing needed)
    //   2. Interactive multi-sign (fallback if executor-only gets auth errors)
    //
    // We MUST try executor-only non-interactive FIRST because:
    //   - Interactive submission requires ALL actAs parties to provide
    //     external signatures. The operator's key is Keycloak-managed,
    //     not available for external signing → FAILED_TO_EXECUTE_TRANSACTION.
    //   - External parties (ext-*) CANNOT be in actAs for non-interactive
    //     (NO_SYNCHRONIZER error), but that's fine — they already signed
    //     the allocation at creation time.

    const isExtParty = (pid) => typeof pid === 'string' && pid.startsWith('ext-');
    const involvedParties = [executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean);
    const hasExternalParty = involvedParties.some(p => isExtParty(p));
    const executorIsHosted = executorPartyId && !isExtParty(executorPartyId);

    // ── Step 1: Try NON-INTERACTIVE executor-only ─────────────────────
    // The executor's key is hosted on the participant → Canton auto-signs.
    // External parties already approved the allocation → no actAs needed.
    if (executorIsHosted) {
      console.log(`[CantonSDK]    ⚡ Executor ${executorPartyId.substring(0, 20)}... is hosted — trying NON-INTERACTIVE executor-only first`);
      try {
        const result = await this._tryNonInteractiveExecutorOnly(
          allocationContractId, executorPartyId, adminToken,
          readAsParties, synchronizerId
        );
        if (result) {
          console.log(`[CantonSDK]    ✅ Executor-only non-interactive succeeded — updateId: ${result?.transaction?.updateId || 'N/A'}`);
          return result;
        }
      } catch (execOnlyErr) {
        const msg = String(execOnlyErr?.message || execOnlyErr || '');
        // Stale / contract not found → fail fast, no retries
        if (msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found') ||
            msg.includes('deadline-exceeded') || msg.includes('settleBefore') ||
            msg.includes('LockedAmulet') || msg.includes('STALE_')) {
          throw execOnlyErr;
        }
        console.warn(`[CantonSDK]    Executor-only non-interactive failed: ${msg.substring(0, 150)}`);
        console.warn(`[CantonSDK]    Will try interactive settlement as fallback...`);
      }
    }

    // ── Step 2: INTERACTIVE fallback (external parties sign) ──────────
    // Only needed if executor-only fails (e.g., DAML_AUTHORIZATION_ERROR
    // meaning the choice controller includes more than just the executor).
    if (hasExternalParty) {
      const externalParties = involvedParties.filter(p => isExtParty(p));
      const keyChecks = await Promise.all(externalParties.map(p => userRegistry.hasSigningKey(p)));
      const allHaveKeys = keyChecks.every(Boolean);

      if (allHaveKeys) {
        console.log(`[CantonSDK]    🔐 Trying INTERACTIVE settlement (external party signing)...`);
        try {
          return await this.executeAllocationInteractive(
            allocationContractId, executorPartyId, symbol, adminToken,
            ownerPartyId, receiverPartyId, synchronizerId
          );
        } catch (interactiveErr) {
          const msg = String(interactiveErr?.message || interactiveErr || '');
          // If stale/expired/not found, propagate immediately — no point retrying further
          if (msg.includes('STALE_') || msg.includes('SIGNING_KEY_MISSING') ||
              msg.includes('deadline-exceeded') || msg.includes('settleBefore') ||
              msg.includes('LockedAmulet') ||
              msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found')) {
            throw interactiveErr;
          }
          console.warn(`[CantonSDK]    Interactive settlement also failed: ${msg.substring(0, 150)} — falling through to broader non-interactive strategies`);
        }
      } else {
        const missingChecks = await Promise.all(externalParties.map(async p => ({ p, has: await userRegistry.hasSigningKey(p) })));
        const missing = missingChecks.filter(c => !c.has).map(c => c.p);
        console.warn(`[CantonSDK]    ⚠️ External parties present but signing keys missing for: ${missing.map(p => p.substring(0, 20) + '...').join(', ')}`);
        console.warn(`[CantonSDK]    Will attempt non-interactive strategies (may fail)`);
      }
    }

    const tryExecuteForActAs = async (actAsParties) => {
      console.log(`[CantonSDK]    Trying actAs (${actAsParties.length}): ${summarize(actAsParties)}`);

      // Attempt 1: SDK-generated command + disclosed contracts.
      if (this.isReady()) {
        try {
          if (this.sdk.tokenStandard && typeof this.sdk.tokenStandard.exerciseAllocationChoice === 'function') {
            // Root-cause fix:
            // Build the execute command in a party context that can definitely read the
            // allocation contract. For external-user allocations this is the owner party.
            // Using executor-only context can fail command generation and force flaky
            // registry fallback timeouts.
            const commandBuilderPartyId = ownerPartyId || executorPartyId;
            console.log(`[CantonSDK]    Building execute command in context: ${commandBuilderPartyId.substring(0, 30)}...`);
            return await this._withPartyContext(commandBuilderPartyId, async () => {
              const [executeCmd, disclosed] = await this.sdk.tokenStandard.exerciseAllocationChoice(
                allocationContractId,
                'ExecuteTransfer'
              );
              const commands = Array.isArray(executeCmd) ? executeCmd : [executeCmd];
              let result = null;
              for (const rawCmd of commands) {
                const cmd = rawCmd.ExerciseCommand || rawCmd;
                result = await cantonService.exerciseChoice({
                  token: adminToken,
                  actAsParty: actAsParties,
                  templateId: cmd.templateId,
                  contractId: cmd.contractId,
                  choice: cmd.choice,
                  choiceArgument: cmd.choiceArgument,
                  readAs: readAsParties,
                  synchronizerId,
                  disclosedContracts: (disclosed || []).map(dc => ({
                    templateId: dc.templateId,
                    contractId: dc.contractId,
                    createdEventBlob: dc.createdEventBlob,
                    synchronizerId: dc.synchronizerId || synchronizerId,
                  })),
                });
              }
              return result;
            });
          }
        } catch (sdkErr) {
          const sdkMsg = String(sdkErr?.message || sdkErr?.error || JSON.stringify(sdkErr) || '');
          // If the SDK already determined the allocation is stale, don't waste time on registry fallback.
          if (sdkMsg.includes('LockedAmulet') || sdkMsg.includes('not found') ||
              sdkMsg.includes('deadline-exceeded') || sdkMsg.includes('settleBefore')) {
            throw new Error(`STALE_ALLOCATION_LOCK_MISSING: ${sdkMsg}`);
          }
          console.warn(`[CantonSDK]    SDK exerciseAllocationChoice failed: ${sdkMsg} — trying registry API`);
        }
      } else {
        console.warn(`[CantonSDK]    SDK not ready — trying registry API`);
      }

      // Attempt 2: Registry choice-context API.
      const registryUrl = CANTON_SDK_CONFIG.REGISTRY_API_URL;
      const encodedCid = encodeURIComponent(allocationContractId);
      const executeContextUrl = `${registryUrl}/registry/allocations/v1/${encodedCid}/choice-contexts/execute-transfer`;
      const { data: context } = await getRegistryApi().post(executeContextUrl, { excludeDebugFields: true });
      const ALLOCATION_INTERFACE = '#splice-api-token-allocation-v1:Splice.Api.Token.AllocationV1:Allocation';
      return cantonService.exerciseChoice({
        token: adminToken,
        actAsParty: actAsParties,
        templateId: ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_ExecuteTransfer',
        choiceArgument: {
          extraArgs: {
            context: context.choiceContextData || { values: {} },
            meta: { values: {} },
          },
        },
        readAs: readAsParties,
        synchronizerId,
        disclosedContracts: (context.disclosedContracts || []).map(dc => ({
          templateId: dc.templateId,
          contractId: dc.contractId,
          createdEventBlob: dc.createdEventBlob,
          synchronizerId: dc.synchronizerId || synchronizerId,
        })),
      });
    };

    // Try NARROW -> broad submitter sets. Executor-only is the FASTEST path because:
    //   1. The operator's key IS hosted on the participant (can submit non-interactively)
    //   2. The Allocation_ExecuteTransfer choice is controlled by the executor
    //   3. Including external parties in actAs causes NO_SYNCHRONIZER errors
    // Only if executor-only fails with authorization errors do we try broader sets.
    const executorOnly = executorPartyId ? [executorPartyId] : [];
    const allParties = [...new Set([executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean))];
    const ownerAndReceiver = [...new Set([ownerPartyId, receiverPartyId].filter(Boolean))];
    const ownerOnly = ownerPartyId ? [ownerPartyId] : [];
    const strategies = [executorOnly, allParties, ownerAndReceiver, ownerOnly]
      .filter((arr) => arr.length > 0)
      .filter((arr, idx, self) => self.findIndex(x => x.join('|') === arr.join('|')) === idx);

    const isDeadlineError = (err) => {
      const msg = String(err?.message || err || '');
      return msg.includes('deadline-exceeded') || msg.includes('settleBefore');
    };
    const isLockMissingError = (err) => {
      const msg = String(err?.message || err || '');
      return msg.includes('LockedAmulet') && msg.includes('not found');
    };
    const isContractNotFoundError = (err) => {
      const msg = String(err?.message || err || '');
      return msg.includes('CONTRACT_NOT_FOUND') ||
             msg.includes('could not be found with id') ||
             msg.includes('Contract could not be found');
    };

    let lastError = null;
    let sawDeadlineError = false;
    let sawLockMissingError = false;
    for (const actAsParties of strategies) {
      try {
        const result = await tryExecuteForActAs(actAsParties);
        console.log(`[CantonSDK]    ✅ Splice allocation executed — updateId: ${result?.transaction?.updateId || 'N/A'}`);
        return result;
      } catch (err) {
        lastError = err;
        console.warn(`[CantonSDK]    Attempt failed for actAs ${summarize(actAsParties)}: ${err.message?.substring(0, 150)}`);

        if (isDeadlineError(err)) sawDeadlineError = true;
        if (isLockMissingError(err)) sawLockMissingError = true;

        // CONTRACT_NOT_FOUND: The allocation contract is already archived on the ledger.
        // There's NO point trying other actAs strategies — the contract doesn't exist.
        // Break immediately to avoid spamming the participant with futile commands.
        if (isContractNotFoundError(err)) {
          console.warn(`[CantonSDK]    ⚠️ Allocation contract is archived — stopping retries immediately`);
          throw new Error(`STALE_ALLOCATION_LOCK_MISSING: CONTRACT_NOT_FOUND: ${err.message?.substring(0, 200)}`);
        }

        // Authorization / synchronizer issues can be dependent on the chosen actAs set.
        // Continue through fallback strategies before failing.
        continue;
      }
    }

    console.error(`[CantonSDK]    ❌ Splice allocation execution failed — all methods exhausted`);
    const anyMsg = String(lastError?.message || lastError || '');
    if (sawLockMissingError) {
      throw new Error(`STALE_ALLOCATION_LOCK_MISSING: ${anyMsg}`);
    }
    if (sawDeadlineError) {
      throw new Error(`STALE_ALLOCATION_EXPIRED: ${anyMsg}`);
    }
    if (isNoSynchronizerError(lastError)) {
      throw new Error(`TRANSIENT_SYNCHRONIZER_ERROR: ${anyMsg}`);
    }
    if (lastError) {
      throw lastError;
    }
    return null;
  }

  /**
   * Try executing Allocation_ExecuteTransfer with ONLY the executor in actAs,
   * using NON-INTERACTIVE submit-and-wait-for-transaction.
   *
   * Per Canton team: if all other parties in the transfer leg have already
   * signed the allocation, the executor alone can execute the transfer.
   * The executor's key is hosted on the participant → Canton auto-signs.
   *
   * This is the FASTEST settlement path — no external signing round-trip.
   */
  async _tryNonInteractiveExecutorOnly(allocationContractId, executorPartyId, adminToken, readAsParties, synchronizerId) {
    console.log(`[CantonSDK]    🚀 Non-interactive executor-only for Allocation_ExecuteTransfer`);

    // Try SDK first (generates disclosed contracts automatically)
    if (this.isReady() && this.sdk.tokenStandard && typeof this.sdk.tokenStandard.exerciseAllocationChoice === 'function') {
      try {
        const sdkResult = await this._withPartyContext(executorPartyId, async () => {
          return await this.sdk.tokenStandard.exerciseAllocationChoice(
            allocationContractId,
            'ExecuteTransfer',
            { actAs: [executorPartyId], readAs: readAsParties }
          );
        });
        if (sdkResult) {
          const updateId = sdkResult?.transaction?.updateId
            || sdkResult?.updateId
            || (Array.isArray(sdkResult) ? sdkResult[0]?.transaction?.updateId : null)
            || null;
          console.log(`[CantonSDK]    ✅ SDK executor-only succeeded (updateId: ${updateId || 'N/A'})`);
          if (updateId && !sdkResult.updateId) {
            if (typeof sdkResult === 'object' && !Array.isArray(sdkResult)) sdkResult.updateId = updateId;
          }
          return sdkResult;
        }
      } catch (sdkErr) {
        const msg = String(sdkErr?.message || sdkErr || '');
        if (msg.includes('LockedAmulet') || msg.includes('not found') ||
            msg.includes('deadline-exceeded') || msg.includes('settleBefore')) {
          throw new Error(`STALE_ALLOCATION_LOCK_MISSING: ${msg}`);
        }
        console.warn(`[CantonSDK]    SDK executor-only failed: ${msg.substring(0, 120)} — trying registry API`);
      }
    }

    // Fallback: build command from registry API (returns updateId via submitAndWaitForTransaction)
    const registryUrl = CANTON_SDK_CONFIG.REGISTRY_API_URL;
    const encodedCid = encodeURIComponent(allocationContractId);
    const executeContextUrl = `${registryUrl}/registry/allocations/v1/${encodedCid}/choice-contexts/execute-transfer`;

    const { data: context } = await getRegistryApi().post(executeContextUrl, { excludeDebugFields: true }, {
      headers: { Authorization: `Bearer ${adminToken}`, Accept: 'application/json' },
    });
    const ALLOCATION_INTERFACE = '#splice-api-token-allocation-v1:Splice.Api.Token.AllocationV1:Allocation';

    const registryResult = await cantonService.exerciseChoice({
      token: adminToken,
      actAsParty: [executorPartyId],
      templateId: ALLOCATION_INTERFACE,
      contractId: allocationContractId,
      choice: 'Allocation_ExecuteTransfer',
      choiceArgument: {
        extraArgs: {
          context: context.choiceContextData || { values: {} },
          meta: { values: {} },
        },
      },
      readAs: readAsParties,
      synchronizerId,
      disclosedContracts: (context.disclosedContracts || []).map(dc => ({
        templateId: dc.templateId,
        contractId: dc.contractId,
        createdEventBlob: dc.createdEventBlob,
        synchronizerId: dc.synchronizerId || synchronizerId,
      })),
    });

    const updateId = registryResult?.transaction?.updateId || null;
    console.log(`[CantonSDK]    Registry API execute succeeded (updateId: ${updateId || 'N/A'})`);
    if (updateId && registryResult && !registryResult.updateId) registryResult.updateId = updateId;
    return registryResult;
  }

  /**
   * Execute Allocation via INTERACTIVE SUBMISSION for external parties.
   *
   * Canton external parties (ext-*) cannot be included in non-interactive
   * submit-and-wait because the participant doesn't hold their keys.
   * Interactive submission flow:
   *   1. Prepare: POST /v2/interactive-submission/prepare (actAs includes ext party)
   *   2. Sign: Backend signs preparedTransactionHash with stored Ed25519 key
   *   3. Execute: POST /v2/interactive-submission/execute (with partySignatures)
   *
   * @param {string} allocationContractId - The Allocation contract ID
   * @param {string} executorPartyId - The exchange operator party
   * @param {string} symbol - Token symbol for routing
   * @param {string} adminToken - Service bearer token
   * @param {string} ownerPartyId - External party that owns the allocation
   * @param {string} receiverPartyId - Counterparty receiving tokens
   * @param {string} synchronizerId - Resolved synchronizer ID
   * @returns {Object} Execute result with transaction/updateId
   */
  async executeAllocationInteractive(allocationContractId, executorPartyId, symbol, adminToken, ownerPartyId, receiverPartyId, synchronizerId) {
    console.log(`[CantonSDK] 🔐 Interactive settlement for external party allocation`);
    console.log(`[CantonSDK]    Allocation: ${allocationContractId.substring(0, 30)}...`);
    console.log(`[CantonSDK]    Owner (ext): ${ownerPartyId?.substring(0, 30)}...`);
    console.log(`[CantonSDK]    Receiver: ${receiverPartyId?.substring(0, 30)}...`);

    // ── 1. Determine which external parties need signing ──────────────
    const isExt = (pid) => typeof pid === 'string' && pid.startsWith('ext-');
    const externalParties = [ownerPartyId, receiverPartyId].filter(p => p && isExt(p));
    
    if (externalParties.length === 0) {
      throw new Error('executeAllocationInteractive called but no external parties found');
    }

    // Verify we have signing keys for ALL external parties involved
    const keyResults = await Promise.all(externalParties.map(async p => ({ p, has: await userRegistry.hasSigningKey(p) })));
    const missingKeys = keyResults.filter(r => !r.has).map(r => r.p);
    if (missingKeys.length > 0) {
      const missing = missingKeys.map(p => p.substring(0, 25) + '...').join(', ');
      throw new Error(`SIGNING_KEY_MISSING: No signing key stored for external parties: ${missing}. Users must re-onboard or re-login to store their signing key.`);
    }

    // Interactive submission requires explicit external signatures for ALL actAs
    // parties. The DvpLegAllocation contract requires BOTH operator + ext-party
    // as authorizers, so we MUST include both in actAs. We try TWO strategies:
    //   A) All parties in actAs (required for DvpLegAllocation — operator + ext-party)
    //   B) External parties only (fallback if operator approval is embedded in allocation)
    const allActAs = [...new Set([executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean))];
    const extOnlyActAs = [...new Set(externalParties)];
    const actAsStrategies = [allActAs, extOnlyActAs]
      .filter(arr => arr.length > 0)
      .filter((arr, i, self) => self.findIndex(x => x.join('|') === arr.join('|')) === i);
    
    // Start with the first strategy (external-only) and fall back
    const actAsParties = actAsStrategies[0];
    const readAsParties = [...new Set([executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean))];

    // ── 2. Build the ExerciseCommand for Allocation_ExecuteTransfer ───
    let commands = [];
    let disclosedContracts = [];

    // Try SDK first to build the command (with disclosed contracts)
    if (this.isReady() && this.sdk.tokenStandard && typeof this.sdk.tokenStandard.exerciseAllocationChoice === 'function') {
      try {
        const commandBuilderPartyId = ownerPartyId || executorPartyId;
        console.log(`[CantonSDK]    Building interactive command in context: ${commandBuilderPartyId.substring(0, 30)}...`);
        
        const result = await this._withPartyContext(commandBuilderPartyId, async () => {
          return await this.sdk.tokenStandard.exerciseAllocationChoice(
            allocationContractId,
            'ExecuteTransfer'
          );
        });
        
        const [executeCmd, disclosed] = result;
        const rawCommands = Array.isArray(executeCmd) ? executeCmd : [executeCmd];
        commands = rawCommands.map(rawCmd => {
          const cmd = rawCmd.ExerciseCommand || rawCmd;
          return {
            ExerciseCommand: {
              templateId: cmd.templateId,
              contractId: cmd.contractId,
              choice: cmd.choice,
              choiceArgument: cmd.choiceArgument,
            }
          };
        });
        disclosedContracts = (disclosed || []).map(dc => ({
          templateId: dc.templateId,
          contractId: dc.contractId,
          createdEventBlob: dc.createdEventBlob,
          synchronizerId: dc.synchronizerId || synchronizerId,
        }));
        console.log(`[CantonSDK]    SDK built ${commands.length} command(s), ${disclosedContracts.length} disclosed contracts`);
      } catch (sdkErr) {
        const sdkMsg = String(sdkErr?.message || sdkErr || '');
        if (sdkMsg.includes('LockedAmulet') || sdkMsg.includes('not found') ||
            sdkMsg.includes('deadline-exceeded') || sdkMsg.includes('settleBefore')) {
          throw new Error(`STALE_ALLOCATION_LOCK_MISSING: ${sdkMsg}`);
        }
        console.warn(`[CantonSDK]    SDK command build failed for interactive: ${sdkMsg} — trying registry API`);
      }
    }

    // Fallback: build command from registry API
    if (commands.length === 0) {
      const registryUrl = CANTON_SDK_CONFIG.REGISTRY_API_URL;
      const encodedCid = encodeURIComponent(allocationContractId);
      const executeContextUrl = `${registryUrl}/registry/allocations/v1/${encodedCid}/choice-contexts/execute-transfer`;
      
      console.log(`[CantonSDK]    Fetching choice-context from registry: ${executeContextUrl}`);
      const { data: context } = await getRegistryApi().post(executeContextUrl, { excludeDebugFields: true });
      const ALLOCATION_INTERFACE = '#splice-api-token-allocation-v1:Splice.Api.Token.AllocationV1:Allocation';
      
      commands = [{
        ExerciseCommand: {
          templateId: ALLOCATION_INTERFACE,
          contractId: allocationContractId,
          choice: 'Allocation_ExecuteTransfer',
          choiceArgument: {
            extraArgs: {
              context: context.choiceContextData || { values: {} },
              meta: { values: {} },
            },
          },
        }
      }];
      disclosedContracts = (context.disclosedContracts || []).map(dc => ({
        templateId: dc.templateId,
        contractId: dc.contractId,
        createdEventBlob: dc.createdEventBlob,
        synchronizerId: dc.synchronizerId || synchronizerId,
      }));
      console.log(`[CantonSDK]    Registry built ${commands.length} command(s), ${disclosedContracts.length} disclosed contracts`);
    }

    // ── 3. Try each actAs strategy: ext-only first, then all parties ──
    let lastInteractiveError = null;
    for (const currentActAs of actAsStrategies) {
      try {
        console.log(`[CantonSDK]    📤 Preparing interactive submission with actAs: [${currentActAs.map(p => p.substring(0, 20) + '...').join(', ')}]`);
    
    const prepareResult = await cantonService.prepareInteractiveSubmission({
      token: adminToken,
          actAsParty: currentActAs,
      commands,
      readAs: readAsParties,
      synchronizerId,
      disclosedContracts: disclosedContracts.length > 0 ? disclosedContracts : null,
      verboseHashing: false,
    });

    if (!prepareResult.preparedTransaction || !prepareResult.preparedTransactionHash) {
      throw new Error('Interactive prepare returned incomplete result: missing preparedTransaction or preparedTransactionHash');
    }

    console.log(`[CantonSDK]    ✅ Prepared — hash: ${prepareResult.preparedTransactionHash.substring(0, 40)}...`);

        // ── 4. SIGN hash with ALL actAs parties that have signing keys ──
        // Canton interactive mode requires explicit external signatures for ALL
        // actAs parties — including the operator if its key is managed externally.
    const partySignatureEntries = [];
        const unsignedParties = [];
        for (const partyId of currentActAs) {
      const hasKey = await userRegistry.hasSigningKey(partyId);
      if (hasKey) {
        const keyInfo = await userRegistry.getSigningKey(partyId);
        if (!keyInfo) continue;
        console.log(`[CantonSDK]    🔑 Signing hash for ${partyId.substring(0, 30)}... (fingerprint: ${keyInfo.fingerprint?.substring(0, 20)}...)`);
      const signatureBase64 = await signHashWithKey(keyInfo.keyBase64, prepareResult.preparedTransactionHash);
      partySignatureEntries.push({
          party: partyId,
        signatures: [{
          format: 'SIGNATURE_FORMAT_RAW',
          signature: signatureBase64,
          signedBy: keyInfo.fingerprint,
          signingAlgorithmSpec: 'SIGNING_ALGORITHM_SPEC_ED25519',
        }],
      });
      } else {
        unsignedParties.push(partyId);
      }
    }

        if (partySignatureEntries.length === 0) {
          throw new Error('SIGNING_KEY_MISSING: No signing keys available for any actAs party');
        }

        if (unsignedParties.length > 0) {
          console.warn(`[CantonSDK]    ⚠️ Missing signing keys for: ${unsignedParties.map(p => p.substring(0, 30) + '...').join(', ')}`);
          console.warn(`[CantonSDK]    💡 To fix: store the operator's signing key via POST /api/onboarding/store-signing-key`);
    }

    const partySignatures = { signatures: partySignatureEntries };
        console.log(`[CantonSDK]    Collected ${partySignatureEntries.length} signature(s) for ${currentActAs.length} actAs parties (${unsignedParties.length} unsigned)`);

        // ── 5. EXECUTE interactive submission ────────────────────────
    const executeResult = await cantonService.executeInteractiveSubmission({
      preparedTransaction: prepareResult.preparedTransaction,
      partySignatures,
      hashingSchemeVersion: prepareResult.hashingSchemeVersion,
    }, adminToken);

    console.log(`[CantonSDK]    ✅ Interactive settlement succeeded — updateId: ${executeResult?.transaction?.updateId || 'N/A'}`);
    return executeResult;

      } catch (strategyErr) {
        lastInteractiveError = strategyErr;
        const msg = String(strategyErr?.message || strategyErr || '');
        console.warn(`[CantonSDK]    Interactive attempt failed for actAs [${currentActAs.map(p => p.substring(0, 15) + '...').join(', ')}]: ${msg.substring(0, 150)}`);

        // Fatal errors — no point trying another actAs strategy
        if (msg.includes('STALE_') || msg.includes('SIGNING_KEY_MISSING') ||
            msg.includes('LockedAmulet') || msg.includes('CONTRACT_NOT_FOUND') ||
            msg.includes('could not be found') || msg.includes('deadline-exceeded') ||
            msg.includes('settleBefore')) {
          throw strategyErr;
        }
        // For FAILED_TO_EXECUTE_TRANSACTION / auth errors, try next actAs strategy
        continue;
      }
    }

    // All interactive strategies exhausted
    if (lastInteractiveError) throw lastInteractiveError;
    throw new Error('Interactive settlement: no actAs strategies available');
  }

  /**
   * Execute Utilities allocation via backend API.
   * 
   * Per Canton team (Huz): Allocation_ExecuteTransfer can be run with JUST
   * the executor if other parties already signed the allocation.
   * 
   * Strategy:
   *   1. Executor-only non-interactive (fastest — operator key is hosted)
   *   2. Interactive with external party signatures (fallback)
   *   3. All-parties non-interactive (last resort)
   * 
   * CRITICAL: Must include synchronizerId in command body.
   */
  async _executeUtilitiesAllocation(allocationContractId, executorPartyId, symbol, adminToken, ownerPartyId = null, receiverPartyId = null) {
    const adminParty = getInstrumentAdmin(symbol);
    const tokenStandardUrl = UTILITIES_CONFIG.TOKEN_STANDARD_URL;
    const encodedCid = encodeURIComponent(allocationContractId);
    const executeContextUrl = `${tokenStandardUrl}/v0/registrars/${encodeURIComponent(adminParty)}/registry/allocations/v1/${encodedCid}/choice-contexts/execute-transfer`;

    const readAsParties = [...new Set([executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean))];
    const isExtParty = (pid) => typeof pid === 'string' && pid.startsWith('ext-');

    try {
      const configModule = require('../config');
      const synchronizerId = await cantonService.resolveSubmissionSynchronizerId(
        adminToken,
        configModule.canton.synchronizerId
      );
      console.log(`[CantonSDK]    Using submission synchronizer: ${synchronizerId || 'none'}`);

      console.log(`[CantonSDK]    📤 Calling Utilities execute-transfer API: ${executeContextUrl}`);
      let context;
      try {
        const { data } = await getRegistryApi().post(executeContextUrl, { meta: {}, excludeDebugFields: true });
        context = data;
      } catch (apiErr) {
        const errText = typeof apiErr.response?.data === 'string'
          ? apiErr.response.data
          : JSON.stringify(apiErr.response?.data ?? apiErr.message);
        console.warn(`[CantonSDK]    ⚠️ Utilities execute-transfer API returned ${apiErr.response?.status}: ${errText.substring(0, 200)}`);
        console.error(`[CantonSDK]    ❌ Utilities allocation execution failed — API error`);
        return null;
      }
      const disclosedContracts = (context.disclosedContracts || []).map(dc => ({
        templateId: dc.templateId,
        contractId: dc.contractId,
        createdEventBlob: dc.createdEventBlob,
        synchronizerId: dc.synchronizerId || synchronizerId,
      }));

      const choiceArgument = {
        extraArgs: {
          context: context.choiceContextData || { values: {} },
          meta: { values: {} },
        },
      };

      // ── Strategy 1: Executor-only non-interactive ─────────────────
      // Per Huz: executor alone can run Allocation_ExecuteTransfer if
      // other parties already signed the allocation. Operator's key is
      // hosted on participant → Canton auto-signs.
      if (executorPartyId && !isExtParty(executorPartyId)) {
        console.log(`[CantonSDK]    ⚡ Utilities: trying executor-only non-interactive (${executorPartyId.substring(0, 20)}...)`);
        try {
      const result = await cantonService.exerciseChoice({
        token: adminToken,
            actAsParty: [executorPartyId],
        templateId: UTILITIES_CONFIG.ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_ExecuteTransfer',
            choiceArgument,
        readAs: readAsParties,
        synchronizerId,
            disclosedContracts,
          });
          console.log(`[CantonSDK]    ✅ Utilities allocation executed (executor-only) — updateId: ${result?.transaction?.updateId || 'N/A'}`);
          return result;
        } catch (execOnlyErr) {
          const msg = String(execOnlyErr?.message || execOnlyErr || '');
          // Fatal errors — stop immediately
          if (msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found') ||
              msg.includes('deadline-exceeded') || msg.includes('settleBefore') ||
              msg.includes('LockedAmulet')) {
            throw execOnlyErr;
          }
          console.warn(`[CantonSDK]    Executor-only failed: ${msg.substring(0, 120)} — trying interactive...`);
        }
      }

      // ── Strategy 2: Interactive with external party signatures ─────
      // Utilities DvpLegAllocation requires BOTH authorizers (operator + owner).
      // In interactive mode, we include BOTH in actAs. We sign for external
      // parties; the participant should auto-sign for hosted keys (operator).
      // Try two actAs strategies:
      //   A) All parties (both operator + external) — most likely to satisfy DAML auth
      //   B) External-only (if operator approval is embedded in allocation)
      const externalParties = [ownerPartyId, receiverPartyId].filter(p => p && isExtParty(p));
      if (externalParties.length > 0) {
        const utilKeyChecks = await Promise.all(externalParties.map(p => userRegistry.hasSigningKey(p)));
        const allHaveKeys = utilKeyChecks.every(Boolean);
        if (allHaveKeys) {
          const allPartiesActAs = [...new Set([executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean))];
          const extOnlyActAs = [...new Set(externalParties)];
          const interactiveStrategies = [allPartiesActAs, extOnlyActAs]
            .filter(arr => arr.length > 0)
            .filter((arr, i, self) => self.findIndex(x => x.join('|') === arr.join('|')) === i);

          for (const strategyActAs of interactiveStrategies) {
            const strategyLabel = strategyActAs.length === allPartiesActAs.length ? 'all-parties' : 'ext-only';
            console.log(`[CantonSDK]    🔐 Utilities: trying interactive settlement (${strategyLabel}) for ${externalParties.length} external party(ies)`);
            try {
              const prepareResult = await cantonService.prepareInteractiveSubmission({
                token: adminToken,
                actAsParty: strategyActAs,
                commands: [{
                  ExerciseCommand: {
                    templateId: UTILITIES_CONFIG.ALLOCATION_INTERFACE,
                    contractId: allocationContractId,
                    choice: 'Allocation_ExecuteTransfer',
                    choiceArgument,
                  }
                }],
                readAs: readAsParties,
                synchronizerId,
                disclosedContracts: disclosedContracts.length > 0 ? disclosedContracts : null,
              });

              if (prepareResult.preparedTransaction && prepareResult.preparedTransactionHash) {
                // Sign for ALL actAs parties that have signing keys in the database.
                // Canton interactive mode requires explicit external signatures for ALL
                // actAs parties — including the operator if its key is managed externally
                // (e.g. Keycloak KMS). The participant does NOT auto-sign in interactive mode.
                const partySignatureEntries = [];
                const unsignedParties = [];
                for (const partyId of strategyActAs) {
                  const hasKey = await userRegistry.hasSigningKey(partyId);
                  if (hasKey) {
                    const keyInfo = await userRegistry.getSigningKey(partyId);
                    if (!keyInfo) continue;
                    console.log(`[CantonSDK]    🔑 Signing hash for ${partyId.substring(0, 30)}... (fingerprint: ${keyInfo.fingerprint?.substring(0, 20)}...)`);
                    const signatureBase64 = await signHashWithKey(keyInfo.keyBase64, prepareResult.preparedTransactionHash);
                    partySignatureEntries.push({
                      party: partyId,
                      signatures: [{
                        format: 'SIGNATURE_FORMAT_RAW',
                        signature: signatureBase64,
                        signedBy: keyInfo.fingerprint,
                        signingAlgorithmSpec: 'SIGNING_ALGORITHM_SPEC_ED25519',
                      }],
                    });
                  } else {
                    unsignedParties.push(partyId);
                  }
                }

                if (partySignatureEntries.length === 0) {
                  throw new Error('SIGNING_KEY_MISSING: No signing keys available for any actAs party');
                }

                if (unsignedParties.length > 0) {
                  console.warn(`[CantonSDK]    ⚠️ Missing signing keys for: ${unsignedParties.map(p => p.substring(0, 30) + '...').join(', ')}`);
                  console.warn(`[CantonSDK]    ⚠️ Participant must auto-sign for these parties, or execute will fail`);
                  console.warn(`[CantonSDK]    💡 To fix: store the operator's signing key via POST /api/onboarding/store-signing-key`);
                }

                console.log(`[CantonSDK]    Collected ${partySignatureEntries.length} signature(s) for ${strategyActAs.length} actAs parties (${unsignedParties.length} unsigned)`);

                const executeResult = await cantonService.executeInteractiveSubmission({
                  preparedTransaction: prepareResult.preparedTransaction,
                  partySignatures: { signatures: partySignatureEntries },
                  hashingSchemeVersion: prepareResult.hashingSchemeVersion,
                }, adminToken);

                console.log(`[CantonSDK]    ✅ Utilities allocation executed (interactive ${strategyLabel}) — updateId: ${executeResult?.transaction?.updateId || 'N/A'}`);
                return executeResult;
              }
            } catch (interactiveErr) {
              const msg = String(interactiveErr?.message || interactiveErr || '');
              if (msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found') ||
                  msg.includes('deadline-exceeded') || msg.includes('settleBefore') ||
                  msg.includes('LockedAmulet') || msg.includes('SIGNING_KEY_MISSING')) {
                throw interactiveErr;
              }
              console.warn(`[CantonSDK]    Interactive (${strategyLabel}) failed: ${msg.substring(0, 120)} — trying next strategy...`);
              continue;
            }
          }
          console.warn(`[CantonSDK]    All interactive strategies failed — trying all-parties non-interactive...`);
        }
      }

      // ── Strategy 3: DISABLED — all-parties non-interactive (last resort) ───
      // DISABLED: Including external parties in actAs for non-interactive submission
      // ALWAYS causes NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT.
      // Per Canton docs: "External parties, Single submitting party: Only transactions
      // requiring authorization from a single party are supported."
      // Sending this command just floods the participant with rejected submissions.
      //
      // The correct fix is to redesign settlement using the Propose-Accept pattern:
      // - Step 1: User authorizes allocation at order time (single-party tx)
      // - Step 2: Operator executes transfer alone (single-party tx)
      // See: https://docs.digitalasset.com/build/3.4/sdlc-howtos/smart-contracts/develop/patterns/propose-accept.html
      const allActAs = [...new Set([executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean))];
      const hasExtInActAs = allActAs.some(p => isExtParty(p));
      if (hasExtInActAs) {
        console.error(`[CantonSDK]    ❌ Cannot settle with external parties in actAs (Canton protocol limitation). Requires Propose-Accept redesign.`);
        throw new Error('CANTON_EXTERNAL_PARTY_LIMITATION: Cannot co-authorize with external party. Redesign needed.');
      }
      // Only attempt if no external parties in actAs (e.g. operator→operator transfers)
      console.log(`[CantonSDK]    Utilities: last resort all-parties actAs: [${allActAs.map(p => p.substring(0, 20) + '...').join(', ')}]`);
      const result = await cantonService.exerciseChoice({
        token: adminToken,
        actAsParty: allActAs,
        templateId: UTILITIES_CONFIG.ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_ExecuteTransfer',
        choiceArgument,
        readAs: readAsParties,
        synchronizerId,
        disclosedContracts,
      });

      console.log(`[CantonSDK]    ✅ Utilities allocation executed — updateId: ${result?.transaction?.updateId || 'N/A'}`);
      return result;
    } catch (err) {
      const msg = String(err.message || err);
      console.error(`[CantonSDK]    ❌ Utilities allocation execution failed: ${msg}`);
      if (msg.includes('deadline-exceeded') || msg.includes('settleBefore')) {
        throw new Error(`STALE_ALLOCATION_EXPIRED: ${msg}`);
      }
      if (msg.includes('LockedAmulet') && msg.includes('not found')) {
        throw new Error(`STALE_ALLOCATION_LOCK_MISSING: ${msg}`);
      }
      if (msg.includes('NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT') || msg.includes('Not connected to a synchronizer')) {
        throw new Error(`TRANSIENT_SYNCHRONIZER_ERROR: ${msg}`);
      }
      throw err;
    }
  }

  // NOTE: _executeDirectAllocation (AllocationRecord fallback) has been REMOVED.
  // Settlement uses Allocation_ExecuteTransfer (preferred) or Transfer API (fallback).

  /**
   * Cancel an Allocation — release locked funds back to sender.
   * 
   * Called when an order is cancelled. Requires sender + executor authorization.
   * The exchange (as executor) can cancel on behalf of the user.
   * 
   * @param {string} allocationContractId - The Allocation contract ID
   * @param {string} ownerPartyId - The allocation owner
   * @param {string} executorPartyId - The exchange party (operator)
   * @param {string} symbol - Token symbol for routing
   * @returns {Promise<{amuletCids: string[], result: Object}>} Unlocked holding CIDs + exercise result
   */
  async withdrawAllocation(allocationContractId, ownerPartyId, executorPartyId, symbol = null) {
    if (!allocationContractId) {
      console.log('[CantonSDK] No allocationContractId — skipping withdrawal');
      return null;
    }

    console.log(`[CantonSDK] Withdrawing Allocation: ${allocationContractId.substring(0, 30)}...`);

    const adminToken = await tokenProvider.getServiceToken();
    const configRef = require('../config');
    const operatorPartyId = configRef.canton.operatorPartyId;
    const synchronizerId = configRef.canton.synchronizerId;

    // Temple Pattern: The allocation OWNER (ext-* party) must be the acting party
    // for Allocation_Withdraw — the DAML contract requires the sender's authorization.
    // The admin token grants the participant the right to submit as any hosted ext-* party.
    // Using owner ONLY as actAs avoids NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT
    // errors that occur when combining operator + ext-* in the same submission.
    const actAsParties = [ownerPartyId];
    const readAsParties = [...new Set([ownerPartyId, executorPartyId, operatorPartyId].filter(Boolean))];

    console.log(`[CantonSDK]    actAs: [${ownerPartyId.substring(0, 30)}...] (owner/sender — required authorizer)`);
    console.log(`[CantonSDK]    readAs: [${readAsParties.map(p => p.substring(0, 20)).join(', ')}...]`);

    const ALLOCATION_INTERFACE = '#splice-api-token-allocation-v1:Splice.Api.Token.AllocationV1:Allocation';

    // Path 1: Registry API — gets proper choice context including expire-lock
    try {
      const registryUrl = CANTON_SDK_CONFIG.REGISTRY_API_URL;
      const encodedCid = encodeURIComponent(allocationContractId);
      const withdrawContextUrl = `${registryUrl}/registry/allocations/v1/${encodedCid}/choice-contexts/withdraw`;

      console.log(`[CantonSDK]    Trying registry withdraw API...`);
      const { data: context } = await getRegistryApi().post(withdrawContextUrl, { excludeDebugFields: true }, {
        headers: { Authorization: `Bearer ${adminToken}`, Accept: 'application/json' },
      });

      const contextData = context.choiceContextData || { values: {} };
      console.log(`[CantonSDK]    Registry returned context keys: ${Object.keys(contextData.values || contextData).join(', ') || '(none)'}`);

      const disclosed = (context.disclosedContracts || []).map(dc => ({
        templateId: dc.templateId,
        contractId: dc.contractId,
        createdEventBlob: dc.createdEventBlob,
        synchronizerId: dc.synchronizerId || synchronizerId,
      }));

      const result = await cantonService.exerciseChoice({
        token: adminToken,
        actAsParty: actAsParties,
        templateId: ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_Withdraw',
        choiceArgument: {
          extraArgs: {
            context: contextData,
            meta: { values: {} },
          },
        },
        readAs: readAsParties,
        synchronizerId,
        disclosedContracts: disclosed,
      });

      const updateId = result?.transaction?.updateId || null;
      console.log(`[CantonSDK]    ✅ Allocation withdrawn via registry API — tokens unlocked (updateId: ${updateId || 'N/A'})`);
      return { amuletCids: this._extractCreatedHoldingCids(result).map(h => h.contractId), result, updateId };
    } catch (registryErr) {
      console.warn(`[CantonSDK]    Registry withdraw failed: ${registryErr.message}`);
    }

    // Path 2: SDK path — owner context (ext-* party must be the actor)
    if (this.isReady() && this.sdk.tokenStandard && typeof this.sdk.tokenStandard.exerciseAllocationChoice === 'function') {
      try {
        const result = await this._withPartyContext(ownerPartyId, async () => {
          const [withdrawCmd, disclosed] = await this.sdk.tokenStandard.exerciseAllocationChoice(
            allocationContractId,
            'Withdraw'
          );
          const commands = Array.isArray(withdrawCmd) ? withdrawCmd : [withdrawCmd];
          let res = null;
          for (const rawCmd of commands) {
            const cmd = rawCmd.ExerciseCommand || rawCmd;
            res = await cantonService.exerciseChoice({
              token: adminToken,
              actAsParty: actAsParties,
              templateId: cmd.templateId,
              contractId: cmd.contractId,
              choice: cmd.choice,
              choiceArgument: cmd.choiceArgument,
              readAs: readAsParties,
              synchronizerId,
              disclosedContracts: (disclosed || []).map(dc => ({
                templateId: dc.templateId,
                contractId: dc.contractId,
                createdEventBlob: dc.createdEventBlob,
                synchronizerId: dc.synchronizerId || synchronizerId,
              })),
            });
          }
          const updateId = res?.transaction?.updateId || null;
          console.log(`[CantonSDK]    ✅ Allocation withdrawn via SDK — tokens unlocked (updateId: ${updateId || 'N/A'})`);
          return { amuletCids: this._extractCreatedHoldingCids(res).map(h => h.contractId), result: res, updateId };
        });
        if (result) return result;
      } catch (sdkErr) {
        console.warn(`[CantonSDK]    SDK withdrawAllocation failed: ${sdkErr.message}`);
      }
    }

    // Path 3: Direct exercise with synthetic expire-lock context
    try {
      const nowMs = Date.now();
      const expireLockIso = new Date(nowMs + 600_000).toISOString();

      console.log(`[CantonSDK]    Trying direct withdraw with expire-lock context (expiry: ${expireLockIso})...`);

      const result = await cantonService.exerciseChoice({
        token: adminToken,
        actAsParty: actAsParties,
        templateId: ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_Withdraw',
        choiceArgument: {
          extraArgs: {
            context: {
              values: {
                'expire-lock': { textValue: expireLockIso },
              },
            },
            meta: { values: {} },
          },
        },
        readAs: readAsParties,
        synchronizerId,
      });

      const updateId = result?.transaction?.updateId || null;
      console.log(`[CantonSDK]    ✅ Allocation withdrawn (direct) — tokens unlocked (updateId: ${updateId || 'N/A'})`);
      return { amuletCids: this._extractCreatedHoldingCids(result).map(h => h.contractId), result, updateId };
    } catch (directErr) {
      console.error(`[CantonSDK]    Direct allocation withdraw failed: ${directErr.message}`);
      throw directErr;
    }
  }

  /**
   * Cancel an Allocation, returning locked tokens to the sender.
   *
   * @param {string} allocationContractId - The Allocation contract ID
   * @param {string} senderPartyId - The order placer (funds returned to)
   * @param {string} executorPartyId - The exchange party
   * @param {string} symbol - Symbol for routing
   * @returns {Object} Exercise result
   */
  async cancelAllocation(allocationContractId, senderPartyId, executorPartyId, symbol = null) {
    if (!allocationContractId) {
      console.log('[CantonSDK] No allocationContractId — skipping cancellation');
      return null;
    }

    console.log(`[CantonSDK] 🔓 Cancelling Allocation: ${allocationContractId.substring(0, 30)}...`);

    const adminToken = await tokenProvider.getServiceToken();
    const configRef = require('../config');
    const operatorPartyId = configRef.canton.operatorPartyId;
    const synchronizerId = configRef.canton.synchronizerId;
    const tokenSystemType = symbol ? getTokenSystemType(symbol) : null;
    const isExternalParty = (partyId) => typeof partyId === 'string' && partyId.startsWith('ext-');
    if (isExternalParty(senderPartyId)) {
      // External parties require interactive submission/signature for actAs.
      // Non-interactive submit-and-wait with actAs ext-* is rejected by Canton with:
      // NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT (unknownSubmitter ext-...).
      // Skip this path and let interactive order-cancel flow proceed.
      console.log('[CantonSDK] ⏭️ Skipping non-interactive Allocation_Cancel for external party; interactive CancelOrder flow will proceed');
      return {
        cancelled: false,
        skipped: true,
        reason: 'EXTERNAL_PARTY_INTERACTIVE_REQUIRED',
      };
    }

    // ALL parties needed for cancel authorization
    const actAsParties = [...new Set([senderPartyId, executorPartyId, operatorPartyId].filter(Boolean))];
    const readAsParties = [...actAsParties];

    // ── Route Utilities tokens (CBTC) to Utilities cancel API ──────────
    if (tokenSystemType === 'utilities') {
      console.log(`[CantonSDK]    ${symbol} is a Utilities token — using Utilities cancel API`);
      return this._cancelUtilitiesAllocation(allocationContractId, senderPartyId, executorPartyId, symbol, adminToken);
    }

    // ── Splice tokens (CC/Amulet): Try SDK method first ──────────────
    if (this.isReady()) {
      try {
        if (this.sdk.tokenStandard && typeof this.sdk.tokenStandard.exerciseAllocationChoice === 'function') {
          return await this._withPartyContext(senderPartyId, async () => {
            const [cancelCmd, disclosed] = await this.sdk.tokenStandard.exerciseAllocationChoice(
              allocationContractId,
              'Cancel'
            );
            const commands = Array.isArray(cancelCmd) ? cancelCmd : [cancelCmd];
            let result = null;
            for (const rawCmd of commands) {
              const cmd = rawCmd.ExerciseCommand || rawCmd;
              result = await cantonService.exerciseChoice({
                token: adminToken,
                actAsParty: actAsParties,
                templateId: cmd.templateId,
                contractId: cmd.contractId,
                choice: cmd.choice,
                choiceArgument: cmd.choiceArgument,
                readAs: readAsParties,
                synchronizerId,
                disclosedContracts: (disclosed || []).map(dc => ({
                  templateId: dc.templateId,
                  contractId: dc.contractId,
                  createdEventBlob: dc.createdEventBlob,
                  synchronizerId: dc.synchronizerId || synchronizerId,
                })),
              });
            }
            console.log(`[CantonSDK]    ✅ Allocation cancelled via SDK — funds released`);
            return { cancelled: true, result };
          });
        }
      } catch (sdkErr) {
        console.warn(`[CantonSDK]    SDK cancelAllocation failed: ${sdkErr.message} — trying registry cancel API`);
      }
    } else {
      console.warn(`[CantonSDK]    SDK not ready — attempting registry cancel API for allocation`);
    }

    // ── Splice direct cancel via registry cancel-context endpoint ──────
    try {
      const registryUrl = CANTON_SDK_CONFIG.REGISTRY_API_URL;
      const encodedCid = encodeURIComponent(allocationContractId);
      const cancelContextUrl = `${registryUrl}/registry/allocations/v1/${encodedCid}/choice-contexts/cancel`;

      console.log(`[CantonSDK]    Trying Splice registry cancel API: ${cancelContextUrl}`);
      const { data: context } = await getRegistryApi().post(cancelContextUrl, { excludeDebugFields: true });
      const ALLOCATION_INTERFACE = '#splice-api-token-allocation-v1:Splice.Api.Token.AllocationV1:Allocation';
      const result = await cantonService.exerciseChoice({
        token: adminToken,
        actAsParty: actAsParties,
        templateId: ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_Cancel',
        choiceArgument: {
          extraArgs: {
            context: context.choiceContextData || { values: {} },
            meta: { values: {} },
          },
        },
        readAs: readAsParties,
        synchronizerId,
        disclosedContracts: (context.disclosedContracts || []).map(dc => ({
          templateId: dc.templateId,
          contractId: dc.contractId,
          createdEventBlob: dc.createdEventBlob,
          synchronizerId: dc.synchronizerId || synchronizerId,
        })),
      });

      console.log(`[CantonSDK]    ✅ Allocation cancelled via registry API — funds released`);
      return { cancelled: true, result };
    } catch (registryErr) {
      console.warn(`[CantonSDK]    ⚠️ Splice registry cancel failed: ${registryErr.message}`);
    }

    // Last resort: direct exercise with extraArgs (required by Splice API)
    try {
      const ALLOCATION_INTERFACE = '#splice-api-token-allocation-v1:Splice.Api.Token.AllocationV1:Allocation';
      const result = await cantonService.exerciseChoice({
        token: adminToken,
        actAsParty: actAsParties,
        templateId: ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_Cancel',
        choiceArgument: {
          extraArgs: {
            context: { values: {} },
            meta: { values: {} },
          },
        },
        readAs: readAsParties,
        synchronizerId,
      });

      console.log(`[CantonSDK]    ✅ Allocation cancelled (direct) — funds released`);
      return { cancelled: true, result };
    } catch (err) {
      console.warn(`[CantonSDK]    ⚠️ Direct allocation cancel failed: ${err.message}`);
    }

    console.warn(`[CantonSDK]    ⚠️ Could not cancel allocation ${allocationContractId.substring(0, 30)}... — may already be cancelled`);
    return {
      cancelled: false,
      skipped: false,
      reason: 'CANCEL_NOT_CONFIRMED',
    };
  }

  /**
   * Cancel a Utilities token allocation (CBTC) via Utilities registry cancel API.
   */
  async _cancelUtilitiesAllocation(allocationContractId, senderPartyId, executorPartyId, symbol, adminToken) {
    const adminParty = getInstrumentAdmin(symbol);
    const tokenStandardUrl = UTILITIES_CONFIG.TOKEN_STANDARD_URL;
    const encodedCid = encodeURIComponent(allocationContractId);
    const cancelContextUrl = `${tokenStandardUrl}/v0/registrars/${encodeURIComponent(adminParty)}/registry/allocations/v1/${encodedCid}/choice-contexts/cancel`;

    const configRef = require('../config');
    const operatorPartyId = configRef.canton.operatorPartyId;
    const synchronizerId = configRef.canton.synchronizerId;

    const actAsParties = [...new Set([senderPartyId, executorPartyId, operatorPartyId].filter(Boolean))];
    const readAsParties = [...actAsParties];

    try {
      console.log(`[CantonSDK]    📤 Calling Utilities cancel API: ${cancelContextUrl}`);
      const { data: context } = await getRegistryApi().post(cancelContextUrl, { meta: {}, excludeDebugFields: true });
      const result = await cantonService.exerciseChoice({
        token: adminToken,
        actAsParty: actAsParties,
        templateId: UTILITIES_CONFIG.ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_Cancel',
        choiceArgument: {
          extraArgs: {
            context: context.choiceContextData || { values: {} },
            meta: { values: {} },
          },
        },
        readAs: readAsParties,
        synchronizerId,
        disclosedContracts: (context.disclosedContracts || []).map(dc => ({
          templateId: dc.templateId,
          contractId: dc.contractId,
          createdEventBlob: dc.createdEventBlob,
          synchronizerId: dc.synchronizerId || synchronizerId,
        })),
      });

      console.log(`[CantonSDK]    ✅ Utilities allocation cancelled — funds released`);
      return result;
    } catch (err) {
      console.warn(`[CantonSDK]    ⚠️ Utilities cancel API failed: ${err.message}`);
    }

    // Direct exercise with extraArgs
    try {
      const result = await cantonService.exerciseChoice({
        token: adminToken,
        actAsParty: actAsParties,
        templateId: UTILITIES_CONFIG.ALLOCATION_INTERFACE,
        contractId: allocationContractId,
        choice: 'Allocation_Cancel',
        choiceArgument: {
          extraArgs: {
            context: { values: {} },
            meta: { values: {} },
          },
        },
        readAs: readAsParties,
        synchronizerId,
      });

      console.log(`[CantonSDK]    ✅ Utilities allocation cancelled (direct) — funds released`);
      return result;
    } catch (err) {
      console.warn(`[CantonSDK]    ⚠️ Direct Utilities cancel failed: ${err.message}`);
    }

    console.warn(`[CantonSDK]    ⚠️ Could not cancel Utilities allocation ${allocationContractId.substring(0, 30)}... — may already be cancelled`);
    return null;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // TOKEN STANDARD ALLOCATION — AllocationFactory_Allocate + ExecuteTransfer
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Try to build a REAL Token Standard allocation command for order placement.
   * 
   * TEMPLE PATTERN: Creates self-allocation (sender = receiver = user) for locking tokens.
   * Settlement will withdraw and create new multi-leg allocation.
   *
   * If the SDK is ready and the Allocation Factory API responds, this creates
   * a real AllocationFactory_Allocate ExerciseCommand that locks real Splice/
   * Utilities holdings on-chain. The user signs via interactive submission.
   *
   * If it fails (SDK not ready, no holdings, API unreachable), returns null
   * so the caller can reject the order.
   *
   * @param {string} senderPartyId - The order placer (will lock their holdings)
   * @param {string} executorPartyId - The exchange operator
   * @param {string} amount - Amount to lock
   * @param {string} symbol - Token symbol (CC, CBTC)
   * @param {string} orderId - Unique order ID (used as settlementRef)
   * @param {string|Array} overrideHoldingCids - Optional exact-amount holding CID(s) from self-transfer
   * @returns {Promise<{command, readAs, disclosedContracts, synchronizerId, allocationType}|null>}
   */
  async tryBuildRealAllocationCommand(senderPartyId, executorPartyId, amount, symbol, orderId, overrideHoldingCids = null) {
    try {
      if (!this.isReady()) {
        console.log(`[CantonSDK] SDK not ready — cannot build real allocation for ${symbol}`);
        return null;
      }

      const tokenSystemType = getTokenSystemType(symbol);
      if (tokenSystemType === 'unknown') {
        console.log(`[CantonSDK] Token ${symbol} has no on-chain system — skipping real allocation`);
        return null;
      }

      // Allocation: user → operator. The operator (executor) is also the receiver,
      // so it CAN execute Allocation_ExecuteTransfer at settlement time using only
      // its own key — no ext-* party authorization needed at settlement.
      // (Self-allocation would require Allocation_Withdraw at settlement,
      //  which needs ext-* party as actAs — not supported for non-interactive submissions.)
      console.log(`[CantonSDK] 🔄 Building allocation for ${amount} ${symbol} (${tokenSystemType})...`);
      console.log(`[CantonSDK]    sender=${senderPartyId.substring(0, 30)}..., receiver=OPERATOR(${executorPartyId.substring(0, 30)}...), executor=${executorPartyId.substring(0, 30)}...`);

      const holdingCidsArray = overrideHoldingCids
        ? (Array.isArray(overrideHoldingCids) ? overrideHoldingCids : [overrideHoldingCids])
        : null;

      const result = await this.buildAllocationInteractiveCommand(
        senderPartyId,
        executorPartyId,  // receiver = operator (executor can execute w/o ext-* party auth)
        amount,
        symbol,
        executorPartyId,
        orderId,
        holdingCidsArray
      );

      if (!result?.command) {
        console.warn(`[CantonSDK] buildAllocationInteractiveCommand returned no command`);
        return null;
      }

      const allocationType = tokenSystemType === 'utilities' ? 'UtilitiesAllocation' : 'SpliceAllocation';
      console.log(`[CantonSDK] ✅ Allocation (user→operator) built for ${orderId}`);

      return {
        ...result,
        allocationType,
      };
    } catch (err) {
      const msg = String(err?.message || err || '');
      console.warn(`[CantonSDK] Allocation build failed for ${symbol}: ${msg.substring(0, 200)}`);
      return null;
    }
  }

  /**
   * Try to execute a REAL Token Standard allocation at settlement time.
   *
   * Uses the operator-only non-interactive path (Huz confirmed: executor
   * alone can run Allocation_ExecuteTransfer if all other parties already
   * signed the allocation at creation time).
   *
   * If successful, real tokens move on-chain — visible in CC View explorer.
   * If it fails, returns null so the caller can handle the error.
   *
   * @param {string} allocationContractId - The DvpLegAllocation contract ID
   * @param {string} executorPartyId - The exchange operator
   * @param {string} symbol - Token symbol (CC, CBTC)
   * @param {string} ownerPartyId - The allocation owner (for readAs)
   * @param {string} receiverPartyId - The receiver (counterparty)
   * @returns {Promise<object|null>} Exercise result or null on failure
   */
  async tryRealAllocationExecution(allocationContractId, executorPartyId, symbol, ownerPartyId, receiverPartyId) {
    if (!allocationContractId) return null;

    try {
      const tokenSystemType = getTokenSystemType(symbol);
      if (tokenSystemType === 'unknown') return null;

      console.log(`[CantonSDK] 🔄 Trying REAL Token Standard allocation execution for ${symbol}...`);
      console.log(`[CantonSDK]    Allocation: ${allocationContractId.substring(0, 30)}...`);
      console.log(`[CantonSDK]    Executor: ${executorPartyId.substring(0, 30)}...`);

      const adminToken = await tokenProvider.getServiceToken();
      const configModule = require('../config');
      const synchronizerId = await cantonService.resolveSubmissionSynchronizerId(
        adminToken,
        configModule.canton.synchronizerId
      );

      const readAsParties = [...new Set([executorPartyId, ownerPartyId, receiverPartyId].filter(Boolean))];

      // Try non-interactive executor-only (fastest, operator auto-signs)
      const result = await this._tryNonInteractiveExecutorOnly(
        allocationContractId, executorPartyId, adminToken,
        readAsParties, synchronizerId
      );

      if (result) {
        const updateId = result?.transaction?.updateId
          || result?.updateId
          || result?.[0]?.transaction?.updateId
          || null;
        console.log(`[CantonSDK] ✅ REAL Token Standard allocation executed — tokens transferred on-chain!`);
        console.log(`[CantonSDK]    UpdateId: ${updateId || 'N/A'}`);
        if (!result.updateId && updateId) result.updateId = updateId;
        return result;
      }

      // Never silently return null — client requirement: allocations must be executed.
      // If we get here, executor-only path failed with no thrown error; treat as fatal.
      const fatalMsg = `Allocation_ExecuteTransfer returned null for ${allocationContractId?.substring(0, 24)}... — tokens NOT transferred`;
      console.error(`[CantonSDK] ❌ ${fatalMsg}`);
      throw new Error(fatalMsg);
    } catch (err) {
      const msg = String(err?.message || err || '');

      // Stale / expired → propagate so caller knows the allocation is dead
      if (msg.includes('STALE_') || msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found')) {
        console.error(`[CantonSDK] ❌ Real allocation is stale/archived: ${msg.substring(0, 150)}`);
        throw err;
      }

      // Always throw — never return null. Client requirement: allocations must execute.
      console.error(`[CantonSDK] ❌ Allocation_ExecuteTransfer failed: ${msg.substring(0, 300)}`);
      throw err;
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // HOLDING CONTRACT VERIFICATION — Source of Truth
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Verify holding state by querying ACTIVE Holding contracts via the
   * splice-api-token-holding-v1 interface (source of truth per client).
   * 
   * This filters ONLY active contracts (not archived) and shows execution results.
   * When Allocation_ExecuteTransfer executes, new Holding contracts are created
   * and should be visible here.
   *
   * @param {string} partyId - Party to verify
   * @param {string} [symbol] - Optional symbol filter
   * @returns {Promise<{holdings: Array, locked: Array, totalAvailable: string, totalLocked: string, executionVisible: boolean}>}
   */
  async verifyHoldingState(partyId, symbol = null) {
    const adminToken = await tokenProvider.getServiceToken();
    const HOLDING_INTERFACE = '#splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding';
    const instrumentId = symbol ? toCantonInstrument(symbol) : null;

    try {
      const activeContracts = await cantonService.queryActiveContracts({
        party: partyId,
        templateIds: [HOLDING_INTERFACE],
      }, adminToken);

      if (!activeContracts || activeContracts.length === 0) {
        console.log(`[CantonSDK] ⚠️ No active Holding contracts found for ${partyId.substring(0, 30)}...${symbol ? ` (${symbol})` : ''}`);
        return { holdings: [], locked: [], totalAvailable: '0', totalLocked: '0', executionVisible: false };
      }

      const holdings = [];
      const locked = [];

      for (const contract of activeContracts) {
        const payload = contract.createArgument || contract.payload || {};
        const tpl = typeof contract.templateId === 'string' ? contract.templateId : '';
        const ifaceView = contract.interfaceView || contract.interfaceViewValue || {};

        // Amount extraction: try interface view first, then template-specific fields
        const amt = ifaceView?.amount
          || payload?.amount?.initialAmount
          || payload?.amount
          || payload?.quantity
          || '0';

        // Instrument identification:
        // 1. Interface view's instrumentId (most reliable when available)
        // 2. Template-based inference (Amulet/LockedAmulet → "Amulet" = CC)
        // 3. Payload's instrumentId field (Utilities tokens)
        let instId = extractInstrumentId(
          ifaceView?.instrumentId
          || payload?.instrumentId
          || payload?.instrument?.id
          || payload?.instrument
        );

        if (!instId && (tpl.includes('Amulet') || tpl.includes('splice-amulet'))) {
          instId = 'Amulet';
        }

        const isLocked = tpl.includes('Locked') || !!payload?.lock || !!payload?.isLocked;

        if (instrumentId && instId !== instrumentId) continue;

        const entry = {
          contractId: contract.contractId,
          templateId: tpl,
          amount: amt,
          instrumentId: instId || 'UNKNOWN',
          exchangeSymbol: instId ? toExchangeSymbol(instId) : 'UNKNOWN',
          isLocked,
          createdAt: contract.createdAt || contract.createdEvent?.eventId,
        };

        if (isLocked) {
          locked.push(entry);
        } else {
          holdings.push(entry);
        }
      }

      const totalAvailable = holdings.reduce((sum, h) => sum.plus(h.amount || '0'), new Decimal(0)).toFixed();
      const totalLocked = locked.reduce((sum, h) => sum.plus(h.amount || '0'), new Decimal(0)).toFixed();
      const executionVisible = holdings.length > 0;

      console.log(`[CantonSDK] ✅ Holding state for ${partyId.substring(0, 30)}...${symbol ? ` (${symbol})` : ''}:`);
      console.log(`[CantonSDK]    Available: ${totalAvailable} (${holdings.length} UTXOs) [${holdings.map(h => `${h.exchangeSymbol}:${h.amount}`).join(', ')}]`);
      console.log(`[CantonSDK]    Locked:    ${totalLocked} (${locked.length} UTXOs) [${locked.map(h => `${h.exchangeSymbol}:${h.amount}`).join(', ')}]`);
      console.log(`[CantonSDK]    Execution visible: ${executionVisible ? '✅ YES' : '❌ NO'}`);

      return { holdings, locked, totalAvailable, totalLocked, executionVisible };
    } catch (err) {
      console.error(`[CantonSDK] ❌ verifyHoldingState failed: ${err.message}`);
      return { holdings: [], locked: [], totalAvailable: '0', totalLocked: '0', executionVisible: false };
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // HOLDING TRANSACTIONS — History
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Get holding transactions (token transfer history)
   */
  async getHoldingTransactions(partyId, startOffset = 0, limit = 100) {
    if (!this.isReady()) {
      return [];
    }

    return this._withPartyContext(partyId, async () => {
      try {
        const transactions = await this.sdk.tokenStandard?.listHoldingTransactions(startOffset, limit) || [];
        return transactions;
      } catch (error) {
        console.error(`[CantonSDK] Failed to get holding transactions:`, error.message);
        return [];
      }
    });
  }
}

// ─── Singleton ───────────────────────────────────────────────────────────────

let cantonSDKClient = null;

function getCantonSDKClient() {
  if (!cantonSDKClient) {
    cantonSDKClient = new CantonSDKClient();
  }
  return cantonSDKClient;
}

module.exports = {
  CantonSDKClient,
  getCantonSDKClient,
};
