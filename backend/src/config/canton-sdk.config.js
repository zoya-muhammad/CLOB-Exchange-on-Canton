/**
 * Canton Wallet SDK Configuration
 * 
 * Configures the SDK to connect to the WolfEdge DevNet Canton validator
 * and access the Splice Token Standard APIs.
 * 
 * Two token systems are supported:
 * - CC (Amulet): Splice Token Standard via Scan Proxy (SDK handles this)
 * - CBTC: Utilities Token (CIP-0056) via Utilities Backend API (direct HTTP)
 * 
 * Key endpoints:
 * - JSON Ledger API: For reading/writing contracts
 * - Scan Proxy: For CC Transfer Factory Registry (transfer instructions)
 * - Utilities Backend: For CBTC Transfer Factory Registry
 * - Keycloak: For JWT authentication
 * 
 * @see https://docs.digitalasset.com/utilities/devnet/how-tos/registry/transfer/transfer.html
 */

require('dotenv').config();

const SCAN_PROXY_BASE = process.env.SCAN_PROXY_BASE || 'https://wallet.validator.dev.canton.wolfedgelabs.com/api/validator/v0/scan-proxy';

const CANTON_SDK_CONFIG = {
  // JSON Ledger API — primary for all reads/writes
  LEDGER_API_URL: process.env.CANTON_JSON_LEDGER_API_BASE || 'http://65.108.40.104:31539',

  // Scan Proxy — serves the CC (Amulet) Transfer Factory Registry API
  // Route: /registry/transfer-instruction/v1/transfer-factory
  // Also exposes: /api/scan/v0/* for Amulet and other lookups
  SCAN_PROXY_URL: SCAN_PROXY_BASE,

  // Validator API URL — used by SDK's ValidatorController and TokenStandardController
  VALIDATOR_API_URL: process.env.VALIDATOR_API_URL || `${SCAN_PROXY_BASE}/api/validator`,

  // Registry API URL — used by SDK's setTransferFactoryRegistryUrl() for CC (Amulet)
  // openapi-fetch concatenates this with "/registry/transfer-instruction/v1/..."
  // so it must be the bare host — NOT the scan-proxy sub-path
  REGISTRY_API_URL: process.env.REGISTRY_API_URL || SCAN_PROXY_BASE,

  // Scan API URL — used by SDK's TokenStandardController for scan-based lookups
  SCAN_API_URL: process.env.SCAN_API_URL || `${SCAN_PROXY_BASE}/api/scan`,

  // Instrument admin party for CC (Amulet) — discovered at runtime via sdk.tokenStandard.getInstrumentAdmin()
  // This is the DSO party. Set this env var to skip discovery.
  INSTRUMENT_ADMIN_PARTY: process.env.INSTRUMENT_ADMIN_PARTY || null,

  // Operator party — the exchange service account
  OPERATOR_PARTY_ID: process.env.OPERATOR_PARTY_ID,

  // ─── Instrument mapping ────────────────────────────────────────────────────
  // Canton uses "Amulet" as the instrumentId for CC (Canton Coin).
  // CBTC is "CBTC" (Utilities Token).
  // This maps exchange symbols → Canton instrument IDs.
  INSTRUMENT_MAP: {
    'CC': 'Amulet',
    'CBTC': 'CBTC',
    'Amulet': 'Amulet',
    'BTC': 'BTC',
    'USDT': 'USDT',
    'ETH': 'ETH',
    'SOL': 'SOL',
  },

  // Reverse map: Canton instrument ID → exchange symbol
  REVERSE_INSTRUMENT_MAP: {
    'Amulet': 'CC',
    'CBTC': 'CBTC',
    'BTC': 'BTC',
    'USDT': 'USDT',
    'ETH': 'ETH',
    'SOL': 'SOL',
  },

  // Trading pairs
  TRADING_PAIRS: {
    'CC/CBTC': {
      base: 'CC',
      quote: 'CBTC',
      baseInstrument: 'Amulet',  // CC = Amulet in Canton
      quoteInstrument: 'CBTC',
    },
    'BTC/USDT': {
      base: 'BTC',
      quote: 'USDT',
      baseInstrument: 'BTC',
      quoteInstrument: 'USDT',
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════
// CBTC Utilities Token Configuration (CIP-0056)
// 
// CBTC uses a DIFFERENT Transfer Factory Registry than CC:
// - CC  → Splice SDK → Scan Proxy at http://65.108.40.104:8088
// - CBTC → Utilities Backend API → https://api.utilities.digitalasset-dev.com/api/utilities
//
// The Utilities Backend API has a different path structure:
//   ${BACKEND}/v0/registrars/${ADMIN_PARTY}/registry/transfer-instruction/v1/transfer-factory
//
// Reference: https://docs.digitalasset.com/utilities/devnet/how-tos/registry/transfer/transfer.html
// ═══════════════════════════════════════════════════════════════════════════
const UTILITIES_CONFIG = {
  // DevNet Utilities Backend API (for operator info, instrument configs, etc.)
  // Ref: https://docs.digitalasset.com/utilities/devnet/reference/operator-backend-api/index.html
  BACKEND_URL: process.env.UTILITIES_BACKEND_URL || 'https://api.utilities.digitalasset-dev.com/api/utilities',

  // Token Standard API — the CORRECT base for transfer-instruction choice context endpoints.
  // Ref: https://docs.digitalasset.com/utilities/devnet/overview/registry-user-guide/token-standard.html
  // Pattern: ${TOKEN_STANDARD_URL}/v0/registrars/${REGISTRAR}/registry/transfer-instruction/v1/${CID}/choice-contexts/accept
  TOKEN_STANDARD_URL: process.env.TOKEN_STANDARD_URL || 'https://api.utilities.digitalasset-dev.com/api/token-standard',

  // CBTC Admin Party (cBTC Network registrar)
  CBTC_ADMIN_PARTY: process.env.CBTC_ADMIN_PARTY || 'cbtc-network::12202a83c6f4082217c175e29bc53da5f2703ba2675778ab99217a5a881a949203ff',

  // ─── Token Standard Interface IDs ────────────────────────────────────────────
  // CRITICAL: Use #package-name prefix (hash-independent interface reference).
  // Canton resolves the interface by package name — NOT by a specific hash.
  // This is version-stable and won't break across Splice upgrades.
  // Ref: https://docs.digitalasset.com/build/3.4/reference/json-api/lf-value-specification.html
  TRANSFER_FACTORY_INTERFACE: '#splice-api-token-transfer-instruction-v1:Splice.Api.Token.TransferInstructionV1:TransferFactory',
  TRANSFER_INSTRUCTION_INTERFACE: '#splice-api-token-transfer-instruction-v1:Splice.Api.Token.TransferInstructionV1:TransferInstruction',
  HOLDING_INTERFACE: '#splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding',

  // ─── Allocation API Interface IDs ───────────────────────────────────────────
  // Allocation-based settlement: user authorizes exchange as executor at order time,
  // exchange settles with its own key at match time.
  // Ref: https://docs.sync.global/app_dev/api/splice-api-token-allocation-v1/
  ALLOCATION_FACTORY_INTERFACE: '#splice-api-token-allocation-v1:Splice.Api.Token.AllocationV1:AllocationFactory',
  ALLOCATION_INTERFACE: '#splice-api-token-allocation-v1:Splice.Api.Token.AllocationV1:Allocation',
  ALLOCATION_INSTRUCTION_FACTORY_INTERFACE: '#splice-api-token-allocation-instruction-v1:Splice.Api.Token.AllocationInstructionV1:AllocationFactory',
  ALLOCATION_INSTRUCTION_INTERFACE: '#splice-api-token-allocation-instruction-v1:Splice.Api.Token.AllocationInstructionV1:AllocationInstruction',
};

/**
 * Determine the token system type for a given exchange symbol.
 * - 'splice': CC (Amulet) — uses SDK + Scan Proxy
 * - 'utilities': CBTC — uses Utilities Backend API
 * - 'unknown': Other tokens (BTC, USDT, etc.) — no on-chain transfer configured
 * 
 * @param {string} symbol - Exchange symbol (e.g., 'CC', 'CBTC')
 * @returns {'splice'|'utilities'|'unknown'} Token system type
 */
function getTokenSystemType(symbol) {
  const instrumentId = CANTON_SDK_CONFIG.INSTRUMENT_MAP[symbol] || symbol;
  if (instrumentId === 'Amulet') return 'splice';
  if (instrumentId === 'CBTC') return 'utilities';
  return 'unknown';
}

/**
 * Get the instrument admin party for a given symbol.
 * - CC (Amulet): Returns the DSO party (discovered at runtime)
 * - CBTC: Returns the cbtc-network registrar party
 * 
 * @param {string} symbol - Exchange symbol
 * @param {string} [discoveredDsoParty] - DSO party discovered at runtime (for CC)
 * @returns {string|null} Admin party ID
 */
function getInstrumentAdmin(symbol, discoveredDsoParty = null) {
  const type = getTokenSystemType(symbol);
  if (type === 'splice') return discoveredDsoParty || CANTON_SDK_CONFIG.INSTRUMENT_ADMIN_PARTY;
  if (type === 'utilities') return UTILITIES_CONFIG.CBTC_ADMIN_PARTY;
  return null;
}

/**
 * Map exchange symbol to Canton instrument ID (always returns a string).
 * @param {string} symbol - Exchange symbol (e.g., 'CC', 'CBTC')
 * @returns {string} Canton instrument ID (e.g., 'Amulet', 'CBTC')
 */
function toCantonInstrument(symbol) {
  return CANTON_SDK_CONFIG.INSTRUMENT_MAP[symbol] || String(symbol);
}

/**
 * Extract the instrument-ID string from the SDK's instrumentId field,
 * which may be a plain string ("Amulet") or an object ({ admin, id }).
 * @param {string|Object} instrumentIdField - Raw instrumentId from UTXO
 * @returns {string} The plain instrument-ID string
 */
function extractInstrumentId(instrumentIdField) {
  if (!instrumentIdField) return '';
  if (typeof instrumentIdField === 'string') return instrumentIdField;
  if (typeof instrumentIdField === 'object') return instrumentIdField.id || '';
  return String(instrumentIdField);
}

/**
 * Map Canton instrument ID to exchange symbol.
 * Handles both string ("Amulet") and object ({ admin: "...", id: "Amulet" }) formats.
 * @param {string|Object} instrumentId - Canton instrument ID
 * @returns {string} Exchange symbol (e.g., 'CC')
 */
function toExchangeSymbol(instrumentId) {
  const id = (typeof instrumentId === 'object' && instrumentId !== null)
    ? (instrumentId.id || instrumentId)
    : instrumentId;
  return CANTON_SDK_CONFIG.REVERSE_INSTRUMENT_MAP[id] || String(id);
}

module.exports = {
  CANTON_SDK_CONFIG,
  UTILITIES_CONFIG,
  toCantonInstrument,
  toExchangeSymbol,
  extractInstrumentId,
  getTokenSystemType,
  getInstrumentAdmin,
};
