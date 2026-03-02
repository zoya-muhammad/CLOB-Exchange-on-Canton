/**
 * CENTRALIZED CONSTANTS - Single Source of Truth
 * 
 * ALL package IDs, template IDs, party IDs, and important constants are defined here.
 * DO NOT hardcode these values anywhere else in the codebase.
 * 
 * To update any value:
 * 1. Update the value here
 * 2. Restart the server
 * 3. All services will automatically use the new value
 */

// =============================================================================
// PARTY IDS - Operator and service accounts
// =============================================================================

/**
 * Operator Party ID - The service account that manages the exchange
 * This party has canActAs rights to manage orders, settlements, etc.
 */
const OPERATOR_PARTY_ID = process.env.OPERATOR_PARTY_ID || 
  '8100b2db-86cf-40a1-8351-55483c151cdc::122087fa379c37332a753379c58e18d397e39cb82c68c15e4af7134be46561974292';

// =============================================================================
// CANTON ENDPOINTS
// =============================================================================

const CANTON_JSON_API_BASE = process.env.CANTON_JSON_LEDGER_API_BASE || 'http://65.108.40.104:31539';
const CANTON_ADMIN_API = process.env.CANTON_ADMIN_API || 'http://65.108.40.104:30100';
const DEFAULT_SYNCHRONIZER_ID = process.env.DEFAULT_SYNCHRONIZER_ID || 
  'global-domain::1220be58c29e65de40bf273be1dc2b266d43a9a002ea5b18955aeef7aac881bb471a';

// Registry Backend API - Used for Splice Token Standard operations (transfer accept, etc.)
// Provides choice context and disclosed contracts needed for Splice transfers
// Client confirmed: BACKEND_API="https://api.utilities.digitalasset-dev.com/api/utilities" (for devnet)
const REGISTRY_BACKEND_API = process.env.REGISTRY_BACKEND_API || 
  'https://api.utilities.digitalasset-dev.com/api/utilities';

// Scan Proxy API - Used for Amulet/CC transfers (accept, send, reject)
// This is the client's Scan Proxy on WolfEdge infrastructure
// For Amulet transfers: POST /api/scan/v0/amulet-rules
// CONFIRMED WORKING: http://65.108.40.104:8088/api/scan/v0/amulet-rules
const SCAN_PROXY_API = process.env.SCAN_PROXY_API || 
  'http://65.108.40.104:8088';

// Legacy Validator Scan Proxy (alternative, requires auth)
const VALIDATOR_SCAN_PROXY_API = process.env.VALIDATOR_SCAN_PROXY_API || 
  'https://wallet.validator.dev.canton.wolfedgelabs.com/api/validator';

// DSO Party ID - Required for Amulet transfers
const DSO_PARTY_ID = process.env.DSO_PARTY_ID || 
  'DSO::1220be58c29e65de40bf273be1dc2b266d43a9a002ea5b18955aeef7aac881bb471a';

// Amulet (Canton Coin/CC) Package ID - For querying Amulet token holdings
// Template: Splice.Amulet:Amulet
// Discovered from user's active contracts on WolfEdge DevNet
const AMULET_PACKAGE_ID = process.env.AMULET_PACKAGE_ID || 
  '67fac2f853bce8dbf0b9817bb5ba7c59f10e8120b7c808696f7010e5f0c8a791';

// =============================================================================
// PACKAGE IDS - Update these when deploying new DARs
// =============================================================================

/**
 * Token Standard Package (clob-wolfedge-tokens v2.4.0)
 * Contains: Instrument, Holding, Settlement, Order, OrderV3
 * 
 * Key feature: Holding contract has custodian-only signatory
 * This allows operator to mint tokens for external parties.
 * 
 * THIS IS THE ONLY PACKAGE ID USED — all old packages are retired.
 */
const TOKEN_STANDARD_PACKAGE_ID = process.env.TOKEN_STANDARD_PACKAGE_ID || '0224efbf74e4ecb40083f7090a9f12145c607d76da220a91eedc21ca491d24fa';

/**
 * Splice Token Standard Package ID
 * Production-ready token standard used by Canton DevNet
 * 
 * Template: splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding
 * This is what CBTC and other production tokens use
 * 
 * NOTE: Package ID needs to be discovered from Canton or provided by client
 * For now, we'll use template name format and let services discover it
 */
const SPLICE_PACKAGE_NAME = 'splice-api-token-holding-v1';

// =============================================================================
// TEMPLATE IDS - Pre-built template ID strings for easy use
// =============================================================================

const TEMPLATE_IDS = {
  // Splice Token Standard Templates (PRODUCTION - Use for CBTC, etc.)
  // Format: #splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding
  // Using "#" prefix allows querying without package ID (Canton feature)
  spliceHolding: `#${SPLICE_PACKAGE_NAME}:Splice.Api.Token.HoldingV1:Holding`,
  spliceTransferOffer: `#${SPLICE_PACKAGE_NAME}:Splice.Api.Token.HoldingV1:TransferOffer`,
  
  // Amulet (Canton Coin / CC) Template - Direct token holdings on Canton Network
  // Symbol: CC (Canton Coin) - shown as "Amulet" in WolfEdge wallet
  // Amount is in payload.amount.initialAmount (not just payload.amount)
  amulet: `${AMULET_PACKAGE_ID}:Splice.Amulet:Amulet`,
  
  // Splice Transfer Instruction interface (for accepting transfer offers)
  // This is the INTERFACE ID (not template ID) - used to exercise TransferInstruction_Accept
  spliceTransferInstruction: `#splice-api-token-transfer-instruction-v1:Splice.Api.Token.TransferInstructionV1:TransferInstruction`,
  spliceTransferInstructionInterfaceId: '#splice-api-token-transfer-instruction-v1:Splice.Api.Token.TransferInstructionV1:TransferInstruction',
  
  // Custom Token Standard Templates (clob-wolfedge-tokens v2.3.0 — with Archive choices)
  instrument: `${TOKEN_STANDARD_PACKAGE_ID}:Instrument:Instrument`,
  tradingPair: `${TOKEN_STANDARD_PACKAGE_ID}:Instrument:TradingPair`,
  holding: `${TOKEN_STANDARD_PACKAGE_ID}:Holding:Holding`,
  transferProposal: `${TOKEN_STANDARD_PACKAGE_ID}:Holding:TransferProposal`,
  mintRequest: `${TOKEN_STANDARD_PACKAGE_ID}:Holding:MintRequest`,
  orderV3: `${TOKEN_STANDARD_PACKAGE_ID}:OrderV3:OrderV3`,
  settlement: `${TOKEN_STANDARD_PACKAGE_ID}:Settlement:SettlementInstruction`,
  trade: `${TOKEN_STANDARD_PACKAGE_ID}:Settlement:Trade`,
  batchSettlement: `${TOKEN_STANDARD_PACKAGE_ID}:Settlement:BatchSettlement`,
  
  // Allocation Record template (v2.2.0) - fallback for Allocation creation
  allocationRecord: `${TOKEN_STANDARD_PACKAGE_ID}:Settlement:AllocationRecord`,
  
  // Order template from new package (v2.2.0) - supports stopPrice, TriggerStopLoss, Allocation tracking
  orderNew: `${TOKEN_STANDARD_PACKAGE_ID}:Order:Order`,
  
  // Legacy-compatible Templates — now using current TOKEN_STANDARD_PACKAGE_ID
  userAccount: `${TOKEN_STANDARD_PACKAGE_ID}:UserAccount:UserAccount`,
  order: `${TOKEN_STANDARD_PACKAGE_ID}:Order:Order`,
  legacyTrade: `${TOKEN_STANDARD_PACKAGE_ID}:Trade:Trade`,
  masterOrderBook: `${TOKEN_STANDARD_PACKAGE_ID}:MasterOrderBook:MasterOrderBook`,
  masterOrderBookV2: `${TOKEN_STANDARD_PACKAGE_ID}:MasterOrderBookV2:MasterOrderBookV2`,
};

// =============================================================================
// TRADING PAIRS - Supported trading pairs
// =============================================================================

const TRADING_PAIRS = [
  { pair: 'BTC/USDT', baseAsset: 'BTC', quoteAsset: 'USDT' },
  { pair: 'ETH/USDT', baseAsset: 'ETH', quoteAsset: 'USDT' },
  { pair: 'SOL/USDT', baseAsset: 'SOL', quoteAsset: 'USDT' },
  // CC (Canton Coin / Amulet) pairs using CBTC as quote
  { pair: 'CC/CBTC', baseAsset: 'CC', quoteAsset: 'CBTC' },
];

// =============================================================================
// SUPPORTED TOKENS - Token symbols and metadata
// =============================================================================

const SUPPORTED_TOKENS = {
  BTC: { symbol: 'BTC', name: 'Bitcoin', decimals: 8 },
  USDT: { symbol: 'USDT', name: 'Tether USD', decimals: 6 },
  ETH: { symbol: 'ETH', name: 'Ethereum', decimals: 18 },
  SOL: { symbol: 'SOL', name: 'Solana', decimals: 9 },
  // Splice/Canton tokens
  CC: { symbol: 'CC', name: 'Canton Coin (Amulet)', decimals: 10 },
  CBTC: { symbol: 'CBTC', name: 'Canton BTC', decimals: 10 },
};

// =============================================================================
// DEFAULT MINT AMOUNTS - For test faucet
// =============================================================================

const DEFAULT_MINT_AMOUNTS = {
  BTC: 10,
  USDT: 100000,
  ETH: 100,
  SOL: 1000,
};

// =============================================================================
// INSTRUMENT ID BUILDER - Create InstrumentId objects
// =============================================================================

/**
 * Build an InstrumentId object for DAML contracts
 * @param {string} symbol - Token symbol (e.g., 'BTC', 'USDT')
 * @param {string} issuer - Issuer party ID (usually operator)
 * @param {string} version - Version string (default '1.0')
 */
function buildInstrumentId(symbol, issuer, version = '1.0') {
  return {
    issuer,
    symbol,
    version,
  };
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Get template ID for a given template name
 * @param {string} templateName - Name from TEMPLATE_IDS (e.g., 'holding', 'order')
 * @returns {string} Full template ID string
 */
function getTemplateId(templateName) {
  const templateId = TEMPLATE_IDS[templateName];
  if (!templateId) {
    throw new Error(`Unknown template: ${templateName}. Available: ${Object.keys(TEMPLATE_IDS).join(', ')}`);
  }
  return templateId;
}

/**
 * Get all Token Standard template IDs
 * @returns {Object} Object with all token standard template IDs
 */
function getTokenStandardTemplateIds() {
  return {
    instrument: TEMPLATE_IDS.instrument,
    tradingPair: TEMPLATE_IDS.tradingPair,
    holding: TEMPLATE_IDS.holding,
    transferProposal: TEMPLATE_IDS.transferProposal,
    mintRequest: TEMPLATE_IDS.mintRequest,
    order: TEMPLATE_IDS.orderNew,
    settlement: TEMPLATE_IDS.settlement,
    trade: TEMPLATE_IDS.trade,
    batchSettlement: TEMPLATE_IDS.batchSettlement,
  };
}

/**
 * Get all Legacy template IDs (now using current package)
 * @returns {Object} Object with all legacy template IDs
 */
function getLegacyTemplateIds() {
  return {
    userAccount: TEMPLATE_IDS.userAccount,
    order: TEMPLATE_IDS.order,
    trade: TEMPLATE_IDS.legacyTrade,
    masterOrderBook: TEMPLATE_IDS.masterOrderBook,
    masterOrderBookV2: TEMPLATE_IDS.masterOrderBookV2,
  };
}

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = {
  // Party IDs
  OPERATOR_PARTY_ID,
  DSO_PARTY_ID,
  
  // Canton Endpoints
  CANTON_JSON_API_BASE,
  CANTON_ADMIN_API,
  DEFAULT_SYNCHRONIZER_ID,
  REGISTRY_BACKEND_API,
  SCAN_PROXY_API,
  VALIDATOR_SCAN_PROXY_API,
  
  // Package IDs
  TOKEN_STANDARD_PACKAGE_ID,
  AMULET_PACKAGE_ID,
  
  // Template IDs
  TEMPLATE_IDS,
  
  // Trading configuration
  TRADING_PAIRS,
  SUPPORTED_TOKENS,
  DEFAULT_MINT_AMOUNTS,
  
  // Helper functions
  buildInstrumentId,
  getTemplateId,
  getTokenStandardTemplateIds,
  getLegacyTemplateIds,
};
