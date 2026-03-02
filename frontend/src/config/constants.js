/**
 * CENTRALIZED CONSTANTS - Frontend Single Source of Truth
 * 
 * ALL package IDs, party IDs, and important constants are defined here.
 * DO NOT hardcode these values anywhere else in the frontend.
 * 
 * To update any value:
 * 1. Update the value here
 * 2. Rebuild the frontend
 * 3. All components will automatically use the new value
 */

// =============================================================================
// PARTY IDS - Must match backend constants!
// =============================================================================

/**
 * Operator Party ID - The service account that manages the exchange
 */
export const OPERATOR_PARTY_ID = import.meta.env.VITE_OPERATOR_PARTY_ID || 
  '8100b2db-86cf-40a1-8351-55483c151cdc::122087fa379c37332a753379c58e18d397e39cb82c68c15e4af7134be46561974292';

// =============================================================================
// API ENDPOINTS
// =============================================================================

export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 
  (import.meta.env.DEV ? 'http://localhost:3001/api' : '/api');

// =============================================================================
// PACKAGE IDS - Must match backend constants!
// =============================================================================

/**
 * Token Standard Package (clob-wolfedge-tokens v2.4.0)
 * Contains: Instrument, Holding, Settlement, Order, OrderV3
 * THIS IS THE ONLY PACKAGE ID — all old packages are retired.
 */
export const TOKEN_STANDARD_PACKAGE_ID = '0224efbf74e4ecb40083f7090a9f12145c607d76da220a91eedc21ca491d24fa';

// =============================================================================
// TRADING PAIRS
// =============================================================================

export const TRADING_PAIRS = [
  { pair: 'BTC/USDT', baseAsset: 'BTC', quoteAsset: 'USDT' },
  { pair: 'ETH/USDT', baseAsset: 'ETH', quoteAsset: 'USDT' },
  { pair: 'SOL/USDT', baseAsset: 'SOL', quoteAsset: 'USDT' },
];

// =============================================================================
// SUPPORTED TOKENS
// =============================================================================

export const SUPPORTED_TOKENS = {
  BTC: { symbol: 'BTC', name: 'Bitcoin', decimals: 8, icon: '₿' },
  USDT: { symbol: 'USDT', name: 'Tether USD', decimals: 6, icon: '₮' },
  ETH: { symbol: 'ETH', name: 'Ethereum', decimals: 18, icon: 'Ξ' },
  SOL: { symbol: 'SOL', name: 'Solana', decimals: 9, icon: '◎' },
};

// =============================================================================
// DEFAULT MINT AMOUNTS - For test faucet
// =============================================================================

export const DEFAULT_MINT_AMOUNTS = {
  BTC: 10,
  USDT: 100000,
  ETH: 100,
  SOL: 1000,
};

// =============================================================================
// DEFAULT EXPORT
// =============================================================================

export default {
  // Party IDs
  OPERATOR_PARTY_ID,
  
  // API
  API_BASE_URL,
  
  // Package IDs
  TOKEN_STANDARD_PACKAGE_ID,
  
  // Trading
  TRADING_PAIRS,
  SUPPORTED_TOKENS,
  DEFAULT_MINT_AMOUNTS,
};
