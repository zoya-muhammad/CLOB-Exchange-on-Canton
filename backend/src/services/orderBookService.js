/**
 * Order Book Service
 *
 * Primary path: reads from StreamingReadModel (in-memory cache fed by
 * persistent WebSocket).  Typical response time: < 5 ms.
 *
 * Fallback path (cold start only): queries Canton directly via
 * cantonService.queryActiveContracts — used only while the
 * StreamingReadModel is still bootstrapping after server restart.
 */

const config = require('../config');
const tokenProvider = require('./tokenProvider');
const cantonService = require('./cantonService');
const { TEMPLATE_IDS } = require('../config/constants');
const { getTokenSystemType } = require('../config/canton-sdk.config');
const { getStreamingReadModel } = require('./streamingReadModel');

const SUPPORTED_PAIRS = [
    'BTC/USDT',
    'ETH/USDT',
    'SOL/USDT',
    'CBTC/USDT',
    'CC/CBTC',
];

const MAX_UTILITY_ALLOCATION_AGE_MS = 24 * 60 * 60 * 1000;  // 24 h
const MAX_SPLICE_ALLOCATION_AGE_MS  = 15 * 60 * 1000;       // 15 min

function isAllocationExpired(order) {
    const [baseAsset, quoteAsset] = String(order.tradingPair || '').split('/');
    const side = String(order.orderType || '').toUpperCase();
    const lockedAsset = side === 'BUY' ? quoteAsset : baseAsset;
    const lockedAssetType = lockedAsset ? getTokenSystemType(lockedAsset) : null;
    const maxAgeMs = lockedAssetType === 'splice'
        ? MAX_SPLICE_ALLOCATION_AGE_MS
        : MAX_UTILITY_ALLOCATION_AGE_MS;
    const orderAgeMs = order.timestamp
        ? (Date.now() - new Date(order.timestamp).getTime())
        : Infinity;
    return !Number.isFinite(orderAgeMs) || orderAgeMs > maxAgeMs;
}

class OrderBookService {
    constructor() {
        this._cache = getStreamingReadModel();
        console.log('[OrderBookService] Initialized — cache-first mode');
    }

    async getOrderBook(tradingPair, userPartyId = null) {
        if (this._cache.isReady()) {
            return this._getOrderBookFromCache(tradingPair, userPartyId);
        }
        return this._getOrderBookFromCanton(tradingPair, userPartyId);
    }

    async getAllOrderBooks() {
        return Promise.all(
            SUPPORTED_PAIRS.map(pair => this.getOrderBook(pair))
        );
    }

    async getTrades(tradingPair, limit = 50) {
        if (this._cache.isReady()) {
            return this._cache.getTradesForPair(tradingPair, limit);
        }
        return this._getTradesFromCanton(tradingPair, limit);
    }

    // ─── Cache path (sub-millisecond) ──────────────────────────────────

    _getOrderBookFromCache(tradingPair, userPartyId = null) {
        const book = this._cache.getOrderBook(tradingPair);

        book.buyOrders  = book.buyOrders.filter(o => !isAllocationExpired(o));
        book.sellOrders = book.sellOrders.filter(o => !isAllocationExpired(o));
        book.source = 'in-memory-cache';
        if (userPartyId) book.userPartyId = userPartyId;

        return book;
    }

    // ─── Canton fallback (cold start only) ─────────────────────────────

    async _getOrderBookFromCanton(tradingPair, userPartyId = null) {
        const token = await tokenProvider.getServiceToken();
        const operatorPartyId = config.canton.operatorPartyId;
        if (!operatorPartyId) {
            return this.emptyOrderBook(tradingPair, 'missing-operator-party');
        }

        const contracts = await cantonService.queryActiveContracts({
            party: operatorPartyId,
            templateIds: [TEMPLATE_IDS.orderNew, TEMPLATE_IDS.order],
            pageSize: 500,
        }, token);

        const openOrders = (Array.isArray(contracts) ? contracts : [])
            .map((c) => {
                const payload = c.payload || c.createArgument || {};
                const qty = parseFloat(payload.quantity || '0');
                const filled = parseFloat(payload.filled || '0');
                const rawPrice = payload.price?.Some ?? payload.price ?? null;
                return {
                    contractId: c.contractId,
                    owner: payload.owner,
                    orderId: payload.orderId,
                    tradingPair: payload.tradingPair,
                    orderType: payload.orderType,
                    orderMode: payload.orderMode,
                    status: payload.status,
                    price: rawPrice,
                    quantity: payload.quantity,
                    filled: payload.filled,
                    remaining: qty - filled,
                    timestamp: payload.timestamp,
                };
            })
            .filter((o) =>
                o.tradingPair === tradingPair &&
                o.status === 'OPEN' &&
                Number.isFinite(o.remaining) &&
                o.remaining > 0.0000001 &&
                !isAllocationExpired(o)
            );

        const buyOrders = openOrders
            .filter((o) => o.orderType === 'BUY')
            .sort((a, b) => parseFloat(b.price || 0) - parseFloat(a.price || 0));

        const sellOrders = openOrders
            .filter((o) => o.orderType === 'SELL')
            .sort((a, b) => parseFloat(a.price || Infinity) - parseFloat(b.price || Infinity));

        return {
            tradingPair,
            buyOrders,
            sellOrders,
            lastPrice: null,
            timestamp: new Date().toISOString(),
            source: 'canton-live-query',
            ...(userPartyId ? { userPartyId } : {}),
        };
    }

    async _getTradesFromCanton(tradingPair, limit = 50) {
        const token = await tokenProvider.getServiceToken();
        const operatorPartyId = config.canton.operatorPartyId;
        if (!operatorPartyId) return [];

        const contracts = await cantonService.queryActiveContracts({
            party: operatorPartyId,
            templateIds: [TEMPLATE_IDS.trade, TEMPLATE_IDS.legacyTrade],
            pageSize: 500,
        }, token);

        return (Array.isArray(contracts) ? contracts : [])
            .map((c) => {
                const p = c.payload || c.createArgument || {};
                return {
                    contractId: c.contractId,
                    tradeId: p.tradeId,
                    tradingPair: p.tradingPair || p.pair,
                    buyer: p.buyer,
                    seller: p.seller,
                    price: p.price,
                    amount: p.amount || p.quantity,
                    quantity: p.quantity || p.amount,
                    timestamp: p.timestamp,
                };
            })
            .filter((t) => t.tradingPair === tradingPair)
            .sort((a, b) => new Date(b.timestamp || 0) - new Date(a.timestamp || 0))
            .slice(0, limit);
    }

    // ─── Helpers ───────────────────────────────────────────────────────

    emptyOrderBook(tradingPair, source = 'empty') {
        return {
            tradingPair,
            buyOrders: [],
            sellOrders: [],
            lastPrice: null,
            timestamp: new Date().toISOString(),
            source,
        };
    }

    async createOrderBook(tradingPair) {
        console.log(`[OrderBookService] Order book creation not needed for ${tradingPair}`);
        return { contractId: `virtual-${tradingPair}`, alreadyExists: true };
    }
}

let instance = null;
function getOrderBookService() {
    if (!instance) {
        instance = new OrderBookService();
    }
    return instance;
}

module.exports = { OrderBookService, getOrderBookService };
