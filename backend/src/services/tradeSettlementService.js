/**
 * Trade Settlement Service — Hybrid Balance Model
 *
 * When a trade is matched, Allocation_ExecuteTransfer moves tokens on-chain.
 * This service records the balance effect of every trade so the balance
 * API can return accurate amounts.
 *
 * Balance formula (applied by the balance API):
 *   available = Splice holdings
 *             + SUM(TradeSettlement credits for asset)
 *             - SUM(TradeSettlement debits  for asset)
 *             - SUM(OpenOrderReservations   for asset)
 *
 * For each trade two rows per party are inserted:
 *   BUYER  → +baseAmount (CC)  credit, −quoteAmount (CBTC) debit
 *   SELLER → −baseAmount (CC)  debit,  +quoteAmount (CBTC) credit
 *
 * @see prisma/schema.prisma  → model TradeSettlement
 */

const Decimal = require('decimal.js');
const { getDb } = require('./db');

Decimal.set({ precision: 20, rounding: Decimal.ROUND_DOWN });

// ─────────────────────────────────────────────────────────
// Record a trade — creates 4 rows (2 per party)
// ─────────────────────────────────────────────────────────

/**
 * @param {object} trade
 * @param {string} trade.tradeId
 * @param {string} trade.buyer       – buyer party ID
 * @param {string} trade.seller      – seller party ID
 * @param {string} trade.baseSymbol  – e.g. "CC"
 * @param {string} trade.quoteSymbol – e.g. "CBTC"
 * @param {string|number} trade.baseAmount  – quantity of base asset traded
 * @param {string|number} trade.quoteAmount – quantity of quote asset traded
 * @param {string|number} trade.price
 * @param {string} trade.tradingPair – e.g. "CC/CBTC"
 * @param {string} [trade.buyOrderId]
 * @param {string} [trade.sellOrderId]
 */
async function recordTradeSettlement(trade) {
  const db = getDb();
  const {
    tradeId,
    buyer,
    seller,
    baseSymbol,
    quoteSymbol,
    baseAmount,
    quoteAmount,
    price,
    tradingPair,
    buyOrderId,
    sellOrderId,
  } = trade;

  const baseAmt = new Decimal(baseAmount).toString();
  const quoteAmt = new Decimal(quoteAmount).toString();
  const negBaseAmt = new Decimal(baseAmount).neg().toString();
  const negQuoteAmt = new Decimal(quoteAmount).neg().toString();
  const priceStr = String(price);
  const qtyStr = baseAmt; // quantity is always in base asset

  // Use a transaction to ensure all 4 rows are written atomically
  await db.$transaction([
    // Buyer receives base asset (CREDIT)
    db.tradeSettlement.create({
      data: {
        tradeId,
        partyId: buyer,
        asset: baseSymbol,
        amount: baseAmt,       // positive = credit
        side: 'BUY',
        orderId: buyOrderId || null,
        tradingPair,
        price: priceStr,
        quantity: qtyStr,
      },
    }),
    // Buyer sends quote asset (DEBIT)
    db.tradeSettlement.create({
      data: {
        tradeId,
        partyId: buyer,
        asset: quoteSymbol,
        amount: negQuoteAmt,   // negative = debit
        side: 'BUY',
        orderId: buyOrderId || null,
        tradingPair,
        price: priceStr,
        quantity: qtyStr,
      },
    }),
    // Seller sends base asset (DEBIT)
    db.tradeSettlement.create({
      data: {
        tradeId,
        partyId: seller,
        asset: baseSymbol,
        amount: negBaseAmt,    // negative = debit
        side: 'SELL',
        orderId: sellOrderId || null,
        tradingPair,
        price: priceStr,
        quantity: qtyStr,
      },
    }),
    // Seller receives quote asset (CREDIT)
    db.tradeSettlement.create({
      data: {
        tradeId,
        partyId: seller,
        asset: quoteSymbol,
        amount: quoteAmt,      // positive = credit
        side: 'SELL',
        orderId: sellOrderId || null,
        tradingPair,
        price: priceStr,
        quantity: qtyStr,
      },
    }),
  ]);

  console.log(`[TradeSettlement] ✅ Recorded 4 rows for trade ${tradeId} (${baseAmt} ${baseSymbol}, ${quoteAmt} ${quoteSymbol})`);
}

// ─────────────────────────────────────────────────────────
// Query net balance adjustment for a party + asset
// ─────────────────────────────────────────────────────────

/**
 * Returns the net balance adjustment from all settled trades.
 *
 * @param {string} partyId
 * @param {string} asset – e.g. "CC" or "CBTC"
 * @returns {Decimal} – positive = net received, negative = net sent
 */
async function getNetTradeBalance(partyId, asset) {
  const db = getDb();
  const rows = await db.tradeSettlement.findMany({
    where: { partyId, asset },
    select: { amount: true },
  });
  let net = new Decimal(0);
  for (const r of rows) {
    net = net.plus(new Decimal(r.amount || '0'));
  }
  return net;
}

/**
 * Returns net balance adjustments for ALL assets for a party.
 *
 * @param {string} partyId
 * @returns {Object.<string, Decimal>} – e.g. { CC: Decimal(50), CBTC: Decimal(-25) }
 */
async function getAllNetTradeBalances(partyId) {
  const db = getDb();
  const rows = await db.tradeSettlement.findMany({
    where: { partyId },
    select: { asset: true, amount: true },
  });
  const balances = {};
  for (const r of rows) {
    if (!balances[r.asset]) balances[r.asset] = new Decimal(0);
    balances[r.asset] = balances[r.asset].plus(new Decimal(r.amount || '0'));
  }
  return balances;
}

/**
 * Check if a trade has already been recorded (idempotency guard).
 */
async function isTradeRecorded(tradeId) {
  const db = getDb();
  const count = await db.tradeSettlement.count({ where: { tradeId } });
  return count > 0;
}

module.exports = {
  recordTradeSettlement,
  getNetTradeBalance,
  getAllNetTradeBalances,
  isTradeRecorded,
};
