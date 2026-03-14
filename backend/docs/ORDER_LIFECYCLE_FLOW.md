# Order Lifecycle Flow — From Placement to Settlement

Detailed flow of activities from order placement through settlement.

---

## 1. Order Placement (User Signs Once)

### 1.1 Balance Check
- Backend verifies user has sufficient available balance for the order
- For **SELL**: requires base token (e.g. CC)
- For **BUY**: requires quote token (e.g. CBTC) = `price × quantity`

### 1.2 Balance Reservation
- Backend reserves the required amount in PostgreSQL (`OrderReservation`)
- Prevents overselling / double-use before on-chain lock

### 1.3 Allocation Creation (Interactive — User Signs)
- User signs a **single** interactive transaction
- **AllocationFactory_Allocate** is exercised:
  - **Sender**: user (order placer)
  - **Receiver**: operator (exchange)
  - **Executor**: operator
  - **Amount**: exact order quantity (e.g. 0.1 CC for sell, 0.001 CBTC for buy)
- This creates an **Allocation** contract on Canton
- The allocation has a **lock** context — tokens are locked, NOT transferred yet
- Allocation contract ID is stored in `OrderReservation` and on the Order contract

### 1.4 Order Contract Creation (Same Session, Auto-Signed)
- After allocation succeeds, backend creates the **Order** contract on Canton
- Order references `allocationCid` (the allocation contract ID)
- Order status: **OPEN**

**Result:** Tokens are locked on-chain. User has signed once.

---

## 2. Matching (Background — No User Action)

### 2.1 Polling
- Matching engine polls Canton every few seconds for active **OPEN** orders
- Filters by trading pair (e.g. CC/CBTC)
- Sorts by price-time priority (FIFO)

### 2.2 Match Detection
- Finds crossing orders: buy price ≥ sell price
- Computes match quantity (min of buy remaining, sell remaining)

### 2.3 Allocation Check
- Both orders must have a valid `allocationContractId`
- If missing, order is skipped (no settlement)

---

## 3. Settlement (Operator Only — No User Signature)

### 3.1 Operator-as-Receiver Settlement (App Provider Only)

Allocations at order placement use receiver=operator. At match, operator executes alone (no user signature).

1. **Execute** seller's allocation (seller to operator) — executor only
2. **Execute** buyer's allocation (buyer to operator) — executor only
3. **Create** allocation operator to buyer (base)
4. **Create** allocation operator to seller (quote)
5. **Execute** both operator legs
6. For partial fills: return remaining base to seller, remaining quote to buyer

No withdraw, no ext-* submission. Operator-only at match time. No net locked holdings.

### 3.2 Fill Orders
- `FillOrder` exercised on both Order contracts
- Updates filled quantity, remaining quantity
- For partial fills: `newAllocationCid` can be null (current implementation)

### 3.4 Record Trade
- Trade record created in PostgreSQL
- Trade contract created on Canton (for history)
- Balance reservations released

**Result:** Tokens transferred. Allocation contracts archived. Holding contracts updated.

---

## 4. Order Cancellation (User-Initiated)

- User cancels order via API
- `CancelOrder` exercised on Order contract
- `Allocation_Cancel` exercised on allocation contract
- Allocation is **consumed** — tokens unlocked and returned to user
- Balance reservation released

---

## 5. Summary Table

| Phase | Who | What |
|-------|-----|------|
| Place order | User (1 signature) | Allocation created + Order created |
| Match | Backend | Find crossing orders |
| Settle | Operator only | Execute allocations, Create operator legs, Execute, FillOrder |
| Cancel | User | CancelOrder + Allocation_Cancel |

---

## 6. Lock / Allocation Contract After Settlement

**Expected:** With operator-as-receiver settlement, executing allocations and operator legs transfers tokens directly with **no net locked holdings** in either party.

**If you still see an active lock holding contract after settlement:**

1. **Order was never matched** — No crossing buy/sell order existed. The allocation stays active until the order is matched or cancelled.
2. **Settlement failed** — Check backend logs for errors during multi-leg settlement (withdraw, create, or execute).
3. **Order ID mismatch** — Confirm the allocation is for the same order that was settled (check order IDs in logs).

**Verification:** Run `node verify_holdings.js` for the parties after a trade to confirm Holding contracts reflect the new balances.
