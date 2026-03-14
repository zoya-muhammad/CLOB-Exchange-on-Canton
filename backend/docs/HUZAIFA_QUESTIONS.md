# Multi-Leg Settlement with External Parties

Huzaifa suggested we implement the multi-leg settlement (withdraw, then create allocations with legs seller to buyer and buyer to seller, then execute both). We tried the approach Huzaifa suggested. It’s failing with the errors below and we need your input on how to proceed.

## What We Implemented

1. Withdraw seller's allocation (unlock base)
2. Withdraw buyer's allocation (unlock quote)
3. Create allocation seller to buyer (base)
4. Create allocation buyer to seller (quote)
5. Execute both allocations (direct transfer, no net locked)

## Error 1: NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT

We get: "This participant cannot submit as the given submitter on any connected synchronizer"

This happens on steps 1–4 (withdraw and create). We’re using `actAs: [ext-cc4ea2fd1868::12...]` (external party). The `Allocation_Withdraw` controller is the sender, and `AllocationFactory_Allocate` requires the sender to authorize, so we have to submit as the external party. The participant returns 404 because it can’t submit as that external party on any synchronizer.

How can the backend submit as an external party (ext-*) for withdraw/create? Is this a Canton configuration thing? Do external parties need to be allocated to the participant in a specific way? Or is there a way for the operator/venue to act on behalf of the sender for these operations? Or is this pattern only for hosted/internal parties?

## Error 2: key not found: tag (LEDGER_API_INTERNAL_ERROR)

We get HTTP 500 with "key not found: tag" when using the direct withdraw path with a synthetic context like:

```json
{
  "extraArgs": {
    "context": { "values": { "expire-lock": { "textValue": "2026-03-13T01:48:24.598Z" } } },
    "meta": { "values": {} }
  }
}
```

Looks like the Canton JSON API expects a different structure (probably a tagged union with a `tag` field), so our choice argument format is wrong. We tried changing the context from `{ textValue: "..." }` to `{ tag: "AV_Text", value: "..." }` per DAML-LF variant encoding. If it still fails, the registry choice-context API response format might be different.

What’s the correct JSON format for `Allocation_Withdraw` `extraArgs` / `choiceContextData`? Should we always use the registry API and avoid building synthetic context?

## CLOB vs TradingApp

In TradingApp, both parties approve the proposal and allocations are created at match time with the right legs. The venue then executes. In our CLOB, orders are placed before match and allocations are created at order placement with receiver=operator. At match we need to convert to direct legs.

For a CLOB where orders are placed before match and counterparty is unknown: how do we get direct transfer legs (seller to buyer, buyer to seller) with no net locked holdings? Is withdraw + create new allocations the right approach, or is there another pattern we should use?

## Environment

Parties are external (ext-*). We’re using Canton JSON Ledger API v2 and Splice Token Standard (CC, CBTC).
