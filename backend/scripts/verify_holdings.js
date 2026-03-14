#!/usr/bin/env node
/**
 * Verify holding contracts via Ledger API (POST /v2/state/active-contracts)
 * Same check the client would do — queries active Holding contracts per party.
 *
 * Usage:
 *   node scripts/verify_holdings.js [partyId1] [partyId2] ...
 *   node scripts/verify_holdings.js                    # uses seller/buyer from last failed trade
 *
 * Output: Available vs Locked holdings per party (from ledger, source of truth)
 */

require('dotenv').config();
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

async function main() {
  const config = require('../src/config');
  const tokenProvider = require('../src/services/tokenProvider');
  const { getCantonSDKClient } = require('../src/services/canton-sdk-client');

  // Default: parties from the failed trade (from logs)
  const defaultParties = [
    'ext-cc4ea2fd1868::1220c47338efab5fcaba74e06679d8aba88ee848398cd5de1f2160904660488ceaef', // seller
    'ext-bc7cd1828d06::1220d5ef41751f53b8cacb10ff1f960bfc8319bb5a0066297ec718fbd6becd895f38', // buyer
  ];

  const partyIds = process.argv.slice(2).length > 0
    ? process.argv.slice(2)
    : defaultParties;

  console.log('═'.repeat(60));
  console.log('Holding Contract Verification (Ledger API)');
  console.log('═'.repeat(60));
  console.log('Parties:', partyIds.length);
  console.log('');

  const sdkClient = getCantonSDKClient();
  const adminToken = await tokenProvider.getServiceToken();

  for (const partyId of partyIds) {
    console.log('─'.repeat(50));
    console.log(`Party: ${partyId.substring(0, 50)}...`);
    try {
      const state = await sdkClient.verifyHoldingState(partyId);
      console.log(`  Available: ${state.totalAvailable} (${state.holdings?.length || 0} UTXOs)`);
      console.log(`  Locked:    ${state.totalLocked} (${state.locked?.length || 0} UTXOs)`);
      if (state.locked?.length > 0) {
        console.log('  Locked contracts:');
        state.locked.forEach((h, i) => {
          console.log(`    [${i + 1}] ${h.contractId?.substring(0, 40)}... ${h.amount} ${h.exchangeSymbol} (${h.templateId?.split(':').pop() || 'Holding'})`);
        });
      }
      if (state.holdings?.length > 0) {
        console.log('  Available contracts:');
        state.holdings.forEach((h, i) => {
          console.log(`    [${i + 1}] ${h.contractId?.substring(0, 40)}... ${h.amount} ${h.exchangeSymbol}`);
        });
      }
      console.log(`  Execution visible: ${state.executionVisible ? 'YES' : 'NO'}`);
    } catch (err) {
      console.error(`  ERROR: ${err.message}`);
    }
    console.log('');
  }

  console.log('═'.repeat(60));
  console.log('Done. Locked > 0 means allocation not executed (trade did not settle).');
  console.log('═'.repeat(60));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
