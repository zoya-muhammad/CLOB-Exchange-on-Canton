/**
 * Verify Holding Contracts Directly from Canton Ledger
 * 
 * This script uses the SDK's verifyHoldingState method to query holding contracts
 * using the splice-api-token-holding-v1 interface (source of truth).
 */

const { getCantonSDKClient } = require('./src/services/canton-sdk-client');

// Parties from recent orders
const PARTIES = {
  'Party 1 (ext-cc4ea2fd1868)': 'ext-cc4ea2fd1868::1220c47338efab5fcaba74e06679d8aba88ee848398cd5de1f2160904660488ceaef',
  'Party 2 (ext-bc7cd1828d06)': 'ext-bc7cd1828d06::1220d5ef41751f53b8cacb10ff1f960bfc8319bb5a0066297ec718fbd6becd895f38',
};

async function verifyHoldingsForParty(partyName, partyId) {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`🔍 Verifying Holdings for: ${partyName}`);
  console.log(`   Party ID: ${partyId.substring(0, 50)}...`);
  console.log(`${'='.repeat(80)}\n`);

  try {
    const sdkClient = getCantonSDKClient();
    
    if (!sdkClient.isReady()) {
      console.log('⏳ Initializing SDK...');
      await sdkClient.initialize();
    }

    // Verify all holdings (no symbol filter)
    const result = await sdkClient.verifyHoldingState(partyId);

    console.log(`📊 Summary:`);
    console.log(`   Total Active Holdings: ${result.holdings.length} (available) + ${result.locked.length} (locked)`);
    console.log(`   Total Available: ${result.totalAvailable}`);
    console.log(`   Total Locked: ${result.totalLocked}`);
    console.log(`   Execution Visible: ${result.executionVisible ? '✅ YES' : '❌ NO'}`);
    console.log(`   (Execution visible means new Holdings were created after Allocation_ExecuteTransfer)\n`);

    // Verify by symbol
    const symbols = ['CC', 'CBTC'];
    for (const symbol of symbols) {
      const symbolResult = await sdkClient.verifyHoldingState(partyId, symbol);
      if (symbolResult.holdings.length > 0 || symbolResult.locked.length > 0) {
        console.log(`💰 ${symbol}:`);
        console.log(`   Available: ${symbolResult.totalAvailable} (${symbolResult.holdings.length} UTXO(s))`);
        console.log(`   Locked: ${symbolResult.totalLocked} (${symbolResult.locked.length} UTXO(s))`);
        console.log(`   Execution Visible: ${symbolResult.executionVisible ? '✅ YES' : '❌ NO'}\n`);
      }
    }

    if (result.holdings.length > 0) {
      console.log(`📋 Available Holdings (Execution Results):`);
      result.holdings.forEach((h, i) => {
        console.log(`   ${i + 1}. ${h.exchangeSymbol || h.instrumentId || 'UNKNOWN'}: ${h.amount}`);
        console.log(`      Instrument: ${h.instrumentId} → ${h.exchangeSymbol}`);
        console.log(`      Contract: ${h.contractId.substring(0, 50)}...`);
        console.log(`      Template: ${h.templateId.substring(0, 60)}...`);
        console.log('');
      });
    }

    if (result.locked.length > 0) {
      console.log(`🔒 Locked Holdings (Self-allocated, awaiting settlement):`);
      result.locked.forEach((h, i) => {
        console.log(`   ${i + 1}. ${h.exchangeSymbol || h.instrumentId || 'UNKNOWN'}: ${h.amount}`);
        console.log(`      Instrument: ${h.instrumentId} → ${h.exchangeSymbol}`);
        console.log(`      Contract: ${h.contractId.substring(0, 50)}...`);
        console.log('');
      });
    }

    return result;
  } catch (err) {
    console.error(`❌ Error verifying holdings for ${partyName}:`);
    console.error(`   ${err.message}`);
    if (err.stack) {
      console.error(`   Stack: ${err.stack.substring(0, 500)}...`);
    }
    return null;
  }
}

async function main() {
  console.log('\n' + '='.repeat(80));
  console.log('🔍 VERIFYING HOLDING CONTRACTS FROM CANTON LEDGER');
  console.log('   Using splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding');
  console.log('   (Source of Truth per Client Requirements)');
  console.log('='.repeat(80));

  const results = {};

  for (const [partyName, partyId] of Object.entries(PARTIES)) {
    results[partyName] = await verifyHoldingsForParty(partyName, partyId);
  }

  // Final summary
  console.log('\n' + '='.repeat(80));
  console.log('📊 FINAL SUMMARY');
  console.log('='.repeat(80));
  
  for (const [partyName, result] of Object.entries(results)) {
    if (!result) {
      console.log(`\n${partyName}: ❌ Verification failed`);
      continue;
    }
    console.log(`\n${partyName}:`);
    console.log(`   Total Holdings: ${result.holdings.length + result.locked.length}`);
    console.log(`   Available: ${result.totalAvailable} (${result.holdings.length} UTXOs)`);
    console.log(`   Locked: ${result.totalLocked} (${result.locked.length} UTXOs)`);
    console.log(`   Execution Visible: ${result.executionVisible ? '✅ YES' : '❌ NO'}`);
  }

  console.log('\n' + '='.repeat(80));
  console.log('✅ Verification Complete');
  console.log('='.repeat(80));
  console.log('\n💡 Note: Execution is visible when active Holding contracts exist.');
  console.log('   After Allocation_ExecuteTransfer executes, new Holding contracts');
  console.log('   are created and should be visible here.\n');
}

main().catch(err => {
  console.error('\n❌ Script failed:');
  console.error(err);
  process.exit(1);
});
