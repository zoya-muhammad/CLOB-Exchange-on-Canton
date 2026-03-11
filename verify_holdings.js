/**
 * Verify Holding Contracts Directly from Canton Ledger
 * 
 * This script queries holding contracts using the splice-api-token-holding-v1 interface
 * to verify execution visibility as per client requirements.
 */

// Run from backend directory: cd backend && node ../verify_holdings.js
const cantonService = require('./src/services/cantonService');
const tokenProvider = require('./src/services/tokenProvider');

// Parties from recent orders
const PARTIES = {
  'Party 1 (ext-cc4ea2fd1868)': 'ext-cc4ea2fd1868::1220c47338efab5fcaba74e06679d8aba88ee848398cd5de1f2160904660488ceaef',
  'Party 2 (ext-bc7cd1828d06)': 'ext-bc7cd1828d06::1220d5ef41751f53b8cacb10ff1f960bfc8319bb5a0066297ec718fbd6becd895f38',
};

const HOLDING_INTERFACE = '#splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding';

function extractInstrumentId(instrumentId) {
  if (!instrumentId) return null;
  if (typeof instrumentId === 'string') {
    // Handle both "CBTC" and instrument object formats
    if (instrumentId.includes('CBTC')) return 'CBTC';
    if (instrumentId.includes('CC')) return 'CC';
    return instrumentId;
  }
  if (instrumentId.symbol) return instrumentId.symbol;
  if (instrumentId.id) return instrumentId.id;
  return null;
}

async function verifyHoldingsForParty(partyName, partyId) {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`🔍 Verifying Holdings for: ${partyName}`);
  console.log(`   Party ID: ${partyId.substring(0, 50)}...`);
  console.log(`${'='.repeat(80)}\n`);

  try {
    const adminToken = await tokenProvider.getServiceToken();
    
    // Query ACTIVE contracts only (source of truth)
    const activeContracts = await cantonService.queryActiveContracts({
      party: partyId,
      templateIds: [HOLDING_INTERFACE],
    }, adminToken);

    if (!activeContracts || activeContracts.length === 0) {
      console.log(`❌ No active Holding contracts found for ${partyName}`);
      console.log(`   This means NO execution has occurred (no new Holdings created)`);
      return { total: 0, holdings: [], locked: [] };
    }

    console.log(`✅ Found ${activeContracts.length} active Holding contract(s)\n`);

    const holdings = [];
    const locked = [];
    const bySymbol = {};

    for (const contract of activeContracts) {
      const payload = contract.createArgument || contract.payload || {};
      const tpl = typeof contract.templateId === 'string' ? contract.templateId : '';
      const amt = payload?.amount?.initialAmount || payload?.amount || payload?.quantity || '0';
      const instId = extractInstrumentId(payload?.instrumentId || payload?.instrument?.id);
      const isLocked = tpl.includes('Locked') || !!payload?.lock || !!payload?.isLocked;
      const owner = payload?.owner || 'unknown';

      const entry = {
        contractId: contract.contractId,
        templateId: tpl,
        amount: amt,
        instrumentId: instId,
        isLocked,
        owner: owner.substring(0, 30) + '...',
        createdAt: contract.createdAt || contract.createdEvent?.eventId,
      };

      if (isLocked) {
        locked.push(entry);
      } else {
        holdings.push(entry);
      }

      // Group by symbol
      if (instId) {
        if (!bySymbol[instId]) {
          bySymbol[instId] = { available: [], locked: [] };
        }
        if (isLocked) {
          bySymbol[instId].locked.push(entry);
        } else {
          bySymbol[instId].available.push(entry);
        }
      }
    }

    // Display summary
    console.log(`📊 Summary:`);
    console.log(`   Total Active Holdings: ${holdings.length} (available) + ${locked.length} (locked) = ${activeContracts.length}`);
    console.log(`   Execution Visible: ${holdings.length > 0 ? '✅ YES' : '❌ NO'} (new Holdings created after Allocation_ExecuteTransfer)\n`);

    // Display by symbol
    for (const [symbol, data] of Object.entries(bySymbol)) {
      const totalAvailable = data.available.reduce((sum, h) => sum + parseFloat(h.amount || 0), 0);
      const totalLocked = data.locked.reduce((sum, h) => sum + parseFloat(h.amount || 0), 0);
      
      console.log(`💰 ${symbol}:`);
      console.log(`   Available: ${totalAvailable} (${data.available.length} UTXO(s))`);
      console.log(`   Locked:    ${totalLocked} (${data.locked.length} UTXO(s))`);
      
      if (data.available.length > 0) {
        console.log(`   ✅ Execution visible for ${symbol} (${data.available.length} active Holding contract(s))`);
      }
      console.log('');
    }

    // Display detailed contract info
    if (holdings.length > 0) {
      console.log(`📋 Available Holdings (Execution Results):`);
      holdings.forEach((h, i) => {
        console.log(`   ${i + 1}. ${h.instrumentId || 'UNKNOWN'}: ${h.amount}`);
        console.log(`      Contract ID: ${h.contractId.substring(0, 50)}...`);
        console.log(`      Template: ${h.templateId.substring(0, 60)}...`);
        console.log('');
      });
    }

    if (locked.length > 0) {
      console.log(`🔒 Locked Holdings (Allocated but not executed):`);
      locked.forEach((h, i) => {
        console.log(`   ${i + 1}. ${h.instrumentId || 'UNKNOWN'}: ${h.amount}`);
        console.log(`      Contract ID: ${h.contractId.substring(0, 50)}...`);
        console.log('');
      });
    }

    return {
      total: activeContracts.length,
      holdings,
      locked,
      executionVisible: holdings.length > 0,
      bySymbol,
    };
  } catch (err) {
    console.error(`❌ Error verifying holdings for ${partyName}:`);
    console.error(`   ${err.message}`);
    console.error(`   Stack: ${err.stack}`);
    return { total: 0, holdings: [], locked: [], executionVisible: false };
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
    console.log(`\n${partyName}:`);
    console.log(`   Total Holdings: ${result.total}`);
    console.log(`   Available: ${result.holdings.length}`);
    console.log(`   Locked: ${result.locked.length}`);
    console.log(`   Execution Visible: ${result.executionVisible ? '✅ YES' : '❌ NO'}`);
    
    if (result.bySymbol) {
      for (const [symbol, data] of Object.entries(result.bySymbol)) {
        const totalAvailable = data.available.reduce((sum, h) => sum + parseFloat(h.amount || 0), 0);
        console.log(`   ${symbol}: ${totalAvailable} available, ${data.locked.reduce((sum, h) => sum + parseFloat(h.amount || 0), 0)} locked`);
      }
    }
  }

  console.log('\n' + '='.repeat(80));
  console.log('✅ Verification Complete');
  console.log('='.repeat(80) + '\n');
}

main().catch(err => {
  console.error('\n❌ Script failed:');
  console.error(err);
  process.exit(1);
});
