/**
 * Direct Ledger API Holding Contract Verification
 * 
 * Queries the Canton Ledger API via WebSocket (same method as SDK)
 * to verify Holding contracts reflect token execution.
 * 
 * Uses: ws://.../v2/state/active-contracts (WebSocket ACS query)
 * Interface: splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding
 */

require('dotenv').config();
const axios = require('axios');
const WebSocket = require('ws');

const LEDGER_API = process.env.CANTON_JSON_LEDGER_API_BASE || 'http://65.108.40.104:31539';
const WS_BASE = LEDGER_API.replace(/^http/, 'ws');
const KEYCLOAK_TOKEN_URL = process.env.KEYCLOAK_TOKEN_URL;
const OAUTH_CLIENT_ID = process.env.OAUTH_CLIENT_ID;
const OAUTH_CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET;

const PARTIES = {
  'Party 1 (Buyer ext-cc4ea2fd1868)': 'ext-cc4ea2fd1868::1220c47338efab5fcaba74e06679d8aba88ee848398cd5de1f2160904660488ceaef',
  'Party 2 (Seller ext-bc7cd1828d06)': 'ext-bc7cd1828d06::1220d5ef41751f53b8cacb10ff1f960bfc8319bb5a0066297ec718fbd6becd895f38',
};

const REVERSE_INSTRUMENT_MAP = { 'Amulet': 'CC', 'CBTC': 'CBTC' };

async function getAdminToken() {
  const params = new URLSearchParams();
  params.append('grant_type', 'client_credentials');
  params.append('client_id', OAUTH_CLIENT_ID);
  params.append('client_secret', OAUTH_CLIENT_SECRET);
  params.append('audience', 'https://canton.network.global');
  const { data } = await axios.post(KEYCLOAK_TOKEN_URL, params, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });
  return data.access_token;
}

async function getLedgerEndOffset(token) {
  const { data } = await axios.get(`${LEDGER_API}/v2/state/ledger-end`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return data.offset || 0;
}

function inferInstrument(templateId, payload, interfaceView) {
  const tpl = typeof templateId === 'string' ? templateId : JSON.stringify(templateId || '');
  
  // Check interface view first
  if (interfaceView?.instrumentId) {
    const id = typeof interfaceView.instrumentId === 'string'
      ? interfaceView.instrumentId
      : interfaceView.instrumentId?.id || '';
    if (id) return id;
  }
  
  // Template-based inference
  if (tpl.includes('Amulet') || tpl.includes('splice-amulet')) return 'Amulet';
  
  // Payload check
  const instId = payload?.instrumentId?.id || payload?.instrument?.id?.id || '';
  return instId || 'UNKNOWN';
}

function queryHoldingsViaWebSocket(partyId, token, offset) {
  return new Promise((resolve, reject) => {
    const url = `${WS_BASE}/v2/state/active-contracts`;
    const ws = new WebSocket(url, ['daml.ws.auth'], {
      handshakeTimeout: 15000,
      headers: { Authorization: `Bearer ${token}` },
    });

    const contracts = [];
    const timeout = setTimeout(() => { ws.close(); resolve(contracts); }, 30000);

    ws.on('open', () => {
      const filter = {
        filtersByParty: {
          [partyId]: {
            cumulative: [{
              identifierFilter: {
                InterfaceFilter: {
                  value: {
                    interfaceId: '#splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding',
                    includeCreatedEventBlob: false,
                    includeInterfaceView: true,
                  },
                },
              },
            }],
          },
        },
      };
      ws.send(JSON.stringify({ filter, verbose: false, activeAtOffset: offset }));
    });

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.code && msg.cause) {
          clearTimeout(timeout); ws.close(); reject(new Error(msg.cause)); return;
        }
        // Extract created event from Canton's wrapper
        const ce = msg.contractEntry?.JsActiveContract?.createdEvent
          || msg.activeContract?.createdEvent
          || msg.createdEvent
          || msg;
        if (ce?.contractId) contracts.push(ce);
      } catch (_) {}
    });

    ws.on('close', () => { clearTimeout(timeout); resolve(contracts); });
    ws.on('error', (err) => { clearTimeout(timeout); reject(err); });
  });
}

async function verifyParty(partyName, partyId, token, offset) {
  console.log(`\n${'═'.repeat(80)}`);
  console.log(`  ${partyName}`);
  console.log(`  Party: ${partyId.substring(0, 50)}...`);
  console.log(`${'═'.repeat(80)}`);

  const contracts = await queryHoldingsViaWebSocket(partyId, token, offset);
  console.log(`\n  Holding contracts from Ledger API (WebSocket): ${contracts.length}`);

  if (contracts.length === 0) {
    console.log(`  ❌ NO Holding contracts found`);
    return { available: 0, locked: 0 };
  }

  const available = [];
  const locked = [];

  for (const event of contracts) {
    const tpl = typeof event.templateId === 'string' ? event.templateId : JSON.stringify(event.templateId || '');
    const payload = event.createArgument || event.payload || {};
    const ifaceView = event.interfaceView || event.interfaceViewValue || {};

    const amount = ifaceView?.amount
      || payload?.amount?.initialAmount
      || payload?.amount
      || '0';

    const instrumentRaw = inferInstrument(event.templateId, payload, ifaceView);
    const exchangeSymbol = REVERSE_INSTRUMENT_MAP[instrumentRaw] || instrumentRaw;
    const isLocked = tpl.includes('Locked') || !!payload?.lock;

    const entry = {
      contractId: event.contractId,
      shortCid: event.contractId?.substring(0, 50) + '...',
      template: tpl.length > 65 ? '...' + tpl.slice(-60) : tpl,
      amount: typeof amount === 'object' ? JSON.stringify(amount) : amount,
      instrument: instrumentRaw,
      symbol: exchangeSymbol,
      isLocked,
    };

    if (isLocked) locked.push(entry);
    else available.push(entry);
  }

  const totalAvailable = available.reduce((sum, h) => sum + parseFloat(h.amount || 0), 0);
  const totalLocked = locked.reduce((sum, h) => sum + parseFloat(h.amount || 0), 0);

  console.log(`\n  ┌─ AVAILABLE Holdings (post-execution) ──────────────`);
  console.log(`  │  Count: ${available.length} | Total: ${totalAvailable.toFixed(6)}`);
  for (const h of available) {
    console.log(`  │  ${h.symbol.padEnd(6)} ${String(h.amount).padEnd(20)} ${h.shortCid}`);
  }
  console.log(`  └────────────────────────────────────────────────────`);

  console.log(`\n  ┌─ LOCKED Holdings (allocated/reserved) ─────────────`);
  console.log(`  │  Count: ${locked.length} | Total: ${totalLocked.toFixed(6)}`);
  for (const h of locked) {
    console.log(`  │  ${h.symbol.padEnd(6)} ${String(h.amount).padEnd(20)} ${h.shortCid}`);
  }
  console.log(`  └────────────────────────────────────────────────────`);

  const ccAvail = available.filter(h => h.symbol === 'CC');
  const ccLocked = locked.filter(h => h.symbol === 'CC');
  const cbtcAvail = available.filter(h => h.symbol === 'CBTC');
  const cbtcLocked = locked.filter(h => h.symbol === 'CBTC');

  console.log(`\n  ── Per-Token Breakdown ──`);
  console.log(`  CC:   ${ccAvail.length} avail (${ccAvail.reduce((s, h) => s + parseFloat(h.amount || 0), 0).toFixed(6)}), ${ccLocked.length} locked (${ccLocked.reduce((s, h) => s + parseFloat(h.amount || 0), 0).toFixed(6)})`);
  console.log(`  CBTC: ${cbtcAvail.length} avail (${cbtcAvail.reduce((s, h) => s + parseFloat(h.amount || 0), 0).toFixed(6)}), ${cbtcLocked.length} locked (${cbtcLocked.reduce((s, h) => s + parseFloat(h.amount || 0), 0).toFixed(6)})`);

  console.log(`\n  ── Verification ──`);
  console.log(`  CC identified:         ${(ccAvail.length + ccLocked.length) > 0 ? '✅ YES' : '❌ NO'}`);
  console.log(`  CBTC identified:       ${(cbtcAvail.length + cbtcLocked.length) > 0 ? '✅ YES' : '❌ NO'}`);
  console.log(`  Execution visible:     ${available.length > 0 ? '✅ YES' : '❌ NO'}`);
  console.log(`  Interface used:        splice-api-token-holding-v1:Splice.Api.Token.HoldingV1:Holding`);

  return { available: available.length, locked: locked.length };
}

async function main() {
  console.log('╔══════════════════════════════════════════════════════════════════════════════╗');
  console.log('║  DIRECT LEDGER API — Holding Contract Verification (WebSocket)             ║');
  console.log('║  Endpoint: ws://.../v2/state/active-contracts                              ║');
  console.log('║  Interface: splice-api-token-holding-v1:...HoldingV1:Holding               ║');
  console.log('╚══════════════════════════════════════════════════════════════════════════════╝');

  console.log(`\nLedger API: ${LEDGER_API}`);
  console.log(`WebSocket:  ${WS_BASE}/v2/state/active-contracts`);

  console.log('\nObtaining admin token...');
  const token = await getAdminToken();
  console.log('Token obtained ✅');

  const offset = await getLedgerEndOffset(token);
  console.log(`Ledger end offset: ${offset}\n`);

  for (const [name, partyId] of Object.entries(PARTIES)) {
    await verifyParty(name, partyId, token, offset);
  }

  console.log(`\n${'═'.repeat(80)}`);
  console.log('  Done. These results come DIRECTLY from the Canton Ledger API,');
  console.log('  querying the same Holding interface visible on ccview.io explorer.');
  console.log(`${'═'.repeat(80)}\n`);
}

main().catch(err => {
  console.error('❌ Verification failed:', err.message);
  if (err.response?.data) console.error('API response:', JSON.stringify(err.response.data).substring(0, 500));
  process.exit(1);
});
