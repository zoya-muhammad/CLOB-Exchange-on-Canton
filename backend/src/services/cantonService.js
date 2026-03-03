/**
 * Canton Service - JSON Ledger API v2
 * 
 * NO PATCHES - Uses only documented Canton endpoints:
 * - POST /v2/commands/submit-and-wait-for-transaction (all writes)
 * - POST /v2/state/active-contracts (reads)
 * - GET /v2/synchronizers (discovery)
 * - POST /v2/parties/external/allocate (external party creation)
 * 
 * Based on:
 * - https://docs.digitalasset.com/build/3.5/reference/json-api/openapi.html
 */

const config = require("../config");
const crypto = require("crypto");

/**
 * Normalize template ID to Identifier object format required by JSON Ledger API v2
 * 
 * JSON Ledger API v2 requires templateId as an IDENTIFIER OBJECT: {packageId, moduleName, entityName}
 * NOT a string "packageId:Module:Entity"
 * 
 * The Identifier object form is the most consistently supported across Canton JSON Ledger API variants.
 * 
 * Accepts:
 * - Object: {packageId, moduleName, entityName} (returns as-is)
 * - String: "packageId:Module:Entity" (parses into object)
 * 
 * Returns: Object {packageId, moduleName, entityName}
 */
function normalizeTemplateId(templateId) {
  // If already an object with required fields, return as-is
  if (templateId && typeof templateId === "object" && templateId.packageId && templateId.moduleName && templateId.entityName) {
    return templateId;
  }
  
  // If string format, check if it uses "#" prefix (package name format)
  // Canton allows "#package-name:Module:Entity" format - keep as string
  if (typeof templateId === "string") {
    // If it starts with "#", it's using package name format - return as-is (string)
    if (templateId.startsWith("#")) {
      return templateId; // Keep as string for API
    }
    
    // Otherwise, parse into object format
    const parts = templateId.split(":");
    if (parts.length === 3) {
      return {
        packageId: parts[0],
        moduleName: parts[1],
        entityName: parts[2]
      };
    }
    // Handle package-name format (2 parts: package-name:Entity)
    if (parts.length === 2) {
      // This is a fallback - ideally use full format
      // We can't determine module name from package-name format alone
      throw new Error(`Invalid templateId string format: "${templateId}". Expected "packageId:ModuleName:EntityName" (3 parts)`);
    }
    throw new Error(`Invalid templateId string format: "${templateId}". Expected "packageId:Module:Entity"`);
  }
  
  throw new Error(`Invalid templateId: expected string or {packageId,moduleName,entityName}, got: ${JSON.stringify(templateId)}`);
}

/**
 * Convert templateId to string format for JSON Ledger API v2 commands
 * Commands require string format: "packageId:ModuleName:EntityName"
 */
function templateIdToString(templateId) {
  if (typeof templateId === "string") {
    return templateId;
  }
  if (templateId && typeof templateId === "object" && templateId.packageId && templateId.moduleName && templateId.entityName) {
    return `${templateId.packageId}:${templateId.moduleName}:${templateId.entityName}`;
  }
  throw new Error(`Invalid templateId: ${JSON.stringify(templateId)}`);
}

/**
 * Decode JWT token payload
 */
function decodeTokenPayload(token) {
  if (!token || typeof token !== "string") {
    throw new Error("Token is required to extract payload");
  }
  const parts = token.split(".");
  if (parts.length !== 3) {
    throw new Error("Invalid token format");
  }
  const payloadBase64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
  const padded = payloadBase64 + "=".repeat((4 - (payloadBase64.length % 4)) % 4);
  const payloadJson = Buffer.from(padded, "base64").toString("utf8");
  return JSON.parse(payloadJson);
}

/**
 * Parse Canton error response and extract useful details
 */
function parseCantonError(text, status) {
  let errorData = {};
  try {
    errorData = JSON.parse(text);
  } catch (e) {
    return {
      code: 'UNKNOWN_ERROR',
      message: text,
      httpStatus: status
    };
  }

  return {
    code: errorData.code || 'UNKNOWN_ERROR',
    message: errorData.cause || errorData.message || text,
    correlationId: errorData.correlationId,
    traceId: errorData.traceId,
    context: errorData.context,
    errorCategory: errorData.errorCategory,
    httpStatus: status
  };
}

class CantonService {
  constructor() {
    this.jsonApiBase = config.canton.jsonApiBase;
    this.operatorPartyId = config.canton.operatorPartyId;
    this.synchronizerId = config.canton.synchronizerId;
    this.packageName = config.canton.packageName;
  }

  /**
   * Get template ID in package-name format
   */
  getTemplateId(entityName) {
    return `${this.packageName}:${entityName}`;
  }

  // ==========================================================================
  // COMMANDS (Writes)
  // ==========================================================================

  /**
   * Submit and wait for transaction - CANONICAL write endpoint
   * POST /v2/commands/submit-and-wait-for-transaction
   * 
   * @param {string} token - Bearer token with actAs rights
   * @param {Object} body - JsSubmitAndWaitForTransactionRequest
   * @returns {Object} JsSubmitAndWaitForTransactionResponse with transaction
   */
  async submitAndWaitForTransaction(token, body, { maxRetries = 3 } = {}) {
    const url = `${this.jsonApiBase}/v2/commands/submit-and-wait-for-transaction`;

    // Extract commandId from nested structure
    const commandId = body.commands?.commandId || body.commandId || 'unknown';
    const actAs = body.commands?.actAs || [];
    console.log(`[CantonService] POST submit-and-wait commandId: ${commandId} actAs: [${actAs.map(p => p.substring(0, 20) + '...').join(', ')}]`);

    let lastError = null;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const res = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${token}`
          },
          body: JSON.stringify(body),
        });

        const text = await res.text();

        if (!res.ok) {
          const error = parseCantonError(text, res.status);

          // Retry on 503 (server overloaded / timeout), 429 (rate limited),
          // and transient NO_SYNCHRONIZER (participant temporarily disconnected).
          // Retry ALL NO_SYNCHRONIZER errors, not just multi-submitter ones,
          // because even single-submitter failures can be transient when the
          // synchronizer connection is flapping.
          // NEVER retry CONTRACT_NOT_FOUND — the contract is archived, retrying won't help
          // and just floods the participant with useless commands.
          const isContractGone = error.code === 'CONTRACT_NOT_FOUND' ||
              (error.message && error.message.includes('could not be found'));
          if (isContractGone) {
            console.warn(`[CantonService] ⚠️ Contract not found (archived) — not retrying`);
            const errMsg = error.code ? `${error.code}: ${error.message}` : error.message;
            const err = new Error(errMsg);
            err.code = error.code;
            err.httpStatus = error.httpStatus;
            throw err;
          }
          const isTransientSync = error.code === 'NO_SYNCHRONIZER_ON_WHICH_ALL_SUBMITTERS_CAN_SUBMIT';
          if ((res.status === 503 || res.status === 429 || isTransientSync) && attempt < maxRetries) {
            const delay = Math.min(2000 * attempt, 8000);
            console.warn(`[CantonService] ⚠️ ${isTransientSync ? 'TRANSIENT_SYNC' : res.status} on attempt ${attempt}/${maxRetries} for ${commandId} — retrying in ${delay}ms...`);
            await new Promise(r => setTimeout(r, delay));
            lastError = error;
            continue;
          }

          const isTransientLockedContract =
            error.code === 'LOCAL_VERDICT_LOCKED_CONTRACTS' &&
            (error.context?.definite_answer === 'false' || error.context?.definite_answer === false);

          if (isTransientLockedContract) {
            // Canton explicitly marks this as an uncertain/transient verdict.
            // Let higher-level reconciliation logic decide final state without
            // polluting error logs with expected transient contention.
            console.warn(`[CantonService] ⚠️ Transient locked-contract verdict (definite_answer=false)`);
          } else {
            console.error(`[CantonService] ❌ Command failed:`, error);
          }
          // Include error CODE in the message so downstream callers (canton-sdk-client,
          // matching-engine) can classify errors by checking error.message.includes(...)
          // without needing to also inspect error.code separately.
          const errMsg = error.code ? `${error.code}: ${error.message}` : error.message;
          const err = new Error(errMsg);
          err.code = error.code;
          err.correlationId = error.correlationId;
          err.traceId = error.traceId;
          err.context = error.context;
          err.httpStatus = error.httpStatus;
          throw err;
        }

        const result = JSON.parse(text);
        if (attempt > 1) {
          console.log(`[CantonService] ✅ Transaction completed on retry ${attempt}: ${result.transaction?.updateId || 'unknown'}`);
        } else {
          console.log(`[CantonService] ✅ Transaction completed: ${result.transaction?.updateId || 'unknown'}`);
        }
        return result;
      } catch (fetchErr) {
        // Network-level errors (ECONNRESET, timeout, etc.) — retry
        if (fetchErr.httpStatus) throw fetchErr; // Already a Canton error, re-throw
        if (attempt < maxRetries) {
          const delay = Math.min(2000 * attempt, 8000);
          console.warn(`[CantonService] ⚠️ Network error on attempt ${attempt}/${maxRetries}: ${fetchErr.message} — retrying in ${delay}ms...`);
          await new Promise(r => setTimeout(r, delay));
          lastError = fetchErr;
          continue;
        }
        throw fetchErr;
      }
    }

    // Should not reach here, but safety net
    throw lastError || new Error('submitAndWaitForTransaction: max retries exhausted');
  }

  /**
   * Create a contract on the ledger
   * Uses POST /v2/commands/submit-and-wait-for-transaction with CreateCommand
   * 
   * Correct JSON Ledger API v2 structure:
   * {
   *   "commands": {
   *     "commandId": "...",
   *     "actAs": ["..."],
   *     "commands": [{
   *       "create": {
   *         "templateId": {packageId, moduleName, entityName},
   *         "createArguments": {...}
   *       }
   *     }]
   *   }
   * }
   */
  async createContract({
    token,
    actAsParty,
    templateId,
    createArguments,
    readAs = [],
    commandId = null,
    synchronizerId = null
  }) {
    const actAs = Array.isArray(actAsParty) ? actAsParty : [actAsParty];

    if (!actAs.length || !actAs[0]) {
      throw new Error("createContract: actAsParty is required");
    }
    if (!templateId) {
      throw new Error("createContract: templateId is required");
    }
    if (!createArguments) {
      throw new Error("createContract: createArguments is required");
    }

    // Convert templateId to string format for legacy single-command mode.
    // In multi-command mode (`commands` provided), template IDs are already embedded.
    const templateIdString = templateId ? templateIdToString(templateId) : null;

    // Resolve synchronizerId — use passed value, or fall back to config default
    const effectiveSyncId = synchronizerId || this.synchronizerId;

    // Build correct v2 API structure with top-level "commands" object
    // CRITICAL: Use "CreateCommand" (capitalized) not "create" per JSON Ledger API v2 spec
    // CRITICAL: domainId is REQUIRED when parties are on multiple synchronizers
    const body = {
      commands: {
        commandId: commandId || `cmd-create-${crypto.randomUUID()}`,
        actAs,
        ...(readAs.length > 0 && { readAs }),
        // domainId tells Canton which synchronizer to use for this transaction
        // Required when parties may be on different domains
        ...(effectiveSyncId && { domainId: effectiveSyncId }),
        commands: [{
          CreateCommand: {
            templateId: templateIdString,
            createArguments
          }
        }]
      }
    };

    return this.submitAndWaitForTransaction(token, body);
  }

  /**
   * Exercise a choice on a contract
   * Uses POST /v2/commands/submit-and-wait-for-transaction with ExerciseCommand
   * 
   * Correct JSON Ledger API v2 structure:
   * {
   *   "commands": {
   *     "commandId": "...",
   *     "actAs": ["..."],
   *     "commands": [{
   *       "exercise": {
   *         "templateId": {packageId, moduleName, entityName},
   *         "contractId": "...",
   *         "choice": "...",
   *         "choiceArgument": {...}
   *       }
   *     }]
   *   }
   * }
   */
  async exerciseChoice({
    token,
    actAsParty,
    templateId,
    contractId,
    choice,
    choiceArgument = {},
    readAs = [],
    commandId = null,
    synchronizerId = null,
    disclosedContracts = null
  }) {
    const actAs = Array.isArray(actAsParty) ? actAsParty : [actAsParty];

    if (!actAs.length || !actAs[0]) {
      throw new Error("exerciseChoice: actAsParty is required");
    }
    if (!templateId) {
      throw new Error("exerciseChoice: templateId is required");
    }
    if (!contractId) {
      throw new Error("exerciseChoice: contractId is required");
    }
    if (!choice) {
      throw new Error("exerciseChoice: choice is required");
    }

    const templateIdString = templateIdToString(templateId);

    // Resolve synchronizerId — use passed value, or fall back to config default
    const effectiveSyncId = synchronizerId || this.synchronizerId;

    // CRITICAL: Every disclosed contract MUST have synchronizerId.
    // Some upstream APIs (e.g., Utilities Backend) return disclosed contracts
    // WITHOUT synchronizerId, causing Canton to reject with:
    //   "Invalid value for: body (Missing required field at 'synchronizerId')"
    // Fix: backfill missing synchronizerId with the command-level synchronizerId.
    let normalizedDisclosed = null;
    if (disclosedContracts && disclosedContracts.length > 0) {
      normalizedDisclosed = disclosedContracts.map(dc => ({
        ...dc,
        synchronizerId: dc.synchronizerId || effectiveSyncId,
      }));
    }

    // Build correct v2 API structure with top-level "commands" object
    // CRITICAL: Use "ExerciseCommand" (capitalized) not "exercise" per JSON Ledger API v2 spec
    const body = {
      commands: {
        commandId: commandId || `cmd-exercise-${crypto.randomUUID()}`,
        actAs,
        ...(readAs.length > 0 && { readAs }),
        // domainId — REQUIRED when using disclosed contracts (e.g., Splice/Utilities transfers)
        ...(effectiveSyncId && { domainId: effectiveSyncId }),
        commands: [{
          ExerciseCommand: {
            templateId: templateIdString,
            contractId,
            choice,
            choiceArgument
          }
        }],
        // Include disclosed contracts if provided (needed for Splice Token Standard transfers)
        ...(normalizedDisclosed && { disclosedContracts: normalizedDisclosed })
      }
    };

    return this.submitAndWaitForTransaction(token, body);
  }

  /**
   * Alias for createContract - maintained for backward compatibility
   * @deprecated Use createContract directly
   */
  async createContractWithTransaction(options) {
    return this.createContract(options);
  }

  // ==========================================================================
  // STATE (Reads)
  // ==========================================================================

  /**
   * Get current ledger end offset
   * GET /v2/state/ledger-end
   */
  async getLedgerEndOffset(token) {
    const url = `${this.jsonApiBase}/v2/state/ledger-end`;

    console.log(`[CantonService] GET ${url}`);

    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`
      }
    });

    if (!res.ok) {
      const error = parseCantonError(await res.text(), res.status);
      console.error(`[CantonService] ❌ Ledger end query failed:`, error);
      throw new Error(error.message);
    }

    const result = await res.json();
    console.log(`[CantonService] ✅ Ledger end offset: ${result.offset}`);
    return result.offset;
  }

  /**
   * Get all packages
   * GET /v2/packages
   */
  async getPackages(token) {
    const url = `${this.jsonApiBase}/v2/packages`;

    console.log(`[CantonService] GET ${url}`);

    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`
      }
    });

    if (!res.ok) {
      const error = parseCantonError(await res.text(), res.status);
      console.error(`[CantonService] ❌ Get packages failed:`, error);
      throw new Error(error.message);
    }

    const result = await res.json();
    // Handle different response formats
    // Canton returns { packageIds: [...] } not a direct array
    let packages = [];
    if (Array.isArray(result)) {
      packages = result;
    } else if (result.packageIds && Array.isArray(result.packageIds)) {
      packages = result.packageIds;
    } else if (result.packages && Array.isArray(result.packages)) {
      packages = result.packages;
    }
    
    console.log(`[CantonService] ✅ Found ${packages.length || 0} packages`);
    return packages;
  }

  /**
   * Lookup a single contract by contract ID
   * POST /v2/contracts/lookup
   */
  async lookupContract(contractId, token) {
    const url = `${this.jsonApiBase}/v2/contracts/lookup`;

    console.log(`[CantonService] Looking up contract: ${contractId?.substring(0, 40)}...`);

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`
      },
      body: JSON.stringify({
        contractId: contractId
      })
    });

    if (!res.ok) {
      const error = parseCantonError(await res.text(), res.status);
      console.error(`[CantonService] ❌ Contract lookup failed:`, error);
      return null;
    }

    const result = await res.json();
    console.log(`[CantonService] ✅ Contract found`);
    
    return {
      contractId: result.contractId || contractId,
      payload: result.payload || result.argument || result.createArgument,
      templateId: result.templateId
    };
  }

  /**
   * Query active contracts — WebSocket PRIMARY
   * 
   * Uses WebSocket ws://.../v2/state/active-contracts as the PRIMARY method
   * (per client requirement). WebSocket streams ALL contracts with no 200-element
   * limit. REST is only used as a last-resort fallback if WebSocket fails.
   * 
   * Auth: subprotocol ['daml.ws.auth'] + Authorization: Bearer header.
   */
  async queryActiveContracts({ party, templateIds = [], interfaceIds = [], activeAtOffset = null, verbose = false, pageSize = 100, pageToken = null }, token) {

    // Get the offset (required for WebSocket filter)
    let effectiveOffset = activeAtOffset;
    if (effectiveOffset === null || effectiveOffset === undefined) {
      try {
        effectiveOffset = await this.getLedgerEndOffset(token);
      } catch (error) {
        console.warn(`[CantonService] Failed to get ledger-end, using 0:`, error.message);
        effectiveOffset = 0;
      }
    }

    // Separate interfaces from templates
    const allIds = [...interfaceIds, ...templateIds];
    const interfaces = allIds.filter(t => typeof t === 'string' && t.startsWith('#'));
    const templates = allIds.filter(t => !(typeof t === 'string' && t.startsWith('#')));

    // Build cumulative filters
    const cumulativeFilters = [];

    if (interfaces.length > 0) {
      cumulativeFilters.push(...interfaces.map(interfaceId => ({
        identifierFilter: {
          InterfaceFilter: {
            value: {
              interfaceId,
              includeCreatedEventBlob: true,
              includeInterfaceView: true
            }
          }
        }
      })));
    }

    if (templates.length > 0) {
      cumulativeFilters.push(...templates.map(t => ({
        identifierFilter: {
          TemplateFilter: {
            value: {
              templateId: typeof t === 'string' ? t : `${t.packageId}:${t.moduleName}:${t.entityName}`,
              includeCreatedEventBlob: false
            }
          }
        }
      })));
    }

    // Default to wildcard if no filters specified
    if (cumulativeFilters.length === 0) {
      cumulativeFilters.push({
        identifierFilter: {
          WildcardFilter: { value: { includeCreatedEventBlob: false } }
        }
      });
    }

    // Build the filter object (party-scoped or any-party)
    const filter = {};
    if (party) {
      filter.filtersByParty = { [party]: { cumulative: cumulativeFilters } };
    } else {
      filter.filtersForAnyParty = { cumulative: cumulativeFilters };
    }

    const templateLabel = templateIds.join(', ') || 'all';
    console.log(`[CantonService] 🔌 WebSocket query — party: ${party || 'any'}, templates: ${templateLabel}`);

    // ─── PRIMARY: WebSocket ──────────────────────────────────────────────
    try {
      const contracts = await this._queryViaWebSocket(filter, effectiveOffset, token);
      console.log(`[CantonService] ✅ WebSocket returned ${contracts.length} contracts`);
      return contracts;
    } catch (wsErr) {
      console.warn(`[CantonService] ⚠️ WebSocket query failed: ${wsErr.message} — falling back to REST`);
    }

    // ─── FALLBACK: REST (only if WebSocket fails) ────────────────────────
    return this._queryViaREST(filter, effectiveOffset, verbose, token, templateLabel);
  }

  /**
   * WebSocket query for active contracts — PRIMARY method.
   * Streams ALL contracts from ws://.../v2/state/active-contracts (no 200 limit).
   */
  async _queryViaWebSocket(filter, offset, token) {
    const WebSocket = require('ws');
    const httpBase = config.canton.jsonApiBase || 'http://localhost:31539';
    const wsBase = httpBase.replace(/^http/, 'ws');
    const url = `${wsBase}/v2/state/active-contracts`;

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url, ['daml.ws.auth'], {
        handshakeTimeout: 15000,
        headers: { 'Authorization': `Bearer ${token}` }
      });

      const contracts = [];
      const timeout = setTimeout(() => {
        ws.close();
        resolve(contracts);
      }, 60000);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          filter,
          verbose: false,
          activeAtOffset: offset
        }));
      });

      ws.on('message', (data) => {
        try {
          const msg = JSON.parse(data.toString());
          if (msg.code && msg.cause) {
            // Canton error message — reject
            clearTimeout(timeout);
            ws.close();
            reject(new Error(msg.cause || msg.code));
            return;
          }
          const normalized = this._normalizeContracts([msg]);
          contracts.push(...normalized.filter(c => c.contractId));
        } catch (_) { /* ignore non-JSON frames */ }
      });

      ws.on('close', () => {
        clearTimeout(timeout);
        resolve(contracts);
      });

      ws.on('error', (err) => {
        clearTimeout(timeout);
        reject(new Error(`WebSocket ACS error: ${err.message}`));
      });
    });
  }

  /**
   * REST fallback for active contract queries — only used when WebSocket fails.
   * POST /v2/state/active-contracts with pagination.
   */
  async _queryViaREST(filter, offset, verbose, token, templateLabel) {
    const url = `${this.jsonApiBase}/v2/state/active-contracts`;
    const allContracts = [];
    let currentPageToken = null;
    let iterations = 0;
    const maxIterations = 20;
    const effectivePageSize = 100;

    console.log(`[CantonService] 📡 REST fallback query — templates: ${templateLabel}`);

    while (iterations < maxIterations) {
      const body = {
        filter,
        verbose: verbose || false,
        activeAtOffset: offset,
        pageSize: effectivePageSize,
      };
      if (currentPageToken) body.pageToken = currentPageToken;

      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`
        },
        body: JSON.stringify(body)
      });

      const text = await res.text();

      if (!res.ok) {
        const error = parseCantonError(text, res.status);
        console.error(`[CantonService] ❌ REST query failed:`, error);
        throw new Error(error.message);
      }

      const result = JSON.parse(text);
      const rawContracts = result.activeContracts || result || [];
      allContracts.push(...this._normalizeContracts(rawContracts));

      currentPageToken = result.nextPageToken || null;
      iterations++;

      if (!currentPageToken) break;
    }

    console.log(`[CantonService] ✅ REST returned ${allContracts.length} contracts`);
    return allContracts;
  }

  /**
   * Normalize Canton JSON API v2 contract format to a simple flat format.
   * Canton wraps contracts in: [{contractEntry: {JsActiveContract: {createdEvent: {...}}}}]
   */
  _normalizeContracts(rawContracts) {
    return (Array.isArray(rawContracts) ? rawContracts : []).map(item => {
      if (item.contractEntry?.JsActiveContract) {
        const activeContract = item.contractEntry.JsActiveContract;
        const createdEvent = activeContract.createdEvent || {};
        return {
          contractId: createdEvent.contractId,
          templateId: createdEvent.templateId,
          payload: createdEvent.createArgument,
          createArgument: createdEvent.createArgument,
          signatories: createdEvent.signatories,
          observers: createdEvent.observers,
          witnessParties: createdEvent.witnessParties,
          offset: createdEvent.offset,
          synchronizerId: activeContract.synchronizerId,
          createdAt: createdEvent.createdAt
        };
      }
      return item;
    });
  }
  
  /**
   * Query active contracts with pagination to handle large result sets
   * NOTE: Canton has a 200 TOTAL element limit before pagination.
   * This method is kept for smaller result sets that need paging.
   */
  async queryActiveContractsPaginated({ party, templateIds = [], activeAtOffset = null, verbose = false }, token) {
    const allContracts = [];
    let pageToken = null;
    const pageSize = 50; // Small page size
    let iterations = 0;
    const maxIterations = 10; // Safety limit
    
    // If activeAtOffset not provided, fetch from ledger-end (required field)
    let effectiveOffset = activeAtOffset;
    if (effectiveOffset === null || effectiveOffset === undefined) {
      try {
        effectiveOffset = await this.getLedgerEndOffset(token);
        console.log(`[CantonService] Paginated query using ledger-end offset: ${effectiveOffset}`);
      } catch (error) {
        console.warn(`[CantonService] Failed to get ledger-end for pagination, using 0:`, error.message);
        effectiveOffset = '0';
      }
    }
    
    // Ensure offset is a string (Canton API requires string)
    if (typeof effectiveOffset === 'number') {
      effectiveOffset = effectiveOffset.toString();
    }
    
    do {
      const url = `${this.jsonApiBase}/v2/state/active-contracts`;
      
      const filter = {};
      if (party) {
        // Separate interfaces from templates
        // Interfaces start with "#" prefix
        const allIds = [...interfaceIds, ...templateIds];
        const interfaces = allIds.filter(t => typeof t === 'string' && t.startsWith('#'));
        const templates = allIds.filter(t => !(typeof t === 'string' && t.startsWith('#')));
        
        console.log(`[CantonService] Separated: ${interfaces.length} interfaces, ${templates.length} templates`);
        if (interfaces.length > 0) {
          console.log(`[CantonService] Interface IDs:`, interfaces);
        }
        
        const filters = [];
        
        // Add interface filters FIRST (per client instructions)
        if (interfaces.length > 0) {
          filters.push(...interfaces.map(interfaceId => {
            console.log(`[CantonService] Adding InterfaceFilter for: ${interfaceId}`);
            return {
              identifierFilter: {
                InterfaceFilter: {
                  value: {
                    interfaceId: interfaceId, // Keep as string with # prefix
                    includeCreatedEventBlob: true,
                    includeInterfaceView: true
                  }
                }
              }
            };
          }));
        }
        
        // Add template filters
        if (templates.length > 0) {
          filters.push(...templates.map(t => {
            const normalized = normalizeTemplateId(t);
            return {
              identifierFilter: {
                TemplateFilter: {
                  value: {
                    templateId: normalized,
                    includeCreatedEventBlob: false
                  }
                }
              }
            };
          }));
        }
        
        filter.filtersByParty = {
          [party]: filters.length > 0 ? {
            cumulative: filters
          } : {
            cumulative: [{
              identifierFilter: {
                WildcardFilter: { value: { includeCreatedEventBlob: false } }
              }
            }]
          }
        };
      } else {
        // Separate interfaces from templates for filtersForAnyParty
        const interfaces = [...interfaceIds, ...templateIds.filter(t => typeof t === 'string' && t.startsWith('#'))];
        const templates = templateIds.filter(t => !(typeof t === 'string' && t.startsWith('#')));
        
        const filters = [];
        
        // Add interface filters
        if (interfaces.length > 0) {
          filters.push(...interfaces.map(interfaceId => ({
            identifierFilter: {
              InterfaceFilter: {
                value: {
                  interfaceId: interfaceId,
                  includeCreatedEventBlob: true,
                  includeInterfaceView: true
                }
              }
            }
          })));
        }
        
        // Add template filters
        if (templates.length > 0) {
          filters.push(...templates.map(t => {
            const normalized = normalizeTemplateId(t);
            return {
            identifierFilter: {
              TemplateFilter: {
                value: {
                    templateId: normalized,
                  includeCreatedEventBlob: false
                }
              }
            }
            };
          }));
        }
        
        filter.filtersForAnyParty = filters.length > 0 ? {
          cumulative: filters
        } : {
          cumulative: [{
            identifierFilter: {
              WildcardFilter: { value: { includeCreatedEventBlob: false } }
            }
          }]
        };
      }

      const body = {
        filter,
        verbose,
        activeAtOffset: effectiveOffset,
        pageSize,
      };
      
      if (pageToken) {
        body.pageToken = pageToken;
      }

      // Log request body for debugging (especially for InterfaceFilter)
      if (templateIds.some(t => typeof t === 'string' && t.startsWith('#')) || interfaceIds.length > 0) {
        console.log(`[CantonService] 🔍 Request body (InterfaceFilter):`);
        console.log(JSON.stringify(body, null, 2));
      }

      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`
        },
        body: JSON.stringify(body)
      });

      let result;
      if (!res.ok) {
        const errorText = await res.text();
        // Check for 200 element limit - try to parse response anyway, might have pageToken
        if (errorText.includes('JSON_API_MAXIMUM_LIST_ELEMENTS_NUMBER_REACHED')) {
          console.log(`[CantonService] ℹ️ 200+ contracts found. Attempting to continue pagination...`);
          try {
            result = JSON.parse(errorText);
            // If we got contracts and a pageToken, continue
            if (result.activeContracts && result.nextPageToken) {
              pageToken = result.nextPageToken;
              // Process the contracts we got
              const rawContracts = result.activeContracts || [];
              const contracts = rawContracts.map(item => {
                if (item.contractEntry?.JsActiveContract) {
                  const activeContract = item.contractEntry.JsActiveContract;
                  const createdEvent = activeContract.createdEvent || {};
                  return {
                    contractId: createdEvent.contractId,
                    templateId: createdEvent.templateId,
                    payload: createdEvent.createArgument,
                    createArgument: createdEvent.createArgument,
                    signatories: createdEvent.signatories,
                    observers: createdEvent.observers,
                    witnessParties: createdEvent.witnessParties,
                    offset: createdEvent.offset,
                    synchronizerId: activeContract.synchronizerId,
                    createdAt: createdEvent.createdAt,
                    createdEvent: createdEvent
                  };
                }
                return item;
              });
              allContracts.push(...contracts);
              iterations++;
              continue; // Continue to next iteration with pageToken
            }
          } catch (parseErr) {
            // Can't parse, return what we have
            console.log(`[CantonService] Cannot parse error response, returning ${allContracts.length} contracts`);
            return allContracts;
          }
        }
        console.error(`[CantonService] Paginated query failed:`, errorText);
        break;
      }

      result = await res.json();
      const rawContracts = result.activeContracts || result || [];
      
      // Normalize Canton JSON API v2 response format (same as regular query)
      const contracts = rawContracts.map(item => {
        if (item.contractEntry?.JsActiveContract) {
          const activeContract = item.contractEntry.JsActiveContract;
          const createdEvent = activeContract.createdEvent || {};
          return {
            contractId: createdEvent.contractId,
            templateId: createdEvent.templateId,
            payload: createdEvent.createArgument, // The actual contract data
            createArgument: createdEvent.createArgument,
            signatories: createdEvent.signatories,
            observers: createdEvent.observers,
            witnessParties: createdEvent.witnessParties,
            offset: createdEvent.offset,
            synchronizerId: activeContract.synchronizerId,
            createdAt: createdEvent.createdAt,
            // Add createdEvent for compatibility
            createdEvent: createdEvent
          };
        }
        // Fallback for other response formats
        return item;
      });
      
      allContracts.push(...contracts);
      
      // Check for next page token
      pageToken = result.nextPageToken || null;
      iterations++;
      
      console.log(`[CantonService] Paginated query: got ${contracts.length} contracts (total: ${allContracts.length}), hasMore: ${!!pageToken}`);
      
    } while (pageToken && iterations < maxIterations);
    
    console.log(`[CantonService] ✅ Paginated query complete: ${allContracts.length} total contracts`);
    
    console.log(`[CantonService] ✅ Paginated query complete: ${allContracts.length} total contracts`);
    return allContracts;
  }

  /**
   * Stream active contracts via WebSocket (recommended for 200+ contracts)
   * Uses ws://host/v2/state/active-contracts WebSocket endpoint
   * 
   * Per Canton team: "You can use either ledger-api or websockets for streaming active contracts"
   */
  async streamActiveContracts({ party, templateIds = [], activeAtOffset = null, verbose = true }, token) {
    return new Promise((resolve, reject) => {
      const WebSocket = require('ws');
      
      // Convert HTTP URL to WebSocket URL
      const wsUrl = this.jsonApiBase.replace(/^http/, 'ws') + '/v2/state/active-contracts';
      
      // Get ledger-end offset if not provided
      let effectiveOffset = activeAtOffset;
      if (!effectiveOffset) {
        this.getLedgerEndOffset(token).then(offset => {
          effectiveOffset = offset;
          startStream();
        }).catch(reject);
      } else {
        startStream();
      }
      
      function startStream() {
        // WebSocket authentication: Use subprotocol jwt.token.{JWT_TOKEN}
        // Per Canton AsyncAPI spec: Sec-WebSocket-Protocol: jwt.token.{token}
        let ws;
        try {
          const wsProtocol = `jwt.token.${token}`;
          ws = new WebSocket(wsUrl, [wsProtocol]);
        } catch (err) {
          // If subprotocol fails, try without (some servers don't require it)
          console.log(`[CantonService] WebSocket subprotocol failed, trying without...`);
          ws = new WebSocket(wsUrl);
        }
        
        const allContracts = [];
        let requestSent = false;
        let timeoutId = null;
        
        // Build filter and request outside handlers so they're accessible everywhere
        const filter = {};
        if (party) {
          filter.filtersByParty = {
            [party]: templateIds.length > 0 ? {
              cumulative: templateIds.map(t => {
                const templateIdStr = typeof t === 'string' ? t : `${t.packageId}:${t.moduleName}:${t.entityName}`;
                // If starts with "#", it's an INTERFACE, use InterfaceFilter
                if (templateIdStr.startsWith("#")) {
                  return {
                    identifierFilter: {
                      InterfaceFilter: {
                        value: {
                          interfaceId: templateIdStr,
                          includeCreatedEventBlob: true,
                          includeInterfaceView: true
                        }
                      }
                    }
                  };
                }
                // Regular template - use TemplateFilter
                return {
                  identifierFilter: {
                    TemplateFilter: {
                      value: {
                        templateId: templateIdStr,
                        includeCreatedEventBlob: false
                      }
                    }
                  }
                };
              })
            } : {
              cumulative: [{
                identifierFilter: {
                  WildcardFilter: { value: { includeCreatedEventBlob: false } }
                }
              }]
            }
          };
        }
        
        const request = {
          filter,
          verbose,
          activeAtOffset: typeof effectiveOffset === 'number' ? effectiveOffset.toString() : effectiveOffset
        };
        
        ws.on('open', () => {
          console.log(`[CantonService] WebSocket connected for active contracts stream`);
          ws.send(JSON.stringify(request));
          requestSent = true;
        });
        
        let messageCount = 0;
        let lastMessageTime = Date.now();
        
        ws.on('message', (data) => {
          try {
            messageCount++;
            lastMessageTime = Date.now();
            const message = JSON.parse(data.toString());
            
            // Handle error response
            if (message.code) {
              console.error(`[CantonService] WebSocket error response:`, message);
              ws.close();
              reject(new Error(message.message || message.cause || 'WebSocket error'));
              return;
            }
            
            // Handle contract entry - check all possible response formats
            const contractEntry = message.contractEntry || message;
            
            if (contractEntry.JsActiveContract) {
              const activeContract = contractEntry.JsActiveContract;
              const createdEvent = activeContract.createdEvent || {};
              allContracts.push({
                contractId: createdEvent.contractId,
                templateId: createdEvent.templateId,
                payload: createdEvent.createArgument,
                createArgument: createdEvent.createArgument,
                signatories: createdEvent.signatories,
                observers: createdEvent.observers,
                witnessParties: createdEvent.witnessParties,
                offset: createdEvent.offset,
                synchronizerId: activeContract.synchronizerId,
                createdAt: createdEvent.createdAt,
                createdEvent: createdEvent
              });
              
              if (messageCount % 50 === 0) {
                console.log(`[CantonService] WebSocket: received ${messageCount} messages, ${allContracts.length} contracts so far`);
              }
            }
            
            // Check for end marker (JsEmpty, JsIncompleteAssigned, JsIncompleteUnassigned)
            if (contractEntry.JsEmpty || contractEntry.JsIncompleteAssigned || contractEntry.JsIncompleteUnassigned) {
              console.log(`[CantonService] ✅ WebSocket stream end marker received: ${allContracts.length} contracts`);
              ws.close();
              resolve(allContracts);
              return;
            }
          } catch (err) {
            console.error(`[CantonService] WebSocket message parse error:`, err.message);
            console.error(`[CantonService] Raw message:`, data.toString().substring(0, 200));
          }
        });
        
        // Timeout after 60 seconds (longer for large streams)
        timeoutId = setTimeout(() => {
          if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CLOSING) {
            console.log(`[CantonService] WebSocket timeout after 60s: ${allContracts.length} contracts received`);
            ws.close();
            if (allContracts.length > 0) {
              resolve(allContracts);
            } else {
              reject(new Error('WebSocket stream timeout - no contracts received'));
            }
          }
        }, 60000);
        
        ws.on('error', (error) => {
          // If subprotocol error, try without subprotocol
          if (error.message && error.message.includes('subprotocol')) {
            console.log(`[CantonService] WebSocket subprotocol error, retrying without subprotocol...`);
            clearTimeout(timeoutId);
            // Retry without subprotocol (might work if server doesn't require it)
            const wsRetry = new WebSocket(wsUrl);
            setupWebSocketHandlers(wsRetry, request, resolve, reject);
            return;
          }
          console.error(`[CantonService] WebSocket connection error:`, error.message);
          clearTimeout(timeoutId);
          reject(error);
        });
        
        // Helper function to setup handlers (for retry)
        function setupWebSocketHandlers(wsInstance, requestObj, resolveFn, rejectFn) {
          const allContractsRetry = [];
          let requestSentRetry = false;
          let timeoutIdRetry = null;
          
          wsInstance.on('open', () => {
            wsInstance.send(JSON.stringify(requestObj));
            requestSentRetry = true;
          });
          
          wsInstance.on('message', (data) => {
            try {
              const message = JSON.parse(data.toString());
              if (message.contractEntry?.JsActiveContract) {
                const activeContract = message.contractEntry.JsActiveContract;
                const createdEvent = activeContract.createdEvent || {};
                allContractsRetry.push({
                  contractId: createdEvent.contractId,
                  templateId: createdEvent.templateId,
                  payload: createdEvent.createArgument,
                  createArgument: createdEvent.createArgument,
                  createdEvent: createdEvent
                });
              }
              if (message.contractEntry?.JsEmpty) {
                wsInstance.close();
                resolveFn(allContractsRetry);
              }
            } catch (err) {
              // Ignore parse errors
            }
          });
          
          wsInstance.on('close', () => {
            if (requestSentRetry && allContractsRetry.length > 0) {
              resolveFn(allContractsRetry);
            }
          });
          
          timeoutIdRetry = setTimeout(() => {
            wsInstance.close();
            if (allContractsRetry.length > 0) {
              resolveFn(allContractsRetry);
            } else {
              rejectFn(new Error('WebSocket timeout'));
            }
          }, 60000);
        }
        
        ws.on('close', (code, reason) => {
          clearTimeout(timeoutId);
          console.log(`[CantonService] WebSocket closed: code=${code}, reason=${reason?.toString() || 'none'}, contracts=${allContracts.length}`);
          if (requestSent) {
            if (allContracts.length > 0) {
              resolve(allContracts);
            } else if (code === 1000) {
              // Normal closure - stream completed (even if empty)
              resolve(allContracts);
            } else {
              reject(new Error(`WebSocket closed unexpectedly: code=${code}`));
            }
          } else {
            reject(new Error('WebSocket closed before request sent'));
          }
        });
      }
    });
  }

  /**
   * Convenience method: query contracts by template
   */
  async queryContracts({ templateId, party }, token) {
    return this.queryActiveContracts({
      party: party || this.operatorPartyId,
      templateIds: [templateId]
    }, token);
  }

  // ==========================================================================
  // SYNCHRONIZERS
  // ==========================================================================

  /**
   * Discover synchronizers
   * GET /v2/synchronizers
   */
  async getSynchronizers(token) {
    const url = `${this.jsonApiBase}/v2/synchronizers`;

    console.log(`[CantonService] GET ${url}`);

    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`
      }
    });

    const text = await res.text();

    if (!res.ok) {
      const error = parseCantonError(text, res.status);
      throw new Error(`Failed to get synchronizers: ${error.message}`);
    }

    const result = JSON.parse(text);
    return result.synchronizers || [];
  }

  /**
   * Discover connected synchronizers from JSON API v2 state endpoint.
   * GET /v2/state/connected-synchronizers
   */
  async getConnectedSynchronizers(token) {
    const url = `${this.jsonApiBase}/v2/state/connected-synchronizers`;
    console.log(`[CantonService] GET ${url}`);

    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`
      }
    });

    const text = await res.text();
    if (!res.ok) {
      const error = parseCantonError(text, res.status);
      throw new Error(`Failed to get connected synchronizers: ${error.message}`);
    }

    const data = JSON.parse(text);
    const candidates = [
      ...(Array.isArray(data?.connectedSynchronizers) ? data.connectedSynchronizers : []),
      ...(Array.isArray(data?.synchronizers) ? data.synchronizers : []),
      ...(Array.isArray(data) ? data : []),
    ];

    return candidates
      .map((s) => s?.synchronizerId || s?.id)
      .filter((id) => typeof id === "string" && id.length > 0);
  }

  /**
   * Resolve the best synchronizer ID for command submission.
   * Prefers a connected configured ID, then a connected global-* ID, then first connected.
   */
  async resolveSubmissionSynchronizerId(token, preferredSynchronizerId = null) {
    const configured = preferredSynchronizerId || this.synchronizerId || null;
    try {
      const connected = [...new Set(await this.getConnectedSynchronizers(token))];
      if (connected.length === 0) {
        return configured;
      }
      if (configured && connected.includes(configured)) {
        return configured;
      }

      const globalSynchronizer = connected.find((id) => id.includes("global-synchronizer::"));
      if (globalSynchronizer) {
        return globalSynchronizer;
      }

      const globalDomain = connected.find((id) => id.includes("global-domain::"));
      if (globalDomain) {
        return globalDomain;
      }

      return connected[0];
    } catch (error) {
      console.warn(`[CantonService] Failed to resolve connected synchronizer, using fallback: ${error.message}`);
      return configured;
    }
  }

  // ==========================================================================
  // PARTIES
  // ==========================================================================

  /**
   * Allocate an external party
   * POST /v2/parties/external/allocate
   */
  async allocateExternalParty({
    partyIdHint,
    annotations = {}
  }, token) {
    const url = `${this.jsonApiBase}/v2/parties/external/allocate`;

    const body = {
      partyIdHint,
      synchronizer: this.synchronizerId,
      localMetadata: {
        resourceVersion: "0",
        annotations: {
          app: "clob-exchange",
          ...annotations
        }
      }
    };

    console.log(`[CantonService] POST ${url}`);
    console.log(`[CantonService] Allocating party: ${partyIdHint}`);

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`
      },
      body: JSON.stringify(body)
    });

    const text = await res.text();

    if (!res.ok) {
      const error = parseCantonError(text, res.status);
      console.error(`[CantonService] ❌ Party allocation failed:`, error);
      throw new Error(`Party allocation failed: ${error.message}`);
    }

    const result = JSON.parse(text);
    console.log(`[CantonService] ✅ Party allocated: ${result.partyDetails?.party}`);

    return result.partyDetails;
  }

  /**
   * List parties
   * GET /v2/parties
   */
  async listParties(token) {
    const url = `${this.jsonApiBase}/v2/parties`;

    console.log(`[CantonService] GET ${url}`);

    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`
      }
    });

    const text = await res.text();

    if (!res.ok) {
      const error = parseCantonError(text, res.status);
      throw new Error(`Failed to list parties: ${error.message}`);
    }

    const result = JSON.parse(text);
    return result.partyDetails || [];
  }

  // ==========================================================================
  // PACKAGES
  // ==========================================================================

  /**
   * List packages
   * GET /v2/packages
   */
  async listPackages(token) {
    const url = `${this.jsonApiBase}/v2/packages`;

    console.log(`[CantonService] GET ${url}`);

    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`
      }
    });

    const text = await res.text();

    if (!res.ok) {
      const error = parseCantonError(text, res.status);
      throw new Error(`Failed to list packages: ${error.message}`);
    }

    const result = JSON.parse(text);
    return result.packageIds || [];
  }

  /**
   * Get package status
   * GET /v2/packages/{packageId}/status
   */
  async getPackageStatus(packageId, token) {
    const url = `${this.jsonApiBase}/v2/packages/${encodeURIComponent(packageId)}/status`;

    console.log(`[CantonService] GET ${url}`);

    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`
      }
    });

    const text = await res.text();

    if (!res.ok) {
      const error = parseCantonError(text, res.status);
      throw new Error(`Failed to get package status: ${error.message}`);
    }

    return JSON.parse(text);
  }

  // ==========================================================================
  // INTERACTIVE SUBMISSION (External signing)
  // ==========================================================================

  /**
   * Prepare interactive submission
   * POST /v2/interactive-submission/prepare
   * 
   * For external parties with Confirmation permission, every transaction must be:
   * 1. Prepared (returns hash for the external party to sign)
   * 2. Signed by the external party's private key (browser-side)
   * 3. Executed with the signature
   * 
   * This replaces submitAndWaitForTransaction when any actAs party is external.
   * 
   * Supports TWO command types:
   *   - ExerciseCommand: { templateId, contractId, choice, choiceArgument }
   *   - CreateCommand:   { templateId, createArguments }
   * 
   * If `createArguments` is provided (and no `contractId`), uses CreateCommand.
   * Otherwise uses ExerciseCommand.
   */
  async prepareInteractiveSubmission({
    token,
    actAsParty,
    commands = null,
    templateId,
    contractId = null,
    choice = null,
    choiceArgument = {},
    createArguments = null,
    readAs = [],
    synchronizerId = null,
    disclosedContracts = null,
    verboseHashing = false
  }) {
    const url = `${this.jsonApiBase}/v2/interactive-submission/prepare`;
    const actAs = Array.isArray(actAsParty) ? actAsParty : [actAsParty];

    // Resolve synchronizerId — use passed value, or fall back to config default
    const effectiveSyncId = synchronizerId || this.synchronizerId;

    // Normalize disclosed contracts (ensure synchronizerId is present)
    let normalizedDisclosed = null;
    if (disclosedContracts && disclosedContracts.length > 0) {
      normalizedDisclosed = disclosedContracts.map(dc => ({
        ...dc,
        synchronizerId: dc.synchronizerId || effectiveSyncId,
      }));
    }

    // Build FLAT request body for /v2/interactive-submission/prepare.
    // Supports either:
    //  - explicit `commands` array (already normalized by caller), or
    //  - legacy single-command inputs (templateId + create/exercise args).
    let commandList = null;
    if (Array.isArray(commands) && commands.length > 0) {
      commandList = commands;
      console.log(`[CantonService] Preparing ${commands.length} command(s) for interactive submission`);
    } else {
      if (!templateId) {
        throw new Error("prepareInteractiveSubmission requires either commands[] or templateId");
      }
      // Convert templateId to string format (required by JSON Ledger API v2)
      const templateIdString = templateIdToString(templateId);
      let command;
      if (createArguments && !contractId) {
        command = {
          CreateCommand: {
            templateId: templateIdString,
            createArguments
          }
        };
        console.log(`[CantonService] Preparing CreateCommand for template: ${templateIdString}`);
      } else {
        command = {
          ExerciseCommand: {
            templateId: templateIdString,
            contractId,
            choice,
            choiceArgument
          }
        };
        console.log(`[CantonService] Preparing ExerciseCommand: ${choice} on ${contractId?.substring(0, 20)}...`);
      }
      commandList = [command];
    }

    const readAsList = Array.isArray(readAs) ? readAs : (readAs ? [readAs] : []);
    const body = {
      commandId: `cmd-prep-${crypto.randomUUID()}`,
      actAs,
      ...(readAsList.length > 0 && { readAs: readAsList }),
      synchronizerId: effectiveSyncId,
      commands: commandList,
      ...(normalizedDisclosed && { disclosedContracts: normalizedDisclosed }),
      packageIdSelectionPreference: [],
      verboseHashing
    };

    console.log(`[CantonService] POST ${url}`);
    console.log(`[CantonService] Prepare request actAs:`, actAs.map(p => p.substring(0, 30) + '...'));
    console.log(`[CantonService] Prepare request body:`, JSON.stringify(body, null, 2));

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`
      },
      body: JSON.stringify(body)
    });

    const text = await res.text();

    if (!res.ok) {
      const error = parseCantonError(text, res.status);
      console.error(`[CantonService] ❌ Prepare failed:`, error);
      const prepErrMsg = error.code ? `Prepare failed: ${error.code}: ${error.message}` : `Prepare failed: ${error.message}`;
      throw new Error(prepErrMsg);
    }

    const result = JSON.parse(text);
    console.log(`[CantonService] ✅ Prepare succeeded, hashToSign length: ${result.preparedTransactionHash?.length || 0}`);
    console.log(`[CantonService] Prepare response keys:`, Object.keys(result));
    if (result.hashingSchemeVersion !== undefined) {
      console.log(`[CantonService] hashingSchemeVersion: ${result.hashingSchemeVersion}`);
    }
    return result;
  }

  /**
   * Execute interactive submission with external party signatures
   * POST /v2/interactive-submission/execute
   * 
   * Uses FLAT request body per Canton OpenAPI spec (JsExecuteSubmissionRequest).
   * Required fields: preparedTransaction, submissionId, hashingSchemeVersion,
   *                  deduplicationPeriod, partySignatures
   * 
   * deduplicationPeriod is a tagged union (oneOf):
   *   { "DeduplicationDuration": { "value": { "seconds": N, "nanos": 0 } } }
   *   { "DeduplicationOffset": { "value": offsetInt } }
   *   { "Empty": {} }
   * 
   * partySignatures is:
   *   { "signatures": [ { "party": "...", "signatures": [ { format, signature, signedBy, signingAlgorithmSpec } ] } ] }
   * 
   * @param {Object} params
   * @param {string} params.preparedTransaction - Base64 opaque blob from prepare step
   * @param {Object} params.partySignatures - { signatures: [{ party, signatures: [{ format, signature, signedBy, signingAlgorithmSpec }] }] }
   * @param {string} params.hashingSchemeVersion - From prepare response (e.g. "HASHING_SCHEME_VERSION_V2")
   * @param {string} params.submissionId - Unique submission ID
   * @param {Object} params.deduplicationPeriod - Tagged union deduplication config
   * @param {string} token - Bearer token
   */
  async executeInteractiveSubmission({
    preparedTransaction,
    partySignatures,
    hashingSchemeVersion = "HASHING_SCHEME_VERSION_V2",
    submissionId = null,
    deduplicationPeriod = null
  }, token) {
    const url = `${this.jsonApiBase}/v2/interactive-submission/execute`;

    // Build FLAT request body per Canton OpenAPI spec (JsExecuteSubmissionRequest)
    // deduplicationPeriod MUST use PascalCase tagged union format per OpenAPI spec:
    //   { "DeduplicationDuration": { "value": { "seconds": N, "nanos": 0 } } }
    const body = {
      preparedTransaction,
      submissionId: submissionId || `submit-exec-${crypto.randomUUID()}`,
      hashingSchemeVersion: String(hashingSchemeVersion),
      deduplicationPeriod: deduplicationPeriod || {
        DeduplicationDuration: {
          value: { seconds: 600, nanos: 0 }   // 10 minutes default
        }
      },
      partySignatures
    };

    console.log(`[CantonService] POST ${url}`);
    console.log(`[CantonService] Execute request body:`, JSON.stringify(body, null, 2));

    const executeOnce = async () => {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`
        },
        body: JSON.stringify(body)
      });
      const text = await res.text();
      return { res, text };
    };

    let attempt = await executeOnce();

    if (!attempt.res.ok) {
      let error = parseCantonError(attempt.text, attempt.res.status);

      // Sequencer timeout can be transient; retry once with same prepared transaction/signature.
      if (error.code === 'NOT_SEQUENCED_TIMEOUT') {
        console.warn('[CantonService] Execute timed out at sequencer. Retrying once...');
        await new Promise((resolve) => setTimeout(resolve, 800));
        attempt = await executeOnce();
        if (!attempt.res.ok) {
          error = parseCantonError(attempt.text, attempt.res.status);
          console.error(`[CantonService] ❌ Execute retry failed:`, error);
          const retryErrMsg = error.code ? `Execute failed: ${error.code}: ${error.message}` : `Execute failed: ${error.message}`;
          throw new Error(retryErrMsg);
        }
      } else {
        console.error(`[CantonService] ❌ Execute failed:`, error);
        const execErrMsg = error.code ? `Execute failed: ${error.code}: ${error.message}` : `Execute failed: ${error.message}`;
        throw new Error(execErrMsg);
      }
    }

    const result = JSON.parse(attempt.text);
    console.log(`[CantonService] ✅ Execute succeeded, updateId: ${result.transaction?.updateId || 'unknown'}`);
    return result;
  }

  // ==========================================================================
  // UPDATES (for streaming)
  // ==========================================================================

  /**
   * Get WebSocket URL for updates streaming
   */
  getUpdatesWebSocketUrl() {
    return this.jsonApiBase
      .replace('http://', 'ws://')
      .replace('https://', 'wss://') + '/v2/updates';
  }

  /**
   * Get WebSocket URL for active contracts streaming
   */
  getActiveContractsWebSocketUrl() {
    return this.jsonApiBase
      .replace('http://', 'ws://')
      .replace('https://', 'wss://') + '/v2/state/active-contracts';
  }

  // ==========================================================================
  // UTILITIES
  // ==========================================================================

  /**
   * Extract contract ID from transaction response
   */
  extractContractId(transactionResponse) {
    const events = transactionResponse?.transaction?.events || [];
    for (const event of events) {
      const created = event.created || event.CreatedEvent;
      if (created?.contractId) {
        return created.contractId;
      }
    }
    return null;
  }

  /**
   * Extract update ID from transaction response
   */
  extractUpdateId(transactionResponse) {
    return transactionResponse?.transaction?.updateId || null;
  }

  /**
   * Get user rights via JSON Ledger API v2
   * GET /v2/users/{user-id}/rights
   * 
   * @param {string} token - Bearer token
   * @param {string} userId - User ID (from JWT 'sub' claim)
   * @returns {Object} User rights including canActAs and canReadAs
   */
  async getUserRights(token, userId) {
    const url = `${this.jsonApiBase}/v2/users/${encodeURIComponent(userId)}/rights`;
    
    console.log(`[CantonService] GET ${url}`);
    
    const res = await fetch(url, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${token}`
      }
    });

    if (!res.ok) {
      const text = await res.text();
      const error = parseCantonError(text, res.status);
      console.error(`[CantonService] ❌ Failed to get user rights:`, error);
      throw new Error(`Failed to get user rights: ${error.message}`);
    }

    const result = await res.json();
    console.log(`[CantonService] ✅ User rights retrieved:`, JSON.stringify(result, null, 2));
    return result;
  }

  /**
   * Grant user rights via JSON Ledger API v2
   * POST /v2/users/{user-id}/rights
   * 
   * CRITICAL: Canton JSON Ledger API v2 requires rights in 'kind' wrapper format:
   * {
   *   "rights": [
   *     { "kind": { "CanActAs": { "value": { "party": "..." } } } },
   *     { "kind": { "CanReadAs": { "value": { "party": "..." } } } }
   *   ]
   * }
   * 
   * @param {string} token - Bearer token
   * @param {string} userId - User ID (from JWT 'sub' claim)
   * @param {Array<string>} partyIds - Array of party IDs to grant rights for
   * @param {string} identityProviderId - Identity provider ID (empty string for default IDP)
   * @returns {Object} Grant result
   */
  async grantUserRights(token, userId, partyIds, identityProviderId = "") {
    const url = `${this.jsonApiBase}/v2/users/${encodeURIComponent(userId)}/rights`;
    
    // Build rights array with CORRECT 'kind' wrapper format
    // Canton JSON Ledger API v2 requires: { kind: { CanActAs: { value: { party } } } }
    const rights = [];
    for (const partyId of partyIds) {
      rights.push({ 
        kind: { 
          CanActAs: { 
            value: { party: partyId } 
          } 
        } 
      });
      rights.push({ 
        kind: { 
          CanReadAs: { 
            value: { party: partyId } 
          } 
        } 
      });
    }
    
    // CRITICAL: Canton JSON API v2 requires userId AND identityProviderId in the body
    const body = { 
      userId, 
      identityProviderId, // Required field - use discovered IDP or empty string for default
      rights 
    };
    
    console.log(`[CantonService] POST ${url}`);
    console.log(`[CantonService] Granting rights for user ${userId}, parties:`, partyIds);
    console.log(`[CantonService] Request body:`, JSON.stringify(body, null, 2));
    
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`
      },
      body: JSON.stringify(body)
    });

    if (!res.ok) {
      const text = await res.text();
      const error = parseCantonError(text, res.status);
      console.error(`[CantonService] ❌ Failed to grant user rights:`, error);
      throw new Error(`Failed to grant user rights: ${error.message}`);
    }

    const result = await res.json();
    console.log(`[CantonService] ✅ User rights granted:`, JSON.stringify(result, null, 2));
    return result;
  }

  /**
   * Parse user rights response from Canton JSON Ledger API v2
   * Handles the 'kind' wrapper format and normalizes to simple format
   * 
   * Input format from API:
   * { "rights": [{ "kind": { "CanActAs": { "value": { "party": "..." } } } }] }
   * 
   * Output format (normalized):
   * { "canActAs": ["party1", "party2"], "canReadAs": ["party1", "party3"] }
   */
  parseUserRights(rightsResponse) {
    const result = {
      canActAs: [],
      canReadAs: []
    };

    const rights = rightsResponse.rights || [];
    for (const right of rights) {
      // Handle 'kind' wrapper format (Canton JSON Ledger API v2)
      if (right.kind) {
        if (right.kind.CanActAs?.value?.party) {
          result.canActAs.push(right.kind.CanActAs.value.party);
        }
        if (right.kind.CanReadAs?.value?.party) {
          result.canReadAs.push(right.kind.CanReadAs.value.party);
        }
      }
      // Also handle direct format for backwards compatibility
      if (right.canActAs?.party) {
        result.canActAs.push(right.canActAs.party);
      }
      if (right.canReadAs?.party) {
        result.canReadAs.push(right.canReadAs.party);
      }
      // Handle can_act_as format (snake_case variant)
      if (right.can_act_as?.party) {
        result.canActAs.push(right.can_act_as.party);
      }
      if (right.can_read_as?.party) {
        result.canReadAs.push(right.can_read_as.party);
      }
    }

    return result;
  }
}

const cantonServiceInstance = new CantonService();
module.exports = cantonServiceInstance;
module.exports.CantonService = CantonService;
module.exports.decodeTokenPayload = decodeTokenPayload;
module.exports.parseUserRights = cantonServiceInstance.parseUserRights.bind(cantonServiceInstance);