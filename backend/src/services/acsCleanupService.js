/**
 * ACS Cleanup Service — Archives completed contracts to keep Active Contract Set lean
 * 
 * Keeps only useful contracts in active set; rest are archived
 * 
 * This service periodically scans for contracts that are no longer needed in the ACS:
 *   - FILLED orders → exercise ArchiveOrder
 *   - CANCELLED orders → exercise ArchiveOrder
 *   - EXECUTED allocations → exercise ArchiveAllocation
 *   - CANCELLED allocations → exercise ArchiveAllocation
 *   - Old trade records → exercise ArchiveTrade
 * 
 * All archive choices are consuming — the contract is removed from the ACS
 * on exercise, freeing space and improving query performance.
 * 
 * Safety:
 *   - Only contracts older than TRADE_RETENTION_MS are archived
 *   - Runs at CLEANUP_INTERVAL_MS intervals (default: 5 minutes)
 *   - Non-critical: if archival fails, the contract stays in ACS (no data loss)
 * 
 * IMPORTANT — Package Version:
 *   All contracts now use TOKEN_STANDARD_PACKAGE_ID (v2.4.0 = 0224efbf...).
 *   Archive choices (ArchiveOrder, ArchiveAllocation, ArchiveTrade)
 *   are available on this package.
 */

const config = require('../config');
const cantonService = require('./cantonService');
const tokenProvider = require('./tokenProvider');
const { getDb } = require('./db');
const { getCantonSDKClient } = require('./canton-sdk-client');
const { TOKEN_STANDARD_PACKAGE_ID } = require('../config/constants');

// ─── Configuration ───────────────────────────────────────────────────────────
const CLEANUP_INTERVAL_MS = parseInt(process.env.ACS_CLEANUP_INTERVAL_MS) || 5 * 60 * 1000; // 5 minutes
const TRADE_RETENTION_MS = parseInt(process.env.TRADE_RETENTION_MS) || 24 * 60 * 60 * 1000;  // 24 hours
const BATCH_SIZE = 20; // Archive N contracts per cycle to avoid overwhelming Canton

class ACSCleanupService {
  constructor() {
    this.isRunning = false;
    this.timer = null;
    this.stats = {
      ordersArchived: 0,
      tradesArchived: 0,
      allocationsArchived: 0,
      lastRunAt: null,
      lastRunDuration: 0,
      errors: 0,
    };
  }

  async start() {
    if (this.isRunning) return;
    this.isRunning = true;
    console.log(`[ACSCleanup] Started (interval: ${CLEANUP_INTERVAL_MS / 1000}s, trade retention: ${TRADE_RETENTION_MS / 3600000}h)`);
    
    // Run first cycle after a short delay (let other services init)
    this.timer = setTimeout(() => this._loop(), 30000);
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    console.log('[ACSCleanup] Stopped');
  }

  async _loop() {
    while (this.isRunning) {
      try {
        await this._runCleanupCycle();
      } catch (err) {
        console.error(`[ACSCleanup] Cycle error: ${err.message}`);
        this.stats.errors++;
      }
      // Wait for next cycle
      await new Promise(r => {
        this.timer = setTimeout(r, CLEANUP_INTERVAL_MS);
      });
    }
  }

  async _runCleanupCycle() {
    const start = Date.now();
    this.stats.lastRunAt = new Date().toISOString();
    
    const token = await tokenProvider.getServiceToken();
    const packageId = config.canton.packageIds?.clobExchange;
    const operatorPartyId = config.canton.operatorPartyId;

    if (!packageId || !operatorPartyId) return;

    let archivedThisCycle = 0;

    // ═══ 1. Archive FILLED and CANCELLED orders ═══
    try {
      const orders = await cantonService.queryActiveContracts({
        party: operatorPartyId,
        templateIds: [`${packageId}:Order:Order`],
        pageSize: 200,
      }, token);

      const archivable = (Array.isArray(orders) ? orders : []).filter(c => {
        const payload = c.payload || c.createArgument || {};
        if (payload.status !== 'FILLED' && payload.status !== 'CANCELLED') return false;
        // Skip contracts from incompatible packages
        if (!this._isCompatiblePackage(c.templateId)) {
          return false;
        }
        return true;
      });

      if (archivable.length > 0) {
        const batch = archivable.slice(0, BATCH_SIZE);
        for (const contract of batch) {
          try {
            // Use current package template ID for archive choice
            await cantonService.exerciseChoice({
              token,
              actAsParty: [operatorPartyId],
              templateId: `${packageId}:Order:Order`,
              contractId: contract.contractId,
              choice: 'ArchiveOrder',
              choiceArgument: {},
              readAs: [operatorPartyId],
            });
            this.stats.ordersArchived++;
            archivedThisCycle++;
          } catch (err) {
            // Contract may already be archived or have a version mismatch — skip
            if (!err.message?.includes('CONTRACT_NOT_FOUND')) {
              console.warn(`[ACSCleanup] Failed to archive order ${contract.contractId?.substring(0, 20)}...: ${err.message}`);
            }
          }
        }
        if (batch.length > 0) {
          console.log(`[ACSCleanup] Archived ${batch.length} completed orders (${archivable.length} total eligible)`);
        }
      }
    } catch (err) {
      console.warn(`[ACSCleanup] Order scan failed: ${err.message}`);
    }

    // ═══ 2. Archive EXECUTED and CANCELLED AllocationRecords ═══
    try {
      const allocations = await cantonService.queryActiveContracts({
        party: operatorPartyId,
        templateIds: [`${packageId}:Settlement:AllocationRecord`],
        pageSize: 200,
      }, token);

      const archivable = (Array.isArray(allocations) ? allocations : []).filter(c => {
        const payload = c.payload || c.createArgument || {};
        if (payload.status !== 'EXECUTED' && payload.status !== 'CANCELLED') return false;
        if (!this._isCompatiblePackage(c.templateId)) {
          return false;
        }
        return true;
      });

      if (archivable.length > 0) {
        const batch = archivable.slice(0, BATCH_SIZE);
        for (const contract of batch) {
          try {
            await cantonService.exerciseChoice({
              token,
              actAsParty: [operatorPartyId],
              templateId: `${packageId}:Settlement:AllocationRecord`,
              contractId: contract.contractId,
              choice: 'ArchiveAllocation',
              choiceArgument: {},
              readAs: [operatorPartyId],
            });
            this.stats.allocationsArchived++;
            archivedThisCycle++;
          } catch (err) {
            if (!err.message?.includes('CONTRACT_NOT_FOUND')) {
              console.warn(`[ACSCleanup] Failed to archive allocation: ${err.message}`);
            }
          }
        }
        if (batch.length > 0) {
          console.log(`[ACSCleanup] Archived ${batch.length} completed allocations (${archivable.length} total eligible)`);
        }
      }
    } catch (err) {
      console.warn(`[ACSCleanup] Allocation scan failed: ${err.message}`);
    }

    // ═══ 3. Archive old trade records (after persisting to cache) ═══
    try {
      const trades = await cantonService.queryActiveContracts({
        party: operatorPartyId,
        templateIds: [`${packageId}:Settlement:Trade`],
        pageSize: 200,
      }, token);

      const now = Date.now();
      const archivable = (Array.isArray(trades) ? trades : []).filter(c => {
        const payload = c.payload || c.createArgument || {};
        const tradeTime = new Date(payload.timestamp || 0).getTime();
        if ((now - tradeTime) <= TRADE_RETENTION_MS) return false;
        // Skip contracts from incompatible packages
        if (!this._isCompatiblePackage(c.templateId)) {
          return false;
        }
        return true;
      });

      if (archivable.length > 0) {
        const batch = archivable.slice(0, BATCH_SIZE);
        for (const contract of batch) {
          try {
            // CRITICAL: Always use CURRENT package template ID (has ArchiveTrade choice)
            await cantonService.exerciseChoice({
              token,
              actAsParty: [operatorPartyId],
              templateId: `${packageId}:Settlement:Trade`,
              contractId: contract.contractId,
              choice: 'ArchiveTrade',
              choiceArgument: {},
              readAs: [operatorPartyId],
            });
            this.stats.tradesArchived++;
            archivedThisCycle++;
          } catch (err) {
            if (!err.message?.includes('CONTRACT_NOT_FOUND')) {
              console.warn(`[ACSCleanup] Failed to archive trade: ${err.message}`);
            }
          }
        }
        if (batch.length > 0) {
          console.log(`[ACSCleanup] Archived ${batch.length} old trades (${archivable.length} total eligible)`);
        }
      }
    } catch (err) {
      console.warn(`[ACSCleanup] Trade scan failed: ${err.message}`);
    }

    this.stats.lastRunDuration = Date.now() - start;

    if (archivedThisCycle > 0) {
      console.log(`[ACSCleanup] Cycle complete: ${archivedThisCycle} contracts archived in ${this.stats.lastRunDuration}ms`);
    }
  }

  /**
   * Check if a contract's templateId is from a compatible package version.
   * All contracts should now be on TOKEN_STANDARD_PACKAGE_ID (v2.4.0 = 0224efbf...).
   * 
   * @param {string|Object} templateId - The contract's templateId from Canton
   * @returns {boolean} true if the contract can be archived
   */
  _isCompatiblePackage(templateId) {
    if (!templateId) return true;

    let contractPackageId;
    if (typeof templateId === 'string') {
      contractPackageId = templateId.split(':')[0];
    } else if (templateId.packageId) {
      contractPackageId = templateId.packageId;
    }

    if (!contractPackageId) return true;

    if (contractPackageId !== TOKEN_STANDARD_PACKAGE_ID) {
      if (!this._warnedPackages) this._warnedPackages = new Set();
      if (!this._warnedPackages.has(contractPackageId)) {
        this._warnedPackages.add(contractPackageId);
        console.warn(
          `[ACSCleanup] Skipping contracts from package ${contractPackageId.substring(0, 12)}... ` +
          `(expected ${TOKEN_STANDARD_PACKAGE_ID.substring(0, 12)}...)`
        );
      }
      return false;
    }

    return true;
  }

  getStats() {
    return { ...this.stats };
  }
}

// ─── Singleton ──────────────────────────────────────────────────────────────
let instance = null;

function getACSCleanupService() {
  if (!instance) {
    instance = new ACSCleanupService();
  }
  return instance;
}

module.exports = { ACSCleanupService, getACSCleanupService };
