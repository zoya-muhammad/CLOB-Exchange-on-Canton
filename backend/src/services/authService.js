/**
 * Auth Service - App-Level Sessions
 * PostgreSQL via Prisma (Neon) — ALL reads/writes go directly to DB.
 * No in-memory cache.
 * 
 * End-users authenticate with cryptographic signatures, NOT Keycloak.
 * Backend issues app-level JWTs for session management.
 * 
 * Flow:
 * 1. POST /v1/auth/challenge - Get nonce to sign
 * 2. POST /v1/auth/unlock - Verify signature, issue app JWT
 */

const config = require('../config');
const walletService = require('./walletService');
const crypto = require('crypto');
const { ValidationError, NotFoundError } = require('../utils/ledgerError');
const { getDb } = require('./db');

class AuthService {
  constructor() {
    this.jwtSecret = process.env.APP_JWT_SECRET || crypto.randomBytes(64).toString('hex');
    
    // Cleanup intervals (DB only)
    setInterval(() => this.cleanupChallenges().catch(() => {}), 5 * 60 * 1000);
    setInterval(() => this.cleanupSessions().catch(() => {}), 60 * 60 * 1000);
  }

  /**
   * Generate authentication challenge for wallet
   */
  async generateChallenge(walletId) {
    if (!walletId) {
      throw new ValidationError('walletId is required');
    }

    const partyInfo = await walletService.getPartyInfo(walletId);
    if (!partyInfo) {
      throw new NotFoundError('Wallet', walletId);
    }

    const nonce = crypto.randomBytes(32).toString('hex');
    const expiresAt = Date.now() + (5 * 60 * 1000);

    const db = getDb();
    await db.authChallenge.create({
      data: {
        nonce,
      walletId,
        expiresAt: new Date(expiresAt),
      },
    });

    console.log(`[AuthService] Generated challenge for ${walletId}: ${nonce.slice(0, 16)}...`);

    return {
      nonce,
      expiresAt: new Date(expiresAt).toISOString(),
      walletId
    };
  }

  /**
   * Verify signature and issue app session token
   */
  async unlockWallet({ walletId, nonce, signatureBase64 }) {
    if (!walletId || !nonce || !signatureBase64) {
      throw new ValidationError('walletId, nonce, and signatureBase64 are required');
    }

    const db = getDb();
    const challenge = await db.authChallenge.findUnique({ where: { nonce } });

    if (!challenge) {
      throw new ValidationError('Invalid or expired challenge');
    }
    if (challenge.walletId !== walletId) {
      throw new ValidationError('Challenge walletId mismatch');
    }
    if (Date.now() > challenge.expiresAt.getTime()) {
      await db.authChallenge.delete({ where: { nonce } }).catch(() => {});
      throw new ValidationError('Challenge expired');
    }

    try {
      const partyInfo = await walletService.getPartyInfo(walletId);
      if (!partyInfo) {
        throw new NotFoundError('Wallet', walletId);
      }

      const publicKeyBase64 = partyInfo.publicKeyBase64Der;
      
      if (!publicKeyBase64) {
        throw new Error('Public key not found for wallet. Wallet may not be fully onboarded.');
      }

      const isValid = walletService.verifySignature(
        nonce,
        signatureBase64,
        publicKeyBase64
      );

      if (!isValid) {
        throw new ValidationError('Invalid signature');
      }

      // Clean up challenge from DB (one-time use)
      await db.authChallenge.delete({ where: { nonce } }).catch(() => {});

      // Issue app JWT
      const sessionId = crypto.randomUUID();
      const expiresAt = Date.now() + (24 * 60 * 60 * 1000);

      // Persist session to DB
      await db.session.create({
        data: {
          id: sessionId,
        walletId,
          expiresAt: new Date(expiresAt),
        },
      });

      const appToken = this.generateAppJWT({
        sessionId,
        walletId,
        sub: walletId,
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(expiresAt / 1000)
      });

      console.log(`[AuthService] ✅ Wallet unlocked: ${walletId}`);

      return {
        sessionToken: appToken,
        walletId,
        expiresAt: new Date(expiresAt).toISOString()
      };

    } catch (error) {
      console.error(`[AuthService] ❌ Wallet unlock failed:`, error.message);
      throw error;
    }
  }

  /**
   * Verify app session token.
   * JWT signature + expiry is the source of truth.
   * No in-memory session cache — we trust the JWT itself.
   */
  verifySessionToken(token) {
    try {
      const parts = token.split('.');
      if (parts.length !== 3) {
        return null;
      }

      const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString());
      const signature = parts[2];

      const expectedSignature = crypto
        .createHmac('sha256', this.jwtSecret)
        .update(`${parts[0]}.${parts[1]}`)
        .digest('base64url');

      if (signature !== expectedSignature) {
        return null;
      }

      if (payload.exp && Date.now() > payload.exp * 1000) {
        return null;
      }

      return {
        walletId: payload.walletId,
        sessionId: payload.sessionId
      };

    } catch (error) {
      console.error('[AuthService] Token verification failed:', error);
      return null;
    }
  }

  /**
   * Generate app JWT
   */
  generateAppJWT(payload) {
    const header = {
      alg: 'HS256',
      typ: 'JWT'
    };

    const encodedHeader = Buffer.from(JSON.stringify(header)).toString('base64url');
    const encodedPayload = Buffer.from(JSON.stringify(payload)).toString('base64url');
    
    const signature = crypto
      .createHmac('sha256', this.jwtSecret)
      .update(`${encodedHeader}.${encodedPayload}`)
      .digest('base64url');

    return `${encodedHeader}.${encodedPayload}.${signature}`;
  }

  /**
   * Invalidate session
   */
  async invalidateSession(sessionId) {
    const db = getDb();
    await db.session.delete({ where: { id: sessionId } }).catch(() => {});
  }

  /**
   * Cleanup expired challenges (DB only)
   */
  async cleanupChallenges() {
    const db = getDb();
    const result = await db.authChallenge.deleteMany({
      where: { expiresAt: { lt: new Date() } },
    });
    if (result.count > 0) {
      console.log(`[AuthService] Cleaned up ${result.count} expired challenge(s)`);
    }
  }

  /**
   * Cleanup expired sessions (DB only)
   */
  async cleanupSessions() {
    const db = getDb();
    const result = await db.session.deleteMany({
      where: { expiresAt: { lt: new Date() } },
    });
    if (result.count > 0) {
      console.log(`[AuthService] Cleaned up ${result.count} expired session(s)`);
    }
  }
}

module.exports = new AuthService();
