/**
 * Database Service — Prisma Client Singleton
 * 
 * Single Prisma client instance shared across the application.
 * Connects to Neon PostgreSQL for persistent storage of:
 * - Users & signing keys
 * - Wallet info
 * - Order reservations
 * - Stop-loss orders
 * - Auth sessions, challenges, refresh tokens
 * - Quota counterss
 */

const { PrismaClient } = require('@prisma/client');

let prisma;

function getDb() {
  if (!prisma) {
    prisma = new PrismaClient({
      log: process.env.NODE_ENV === 'development'
        ? ['warn', 'error']
        : ['error'],
    });
    console.log('[DB] 🗄️  Prisma client initialized (Neon PostgreSQL)');
  }
  return prisma;
}

/**
 * Graceful shutdown — close DB connections
 */
async function disconnectDb() {
  if (prisma) {
    await prisma.$disconnect();
    console.log('[DB] Disconnected from PostgreSQL');
  }
}

// Handle process exit
process.on('beforeExit', async () => {
  await disconnectDb();
});

module.exports = { getDb, disconnectDb };
