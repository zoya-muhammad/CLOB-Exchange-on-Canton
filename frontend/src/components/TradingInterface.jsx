import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import React from 'react';
import { motion } from 'framer-motion';
import { AlertCircle } from 'lucide-react';
import { useConfirmationModal } from './ConfirmationModal';
import { useToast, OrderSuccessModal } from './ui/toast';

// Import trading components
import OrderForm from './trading/OrderForm';
import OrderBookCard from './trading/OrderBookCard';
import ActiveOrdersTable from './trading/ActiveOrdersTable';
import DepthChart from './trading/DepthChart';
import PriceChart from './trading/PriceChart';
import RecentTrades from './trading/RecentTrades';
import GlobalTrades from './trading/GlobalTrades';
import TransactionHistory from './trading/TransactionHistory';
import PortfolioView from './trading/PortfolioView';
import MarketData from './trading/MarketData';
import TransferOffers from './trading/TransferOffers';
import BalanceCard from './trading/BalanceCard';

// Import skeleton components
import OrderBookSkeleton from './trading/OrderBookSkeleton';
import TradingPageSkeleton from './trading/TradingPageSkeleton';

// Import services
import { getAvailableTradingPairs, getGlobalOrderBook } from '../services/cantonApi';
import { apiClient, API_ROUTES } from '../config/config';
import websocketService from '../services/websocketService';
// Token Standard V2 services
import * as balanceService from '../services/balanceService';
import * as orderService from '../services/orderService';
// Wallet signing (for external party interactive submission)
import { loadWallet, decryptPrivateKey, signMessage, bytesToBase64 } from '../wallet/keyManager';

export default function TradingInterface({ partyId }) {
  // === PHASE 1: ALL HOOKS MUST BE DECLARED FIRST - NO EXCEPTIONS ===
  const toast = useToast();
  const [hasError, setHasError] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');
  const [isAlive, setIsAlive] = useState(true);
  const heartbeatRef = useRef(Date.now());
  
  const [tradingPair, setTradingPair] = useState('BTC/USDT');
  // Trading pairs loaded from API - no hardcoding
  const [availablePairs, setAvailablePairs] = useState([]);
  const [orderType, setOrderType] = useState('BUY');
  const [orderMode, setOrderMode] = useState('LIMIT');
  const [price, setPrice] = useState('');
  const [quantity, setQuantity] = useState('');
  // Dynamic balance objects - available and locked, populated from API (includes CBTC)
  const [balance, setBalance] = useState({});         // Available balance
  const [lockedBalance, setLockedBalance] = useState({}); // Locked in open orders
  const [balanceLoading, setBalanceLoading] = useState(false);
  const isMintingRef = useRef(false);
  const lastMintAtRef = useRef(0);
  const hasLoadedBalanceRef = useRef(false);
  const [orders, setOrders] = useState([]);
  const [orderBook, setOrderBook] = useState({ buys: [], sells: [] });
  const [loading, setLoading] = useState(false);
  const [orderPlacing, setOrderPlacing] = useState(false); // Separate state for order placement
  const [orderBookLoading, setOrderBookLoading] = useState(true);
  const [orderBookExists, setOrderBookExists] = useState(true);
  const [error, setError] = useState('');
  const [creatingOrderBook, setCreatingOrderBook] = useState(false);
  const [trades, setTrades] = useState([]);
  const [tradesLoading, setTradesLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('trading');
  const [showOrderSuccess, setShowOrderSuccess] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [lastOrderData, setLastOrderData] = useState(null);
  const { showModal, ModalComponent, isOpenRef: modalIsOpenRef } = useConfirmationModal();
  const isLoadingRef = useRef(false);

  // Minting state
  const [mintingLoading, setMintingLoading] = useState(false);

  // Interactive signing state (for external party order placement/cancellation)
  const [signingState, setSigningState] = useState(null);
  // { action: 'PLACE'|'CANCEL', preparedTransaction, preparedTransactionHash, hashingSchemeVersion, orderMeta/cancelMeta }
  const [walletPassword, setWalletPassword] = useState('');
  const [signingError, setSigningError] = useState(null);

  // === PHASE 3: ALL USECALLBACK HOOKS - NO CONDITIONALS ===
  // TOKEN STANDARD V2: Mint creates transfer offers/holdings through backend mint API
  const handleMintTokens = useCallback(async (tokensToMint = null) => {
    console.log('[Mint V2] Manual mint button clicked for party:', partyId);
    
    if (mintingLoading || isMintingRef.current) {
      console.log('[Mint V2] Already minting, skipping...');
      return;
    }
    
    isMintingRef.current = true;
    setMintingLoading(true);
    
    try {
      // TOKEN STANDARD V2: Create Holding contracts (real tokens, not text balances)
      const mintPayload = Array.isArray(tokensToMint) && tokensToMint.length > 0
        ? tokensToMint
        : [
        { symbol: 'BTC', amount: 10 },
        { symbol: 'USDT', amount: 100000 },
        { symbol: 'ETH', amount: 100 },
        { symbol: 'SOL', amount: 1000 },
        { symbol: 'CC', amount: 50 },
        { symbol: 'CBTC', amount: 5 }
          ];

      // Force V2 mint endpoint so UI matches the exact manual API flow:
      // POST /api/balance/v2/mint
      const mintResult = await balanceService.mintTokens(partyId, mintPayload, true);
      
      if (mintResult.success) {
        console.log('[Mint V2] Holdings created:', mintResult);
        const mintedSummary = mintPayload
          .map((t) => `${t.symbol}: ${t.amount}`)
          .join(' | ');

        toast.success('Mint request submitted successfully', {
          title: '🪙 Holdings Created',
          details: mintedSummary
        });
        
        // Refresh balance from V2 Holdings (includes CBTC)
        setTimeout(async () => {
          try {
            const balanceData = await balanceService.getBalances(partyId);
            if (balanceData.available) {
              // Dynamic balance - show ALL tokens from API (including CBTC)
              const dynamicBalance = {};
              Object.keys(balanceData.available).forEach(token => {
                dynamicBalance[token] = balanceData.available[token]?.toString() || '0.0';
              });
              setBalance(dynamicBalance);
              console.log('[Mint V2] Balance refreshed from Holdings:', balanceData.available);
            }
          } catch (err) {
            console.error('[Mint V2] Failed to refresh balance:', err);
          }
        }, 1000);
        
        return;
      }
      
      throw new Error(mintResult.error || 'Failed to mint tokens');
      
    } catch (err) {
      console.error('[Mint V2] Failed:', err);
      toast.error(err.message || 'Failed to mint tokens. Please complete onboarding first.', {
        title: '❌ Mint Failed'
      });
    } finally {
      isMintingRef.current = false;
      setMintingLoading(false);
    }
  }, [partyId, toast, mintingLoading]);

  // ═══ SHARED REFRESH FUNCTION ═══
  // Refreshes order book, user orders, balance, and trades in one call
  const refreshAllData = useCallback(async (pair) => {
    const activePair = pair || tradingPair;
    console.log('[Refresh] Refreshing all data for', activePair);
    
    // Refresh order book
    try {
      const bookData = await getGlobalOrderBook(activePair);
      if (bookData) {
        setOrderBook({
          buys: bookData.buyOrders || [],
          sells: bookData.sellOrders || []
        });
      }
    } catch (e) { console.warn('[Refresh] Order book error:', e.message); }

    // Refresh user orders - ALWAYS use fresh data
    try {
      const ordersData = await apiClient.get(API_ROUTES.ORDERS.GET_USER(partyId, 'OPEN'));
      const ordersList = ordersData?.data?.orders || [];
      setOrders(ordersList.map(order => ({
        id: order.orderId || order.contractId,
        contractId: order.contractId,
        type: order.orderType,
        mode: order.orderMode,
        price: order.price,
        quantity: order.quantity,
        filled: order.filled || '0',
        remaining: order.remaining,
        status: order.status,
        tradingPair: order.tradingPair,
        timestamp: order.timestamp,
        stopPrice: order.stopPrice || null,
        triggeredAt: order.triggeredAt || null,
      })));
    } catch (e) { console.warn('[Refresh] User orders error:', e.message); }

    // Refresh balance from V2 Holdings (available + locked)
    try {
      const balanceData = await balanceService.getBalances(partyId);
      if (balanceData.available) {
        const dynamicBalance = {};
        Object.keys(balanceData.available).forEach(token => {
          dynamicBalance[token] = balanceData.available[token]?.toString() || '0.0';
        });
        setBalance(dynamicBalance);
      }
      if (balanceData.locked) {
        const dynamicLocked = {};
        Object.keys(balanceData.locked).forEach(token => {
          dynamicLocked[token] = balanceData.locked[token]?.toString() || '0.0';
        });
        setLockedBalance(dynamicLocked);
      }
    } catch (e) { console.warn('[Refresh] Balance error:', e.message); }

    // Refresh trades — ALWAYS use fresh data from API (even if empty, to clear stale trades)
    try {
      const tradesData = await apiClient.get(API_ROUTES.TRADES.GET(activePair, 50));
      const tradesList = tradesData?.data?.trades || [];
      // Deduplicate by tradeId
      const seen = new Set();
      const uniqueTrades = tradesList.filter(t => {
        const key = t.tradeId || `${t.price}-${t.quantity}-${t.timestamp}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
      setTrades(uniqueTrades.slice(0, 50));
    } catch (e) { console.warn('[Refresh] Trades error:', e.message); }
  }, [partyId, tradingPair]);

  // Place order - uses legacy API but balances are V2 Holdings
  const handlePlaceOrder = useCallback(async (orderData) => {
    try {
      setOrderPlacing(true);
      console.log('[Place Order] Placing order:', orderData);
      
      // Use legacy order API (OrderV3 DAML encoding needs more work)
      const result = await apiClient.post(API_ROUTES.ORDERS.PLACE, {
          tradingPair: orderData.tradingPair,
          orderType: orderData.orderType,
          orderMode: orderData.orderMode,
          price: orderData.price,
          quantity: orderData.quantity,
          partyId: partyId,
          stopPrice: orderData.stopPrice || null,
      }, {
        headers: {
          'x-user-id': partyId || 'anonymous'
        }
      });
      
      if (!result.success) {
        throw new Error(result.error || result.message || 'Failed to place order');
      }

      const data = result.data;

      // Check if external party requires interactive signing
      if (data?.requiresSignature) {
        console.log('[Place Order] External party — interactive signing required');
        setSigningState({
          action: 'PLACE',
          preparedTransaction: data.preparedTransaction,
          preparedTransactionHash: data.preparedTransactionHash,
          hashingSchemeVersion: data.hashingSchemeVersion,
          orderMeta: {
            orderId: data.orderId,
            tradingPair: data.tradingPair,
            orderType: data.orderType,
            orderMode: data.orderMode,
            price: data.price,
            quantity: data.quantity,
            stopPrice: data.stopPrice,
            lockInfo: data.lockInfo,
            stage: data.stage || 'ALLOCATION_PREPARED',
            allocationType: data.allocationType || null,
          },
          orderData, // original form data for success toast
        });
        setOrderPlacing(false);
        return;
      }

      console.log('[Place Order] Order placed:', result);
      
      // Clear form fields
      setPrice('');
      setQuantity('');
      
      // Show success toast
      const priceLabel = orderData.orderMode === 'MARKET' ? 'Market Price' 
        : orderData.orderMode === 'STOP_LOSS' ? `Stop @ ${orderData.stopPrice}` 
        : orderData.price;
      toast.success(
        `${orderData.orderType} ${orderData.quantity} ${orderData.tradingPair?.split('/')[0] || ''} @ ${priceLabel}`, 
        {
          title: orderData.orderMode === 'STOP_LOSS' ? `🛡️ Stop-Loss ${orderData.orderType} Order Set` : `✅ ${orderData.orderType} Order Placed`,
          duration: 5000
        }
      );
      
      // Show success modal
      setLastOrderData({
        orderId: data?.orderId || result.data?.orderId,
        orderType: orderData.orderType,
        orderMode: orderData.orderMode,
        tradingPair: orderData.tradingPair,
        price: orderData.price,
        quantity: orderData.quantity,
        stopPrice: orderData.stopPrice || null,
      });
      setShowOrderSuccess(true);
      
      // Refresh data after order placement
      setTimeout(() => refreshAllData(orderData.tradingPair || tradingPair), 1000);
      setTimeout(() => refreshAllData(orderData.tradingPair || tradingPair), 3000);

      // Trigger matching engine so allocations get executed promptly
      setTimeout(() => {
        apiClient.post('/match/trigger', { pair: orderData.tradingPair || tradingPair }).catch(() => {});
      }, 4000);
      
    } catch (error) {
      console.error('[Place Order] Failed:', error);
      toast.error(error.message || 'Failed to place order', {
        title: '❌ Order Failed',
        duration: 6000
      });
    } finally {
      setOrderPlacing(false);
    }
  }, [partyId, tradingPair, toast]);

  // Cancel order - uses legacy API but balances are V2 Holdings
  const handleCancelOrder = useCallback(async (contractId) => {
    if (!contractId) {
      console.error('[Cancel Order] No contractId provided');
      toast.error('Cannot cancel order: missing contract ID');
      throw new Error('Missing contract ID');
    }
    
    if (!partyId) {
      console.error('[Cancel Order] No partyId available');
      toast.error('Cannot cancel order: not logged in');
      throw new Error('Not logged in');
    }
    
    console.log('[Cancel Order] Cancelling order:', contractId);
    
    // Use legacy cancel API (POST, not DELETE)
    const result = await apiClient.post(API_ROUTES.ORDERS.CANCEL_BY_ID(contractId), {
      partyId: partyId
    }, {
      headers: {
        'x-user-id': partyId
      }
    });
    
    if (!result.success) {
      const errorMsg = result.error || 'Failed to cancel order';
      console.error('[Cancel Order] Failed:', errorMsg);
      toast.error(errorMsg);
      throw new Error(errorMsg);
    }

    const data = result.data;

    // Check if external party requires interactive signing
    if (data?.requiresSignature) {
      console.log('[Cancel Order] External party — interactive signing required for cancel');
      setSigningState({
        action: 'CANCEL',
        preparedTransaction: data.preparedTransaction,
        preparedTransactionHash: data.preparedTransactionHash,
        hashingSchemeVersion: data.hashingSchemeVersion,
        cancelMeta: {
          orderContractId: data.orderContractId || contractId,
          orderId: data.orderId,
          tradingPair: data.tradingPair,
          orderDetails: data.orderDetails,
        },
      });
      return;
    }
    
    console.log('[Cancel Order] Order cancelled:', result);
    toast.success('Order cancelled successfully! Funds unlocked.');
    
    // Wait for Canton to propagate the cancellation
    await new Promise(r => setTimeout(r, 800));
    
    // Refresh orders list - ALWAYS use fresh data
    try {
      const ordersData = await apiClient.get(API_ROUTES.ORDERS.GET_USER(partyId, 'OPEN'));
          const ordersList = ordersData?.data?.orders || [];
          setOrders(ordersList.map(order => ({
            id: order.orderId || order.contractId,
            contractId: order.contractId,
            type: order.orderType,
            mode: order.orderMode,
            price: order.price,
            quantity: order.quantity,
            filled: order.filled || '0',
            remaining: order.remaining,
            status: order.status,
            tradingPair: order.tradingPair,
            timestamp: order.timestamp,
            stopPrice: order.stopPrice || null,
            triggeredAt: order.triggeredAt || null,
          })));
    } catch (e) {
      console.warn('[Cancel Order] Failed to refresh orders:', e);
    }
    
    // Refresh order book (cancelled order should be removed)
    try {
      const bookData = await getGlobalOrderBook(tradingPair);
      if (bookData) {
        setOrderBook({
          buys: bookData.buyOrders || [],
          sells: bookData.sellOrders || []
        });
      }
    } catch (e) {
      console.warn('[Cancel Order] Failed to refresh order book:', e);
    }
    
    // Refresh balance from V2 Holdings (includes CBTC)
    try {
      const balanceData = await balanceService.getBalances(partyId);
      if (balanceData.available) {
        // Dynamic balance - show ALL tokens from API
        const dynamicBalance = {};
        Object.keys(balanceData.available).forEach(token => {
          dynamicBalance[token] = balanceData.available[token]?.toString() || '0.0';
        });
        setBalance(dynamicBalance);
      }
    } catch (e) {
      console.warn('[Cancel Order] Failed to refresh balance:', e);
    }
    
    return result;
  }, [partyId, tradingPair, toast, setOrders, setBalance]);

  // ═══ INTERACTIVE SIGNING: Sign and execute order (place or cancel) ═══
  const handleSignAndExecuteOrder = useCallback(async () => {
    if (!signingState || !walletPassword) return;
    
    const { action, preparedTransaction, preparedTransactionHash, hashingSchemeVersion } = signingState;
    setSigningError(null);
    setOrderPlacing(true);
    
    try {
      // 1. Load wallet and decrypt private key
      const wallet = loadWallet();
      if (!wallet) throw new Error('Wallet not found. Please create/import wallet again.');
      
      const privateKey = await decryptPrivateKey(wallet.encryptedPrivateKey, walletPassword);
      console.log(`[SignOrder] Wallet unlocked, signing ${action} transaction hash...`);

      // 1b. Immediately sync signing key to backend for server-side settlement.
      //     This ensures the matching engine can use interactive settlement (Strategy 2)
      //     even if the user hasn't placed a new order since their last page refresh.
      const privateKeyBase64ForSync = bytesToBase64(privateKey);
      const fingerprint = localStorage.getItem('canton_key_fingerprint')
        || (partyId && partyId.includes('::') ? partyId.split('::')[1] : null);
      if (fingerprint && partyId) {
        // Fire-and-forget: don't block order signing on key sync
        apiClient.post('/onboarding/store-signing-key', {
          partyId,
          signingKeyBase64: privateKeyBase64ForSync,
          publicKeyFingerprint: fingerprint,
        }).then(() => {
          console.log(`[SignOrder] 🔑 Signing key synced to backend for settlement`);
          // Cache in sessionStorage for rehydration on page refresh
          try { sessionStorage.setItem('canton_signing_key_b64', privateKeyBase64ForSync); } catch (_) {}
        }).catch((syncErr) => {
          console.warn(`[SignOrder] ⚠️ Signing key sync failed (non-fatal):`, syncErr?.message || syncErr);
        });
      }
      
      // 2. Sign the prepared transaction hash
      const signatureBase64 = await signMessage(privateKey, preparedTransactionHash);
      console.log(`[SignOrder] Transaction signed, executing ${action}...`);
      
      // 3. Get public key fingerprint
      const signedBy = fingerprint;
      if (!signedBy) {
        throw new Error('Public key fingerprint not found. Please re-onboard your wallet.');
      }
      
      let response;
      
      if (action === 'PLACE') {
        // 4a. Execute order placement (include signing key for server-side settlement)
        const privateKeyBase64 = bytesToBase64(privateKey);
        response = await apiClient.post('/orders/execute-place', {
          preparedTransaction,
          partyId,
          signatureBase64,
          signedBy,
          hashingSchemeVersion,
          orderMeta: signingState.orderMeta,
          signingKeyBase64: privateKeyBase64,
        });

        if (response?.data?.requiresSignature) {
          // Step 2: Order Create prepared — auto-sign with the same private key
          // (still in memory) so the user does NOT need to enter password again.
          console.log('[SignOrder] Step 1 done. Auto-signing step 2 (order create)...');
          const sig2 = await signMessage(privateKey, response.data.preparedTransactionHash);
          const response2 = await apiClient.post('/orders/execute-place', {
            preparedTransaction: response.data.preparedTransaction,
            partyId,
            signatureBase64: sig2,
            signedBy,
            hashingSchemeVersion: response.data.hashingSchemeVersion,
            orderMeta: response.data.orderMeta || signingState.orderMeta,
            signingKeyBase64: privateKeyBase64,
          });
          // Use the second response as the final response
          response = response2;
        }

        if (response.success) {
          const od = signingState.orderData;
          setPrice('');
          setQuantity('');
          
          const priceLabel = od.orderMode === 'MARKET' ? 'Market Price'
            : od.orderMode === 'STOP_LOSS' ? `Stop @ ${od.stopPrice}`
            : od.price;
          toast.success(
            `${od.orderType} ${od.quantity} ${od.tradingPair?.split('/')[0] || ''} @ ${priceLabel}`,
            {
              title: od.orderMode === 'STOP_LOSS' ? '🛡️ Stop-Loss Order Set' : `✅ ${od.orderType} Order Placed`,
              duration: 5000
            }
          );
          
          setLastOrderData({
            orderId: response.data?.orderId,
            orderType: od.orderType,
            orderMode: od.orderMode,
            tradingPair: od.tradingPair,
            price: od.price,
            quantity: od.quantity,
            stopPrice: od.stopPrice || null,
          });
          setShowOrderSuccess(true);
          
          setTimeout(() => refreshAllData(od.tradingPair || tradingPair), 1000);
          setTimeout(() => refreshAllData(od.tradingPair || tradingPair), 3000);

          // Trigger matching engine so allocations get executed promptly
          setTimeout(() => {
            apiClient.post('/match/trigger', { pair: od.tradingPair || tradingPair }).catch(() => {});
          }, 4000);
        } else {
          throw new Error(response.error || 'Failed to execute order placement');
        }
        
      } else if (action === 'CANCEL') {
        // 4b. Execute order cancellation
        response = await apiClient.post('/orders/execute-cancel', {
          preparedTransaction,
          partyId,
          signatureBase64,
          signedBy,
          hashingSchemeVersion,
          cancelMeta: signingState.cancelMeta,
        });
        
        if (response.success) {
          toast.success('Order cancelled successfully! Funds unlocked.');
          
          // Refresh orders and balance
          setTimeout(async () => {
            try {
              const ordersData = await apiClient.get(API_ROUTES.ORDERS.GET_USER(partyId, 'OPEN'));
              const ordersList = ordersData?.data?.orders || [];
              setOrders(ordersList.map(order => ({
                id: order.orderId || order.contractId,
                contractId: order.contractId,
                type: order.orderType,
                mode: order.orderMode,
                price: order.price,
                quantity: order.quantity,
                filled: order.filled || '0',
                remaining: order.remaining,
                status: order.status,
                tradingPair: order.tradingPair,
                timestamp: order.timestamp,
              })));
            } catch (e) {
              console.warn('[SignOrder] Failed to refresh orders:', e);
            }
          }, 800);
          
          refreshAllData(tradingPair);
        } else {
          throw new Error(response.error || 'Failed to execute order cancellation');
        }
      }
      
      // Clear signing state
      setSigningState(null);
      setWalletPassword('');
      
    } catch (err) {
      console.error(`[SignOrder] ${action} sign & execute error:`, err);
      if (err.message.includes('decrypt') || err.message.includes('password')) {
        setSigningError('Incorrect wallet password. Please try again.');
      } else {
        setSigningError(err.message?.substring(0, 150));
      }
    } finally {
      setOrderPlacing(false);
    }
  }, [signingState, walletPassword, partyId, tradingPair, toast, setOrders]);

  const handleCancelSigning = useCallback(() => {
    setSigningState(null);
    setWalletPassword('');
    setSigningError(null);
  }, []);

  // Handle when a transfer offer is accepted - refresh balances
  const handleTransferAccepted = useCallback(async (offer) => {
    console.log('[TradingInterface] Transfer accepted:', offer);
    toast.success(`Received ${offer.amount} ${offer.token}!`);
    
    // Refresh balance after accepting transfer
    try {
      const balanceData = await balanceService.getBalances(partyId);
      if (balanceData.available) {
        const dynamicBalance = {};
        Object.keys(balanceData.available).forEach(token => {
          dynamicBalance[token] = balanceData.available[token]?.toString() || '0.0';
        });
        setBalance(dynamicBalance);
        console.log('[TradingInterface] Balance refreshed after transfer:', dynamicBalance);
      }
    } catch (e) {
      console.warn('[TradingInterface] Failed to refresh balance after transfer:', e);
    }
  }, [partyId, toast, setBalance]);


  // === PHASE 2: ALL USEEFFECT HOOKS - NO CONDITIONALS ===
  useEffect(() => {
    const handleError = (event) => {
      console.error('[TradingInterface] Global error caught:', event.error);
      setErrorMessage(`${event.error?.message || 'Unknown error'} (${event.filename}:${event.lineno}:${event.colno})`);
      setHasError(true);
    };

    const handleUnhandledRejection = (event) => {
      console.error('[TradingInterface] Unhandled promise rejection:', event.reason);
      setErrorMessage(`Promise rejection: ${event.reason?.message || event.reason}`);
      setHasError(true);
    };

    window.addEventListener('error', handleError);
    window.addEventListener('unhandledrejection', handleUnhandledRejection);

    return () => {
      window.removeEventListener('error', handleError);
      window.removeEventListener('unhandledrejection', handleUnhandledRejection);
    };
  }, []);

  useEffect(() => {
    // Heartbeat checker — log-only, NEVER crash the UI
    // The main thread can be blocked during heavy Canton API calls,
    // WebSocket reconnections, or browser tab throttling. 
    // Crashing the UI is worse than a brief stall.
    const heartbeatInterval = setInterval(() => {
      const now = Date.now();
      const timeSinceLastHeartbeat = now - heartbeatRef.current;
      
      // Only log a warning; never crash the UI
      if (timeSinceLastHeartbeat > 60000) {
        console.warn('[TradingInterface] Heartbeat stale for', timeSinceLastHeartbeat, 'ms — background tab or heavy load');
        // Do NOT setHasError — that crashes the entire interface
        // The polling intervals will catch up when the tab becomes active
      }
    }, 10000); // Check every 10 seconds

    return () => clearInterval(heartbeatInterval);
  }, []);

  useEffect(() => {
    // Robust heartbeat updater using recursive setTimeout
    // This is more reliable than setInterval when the main thread is busy
    let timeoutId;
    
    const updateHeartbeat = () => {
      heartbeatRef.current = Date.now();
      setIsAlive(true);
      // Use recursive setTimeout instead of setInterval for better reliability
      // This ensures the next update is scheduled after the current one completes
      timeoutId = setTimeout(updateHeartbeat, 2000);
    };
    
    // Initial update
    updateHeartbeat();

    return () => {
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, []);

  useEffect(() => {
    if (!partyId) return;

    const initializeData = async () => {
      try {
        console.log('[TradingInterface] Initializing with partyId:', partyId);
        console.log('[TradingInterface] Loading available trading pairs...');
        
        const pairs = await getAvailableTradingPairs(partyId);
        console.log('[TradingInterface] Found pairs:', pairs);
        
        setAvailablePairs(pairs.length > 0 ? pairs : ['BTC/USDT']);
        console.log('[TradingInterface] Data initialization completed');
        // Set initial loading to false after a short delay to ensure all components render
        setTimeout(() => setInitialLoading(false), 500);
      } catch (error) {
        console.error('[TradingInterface] Failed to initialize data:', error);
        setError('Failed to initialize trading interface');
        setInitialLoading(false);
      }
    };

    initializeData();
  }, [partyId]);

  useEffect(() => {
    if (!partyId) return;

    // ═══════════════════════════════════════════════════════════════════
    // PURE WEBSOCKET architecture — no polling fallback
    // ═══════════════════════════════════════════════════════════════════
    //   All data streamed in real-time from backend streaming read model.
    //   Initial load via REST, then WebSocket pushes all updates.
    //   Client requirement: remove polling completely.
    // ═══════════════════════════════════════════════════════════════════

    // ── Connect WebSocket (auto-reconnects with exponential backoff) ──
    if (!websocketService.isConnected()) {
      websocketService.connect();
    }

    // ── WebSocket handlers for real-time push ──
    const onOrderBookUpdate = (data) => {
      if (data?.type === 'ORDER_CREATED' && data.order) {
        setOrderBook(prev => {
          const order = data.order;
          if (order.orderType === 'BUY') {
            const updated = [...prev.buys, order].sort((a, b) => parseFloat(b.price || 0) - parseFloat(a.price || 0));
            return { ...prev, buys: updated };
          } else if (order.orderType === 'SELL') {
            const updated = [...prev.sells, order].sort((a, b) => parseFloat(a.price || 0) - parseFloat(b.price || 0));
            return { ...prev, sells: updated };
          }
          return prev;
        });
      } else if (data?.type === 'ORDER_ARCHIVED' && data.contractId) {
        setOrderBook(prev => ({
          buys: prev.buys.filter(o => o.contractId !== data.contractId),
          sells: prev.sells.filter(o => o.contractId !== data.contractId),
        }));
      } else if (data?.type === 'TRADE_EXECUTED') {
        // A trade was executed — trigger a full order book refresh
        getGlobalOrderBook(tradingPair).then(bookData => {
          if (bookData) {
            setOrderBook({ buys: bookData.buyOrders || [], sells: bookData.sellOrders || [] });
          }
        }).catch(() => {});
      } else if (data?.type === 'FULL_ORDERBOOK' && data.buys && data.sells) {
        // Full order book snapshot from backend
        setOrderBook({ buys: data.buys, sells: data.sells });
      }
    };

    const onTradeUpdate = (data) => {
      if (data?.type === 'NEW_TRADE') {
        setTrades(prev => {
          const newTrade = {
            tradeId: data.tradeId,
            tradingPair: data.tradingPair,
            price: data.price,
            quantity: data.quantity || data.baseAmount,
            buyer: data.buyer,
            seller: data.seller,
            timestamp: data.timestamp,
          };
          // Prepend (newest first) and deduplicate
          const updated = [newTrade, ...prev];
        const seen = new Set();
          return updated.filter(t => {
          const key = t.tradeId || `${t.price}-${t.quantity}-${t.timestamp}`;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
          }).slice(0, 50);
        });
      }
    };

    // Subscribe to channels for current trading pair
    websocketService.subscribe(`orderbook:${tradingPair}`, onOrderBookUpdate);
    websocketService.subscribe(`trades:${tradingPair}`, onTradeUpdate);
    websocketService.subscribe('trades:all', onTradeUpdate);

    // ── Initial load: order book (one-time REST, then WebSocket takes over) ──
    const loadInitialOrderBook = async () => {
        try {
          const bookData = await getGlobalOrderBook(tradingPair);
        if (bookData) {
          setOrderBook({
            buys: bookData.buyOrders || [],
            sells: bookData.sellOrders || []
          });
        }
        } catch (error) {
        console.error('[TradingInterface] Failed to load initial order book:', error);
      } finally {
        setOrderBookLoading(false);
      }
    };

    // ── Initial load: trades (one-time REST, then WebSocket takes over) ──
    const loadInitialTrades = async () => {
      try {
        const tradesData = await apiClient.get(API_ROUTES.TRADES.GET(tradingPair, 50));
        const tradesList = tradesData?.data?.trades || [];
        const seen = new Set();
        const uniqueTrades = tradesList.filter(t => {
          const key = t.tradeId || `${t.price}-${t.quantity}-${t.timestamp}`;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
        setTrades(uniqueTrades.slice(0, 50));
      } catch (error) {
        console.error('[TradingInterface] Failed to load initial trades:', error);
      } finally {
        setTradesLoading(false);
      }
    };
    
    loadInitialOrderBook();
    loadInitialTrades();

    // No polling — all subsequent updates come via WebSocket

    return () => {
      websocketService.unsubscribe(`orderbook:${tradingPair}`, onOrderBookUpdate);
      websocketService.unsubscribe(`trades:${tradingPair}`, onTradeUpdate);
      websocketService.unsubscribe('trades:all', onTradeUpdate);
    };
  }, [partyId, tradingPair]);

  // Load user's active orders + subscribe to WebSocket for updates
  useEffect(() => {
    if (!partyId) return;

    const loadUserOrders = async () => {
      try {
        // Fetch OPEN orders only - filled/cancelled orders should not appear
        const ordersData = await apiClient.get(API_ROUTES.ORDERS.GET_USER(partyId, 'OPEN'));
        const ordersList = ordersData?.data?.orders || [];
        
          const formattedOrders = ordersList.map(order => ({
            id: order.orderId || order.contractId,
            contractId: order.contractId,
            type: order.orderType,
            mode: order.orderMode,
            price: order.price,
            quantity: order.quantity,
            filled: order.filled || '0',
            remaining: order.remaining,
            status: order.status,
            tradingPair: order.tradingPair,
            timestamp: order.timestamp,
            stopPrice: order.stopPrice || null,
            triggeredAt: order.triggeredAt || null,
          }));
          
          setOrders(formattedOrders);
          console.log('[TradingInterface] User orders loaded:', formattedOrders.length);
      } catch (error) {
        console.error('[TradingInterface] Failed to load user orders:', error);
      }
    };

    // Initial load (one-time REST)
    loadUserOrders();

    // WebSocket handler for user order updates
    const onUserOrderUpdate = (data) => {
      if (data?.type === 'ORDER_CREATED' && data.order) {
        setOrders(prev => {
          // Add new order, deduplicate by contractId
          const exists = prev.some(o => o.contractId === data.order.contractId);
          if (exists) return prev;
          return [...prev, {
            id: data.order.orderId || data.order.contractId,
            contractId: data.order.contractId,
            type: data.order.orderType,
            mode: data.order.orderMode,
            price: data.order.price,
            quantity: data.order.quantity,
            filled: data.order.filled || '0',
            remaining: data.order.remaining,
            status: data.order.status,
            tradingPair: data.order.tradingPair,
            timestamp: data.order.timestamp,
            stopPrice: data.order.stopPrice || null,
            triggeredAt: data.order.triggeredAt || null,
          }];
        });
      } else if (data?.type === 'ORDER_FILLED' || data?.type === 'ORDER_CANCELLED' || data?.type === 'ORDER_ARCHIVED') {
        // Remove filled/cancelled/archived orders from active list
        const cid = data.contractId || data.order?.contractId;
        if (cid) {
          setOrders(prev => prev.filter(o => o.contractId !== cid));
        }
      } else if (data?.type === 'ORDERS_SNAPSHOT' && Array.isArray(data.orders)) {
        // Full snapshot pushed by backend
        setOrders(data.orders.map(order => ({
          id: order.orderId || order.contractId,
          contractId: order.contractId,
          type: order.orderType,
          mode: order.orderMode,
          price: order.price,
          quantity: order.quantity,
          filled: order.filled || '0',
          remaining: order.remaining,
          status: order.status,
          tradingPair: order.tradingPair,
          timestamp: order.timestamp,
          stopPrice: order.stopPrice || null,
          triggeredAt: order.triggeredAt || null,
        })));
      }
    };

    websocketService.subscribe(`orders:${partyId}`, onUserOrderUpdate);

    // No polling — all subsequent updates come via WebSocket
    
    return () => {
      websocketService.unsubscribe(`orders:${partyId}`, onUserOrderUpdate);
    };
  }, [partyId]);

  // Load balance from Transfer Registry (CC/CBTC) + Holdings (others) - useCallback so it can be called from child components
  const loadBalance = useCallback(async (showLoader = false) => {
    if (!partyId) return;

    try {
      if (showLoader) setBalanceLoading(true);
      
      const balanceData = await balanceService.getBalances(partyId);
      
      if (balanceData.available && Object.keys(balanceData.available).length > 0) {
        const dynamicBalance = {};
        Object.keys(balanceData.available).forEach(token => {
          dynamicBalance[token] = balanceData.available[token]?.toString() || '0.0';
        });
        setBalance(dynamicBalance);
        
        // Also capture locked balance
        if (balanceData.locked && Object.keys(balanceData.locked).length > 0) {
          const dynamicLocked = {};
          Object.keys(balanceData.locked).forEach(token => {
            dynamicLocked[token] = balanceData.locked[token]?.toString() || '0.0';
          });
          setLockedBalance(dynamicLocked);
        } else {
          setLockedBalance({});
        }
        
              hasLoadedBalanceRef.current = true;
              return;
            }
      
      // No Holdings found
      setBalance({});
      setLockedBalance({});
          hasLoadedBalanceRef.current = true;
      } catch (error) {
      console.error('[Balance V2] Failed to load balance:', error);
      setBalance({});
      setLockedBalance({});
        hasLoadedBalanceRef.current = true;
      } finally {
        setBalanceLoading(false);
      }
  }, [partyId]);

  // Load balance on mount + subscribe to WebSocket for updates (no polling)
  useEffect(() => {
    if (!partyId) return;

    // Add keyboard shortcut for minting (Ctrl+M)
    const handleKeyPress = (event) => {
      if (event.ctrlKey && event.key === 'm') {
        event.preventDefault();
        console.log('[Mint] Keyboard shortcut triggered (Ctrl+M)');
        handleMintTokens();
      }
    };

    // Initial load (one-time REST)
    loadBalance(true);

    // WebSocket handler for balance updates
    const onBalanceUpdate = (data) => {
      if (data?.type === 'BALANCE_UPDATE') {
        if (data.balances && Object.keys(data.balances).length > 0) {
          const dynamicBalance = {};
          Object.keys(data.balances).forEach(token => {
            dynamicBalance[token] = data.balances[token]?.toString() || '0.0';
          });
          setBalance(dynamicBalance);
        }
        if (data.lockedBalances && Object.keys(data.lockedBalances).length > 0) {
          const dynamicLocked = {};
          Object.keys(data.lockedBalances).forEach(token => {
            dynamicLocked[token] = data.lockedBalances[token]?.toString() || '0.0';
          });
          setLockedBalance(dynamicLocked);
        }
      }
    };

    websocketService.subscribe(`balance:${partyId}`, onBalanceUpdate);
    window.addEventListener('keydown', handleKeyPress);

    // No polling — all subsequent updates come via WebSocket

    return () => {
      websocketService.unsubscribe(`balance:${partyId}`, onBalanceUpdate);
      window.removeEventListener('keydown', handleKeyPress);
    };
  }, [partyId, loadBalance]);

  // === PHASE 4: ALL USEMEMO HOOKS - NO CONDITIONALS ===
  const memoizedModal = useMemo(() => <ModalComponent />, [ModalComponent]);

  // === PHASE 5: CONDITIONAL LOGIC - AFTER ALL HOOKS ===
  // Early returns based on state
  if (hasError) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-[#0B0E11]">
        <div className="text-center space-y-4 p-8">
          <AlertCircle className="w-16 h-16 text-red-500 mx-auto" />
          <h3 className="text-2xl font-bold text-white">Trading Interface Error</h3>
          <p className="text-gray-300 max-w-md">{errorMessage}</p>
          <div className="space-y-2">
            <button
              onClick={() => {
                setHasError(false);
                setErrorMessage('');
                window.location.reload();
              }}
              className="px-6 py-3 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
            >
              Reload Page
            </button>
            <button
              onClick={() => window.location.href = '/'}
              className="block mx-auto px-6 py-3 bg-gray-600 text-white rounded-md hover:bg-gray-700 transition-colors"
            >
              Go to Home
            </button>
          </div>
          <details className="text-left text-gray-400 text-sm">
            <summary>Technical Details</summary>
            <pre className="mt-2 p-2 bg-gray-800 rounded overflow-auto">
              {errorMessage}
            </pre>
          </details>
        </div>
      </div>
    );
  }

  if (!partyId) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="text-center">
          <AlertCircle className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <p className="text-muted-foreground">Party ID is required</p>
        </div>
      </div>
    );
  }

  // REMOVED: Full-screen error display - errors are now shown as toasts
  // Non-blocking error display is handled in the main render via motion.div

  // REMOVED: Full-screen loading that was causing the screen change issue
  // Order placement now uses orderPlacing state and shows inline loading on button

  // === PHASE 6: MAIN RENDER - NO HOOKS HERE ===
  
  // Show full-page skeleton during initial load
  if (initialLoading) {
    return <TradingPageSkeleton />;
  }

  return (
    <>
      {memoizedModal}
      
      {/* Order Success Modal */}
      <OrderSuccessModal
        isOpen={showOrderSuccess}
        onClose={() => setShowOrderSuccess(false)}
        orderData={lastOrderData}
      />
      
      {/* Interactive Signing Dialog (External Party) */}
      {signingState && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-card border border-border rounded-xl shadow-2xl max-w-md w-full p-6 space-y-4">
            <div className="text-center">
              <h3 className="text-lg font-bold text-foreground">
                {signingState.action === 'PLACE' ? '🔐 Sign Order' : '🔐 Sign Cancellation'}
              </h3>
              <p className="text-sm text-muted-foreground mt-1">
                External party detected. Enter your wallet password to sign this transaction.
              </p>
            </div>

            {/* Order Details */}
            <div className="bg-muted rounded-lg p-3 space-y-1 text-sm">
              {signingState.action === 'PLACE' && signingState.orderMeta && (
                <>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Action:</span>
                    <span className={`font-semibold ${signingState.orderMeta.orderType === 'BUY' ? 'text-green-500' : 'text-red-500'}`}>
                      {signingState.orderMeta.orderType} {signingState.orderMeta.tradingPair}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Quantity:</span>
                    <span className="font-mono">{signingState.orderMeta.quantity}</span>
                  </div>
                  {signingState.orderMeta.price && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Price:</span>
                      <span className="font-mono">{signingState.orderMeta.price}</span>
                    </div>
                  )}
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Mode:</span>
                    <span>{signingState.orderMeta.orderMode}</span>
                  </div>
                </>
              )}
              {signingState.action === 'CANCEL' && signingState.cancelMeta && (
                <>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Action:</span>
                    <span className="font-semibold text-amber-500">Cancel Order</span>
                  </div>
                  {signingState.cancelMeta.orderId && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Order ID:</span>
                      <span className="font-mono text-xs">{signingState.cancelMeta.orderId.substring(0, 25)}...</span>
                    </div>
                  )}
                </>
              )}
            </div>

            {/* Password Input */}
            <div className="space-y-2">
              <label className="text-sm font-medium text-foreground">Wallet Password</label>
              <input
                type="password"
                className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                placeholder="Enter wallet password"
                value={walletPassword}
                onChange={(e) => setWalletPassword(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSignAndExecuteOrder()}
                autoFocus
              />
            </div>

            {/* Error */}
            {signingError && (
              <div className="p-3 bg-destructive/10 border border-destructive/20 rounded-lg text-sm text-destructive">
                {signingError}
              </div>
            )}

            {/* Buttons */}
            <div className="flex gap-3">
              <button
                onClick={handleCancelSigning}
                className="flex-1 px-4 py-2 text-sm font-medium border border-border rounded-lg hover:bg-muted transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleSignAndExecuteOrder}
                disabled={!walletPassword || orderPlacing}
                className="flex-1 px-4 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {orderPlacing ? 'Signing...' : 'Sign & Submit'}
              </button>
            </div>
          </div>
        </div>
      )}
      
    <div className="space-y-4 sm:space-y-6">
      <div className="flex items-center justify-between">
          <h2 className="text-xl sm:text-2xl md:text-3xl font-bold text-foreground">Trading</h2>
        <div className="flex items-center space-x-2">
          <div className="w-2 h-2 bg-success rounded-full animate-pulse"></div>
          <span className="text-xs sm:text-sm text-muted-foreground">Connected</span>
        </div>
      </div>
      
      {error && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-destructive/10 border border-destructive rounded-lg p-4"
          >
            <p className="text-destructive text-sm">{error}</p>
          </motion.div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
            <OrderForm
              tradingPair={tradingPair}
              availablePairs={availablePairs}
              onTradingPairChange={setTradingPair}
              orderBookExists={orderBookExists}
              orderType={orderType}
              onOrderTypeChange={(e) => setOrderType(e.target.value)}
              orderMode={orderMode}
              onOrderModeChange={(e) => setOrderMode(e.target.value)}
              price={price}
              onPriceChange={setPrice}
              quantity={quantity}
              onQuantityChange={setQuantity}
              loading={orderPlacing}
              onSubmit={handlePlaceOrder}
              balance={balance}
              lockedBalance={lockedBalance}
              orderBook={orderBook}
              lastTradePrice={trades.length > 0 ? trades[0]?.price : null}
            />

          {/* Balance Card - Shows all token holdings including CBTC */}
          <BalanceCard
            partyId={partyId}
            balance={balance}
            lockedBalance={lockedBalance}
            loading={balanceLoading}
            onRefresh={() => loadBalance(true)}
            mintingLoading={mintingLoading}
            onMintToken={async (symbol, amount) => {
              await handleMintTokens([{ symbol, amount }]);
            }}
          />
          
          {/* Transfer Offers - Accept incoming tokens (CBTC from faucet, etc.) */}
          <TransferOffers
            partyId={partyId}
            onTransferAccepted={handleTransferAccepted}
          />
        </div>

        {/* Price Chart - Full Width */}
        <PriceChart 
          tradingPair={tradingPair}
          trades={trades}
          currentPrice={
            // Priority: last trade price > best bid > best ask
            (trades.length > 0 && parseFloat(trades[0]?.price) > 0) ? parseFloat(trades[0].price) :
            orderBook.buys?.[0]?.price ? parseFloat(orderBook.buys[0].price) : 
            orderBook.sells?.[0]?.price ? parseFloat(orderBook.sells[0].price) : 0
          }
          priceChange24h={0}
          high24h={trades.length > 0 ? Math.max(...trades.map(t => parseFloat(t.price) || 0)) : 0}
          low24h={trades.length > 0 ? Math.min(...trades.filter(t => parseFloat(t.price) > 0).map(t => parseFloat(t.price))) : 0}
          volume24h={trades.reduce((sum, t) => sum + (parseFloat(t.quantity) || 0), 0)}
        />

        {/* Order Book, Market Data & Recent Trades */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-6">
          <div className="lg:col-span-2">
            {/* Order Book */}
            <OrderBookCard 
              orderBook={orderBook}
              loading={orderBookLoading}
              tradingPair={tradingPair}
              userOrders={orders}
            />
          </div>

          {/* Market Data and Recent Trades */}
          <div className="space-y-4 sm:space-y-6">
            <MarketData 
              tradingPair={tradingPair}
              orderBook={orderBook}
              trades={trades}
            />
            
            <RecentTrades 
              trades={trades}
              loading={tradesLoading}
              tradingPair={tradingPair}
            />
          </div>
        </div>

        {/* Active Orders - Full Width */}
        <ActiveOrdersTable
          orders={orders}
          loading={loading}
          onCancelOrder={handleCancelOrder}
          partyId={partyId}
        />

        {/* Depth Chart and History */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
          <DepthChart 
            orderBook={{
              bids: orderBook.buys || [],
              asks: orderBook.sells || []
            }}
            currentPrice={
              (trades.length > 0 && parseFloat(trades[0]?.price) > 0) ? parseFloat(trades[0].price) :
              orderBook.buys?.[0]?.price ? parseFloat(orderBook.buys[0].price) : 
              orderBook.sells?.[0]?.price ? parseFloat(orderBook.sells[0].price) : 0
            }
          />
          
          <TransactionHistory 
            partyId={partyId}
            tradingPair={tradingPair}
          />
        </div>
      </div>
    </>
  );
}
