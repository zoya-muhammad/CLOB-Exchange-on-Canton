/**
 * PendingSettlements Component
 *
 * TradingApp pattern (USE_TRADING_APP_PATTERN=true):
 * Tokens flow only between users — no operator custody.
 * When a trade matches, both parties must sign:
 *   1. Withdraw (each party unlocks their allocation)
 *   2. Multi-leg (both sign 2-leg allocation: seller→buyer base, buyer→seller quote)
 *
 * Per client requirement & Huzaifa: 2 transfer legs, execute allocation → no net locked holdings.
 * Reference: https://github.com/hyperledger-labs/splice/blob/bca52d362f8243369381b32aa16279e5b0ebafdf/token-standard/examples/splice-token-test-trading-app/daml/Splice/Testing/Apps/TradingApp.daml
 */

import React, { useState, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  ArrowRightLeft,
  RefreshCw,
  Loader2,
  KeyRound,
  AlertCircle,
  FileSignature,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { Button } from '../ui/button';
import { useToast } from '../ui/toast';
import { apiClient, API_ROUTES } from '../../config/config';
import { loadWallet, decryptPrivateKey, signMessage } from '../../wallet/keyManager';
import websocketService from '../../services/websocketService';

const API = API_ROUTES.SETTLEMENT_TRADING_APP;

export default function PendingSettlements({ partyId, onSettlementComplete }) {
  const [pending, setPending] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const toast = useToast();

  // Withdraw signing state
  const [withdrawSigning, setWithdrawSigning] = useState(null);
  const [walletPassword, setWalletPassword] = useState('');
  const [signingError, setSigningError] = useState(null);
  const [preparingWithdrawId, setPreparingWithdrawId] = useState(null);

  // Multi-leg signing state
  const [multilegSigning, setMultilegSigning] = useState(null);
  const [preparingMultilegId, setPreparingMultilegId] = useState(null);

  const getToken = () =>
    localStorage.getItem('accessToken') ||
    localStorage.getItem('canton_session_token') ||
    '';

  const fetchPending = useCallback(async (isInitial = false) => {
    if (!partyId) return;
    if (isInitial) setLoading(true);
    setError(null);
    try {
      const res = await apiClient.get(API.PENDING, {
        params: { partyId },
        headers: { 'X-Party-Id': partyId, Authorization: `Bearer ${getToken()}` },
      });
      const list = res?.pending ?? res?.data?.pending ?? [];
      setPending(Array.isArray(list) ? list : []);
    } catch (err) {
      if (err?.response?.status === 503) {
        setPending([]);
        setError('TradingApp pattern disabled. Settlement signing not available.');
      } else {
        setError(err?.message || 'Failed to fetch pending settlements');
      }
    } finally {
      if (isInitial) setLoading(false);
    }
  }, [partyId]);

  useEffect(() => {
    fetchPending(true);
    if (!websocketService.isConnected()) websocketService.connect();

    const onSettlementUpdate = (data) => {
      if (data?.type === 'PENDING_SIGNATURE') {
        fetchPending();
      }
    };

    websocketService.subscribe(`settlement:${partyId}`, onSettlementUpdate);
    return () => websocketService.unsubscribe(`settlement:${partyId}`, onSettlementUpdate);
  }, [partyId, fetchPending]);

  const isSeller = (p) => p.sellerPartyId === partyId;
  const isBuyer = (p) => p.buyerPartyId === partyId;
  const myRole = (p) => (isSeller(p) ? 'seller' : 'buyer');
  const needsWithdraw = (p) => {
    if (isSeller(p)) return !p.sellerWithdrawn;
    return !p.buyerWithdrawn;
  };
  const needsMultileg = (p) => p.sellerWithdrawn && p.buyerWithdrawn && p.status === 'PENDING_MULTILEG';

  const handlePrepareWithdraw = async (p) => {
    setSigningError(null);
    setPreparingWithdrawId(p.id);
    try {
      const res = await apiClient.post(API.PREPARE_WITHDRAW(p.id), {
        partyId,
        token: getToken(),
      }, {
        headers: {
          'X-Party-Id': partyId,
          Authorization: `Bearer ${getToken()}`,
        },
      });
      const data = res?.data ?? res;
      if (data?.alreadyWithdrawn) {
        toast.success('Already withdrawn. List refreshed.');
        fetchPending();
        return;
      }
      setWithdrawSigning({
        pending: p,
        preparedTransaction: data.preparedTransaction,
        preparedTransactionHash: data.preparedTransactionHash,
        hashingSchemeVersion: data.hashingSchemeVersion || 'HASHING_SCHEME_VERSION_V2',
      });
    } catch (err) {
      const msg = err?.response?.data?.error ?? err?.message ?? 'Failed to prepare withdraw';
      const isStale = msg.includes('CONTRACT_NOT_FOUND') || msg.includes('ALREADY_WITHDRAWN') || msg.includes('could not be found');
      if (isStale) {
        toast.success('Already withdrawn. List refreshed.');
        fetchPending();
      } else {
        toast.error(msg);
      }
    } finally {
      setPreparingWithdrawId(null);
    }
  };

  const handleSignAndSubmitWithdraw = async () => {
    if (!withdrawSigning || !walletPassword) return;
    const { pending: p, preparedTransaction, preparedTransactionHash, hashingSchemeVersion } = withdrawSigning;
    setSigningError(null);
    try {
      const wallet = loadWallet();
      if (!wallet) throw new Error('Wallet not found. Please unlock your wallet.');
      const privateKey = await decryptPrivateKey(wallet.encryptedPrivateKey, walletPassword);
      const signatureBase64 = await signMessage(privateKey, preparedTransactionHash);
      const signedBy = localStorage.getItem('canton_key_fingerprint') ||
        (partyId?.includes('::') ? partyId.split('::')[1] : null);
      if (!signedBy) throw new Error('Public key fingerprint not found. Re-onboard your wallet.');

      const partySignatures = {
        signatures: [{
          party: partyId,
          signatures: [{
            format: 'SIGNATURE_FORMAT_RAW',
            signature: signatureBase64,
            signedBy,
            signingAlgorithmSpec: 'SIGNING_ALGORITHM_SPEC_ED25519',
          }],
        }],
      };

      const res = await apiClient.post(API.SUBMIT_WITHDRAW(p.id), {
        partyId,
        preparedTransaction,
        partySignatures,
        hashingSchemeVersion,
        token: getToken(),
      }, {
        headers: { 'X-Party-Id': partyId, Authorization: `Bearer ${getToken()}` },
      });

      const data = res?.data ?? res;
      const hs = data?.holdingState;
      const msg = hs?.unlocked !== false
        ? `Withdraw signed! Tokens unlocked (${hs?.totalAvailable ?? '?'} ${hs?.symbol ?? ''} available)`
        : hs?.totalLocked !== '0'
          ? `Withdraw signed. ⚠️ Still ${hs.totalLocked} locked — check holdings.`
          : 'Withdraw signed!';
      toast.success(msg);
      setWithdrawSigning(null);
      setWalletPassword('');
      fetchPending();
      if (onSettlementComplete) onSettlementComplete();
    } catch (err) {
      if (err?.message?.includes('decrypt') || err?.message?.includes('password')) {
        setSigningError('Incorrect wallet password.');
      } else {
        setSigningError(err?.message || 'Sign failed');
      }
    }
  };

  const handlePrepareMultileg = async (p) => {
    setSigningError(null);
    setPreparingMultilegId(p.id);
    try {
      const res = await apiClient.post(API.PREPARE_MULTILEG(p.id), {
        token: getToken(),
      }, {
        headers: {
          'X-Party-Id': partyId,
          Authorization: `Bearer ${getToken()}`,
        },
      });
      const data = res?.data ?? res;
      setMultilegSigning({
        pending: p,
        preparedTransaction: data.preparedTransaction,
        preparedTransactionHash: data.preparedTransactionHash,
        hashingSchemeVersion: data.hashingSchemeVersion || 'HASHING_SCHEME_VERSION_V2',
      });
    } catch (err) {
      const msg = err?.response?.data?.error ?? err?.message ?? 'Failed to prepare multi-leg';
      const isContractNotFound = msg.includes('CONTRACT_NOT_FOUND') || msg.includes('could not be found');
      if (isContractNotFound) {
        toast.error('Settlement already completed or expired. Refreshing list…');
        fetchPending();
      } else {
        toast.error(msg);
      }
    } finally {
      setPreparingMultilegId(null);
    }
  };

  const handleSignAndSubmitMultileg = async () => {
    if (!multilegSigning || !walletPassword) return;
    const { pending: p, preparedTransactionHash, hashingSchemeVersion } = multilegSigning;
    setSigningError(null);
    try {
      const wallet = loadWallet();
      if (!wallet) throw new Error('Wallet not found.');
      const privateKey = await decryptPrivateKey(wallet.encryptedPrivateKey, walletPassword);
      const signatureBase64 = await signMessage(privateKey, preparedTransactionHash);
      const signedBy = localStorage.getItem('canton_key_fingerprint') ||
        (partyId?.includes('::') ? partyId.split('::')[1] : null);
      if (!signedBy) throw new Error('Public key fingerprint not found.');

      const partySignatures = {
        party: partyId,
        signatures: [{
          format: 'SIGNATURE_FORMAT_RAW',
          signature: signatureBase64,
          signedBy,
          signingAlgorithmSpec: 'SIGNING_ALGORITHM_SPEC_ED25519',
        }],
      };

      await apiClient.post(API.SUBMIT_MULTILEG_SIGNATURE(p.id), {
        partyId,
        partySignatures,
        token: getToken(),
      }, {
        headers: { Authorization: `Bearer ${getToken()}` },
      });

      toast.success('Multi-leg signed!');
      setMultilegSigning(null);
      setWalletPassword('');
      fetchPending();
      if (onSettlementComplete) onSettlementComplete();
    } catch (err) {
      if (err?.message?.includes('decrypt') || err?.message?.includes('password')) {
        setSigningError('Incorrect wallet password.');
      } else {
        setSigningError(err?.message || 'Sign failed');
      }
    }
  };

  const cancelSigning = () => {
    setWithdrawSigning(null);
    setMultilegSigning(null);
    setWalletPassword('');
    setSigningError(null);
  };

  // Don't render if TradingApp pattern disabled (503)
  if (error && error.includes('disabled')) {
    return null;
  }

  return (
    <Card className="bg-gradient-to-br from-card to-background border-2 border-border shadow-xl">
      <CardHeader className="pb-3 px-3 sm:px-6">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm sm:text-lg font-bold flex items-center space-x-2">
            <FileSignature className="w-4 h-4 sm:w-5 sm:h-5 text-primary" />
            <span>Pending Settlements</span>
            {pending.length > 0 && (
              <span className="ml-2 px-2 py-0.5 text-xs bg-primary/20 text-primary rounded-full">
                {pending.length}
              </span>
            )}
          </CardTitle>
          <Button variant="ghost" size="icon" onClick={() => fetchPending(true)} disabled={loading} className="h-8 w-8">
            {loading ? <Loader2 className="w-4 h-4 animate-spin text-primary" /> : <RefreshCw className="w-4 h-4 text-muted-foreground" />}
          </Button>
        </div>
        <p className="text-xs text-muted-foreground mt-1">
          Sign to complete trades — tokens flow only between users (no operator custody)
        </p>
      </CardHeader>

      <CardContent className="space-y-3 px-3 sm:px-6">
        {error && !error.includes('disabled') && (
          <div className="flex gap-2 p-3 bg-destructive/10 border border-destructive/30 rounded-lg text-sm text-destructive">
            <AlertCircle className="w-4 h-4 flex-shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {loading && pending.length === 0 ? (
          <div className="flex justify-center py-8">
            <Loader2 className="w-6 h-6 animate-spin text-primary" />
          </div>
        ) : pending.length === 0 ? (
          <p className="text-sm text-muted-foreground py-6 text-center">No pending settlements</p>
        ) : (
          <div className="space-y-3">
            <AnimatePresence>
              {pending.map((p) => (
                <motion.div
                  key={p.id}
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0 }}
                  className="p-4 rounded-lg border border-border bg-background/50 space-y-3"
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <span className="font-medium">{p.tradingPair}</span>
                      <span className="text-muted-foreground text-sm ml-2">
                        {p.matchQty} {p.baseSymbol} @ {p.matchPrice} {p.quoteSymbol}
                      </span>
                    </div>
                    <span className="text-xs px-2 py-1 rounded bg-primary/20 text-primary">
                      {myRole(p)}
                    </span>
                  </div>

                  <div className="flex flex-wrap gap-2">
                    {needsWithdraw(p) && (
                      <Button
                        size="sm"
                        onClick={() => handlePrepareWithdraw(p)}
                        disabled={!!withdrawSigning || !!preparingWithdrawId}
                        className="flex items-center gap-1"
                      >
                        {preparingWithdrawId === p.id ? (
                          <Loader2 className="w-3 h-3 animate-spin" />
                        ) : (
                          <KeyRound className="w-3 h-3" />
                        )}
                        {preparingWithdrawId === p.id ? 'Preparing…' : 'Sign Withdraw'}
                      </Button>
                    )}
                    {needsMultileg(p) && (
                      <Button
                        size="sm"
                        variant="secondary"
                        onClick={() => handlePrepareMultileg(p)}
                        disabled={!!multilegSigning || !!preparingMultilegId}
                        className="flex items-center gap-1"
                      >
                        {preparingMultilegId === p.id ? (
                          <Loader2 className="w-3 h-3 animate-spin" />
                        ) : (
                          <ArrowRightLeft className="w-3 h-3" />
                        )}
                        {preparingMultilegId === p.id ? 'Preparing…' : 'Sign Multi-Leg'}
                      </Button>
                    )}
                  </div>
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        )}

        {/* Withdraw signing modal */}
        {withdrawSigning && (
          <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
            <div className="bg-card border border-border rounded-xl shadow-2xl max-w-md w-full p-6 space-y-4">
              <h3 className="text-lg font-bold">Sign Withdraw</h3>
              <p className="text-sm text-muted-foreground">
                {withdrawSigning.pending.tradingPair} — {withdrawSigning.pending.matchQty} {withdrawSigning.pending.baseSymbol}
              </p>
              <p className="text-sm text-muted-foreground">Enter wallet password to sign.</p>
              <input
                type="password"
                placeholder="Wallet password"
                value={walletPassword}
                onChange={(e) => setWalletPassword(e.target.value)}
                className="w-full px-4 py-2 rounded-lg border border-border bg-background"
              />
              {signingError && <p className="text-sm text-destructive">{signingError}</p>}
              <div className="flex gap-2">
                <Button onClick={handleSignAndSubmitWithdraw} disabled={!walletPassword} className="flex-1">
                  Sign & Submit
                </Button>
                <Button variant="outline" onClick={cancelSigning}>Cancel</Button>
              </div>
            </div>
          </div>
        )}

        {/* Multi-leg signing modal */}
        {multilegSigning && (
          <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
            <div className="bg-card border border-border rounded-xl shadow-2xl max-w-md w-full p-6 space-y-4">
              <h3 className="text-lg font-bold">Sign Multi-Leg</h3>
              <p className="text-sm text-muted-foreground">
                {multilegSigning.pending.tradingPair} — Direct transfer (seller→buyer, buyer→seller)
              </p>
              <p className="text-sm text-muted-foreground">Enter wallet password to sign.</p>
              <input
                type="password"
                placeholder="Wallet password"
                value={walletPassword}
                onChange={(e) => setWalletPassword(e.target.value)}
                className="w-full px-4 py-2 rounded-lg border border-border bg-background"
              />
              {signingError && <p className="text-sm text-destructive">{signingError}</p>}
              <div className="flex gap-2">
                <Button onClick={handleSignAndSubmitMultileg} disabled={!walletPassword} className="flex-1">
                  Sign & Submit
                </Button>
                <Button variant="outline" onClick={cancelSigning}>Cancel</Button>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
