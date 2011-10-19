/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import com.persistit.exception.TimeoutException;

/**
 * Keep track of concurrent transactions and those that committed or aborted
 * recently.
 * 
 * @author peter
 */
public class TransactionIndex {

    private final static int INITIAL_active_TRANSACTIONS_SIZE = 1000;

    private final Persistit _persistit;

    private final TransactionIndexBucket[] _hashTable;

    private ActiveTransactionCache _atCache1 = new ActiveTransactionCache();

    private ActiveTransactionCache _atCache2 = new ActiveTransactionCache();

    private ActiveTransactionCache _atCache = _atCache1;

    private ReentrantLock _atCacheLock = new ReentrantLock();

    /**
     * <p>
     * Cache copy of currently active transactions. We think in general it is
     * expensive to look at transaction status directly while pruning due to the
     * need to lock the TransactionIndexBuckets to read their lists. Instead of
     * scanning all of these on each pruning operation we periodically compute
     * an array of ts values for transactions that are currently running. These
     * are assembled into a sorted array in this Class. There are two instances
     * of this class in the TransactionIndex, one used for concurrent pruning
     * and the other available to recompute a more recent array of transactions.
     * </p>
     * <p>
     * Any thread may call recompute on the ActiveTransactionCache that is not
     * in service, but it must first lock the atCacheLock to prevent another
     * from overwriting its work.
     * </p>
     * 
     */
    private class ActiveTransactionCache {
        /**
         * Largest timestamp for which the current copy of runningTransactions
         * is accurate.
         */
        private volatile long _activeTransactionsCeiling;

        /**
         * Cache for a recent concurrent transaction scan.
         */
        private volatile long[] _activeTransactions = new long[INITIAL_active_TRANSACTIONS_SIZE];

        private volatile int _activeTransactionCount;

        void recompute() {
            _activeTransactionCount = 0;
            long timestampAtStart = _persistit.getTimestampAllocator().getCurrentTimestamp();
            for (TransactionIndexBucket bucket : _hashTable) {
                if (bucket.getCurrent() != null || bucket.getLongRunning() != null) {
                    bucket.lock();
                    try {
                        for (TransactionStatus status = bucket.getCurrent(); status != null; status = status.getNext()) {
                            if (status.getTc() == TransactionStatus.UNCOMMITTED && status.getTs() <= timestampAtStart) {
                                add(status.getTs());
                            }
                        }
                        for (TransactionStatus status = bucket.getLongRunning(); status != null; status = status
                                .getNext()) {
                            if (status.getTc() == TransactionStatus.UNCOMMITTED && status.getTs() <= timestampAtStart) {
                                add(status.getTs());
                            }
                        }
                    } finally {
                        bucket.unlock();
                    }
                }
            }
            Arrays.sort(_activeTransactions, 0, _activeTransactionCount);
        }

        private void add(final long ts) {
            int index = _activeTransactionCount;
            if (++_activeTransactionCount >= _activeTransactions.length) {
                long[] temp = new long[_activeTransactionCount + INITIAL_active_TRANSACTIONS_SIZE];
                System.arraycopy(_activeTransactions, 0, temp, 0, _activeTransactions.length);
                _activeTransactions = temp;
            }
            _activeTransactions[index] = ts;
        }
    }

    TransactionIndex(final Persistit persistit, final int hashTableSize) {
        _persistit = persistit;
        _hashTable = new TransactionIndexBucket[hashTableSize];
        for (int index = 0; index < hashTableSize; index++) {
            _hashTable[index] = new TransactionIndexBucket();
        }
    }

    /**
     * Given a start timestamp <code>ts</code> of the current transaction,
     * determine the commit status of a value at the specified
     * <code>versionHandle</code>. The result depends on the status of the
     * transaction T identified by the <code>versionHandle</code> as follows:
     * <ul>
     * <li>If T has committed, the result is T's commit timestamp
     * <code>tc</code>.</li>
     * <li>If T has aborted, the result is -1.</li>
     * <li>If T has not requested to commit, i.e., does not have proposal
     * timestamp <code>tp</code>, or than proposal timestamp is greater than
     * <code>ts</code> the result is Long.MAX_VALUE.</li>
     * <li>If T has requested to commit and its proposal timestamp is less than
     * <code>ts</code>, but has not yet completed nor failed, then this method
     * waits until T's commit status has been determined.</li>
     * </ul>
     * 
     * @param versionHandle
     *            the version handle of a value version
     * @param ts
     *            the transaction start timestamp of the current transaction
     * @param timeout
     *            Time in milliseconds to wait. If the <code>to</code>
     *            transaction has neither committed nor aborted within this time
     *            interval then a TimeoutException is thrown.
     * @throws TimeoutException
     *             if the timeout interval is exceeded
     * @throws InterruptedException
     *             if the waiting thread is interrupted
     * @return the commit status
     */
    long commitStatus(long versionHandle, long ts, long timeout) throws TimeoutException, InterruptedException {
        return -1;
    }

    /**
     * Atomically assign a start timestamp and registers a transaction within
     * the <code>TransactionIndex</code>. Once registered, the transaction's
     * commit status can be found by calling {@link #commitStatus(long, long)}.
     * It is important that assigning the timestamp and making the transaction
     * accessible within the TransactioIndex is atomic because otherwise a
     * concurrent transaction with a larger start timestamp could fail to see
     * this one and cause inconsistent results.
     * 
     * @param txn
     *            the Transaction to register
     * @return the Transaction's start timestamp.
     */
    synchronized long registerTransaction(Transaction txn) {
        final long ts = _persistit.getTimestampAllocator().updateTimestamp();
        int index = hashIndex(ts);
        TransactionIndexBucket bucket = _hashTable[index];
        bucket.lock();
        try {
            TransactionStatus status = bucket.allocateTransactionStatus();
            status.initialize(ts);
            bucket.addCurrent(status);
        } finally {
            bucket.unlock();
        }
        return ts;
    }

    /**
     * Notify the TransactionIndex that the specified Transaction has committed
     * or aborted. This method allows the TransactionIndex to awaken any threads
     * waiting for resolution of commit status or a write-write dependency.
     * 
     * @param txn
     */
    void notifyCompleted(Transaction txn) {

    }

    /**
     * Determine whether there exists a registered transaction that has neither
     * committed nor aborted having a starting timestamp <code>ts</code> such
     * that <code>ts1</code> &lt; <code>ts</code> &lt; <code>ts2</code>.
     * <p />
     * This method is not synchronized and therefore may return a
     * <code>true</code> value for a transaction which then immediately before
     * the caller acts on the result value either commits or aborts. However,
     * the converse is not true. Provided <code>ts2</code> is a valid timestamp
     * value created by the {@link TimestampAllocator}, then if there exists a
     * concurrent transaction with a start timestamp in the specified range,
     * this method is guaranteed to return <code>true</code>
     * 
     * @param ts1
     * @param ts2
     * @return <code>true</code> if there is an
     */
    boolean hasConcurrentTransaction(long ts1, long ts2) {
        return false;
    }

    /**
     * Marks a write-write dependency from one transaction to another. This
     * method is called when transaction identified by <code>from</code> detects
     * that there is an update from concurrent transaction identified by
     * <code>to</code>. If the <code>to</code> transaction has already committed
     * or aborted then this method immediately returns a value depending on the
     * outcome of the <code>to</code> transaction:
     * <ul>
     * <li>If the <code>to</code> transaction committed, then this method
     * returns <code>true<code>, indicating that the <code>from</code>
     * transaction must abort.</li>
     * <li>If the <code>to</code> transaction aborted, then this method returns
     * <code>false</code> meaning that the write-write dependency has been
     * cleared and the <code>from</code> transaction may proceed.</li>
     * </ul>
     * If the <code>to</code> transaction has neither committed nor blocked,
     * then this method waits for the <code>to</code> transaction's status to be
     * resolved.
     * 
     * 
     * @param from
     * @param to
     * @param timeout
     *            Time in milliseconds to wait. If the <code>to</code>
     *            transaction has neither committed nor aborted within this time
     *            interval then a TimeoutException is thrown.
     * @return <code>true</code> if the <code>from</code> transaction must
     *         abort, or <code>false</code> if it may proceed.
     * @throws TimeoutException
     *             if the timeout interval is exceeded
     * @throws InterruptedException
     *             if the waiting thread is interrupted
     */
    boolean wwDependency(long from, long to, long timeout) throws TimeoutException, InterruptedException {
        return false;
    }

    private int hashIndex(final long ts) {
        return (((int) ts ^ (int) (ts >>> 32)) & Integer.MAX_VALUE) % _hashTable.length;
    }
    
    private void updateActiveTransactionCache() {
        _atCacheLock.lock();
        try {
            ActiveTransactionCache atCache = _atCache == _atCache1 ? _atCache2 : _atCache1;
            atCache.recompute();
            _atCache = atCache;
        } finally {
            _atCacheLock.unlock();
        }
    }
}
