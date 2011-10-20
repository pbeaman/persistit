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

    /**
     * TODO - more thought on timeout processing.
     */
    final static long VERY_LONG_TIMEOUT = 60000; // sixty seconds

    /**
     * Initial size of arrays in ActiveTransactionCaches.
     */
    private final static int INITIAL_ACTIVE_TRANSACTIONS_SIZE = 1000;

    /**
     * Maximum version handle "steps" within one transaction
     */
    private final static int VERSION_HANDLE_MULTIPLIER = 256;

    /**
     * The hash table.
     */
    private final TransactionIndexBucket[] _hashTable;

    /**
     * One of two ActiveTransactionCache instances
     */
    private ActiveTransactionCache _atCache1 = new ActiveTransactionCache();

    /**
     * One of two ActiveTransactionCache instances
     */
    private ActiveTransactionCache _atCache2 = new ActiveTransactionCache();

    /**
     * Reference to the more recently updated ActiveTransactionCache instance.
     * In all the cases the new instance is used and the older instance is
     * eligible to be updated to become an even new instance.
     */
    private ActiveTransactionCache _atCache = _atCache1;

    /**
     * Lock held by a thread updating the ActiveTransactionCache to prevent a
     * race by another thread attempting also to update.
     */
    private ReentrantLock _atCacheLock = new ReentrantLock();

    /**
     * The base Persistit instance
     */
    private final Persistit _persistit;

    /**
     * <p>
     * Cached copy of currently active transactions. Instances of this class
     * support the {@link TransactionIndex#hasConcurrentTransaction(long, long)}
     * method. We think in general it is expensive to look at transaction status
     * directly while pruning due to the need to lock each
     * TransactionIndexBucket to read its lists. Instead of scanning all of
     * these on each pruning operation we periodically compute an array of ts
     * values for transactions that are currently running. These are assembled
     * into a sorted array in this object. There are two instances of this class
     * in the TransactionIndex, one used for concurrent pruning and the other
     * available to recompute a more recent array of transactions.
     * </p>
     * <p>
     * Any thread may call recompute on the ActiveTransactionCache that is not
     * in service, but it must first lock the atCacheLock to prevent another
     * from overwriting its work.
     * </p>
     * <p>
     * Each time the cache is recomputed, this instance first gets the current
     * timestamp t. Due to the write-ordering protocol, it is guaranteed that if
     * a transaction having a start timestamp less than t is currently active,
     * its entry will be in the hash table. Therefore, scanning the hash table
     * will find every currently active transaction having a start timestamp
     * less than t. Note that by the time the scan is done some of those transactions
     * may have committed or aborted; therefore the set of transactions added to
     * the cache may be a superset of those that are active at the conclusion of
     * the scan, but that is okay. The result of that imprecision is that in
     * some cases an MVV may not be optimally pruned until a later attempt.
     * </p>
     * <p>
     * By the time this cache is read there may be newly registered transactions
     * having start timestamps greater than t. Again, such a transaction may
     * have registered and committed in the time since the scan was performed;
     * nonetheless the {@link #hasConcurrentTransaction(long, long)} method will
     * indicate that such a transaction is still active. Again, the result of
     * that imprecision is that in some cases an MVV may not be optimally pruned
     * until a later attempt.
     * 
     */
    private class ActiveTransactionCache {
        /**
         * Largest timestamp for which the current copy of runningTransactions
         * is accurate.
         */
        private volatile long _ceilingTimestamp;

        /**
         * Cache for a recent concurrent transaction scan.
         */
        private volatile long[] _tsArray = new long[INITIAL_ACTIVE_TRANSACTIONS_SIZE];

        private volatile int _count;

        void recompute() {
            _count = 0;
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
            Arrays.sort(_tsArray, 0, _count);
            _ceilingTimestamp = timestampAtStart;
        }

        private void add(final long ts) {
            int index = _count;
            if (++_count >= _tsArray.length) {
                long[] temp = new long[_count + INITIAL_ACTIVE_TRANSACTIONS_SIZE];
                System.arraycopy(_tsArray, 0, temp, 0, _tsArray.length);
                _tsArray = temp;
            }
            _tsArray[index] = ts;
        }

        boolean hasConcurrentTransaction(final long ts1, final long ts2) {
            if (ts2 > _ceilingTimestamp) {
                return true;
            }
            if (ts1 > ts2) {
                return false;
            }
            //
            // Note: we may consider a binary search here depending on the
            // length of
            // this array.
            //
            for (int index = 0; index < _count; index++) {
                long ts = _tsArray[index];
                if (ts > ts2) {
                    return false;
                }
                if (ts > ts1) {
                    return true;
                }
            }
            return false;
        }
    }

    TransactionIndex(final Persistit persistit, final int hashTableSize) {
        _persistit = persistit;
        _hashTable = new TransactionIndexBucket[hashTableSize];
        for (int index = 0; index < hashTableSize; index++) {
            _hashTable[index] = new TransactionIndexBucket();
        }
    }

    static long vh2ts(final long versionHandle) {
        return versionHandle / VERSION_HANDLE_MULTIPLIER;
    }

    static long ts2vh(final long ts) {
        return ts * VERSION_HANDLE_MULTIPLIER;
    }

    static int vh2step(final long versionHandle) {
        return (int) (versionHandle % VERSION_HANDLE_MULTIPLIER);
    }

    /**
     * Given a start timestamp <code>ts</code> of the current transaction,
     * determine the commit status of a value at the specified
     * <code>versionHandle</code>. The result depends on the status of the
     * transaction T identified by the <code>versionHandle</code> as follows:
     * <ul>
     * <li>If T has committed, the result is T's commit timestamp
     * <code>tc</code>.</li>
     * <li>If T has aborted, the result is Long.MIN_VALUE.</li>
     * <li>If T has not requested to commit, i.e., does not have proposal
     * timestamp <code>tp</code>, or than proposal timestamp is greater than
     * <code>ts</code> the result is Long.MAX_VALUE.</li>
     * <li>If T has requested to commit and its proposal timestamp is less than
     * <code>ts</code>, but has neither completed nor failed yet, then this
     * method waits until T's commit status has been determined.</li>
     * </ul>
     * 
     * @param versionHandle
     *            the version handle of a value version
     * @param ts
     *            the transaction start timestamp of the current transaction
     * @param step
     *            Step number within the current transaction.
     * @return the commit status
     * @throws InterruptedException
     *             if the waiting thread is interrupted
     * @throws TimeoutException
     *             if the thread waited a very long time without resolving the
     *             commit status; this signifies a serious software error.
     */
    long commitStatus(final long versionHandle, final long ts, final int step) throws InterruptedException,
            TimeoutException {
        final long tsv = vh2ts(versionHandle);
        if (tsv == ts) {
            /*
             * The update was created by this transaction. Policy is that if the
             * version we written by an earlier step or by step 0, return
             * COMMITTED to allow it to be read. Otherwise return UNCOMITTED to
             * prevent it from being read.
             */
            int stepv = vh2step(versionHandle);
            if (stepv == 0 || stepv < step) {
                return tsv;
            } else {
                return TransactionStatus.UNCOMMITTED;
            }
        }
        final int hashIndex = hashIndex(tsv);
        final TransactionIndexBucket bucket = _hashTable[hashIndex];
        /*
         * First check whether there are any TransactionStatus instances in the
         * bucket. In a (hopefully) common case where there are none, then we
         * can assume the value did commit, and do so without locking the
         * bucket.
         * 
         * We can read these without locking because (a) they are volatile, and
         * (b) write-ordering guarantees that a TransactionStatus that is being
         * moved to either the aborted or long-running list will be added to the
         * new list before being removed from the current list. These values are
         * all visible to us with respect to a particular tsv because we could
         * not have seen the tsv without its corresponding transaction status
         * having been registered.
         */
        if (bucket.getCurrent() == null && bucket.getLongRunning() == null && bucket.getAborted() == null) {
            return tsv;
        }
        long commitTimestamp = tsv;
        /*
         * There were members on at least one of the lists. Need to lock the
         * bucket so we can traverse the lists.
         */
        bucket.lock();
        try {
            /*
             * A transaction with a start timestamp less than or equal to the
             * floor is committed unless it is found on either the aborted or
             * longRunning lists.
             */
            if (tsv > bucket.getFloor()) {
                for (TransactionStatus status = bucket.getCurrent(); status != null; status = status.getNext()) {
                    if (status.getTs() == tsv) {
                        return status.getSettledTc();
                    }
                }
            }
            for (TransactionStatus status = bucket.getAborted(); status != null; status = status.getNext()) {
                if (status.getTs() == tsv) {
                    return status.getSettledTc();
                }
            }
            for (TransactionStatus status = bucket.getLongRunning(); status != null; status = status.getNext()) {
                if (status.getTs() == tsv) {
                    return status.getSettledTc();
                }
            }
        } finally {
            bucket.unlock();
        }
        return commitTimestamp;
    }

    /**
     * Atomically assign a start timestamp and register a transaction within
     * the <code>TransactionIndex</code>. Once registered, the transaction's
     * commit status can be found by calling {@link #commitStatus(long, long)}.
     * It is important that assigning the timestamp and making the transaction
     * accessible within the TransactionIndex is atomic because otherwise a
     * concurrent transaction with a larger start timestamp could fail to see
     * this one and cause inconsistent results.
     * 
     * @param txn
     *            the Transaction to register
     * @return the Transaction's start timestamp.
     * @throws InterruptedException
     * @throws TimeoutException
     */
    synchronized long registerTransaction(Transaction txn) throws TimeoutException, InterruptedException {
        final long ts = _persistit.getTimestampAllocator().updateTimestamp();
        int index = hashIndex(ts);
        TransactionIndexBucket bucket = _hashTable[index];
        final TransactionStatus status;
        bucket.lock();
        try {
            status = bucket.allocateTransactionStatus();
            status.initialize(ts);
            bucket.addCurrent(status);
        } finally {
            bucket.unlock();
        }
        /*
         * The TransactionStatus is locked for the entire duration of the
         * running transaction. The following call should always succeed
         * immediately; a TimeoutException here signifies a software failure or
         * a thread terminated by {@link Thread#stop()} somewhere else.
         */
        status.wwLock(VERY_LONG_TIMEOUT);
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
        return _atCache.hasConcurrentTransaction(ts1, ts2);
    }

    /**
     * Detects a write-write dependency from one transaction to another. This
     * method is called when transaction having start timestamp <code>ts</code>
     * detects that there is an update from another transaction identified by
     * <code>versionHandle</code>. If the other transaction has already
     * committed or aborted then this method immediately returns a value
     * depending on its outcome:
     * <ul>
     * <li>If the transaction identified by <code>versionHandle</code> is
     * concurrent with this transaction and committed, then this method returns
     * <code>true<code>, indicating that this
     * transaction must abort.</li>
     * <li>If the <code>to</code> transaction aborted or committed before this
     * transaction started, then this method returns <code>false</code> meaning
     * that the write-write dependency has been cleared and the
     * <code>from</code> transaction may proceed.</li>
     * </ul>
     * If the <code>to</code> transaction has neither committed nor blocked,
     * then this method waits for the other transaction's status to be resolved.
     * 
     * @param versionHandle
     *            versionHandle of a value version found in an MVV that the
     *            current transaction intends to update
     * @param ts
     *            this transaction's start timestamp
     * @param timeout
     *            Time in milliseconds to wait. If the other transaction has
     *            neither committed nor aborted within this time interval then a
     *            TimeoutException is thrown.
     * @return <code>true</code> if the <code>from</code> transaction must
     *         abort, or <code>false</code> if it may proceed.
     * @throws TimeoutException
     *             if the timeout interval is exceeded
     * @throws InterruptedException
     *             if the waiting thread is interrupted
     */
    boolean wwDependency(long versionHandle, long ts, long timeout) throws TimeoutException, InterruptedException {
        return false;
    }

    /**
     * Compute hash index for a given timestamp.
     * 
     * @param ts
     * @return the hash table index
     */
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
