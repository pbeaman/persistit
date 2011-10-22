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

import static com.persistit.TransactionStatus.ABORTED;
import static com.persistit.TransactionStatus.UNCOMMITTED;

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
     * Default threshold value for moving long-running transactions to the
     * {@link #_longRunning} list.
     */
    final static int DEFAULT_LONG_RUNNING_THRESHOLD = 5;

    /**
     * Default maximum number of TransactionStatus instances to hold on the free
     * list.
     */
    final static int DEFAULT_MAX_FREE_LIST_SIZE = 20;

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
     * Adjustable threshold count at which a transaction on the _current list is
     * moved to the {@link #_longRunning} list so that the {@link #_floor} can
     * be raised.
     */
    volatile int _longRunningThreshold = DEFAULT_LONG_RUNNING_THRESHOLD;

    /**
     * Maximum number of TransactionStatus objects to hold on the free list.
     * Once this number is reached any addition deallocated instances are
     * released for garbage collection.
     */
    volatile int _maxFreeListSize = DEFAULT_MAX_FREE_LIST_SIZE;

    /**
     * One of two ActiveTransactionCache instances
     */
    private ActiveTransactionCache _atCache1 = new ActiveTransactionCache();

    /**
     * One of two ActiveTransactionCache instances
     */
    private ActiveTransactionCache _atCache2 = new ActiveTransactionCache();

    /**
     * Lock held by a thread updating the ActiveTransactionCache to prevent a
     * race by another thread attempting also to update.
     */
    private ReentrantLock _atCacheLock = new ReentrantLock();
    /**
     * Reference to the more recently updated of two ActiveTransactionCache
     * instances.
     */
    private volatile ActiveTransactionCache _atCache = _atCache1;

    /**
     * The system-wide timestamp allocator
     */
    private final TimestampAllocator _timestampAllocator;

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
     * less than t. Note that by the time the scan is done some of those
     * transactions may have committed or aborted; therefore the set of
     * transactions added to the cache may be a superset of those that are
     * active at the conclusion of the scan, but that is okay. The result of
     * that imprecision is that in some cases an MVV may not be optimally pruned
     * until a later attempt.
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
         * Largest timestamp for which the current copy of _tsArray is accurate.
         */
        private volatile long _ceiling;

        /**
         * Smallest timestamp in _tsArray
         */
        private volatile long _floor;

        /**
         * Cache for a recent concurrent transaction scan.
         */
        private volatile long[] _tsArray = new long[INITIAL_ACTIVE_TRANSACTIONS_SIZE];

        private volatile int _count;

        void recompute() {
            _count = 0;
            final long timestampAtStart = _timestampAllocator.updateTimestamp();
            long floor = timestampAtStart;
            for (TransactionIndexBucket bucket : _hashTable) {
                if (bucket.getCurrent() != null || bucket.getLongRunning() != null) {
                    bucket.lock();
                    try {
                        for (TransactionStatus status = bucket.getCurrent(); status != null; status = status.getNext()) {
                            if (status.getTc() == UNCOMMITTED && status.getTs() <= timestampAtStart) {
                                add(status.getTs());
                                if (status.getTs() < floor) {
                                    floor = status.getTs();
                                }
                            }
                        }
                        for (TransactionStatus status = bucket.getLongRunning(); status != null; status = status
                                .getNext()) {
                            if (status.getTc() == UNCOMMITTED && status.getTs() <= timestampAtStart) {
                                add(status.getTs());
                                if (status.getTs() < floor) {
                                    floor = status.getTs();
                                }
                            }
                        }
                    } finally {
                        bucket.unlock();
                    }
                }
            }
            Arrays.sort(_tsArray, 0, _count);
            _ceiling = timestampAtStart;
            _floor = floor;
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
            if (ts2 > _ceiling) {
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

    static long vh2ts(final long versionHandle) {
        return versionHandle / VERSION_HANDLE_MULTIPLIER;
    }

    static long ts2vh(final long ts) {
        return ts * VERSION_HANDLE_MULTIPLIER;
    }

    static int vh2step(final long versionHandle) {
        return (int) (versionHandle % VERSION_HANDLE_MULTIPLIER);
    }

    TransactionIndex(final TimestampAllocator timestampAllocator, final int hashTableSize) {
        _timestampAllocator = timestampAllocator;
        _hashTable = new TransactionIndexBucket[hashTableSize];
        for (int index = 0; index < hashTableSize; index++) {
            _hashTable[index] = new TransactionIndexBucket(this);
        }
    }

    int getMaxFreeListSize() {
        return _maxFreeListSize;
    }

    int getLongRunningThreshold() {
        return _longRunningThreshold;
    }

    TimestampAllocator getTimestampAllocator() {
        return _timestampAllocator;
    }

    /**
     * Given a start timestamp <code>ts</code> of the current transaction,
     * determine the commit status of a value at the specified
     * <code>versionHandle</code>. The result depends on the status of the
     * transaction T identified by the <code>versionHandle</code> as follows:
     * <ul>
     * <li>If T is the same transaction as this one (the transaction identified
     * by <code>ts</code>) then the result depends on the relationship between
     * the "step" number encoded in the supplied <code>versionHandle</code>
     * (stepv) and the supplied <code>step</code>parameter:
     * <ul>
     * <li>If stepv &eq; 0 or stepv &lt; step then return tsv as the "commit"
     * timestamp. (Note that the transaction has not actually committed, but for
     * the purpose of reading values during the execution of the transaction it
     * is as if that transaction's own updates are present.)</li>
     * <li>Else return {@link TransactionStatus#UNCOMMITTED}.</li>
     * </ul>
     * <li>If T has committed, the result is T's commit timestamp
     * <code>tc</code>.</li>
     * <li>If T has aborted, the result is {@link TransactionStatus#ABORTED}.</li>
     * <li>If T has not requested to commit, i.e., does not have proposal
     * timestamp <code>tp</code>, or than proposal timestamp is greater than
     * <code>ts</code> the result is {@link TransactionStatus#UNCOMMITTED}.</li>
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
                return UNCOMMITTED;
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
         * We can read these members without locking because (a) they are
         * volatile, and (b) write-ordering guarantees that a TransactionStatus
         * that is being moved to either the aborted or long-running list will
         * be added to the new list before being removed from the current list.
         * These values are all visible to us with respect to a particular tsv
         * because we could not have seen the tsv without its corresponding
         * transaction status having been registered.
         */
        if ((bucket.getCurrent() == null || tsv < bucket.getFloor()) && bucket.getLongRunning() == null
                && bucket.getAborted() == null) {
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
            if (tsv >= bucket.getFloor()) {
                for (TransactionStatus status = bucket.getCurrent(); status != null; status = status.getNext()) {
                    if (status.getTs() == tsv) {
                        return status.getSettledTc();
                    }
                }
            }
            for (TransactionStatus status = bucket.getAborted(); status != null; status = status.getNext()) {
                if (status.getTs() == tsv) {
                    return ABORTED;
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
     * Atomically assign a start timestamp and register a transaction within the
     * <code>TransactionIndex</code>. Once registered, the transaction's commit
     * status can be found by calling {@link #commitStatus(long, long)}. It is
     * important that assigning the timestamp and making the transaction
     * accessible within the TransactionIndex is atomic because otherwise a
     * concurrent transaction with a larger start timestamp could fail to see
     * this one and cause inconsistent results.
     * 
     * @return the TransactionStatus.
     * @throws InterruptedException
     * @throws TimeoutException
     */
    synchronized TransactionStatus registerTransaction() throws TimeoutException, InterruptedException {
        final long ts = _timestampAllocator.updateTimestamp();
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
        return status;
    }

    /**
     * Notify the TransactionIndex that the specified Transaction has committed
     * or aborted. This method allows the TransactionIndex to awaken any threads
     * waiting for resolution of commit status or a write-write dependency.
     * 
     * @param ts
     *            The start timestamp of a transaction that has committed or
     *            aborted.
     */
    void notifyCompleted(final long ts) {
        final int hashIndex = hashIndex(ts);
        final TransactionIndexBucket bucket = _hashTable[hashIndex];
        final TransactionStatus status;
        bucket.lock();
        try {
            status = bucket.notifyCompleted(ts);
        } finally {
            bucket.unlock();
        }
        status.wwUnlock();
    }

    /**
     * Notify the TransactionIndex that the MVV count for an aborted transaction
     * has become zero. The caller determines this when the value returned by
     * {@link TransactionStatus#decrementMvvCount()} is zero. This method cleans
     * now-obsolete <code>TransactionStatus</code> instances from the aborted
     * list.
     * 
     * @param ts
     *            Timestamp of an aborted transaction which no longer hasany
     *            remaining MVV version values.
     */
    void notifyPruned(final long ts) {
        final int hashIndex = hashIndex(ts);
        final TransactionIndexBucket bucket = _hashTable[hashIndex];
        bucket.lock();
        try {
            bucket.pruned();
        } finally {
            bucket.unlock();
        }
    }

    /**
     * <p>
     * Determine whether there exists a registered transaction that has neither
     * committed nor aborted having a starting timestamp <code>ts</code> such
     * that <code>ts1</code> &lt; <code>ts</code> &lt; <code>ts2</code>.
     * </p>
     * <p>
     * This method is not synchronized and therefore may return a
     * <code>true</code> value for a transaction which then immediately before
     * the caller acts on the result value either commits or aborts. However,
     * the converse is not true. Provided <code>ts2</code> is a valid timestamp
     * value created by the {@link TimestampAllocator}, then if there exists a
     * concurrent transaction with a start timestamp in the specified range,
     * this method is guaranteed to return <code>true</code>
     * </p>
     * 
     * @param ts1
     * @param ts2
     * @return <code>true</code> if there is an
     */
    boolean hasConcurrentTransaction(long ts1, long ts2) {
        return _atCache.hasConcurrentTransaction(ts1, ts2);
    }

    /**
     * Timestamp known to be less than or equal to the start timestamp of any
     * currently executing transaction. This value is computed by
     * {@link #updateActiveTransactionCache()} and is therefore may be less the
     * timestamp of any currently executing transaction at the instant this
     * value is returned. However, it is guaranteed that no running transaction
     * has a lower start timestamp.
     * 
     * @return Lower bound on start timestamps of currently active transactions.
     */
    public long getActiveTransactionFloor() {
        return _atCache._floor;
    }

    /**
     * Timestamp recorded at the start of the last invocation of
     * {@link #updateActiveTransactionCache()}. The
     * {@link #hasConcurrentTransaction(long, long)} method will indicate that
     * any transaction newer than that ceiling is currently active even if that
     * transaction subsequently committed or aborted.
     * 
     * @return Upper bound on timestamps for which
     *         {@link #hasConcurrentTransaction(long, long)} returns accurate
     *         information.
     */
    public long getActiveTransactionCeiling() {
        return _atCache._ceiling;
    }

    /**
     * Count of active transactions measured when
     * {@link #updateActiveTransactionCache()} was last called. The count may
     * have changed to due new transactions starting or existing transactions
     * committing since that invocation, and therefore the value returned by
     * this method is an estimate.
     * 
     * @return the count
     */
    public long getActiveTransactionCount() {
        return _atCache._count;
    }

    /**
     * <p>
     * Detects a write-write dependency from one transaction to another. This
     * method is called when transaction having start timestamp <code>ts</code>
     * detects that there is already an update from another transaction
     * identified by <code>versionHandle</code>. If the other transaction has
     * already committed or aborted then this method immediately returns a value
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
     * </p>
     * <p>
     * It is invalid for <code>versionHandle</code> to refer to the current
     * transaction. The caller should recognize versions written by the current
     * transaction and handle the "step" logic independently of this method. If
     * <code>versionHandle</code> refers to the same transaction as
     * <code>ts</code> then this code throws an IllegalArgumentException.
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
     * @throws IllegalArgumentException
     *             if <code>versionHandle</code> and <code>ts</code> do not
     *             refer to different valid transactions
     */
    boolean wwDependency(long versionHandle, long ts, long timeout) throws TimeoutException, InterruptedException,
            IllegalArgumentException {
        final long tsv = vh2ts(versionHandle);
        if (tsv == ts) {
            throw new IllegalArgumentException("Attempt to create ww-dependency on self");
        }
        final int hashIndex = hashIndex(tsv);
        TransactionIndexBucket bucket = _hashTable[hashIndex];
        /*
         * First check whether there are any TransactionStatus instances in the
         * bucket. If not then the transaction that committed this value is not
         * concurrent.
         * 
         * We can read these members without locking because (a) they are
         * volatile, and (b) write-ordering guarantees that a TransactionStatus
         * that is being moved to either the aborted or long-running list will
         * be added to the new list before being removed from the current list.
         * These values are all visible to us with respect to a particular tsv
         * because we could not have seen the tsv without its corresponding
         * transaction status having been registered.
         */
        if ((bucket.getCurrent() == null || tsv < bucket.getFloor()) && bucket.getLongRunning() == null
                && bucket.getAborted() == null) {
            return false;
        }

        /*
         * There were members on at least one of the lists. Need to lock the
         * bucket so we can traverse the lists.
         */
        TransactionStatus status = null;
        bucket.lock();
        try {
            /*
             * > A transaction with a start timestamp less than or equal to the
             * floor is committed unless it is found on either the aborted or
             * longRunning lists.
             */
            if (tsv >= bucket.getFloor()) {
                for (TransactionStatus s = bucket.getCurrent(); s != null && status == null; s = s.getNext()) {
                    if (s.getTs() == tsv) {
                        status = s;
                    }
                }
            }
            for (TransactionStatus s = bucket.getAborted(); s != null && status == null; s = s.getNext()) {
                if (s.getTs() == tsv) {
                    status = s;
                }
            }
            for (TransactionStatus s = bucket.getLongRunning(); s != null && status == null; s = s.getNext()) {
                if (s.getTs() == tsv) {
                    status = s;
                }
            }
        } finally {
            bucket.unlock();
        }
        if (status == null) {
            return false;
        }
        /*
         * Blocks until the target transaction finishes, either by committing or
         * aborting.
         * 
         * TODO: I'm concerned that by the time we get here the
         * TransactionStatus object may have recycled to the free list and then
         * become initialized for a different transaction. That would cause the
         * current thread to wait for completion of an entirely different
         * transaction.
         */
        if (status.wwLock(timeout)) {
            try {
                final long tc = status.getTc();
                if (tc == ABORTED) {
                    return false;
                }
                if (tc < 0 || tc == UNCOMMITTED) {
                    throw new IllegalStateException("Commit incomplete");
                }
                /*
                 * true if and only if this is a concurrent transaction
                 */
                return tc > ts;

            } finally {
                status.wwUnlock();
            }
        }
        return false;
    }

    /**
     * Apply {@link TransactionIndexBucket#reduce()} and
     * {@link TransactionIndexBucket#pruned()} to each bucket. This is useful
     * primarily in unit tests to yield a canonical state.
     */
    void cleanup() {
        for (final TransactionIndexBucket bucket : _hashTable) {
            bucket.lock();
            try {
                bucket.reduce();
                bucket.pruned();
            } finally {
                bucket.unlock();
            }
        }
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

    /**
     * Refresh the ActiveTransactionCache. This method walks the hashTable to
     * update the non-current ActiveTransactionCache instance and then makes it
     * current. This method will block until it can acquire an exclusive lock on
     * _atCacheLock. Therefore there can never be more than one thread
     * performing this method at a time.
     */
    void updateActiveTransactionCache() {
        _atCacheLock.lock();
        try {
            ActiveTransactionCache alternate = _atCache == _atCache1 ? _atCache2 : _atCache1;
            alternate.recompute();
            _atCache = alternate;
        } finally {
            _atCacheLock.unlock();
        }
    }

    int getCurrentCount() {
        int currentCount = 0;
        for (final TransactionIndexBucket bucket : _hashTable) {
            currentCount += bucket.getCurrentCount();
        }
        return currentCount;
    }

    int getLongRunningCount() {
        int longRunningCount = 0;
        for (final TransactionIndexBucket bucket : _hashTable) {
            longRunningCount += bucket.getLongRunningCount();
        }
        return longRunningCount;
    }

    int getAbortedCount() {
        int aortedCount = 0;
        for (final TransactionIndexBucket bucket : _hashTable) {
            aortedCount += bucket.getAbortedCount();
        }
        return aortedCount;
    }

    int getFreeCount() {
        int freeCount = 0;
        for (final TransactionIndexBucket bucket : _hashTable) {
            freeCount += bucket.getFreeCount();
        }
        return freeCount;
    }

    int getDroppedCount() {
        int droppedCount = 0;
        for (final TransactionIndexBucket bucket : _hashTable) {
            droppedCount += bucket.getDroppedCount();
        }
        return droppedCount;
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int index = 0; index < _hashTable.length; index++) {
            final TransactionIndexBucket bucket = _hashTable[index];
            sb.append(String.format("%5d: %s\n", index, bucket));
        }
        return sb.toString();
    }

}
