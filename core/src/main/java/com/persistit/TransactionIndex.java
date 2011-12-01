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
import static com.persistit.TransactionStatus.TIMED_OUT;
import static com.persistit.TransactionStatus.UNCOMMITTED;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.persistit.Accumulator.Delta;
import com.persistit.exception.RetryException;
import com.persistit.exception.TimeoutException;

/**
 * Keep track of concurrent transactions and those that committed or aborted
 * recently.
 * 
 * @author peter
 */
public class TransactionIndex {

    /**
     * Thread name of the polling task
     */
    final static String POLLING_TASK_NAME = "TXN_UPDATE";

    /**
     * Interval in milliseconds for updating the active transaction cache
     */
    final static long POLLING_TASK_INTERVAL = 50L;

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
     * Default maximum number of Delta instances to hold on the free list.
     */
    final static int DEFAULT_MAX_FREE_DELTA_LIST_SIZE = 50;

    /**
     * TODO - more thought on timeout processing.
     */
    final static long VERY_LONG_TIMEOUT = 60000; // sixty seconds

    /**
     * Short timeout for lock polling
     */
    final static long SHORT_TIMEOUT = 10;

    /**
     * Maximum length of path in deadlock detector before deadlock is assumed.
     */
    final static int CYCLE_LIMIT = 10;
    /**
     * Initial size of arrays in ActiveTransactionCaches.
     */
    private final static int INITIAL_ACTIVE_TRANSACTIONS_SIZE = 1000;

    /**
     * Maximum version handle "steps" within one transaction
     */
    final static int VERSION_HANDLE_MULTIPLIER = 256;

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
     * Maximum number of {@link TransactionStatus) objects to hold on the free
     * list. Once this number is reached any addition deallocated instances are
     * released for garbage collection.
     */
    volatile int _maxFreeListSize = DEFAULT_MAX_FREE_LIST_SIZE;

    /**
     * Maximum number of {@link Delta} instances to hold on the free list.
     */
    volatile int _maxFreeDeltaListSize = DEFAULT_MAX_FREE_DELTA_LIST_SIZE;

    /**
     * One of two ActiveTransactionCache instances
     */
    private final ActiveTransactionCache _atCache1;

    /**
     * One of two ActiveTransactionCache instances
     */
    private final ActiveTransactionCache _atCache2;

    /**
     * Lock held by a thread updating the ActiveTransactionCache to prevent a
     * race by another thread attempting also to update.
     */
    private ReentrantLock _atCacheLock = new ReentrantLock();
    /**
     * Reference to the more recently updated of two ActiveTransactionCache
     * instances.
     */
    private volatile ActiveTransactionCache _atCache;

    /**
     * The system-wide timestamp allocator
     */
    private final TimestampAllocator _timestampAllocator;

    private ActiveTransactionCachePollTask _activeTransactionCachePollTask;

    class ActiveTransactionCachePollTask extends IOTaskRunnable {
        AtomicBoolean _closed = new AtomicBoolean();

        ActiveTransactionCachePollTask(final Persistit persistit) {
            super(persistit);
        }

        void close() {
            _closed.set(true);
        }

        @Override
        protected boolean shouldStop() {
            return _closed.get();
        }

        @Override
        protected void runTask() throws Exception {
            updateActiveTransactionCache();
        }
    }

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
     * </p>
     * 
     */
    class ActiveTransactionCache {
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
            for (final TransactionIndexBucket bucket : _hashTable) {
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
            if (ts1 > ts2 || ts2 < _floor) {
                return false;
            }
            /*
             * Note: we may consider a binary search here depending on the
             * length of this array.
             */
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

        @Override
        public String toString() {
            long low = Long.MAX_VALUE;
            long high = Long.MIN_VALUE;
            for (int index = 0; index < _count; index++) {
                low = Math.min(low, _tsArray[index]);
                high = Math.max(high, _tsArray[index]);
            }
            return String.format("Floor=%,d Ceiling=%,d Low=%s High=%s Count=%,d", _floor, _ceiling, minMaxString(low),
                    minMaxString(high), _count);
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
        for (int hashIndex = 0; hashIndex < hashTableSize; hashIndex++) {
            _hashTable[hashIndex] = new TransactionIndexBucket(this, hashIndex);
        }
        _atCache1 = new ActiveTransactionCache();
        _atCache2 = new ActiveTransactionCache();
        _atCache = _atCache1;
    }

    int getHashTableSize() {
        return _hashTable.length;
    }

    int getMaxFreeListSize() {
        return _maxFreeListSize;
    }

    int getMaxFreeDeltaListSize() {
        return _maxFreeDeltaListSize;
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
     * <li>If T has committed, the result depends on the start timestamp
     * <code>ts</code> of the current transaction: if T committed before
     * <code>ts</code> the result is T's commit timestamp <code>tc</code>,
     * otherwise it is {@link TransactionStatus#UNCOMMITTED}.</li>
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
             * version was written by an earlier step or by step 0, return a
             * valid commit timestamp (even though that version has not yet been
             * committed). Otherwise return UNCOMITTED to prevent it from being
             * read.
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

        /*
         * Otherwise search the bucket and find the TransactionStatus for tsv.
         */
        long commitTimestamp = tsv;
        /*
         * There were members on at least one of the lists so we need to try to
         * find the corresponding TransactionStatus identified by tsv.
         */
        TransactionStatus status = getStatus(tsv);
        /*
         * The result can be null in the event the TransactionStatus was freed.
         * It could only have been freed if its transaction committed at a tc
         * that is now primordial. Therefore if status is null we can return tsv
         * as the imputed tc value.
         */
        if (status != null) {
            /*
             * Found the TransactionStatus identified by tsv, but by the time we
             * we read its tc, that TransactionStatus may already be committed
             * to a new transaction with a different ts. Therefore we briefly to
             * lock it to get an accurate reading.
             * 
             * If the TransactionStatus was concurrently freed and reallocated
             * to a different transaction, then it must have committed before
             * the floor timestamp.
             */
            long tc = status.getTc();
            while (status.getTs() == tsv) {
                if (tc >= ts) {
                    return UNCOMMITTED;
                }
                if (tc >= 0) {
                    return tc;
                }
                if (tc == ABORTED) {
                    return tc;
                }

                if (status.wwLock(SHORT_TIMEOUT)) {
                    tc = status.getTc();
                    status.wwUnlock();
                }
            }
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
    TransactionStatus registerTransaction() throws TimeoutException, InterruptedException {
        final TransactionStatus status;
        final TransactionIndexBucket bucket;
        synchronized (this) {
            final long ts = _timestampAllocator.updateTimestamp();
            int index = hashIndex(ts);
            bucket = _hashTable[index];
            bucket.lock();
            try {
                status = bucket.allocateTransactionStatus();
                status.initialize(ts);
                bucket.addCurrent(status);
            } finally {
                bucket.unlock();
            }
        }

        /*
         * The TransactionStatus is locked for the entire duration of the
         * running transaction. The following call should always succeed
         * immediately; a TimeoutException here signifies a software failure or
         * a thread terminated by {@link Thread#stop()} somewhere else.
         */
        if (!status.wwLock(VERY_LONG_TIMEOUT)) {
            throw new IllegalStateException("wwLock was unavailable on newly allocated TransactionStatus");
        }
        /*
         * General hygiene - call reduce if the current count is bigger than the
         * threshold - but this is merely an optimization and the test does not
         * need to be synchronized.
         */
        if (bucket.getCurrentCount() > _longRunningThreshold) {
            bucket.lock();
            try {
                bucket.reduce();
            } finally {
                bucket.unlock();
            }
        }
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
    void notifyCompleted(final TransactionStatus status) {
        final int hashIndex = hashIndex(status.getTs());
        final TransactionIndexBucket bucket = _hashTable[hashIndex];
        bucket.lock();
        try {
            bucket.notifyCompleted(status);
        } finally {
            bucket.unlock();
        }
        status.wwUnlock();
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
     * @return current ActiveTransactionCache instance
     */
    ActiveTransactionCache getActiveTransactionCache() {
        return _atCache;
    }

    TransactionStatus getStatus(final long tsv) {
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
            return null;
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
                        return s;
                    }
                }
            }
            if (status == null) {
                for (TransactionStatus s = bucket.getAborted(); s != null && status == null; s = s.getNext()) {
                    if (s.getTs() == tsv) {
                        return s;
                    }
                }
            }
            if (status == null) {
                for (TransactionStatus s = bucket.getLongRunning(); s != null && status == null; s = s.getNext()) {
                    if (s.getTs() == tsv) {
                        return s;
                    }
                }
            }
        } finally {
            bucket.unlock();
        }
        return null;
    }

    /**
     * <p>
     * Detects a write-write dependency from one transaction to another. This
     * method is called when transaction having start timestamp <code>ts</code>
     * detects that there is already an update from another transaction, the
     * <i>target</i> transaction, identified by its <code>versionHandle</code>.
     * If the target has already committed or aborted then this method
     * immediately returns a value depending on its outcome:
     * <ul>
     * <li>If the target is concurrent with this transaction and committed, then
     * this method returns its commit status, indicating that this transaction
     * must abort.</li>
     * <li>If the target aborted or committed before this transaction started,
     * then this method returns 0 meaning that the write-write dependency has
     * been cleared and this transaction may proceed.</li>
     * <li>If the target is identified by the <code>versionHandle</code> is the
     * same as the current transaction, then this method returns 0.
     * </ul>
     * If the target is concurrent but has neither committed nor aborted, then
     * this method waits up to <code>timeout</code> milliseconds for the
     * target's status to be resolved. If the timeout expires without
     * resolution, this method return {@link TransactionStatus#TIMED_OUT}.
     * </p>
     * <p>
     * Design note: this method is called when a transaction intending to add a
     * new version discovers there is already an MVV for the same key. The
     * transaction is required to determine whether any of the other versions in
     * that MVV are from concurrent transactions, and to abort if so. We expect
     * this method to be called with a timeout of zero to perform a fast,
     * usually non-conflicting, outcome when the page holding the MVV is
     * latched. The TIMED_OUT return value indicates that the caller must back
     * off the latch, reevaluate the wwDependency with no locks, and then retry.
     * </p>
     * </p>
     * 
     * @param versionHandle
     *            versionHandle of a value version found in an MVV that the
     *            current transaction intends to update
     * @param source
     *            this transaction's <code>TransactionStatus</code>
     * @param timeout
     *            Time in milliseconds to wait. If the other transaction has
     *            neither committed nor aborted within this time interval then a
     *            TimeoutException is thrown.
     * @return commit status of .
     * @throws TimeoutException
     *             if the timeout interval is exceeded
     * @throws InterruptedException
     *             if the waiting thread is interrupted
     */
    long wwDependency(final long versionHandle, final TransactionStatus source, final long timeout)
            throws TimeoutException, InterruptedException, IllegalArgumentException {
        final long tsv = vh2ts(versionHandle);
        if (tsv == source.getTs()) {
            return 0;
        }
        final TransactionStatus target = getStatus(tsv);
        if (target == null) {
            return 0;
        }
        /*
         * By the time the selected TransactionStatus has been found, it may
         * already be allocated to another transaction. If that's true the the
         * original transaction must have committed. The following code checks
         * the identity of the transaction on each iteration after short lock
         * attempts.
         */
        if (target.getTs() != tsv) {
            return 0;
        }

        final long start = System.currentTimeMillis();
        /*
         * Blocks until the target transaction finishes, either by committing or
         * aborting.
         */
        do {
            source.setDepends(target);
            try {
                if (target.wwLock(Math.min(timeout, SHORT_TIMEOUT))) {
                    try {
                        final long tc = target.getTc();
                        if (target.getTs() != tsv) {
                            return 0;
                        }
                        if (tc == ABORTED) {
                            return tc;
                        }
                        if (tc < 0 || tc == UNCOMMITTED) {
                            throw new IllegalStateException("Commit incomplete");
                        }
                        /*
                         * true if and only if this is a concurrent transaction
                         */
                        if (tc > source.getTs()) {
                            return tc;
                        } else {
                            return 0;
                        }

                    } finally {
                        target.wwUnlock();
                    }
                } else {
                    if (timeout == 0) {
                        return TIMED_OUT;
                    }
                    if (isDeadlocked(source)) {
                        return UNCOMMITTED;
                    }
                }
            } finally {
                source.setDepends(null);
            }
        } while (timeout > 0 && System.currentTimeMillis() - start < timeout);
        return TIMED_OUT;
    }

    boolean isDeadlocked(final TransactionStatus source) {
        TransactionStatus s = source;
        for (int count = 0; count < CYCLE_LIMIT; count++) {
            s = s.getDepends();
            if (s == null) {
                return false;
            } else if (s == source) {
                System.out.println("deadlock detected on " + source);
                return true;
            }
        }
        System.out.println("cycle length > " + CYCLE_LIMIT + " detected on " + source);
        return true;
    }

    /**
     * Atomically decrement the MVV count for the aborted
     * <code>TransactionStatus</code> identified by the suppled version handle.
     * 
     * @param versionHandle
     * @return The resulting count
     * @throws IllegalArgumentException
     *             if the supplied <code>versionHandle</code> does not identify
     *             an aborted transaction.
     */
    long decrementMvvCount(final long versionHandle) {
        final long tsv = vh2ts(versionHandle);
        final TransactionStatus status = getStatus(tsv);
        if (status == null || status.getTs() != tsv || status.getTc() != ABORTED) {
            throw new IllegalArgumentException("No such aborted transaction " + versionHandle);
        }
        return status.decrementMvvCount();
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
     * Add a TransactionStatus with in the ABORTED state to the appropriate bucket.
     * This method is called during recovery processing to register transactions that
     * were 
     * @param timestamp
     * @throws InterruptedException 
     */
    void injectAbortedTransaction(final long ts) throws InterruptedException {
        final TransactionStatus status;
        final TransactionIndexBucket bucket;
        synchronized (this) {
            int index = hashIndex(ts);
            bucket = _hashTable[index];
            bucket.lock();
            try {
                status = bucket.allocateTransactionStatus();
                status.initialize(ts);
                status.abort();
                status.setMvvCount(Integer.MAX_VALUE);
                bucket.addAborted(status);
            } finally {
                bucket.unlock();
            }
        }

        /*
         * The TransactionStatus is locked for the entire duration of the
         * running transaction. The following call should always succeed
         * immediately; a TimeoutException here signifies a software failure or
         * a thread terminated by {@link Thread#stop()} somewhere else.
         */
        if (!status.wwLock(VERY_LONG_TIMEOUT)) {
            throw new IllegalStateException("wwLock was unavailable on newly allocated TransactionStatus");
        }

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

    /**
     * Invoke the {@link TransactionIndexBucket#cleanup()} method on each bucket
     * to remove all obsolete long-running and aborted
     * <code>TransactionStatus</code> instances. This is useful to generate a
     * canonical state for unit tests. Cleanup logic is normally called during
     * the {@link TransactionIndexBucket#reduce()} process.
     */
    void cleanup() {
        updateActiveTransactionCache();
        for (final TransactionIndexBucket bucket : _hashTable) {
            bucket.lock();
            try {
                bucket.cleanup(getActiveTransactionFloor());
            } finally {
                bucket.unlock();
            }
        }
    }

    /**
     * Compute and return the snapshot value of an Accumulator
     * 
     * @throws InterruptedException
     */
    long getAccumulatorSnapshot(final Accumulator accumulator, final long timestamp, final int step,
            final long initialValue) throws InterruptedException {
        long result = initialValue;
        for (final TransactionIndexBucket bucket : _hashTable) {
            boolean again = true;
            while (again) {
                again = false;
                bucket.lock();
                try {
                    result = accumulator
                            .applyValue(result, bucket.getAccumulatorSnapshot(accumulator, timestamp, step));
                } catch (RetryException e) {
                    again = true;
                } finally {
                    bucket.unlock();
                }
            }
        }
        return result;
    }

    /**
     * <p>
     * Compute a snapshot value for each of the supplied Accumulators and store
     * the resulting value in the Accumulator's checkpointValue field. This
     * method performs the same function as calling
     * {@link #getAccumulatorSnapshot(Accumulator, long, int, long)} on each
     * Accumulator, but is more efficient because it visits each bucket once
     * rather than once per Accumulator.
     * </p>
     * <p>
     * This method is sensitive to the transactional context in which it is
     * called. It is intended to be called only within the Transaction context
     * created during the {@link CheckpointManager#createCheckpoint()} method.
     * </p>
     * 
     * @param timestamp
     *            checkpoint timestamp
     * @param accumulators
     *            List of Accumulators that need to be check-pointed
     * @throws InterruptedException
     */
    void checkpointAccumulatorSnapshots(long timestamp, final List<Accumulator> accumulators)
            throws InterruptedException {
        Map<Accumulator, Accumulator> lookupMap = new HashMap<Accumulator, Accumulator>();
        for (final Accumulator accumulator : accumulators) {
            lookupMap.put(accumulator, accumulator);
            accumulator.setCheckpointValueAndTimestamp(accumulator.getBaseValue(), Long.MIN_VALUE);
        }
        for (final TransactionIndexBucket bucket : _hashTable) {
            boolean again = true;
            while (again) {
                again = false;
                bucket.lock();
                try {
                    for (final Accumulator accumulator : accumulators) {
                        accumulator.setCheckpointTemp(accumulator.getBucketValue(bucket.getIndex()));
                    }
                    bucket.checkpointAccumulatorSnapshots(timestamp);
                    for (final Accumulator accumulator : accumulators) {
                        accumulator.setCheckpointValueAndTimestamp(accumulator.applyValue(accumulator
                                .getCheckpointValue(), accumulator.getCheckpointTemp()), timestamp);
                    }
                } catch (RetryException e) {
                    again = true;
                } finally {
                    bucket.unlock();
                }
            }
        }
    }

    Delta addDelta(final TransactionStatus status) {
        final int hashIndex = hashIndex(status.getTs());
        final TransactionIndexBucket bucket = _hashTable[hashIndex];
        bucket.lock();
        try {
            Delta delta = bucket.allocateDelta();
            status.addDelta(delta);
            return delta;
        } finally {
            bucket.unlock();
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

    static String minMaxString(final long floor) {
        return floor == Long.MAX_VALUE ? "MAX" : floor == Long.MIN_VALUE ? "MIN" : String.format("%,d", floor);
    }

    void start(final Persistit persistit) {
        _activeTransactionCachePollTask = new ActiveTransactionCachePollTask(persistit);
        _activeTransactionCachePollTask.start(POLLING_TASK_NAME, POLLING_TASK_INTERVAL);
    }

    void close() {
        ActiveTransactionCachePollTask task = _activeTransactionCachePollTask;
        if (task != null) {
            task.close();
            _activeTransactionCachePollTask = null;
        }
    }

    void crash() {
        ActiveTransactionCachePollTask task = _activeTransactionCachePollTask;
        if (task != null) {
            task.crash();
            _activeTransactionCachePollTask = null;
        }
    }

}
