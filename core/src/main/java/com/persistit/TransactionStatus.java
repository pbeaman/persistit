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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.persistit.exception.TimeoutException;

public class TransactionStatus {

    /**
     * Distinguished commit timestamp for any value that has become primordial,
     * meaning that there are no longer any active concurrent transactions.
     */
    final static long PRIMORDIAL = 0;

    /**
     * Distinguished commit timestamp for any value created by a transaction
     * which subsequently aborted.
     */
    final static long ABORTED = Long.MIN_VALUE;

    /**
     * Distinguished commit timestamp for any value created by a transaction
     * that is still running.
     */
    final static long UNCOMMITTED = Long.MAX_VALUE;

    /**
     * Distinguished synthetic timestamp signifying that a thread waiting for
     * a stable result timed out.
     */
    final static long TIMED_OUT = Long.MIN_VALUE + 1;
    
    /**
     * The bucket to which this <code>TransactionStatus</code> belongs.
     */
    private final TransactionIndexBucket _bucket;
    /**
     * Start timestamp. This value may only be assigned by
     * {@link TransactionIndex#registerTransaction(Transaction)}.
     */
    private volatile long _ts;

    /**
     * Commit timestamp and status. Value is one of
     * <dl>
     * <dt>PRIMORDIAL</dt>
     * <dd>The transaction committed long ago, meaning before any concurrent
     * activity. A TrasactionStatus will never have this value, but it is used
     * in other places to communicate the fact that a transaction committed
     * before any recent history.</dd>
     * <dt>ABORTED</dt>
     * <dd>the transaction aborted.</dd>
     * <dt>UNCOMMITTED</dt>
     * <dd>The transaction is running and has not yet asked to commit.</dd>
     * <dt>v &lt; 0</dt>
     * <dd>Any negative value other than Long.MIN_VALUE indicates the timestamp
     * value at which the transaction started to commit; the commit process is
     * incomplete.</dd>
     * <dt>v &gt; 0</dt>
     * <dd>Any positive value other than Long.MAX_VALUE indicates the commit
     * timestamp; the transaction commit is complete.</dd>
     * </dl>
     */
    private volatile long _tc;

    /**
     * Timestamp at which the last MMV version written by this transaction has
     * been pruned (if the transaction aborted). The
     * <code>TransactionStatus</code> may not be removed until there are no
     * currently active transactions have start timestamps older than this
     * value.
     */
    private volatile long _ta;

    /**
     * Count of MVV versions created by associated transaction.
     */
    private AtomicInteger _mvvCount = new AtomicInteger();

    /**
     * Lock used to manage ww dependencies. An attempt to update an MVV that
     * already contains a value version from a concurrently executing
     * transaction must wait for that other transaction to commit or abort.
     */
    private ReentrantLock _wwLock = new ReentrantLock(true);
    /**
     * Pointer to next member of singly-linked list.
     */
    private TransactionStatus _next;

    /**
     * Indicates whether the transaction has called
     * {@link TransactionIndexBucket#notifyCompleted(long)}. Until then the
     * <code>TransactionStatus</code> may not be placed on the free list.
     */
    boolean _notified;

    TransactionStatus(final TransactionIndexBucket bucket) {
        _bucket = bucket;
    }

    /**
     * @return The next TransactionStatus on linked list, or <code>null</code>
     *         if there is none
     */
    TransactionStatus getNext() {
        return _next;
    }

    /**
     * Link another TransactionStatus to this one.
     * 
     * @param next
     *            the TransactionStatus to link
     */
    void setNext(final TransactionStatus next) {
        _next = next;
    }

    /**
     * 
     * @return The associated transaction's start timestamp
     */
    long getTs() {
        return _ts;
    }

    /**
     * @return the commit status of the associated transaction.
     */
    long getTc() {
        return _tc;
    }
    
    /**
     * @return the abort cleanup timestamp
     */
    long getTa() {
        return _ta;
    }

    /**
     * @return whether the {@link TransactionIndexBucket#notifyCompleted(long)}
     *         has been called.
     */
    boolean isNotified() {
        return _notified;
    }

    /**
     * Start commit processing. This method leaves the
     * <code>TransactionStatus</code> in a state indicating commit processing us
     * underway. The {@link #commit(long)} method completes the process. Note
     * that until we implement SSI this method is unnecessary, but is included
     * so that unit tests can test the interim state.
     * 
     * @param timestamp
     */
    void commit(final long timestamp) {
        if (timestamp <= _ts || timestamp == UNCOMMITTED) {
            throw new IllegalArgumentException("Attempt to commit before start: " + this);
        }
        if (_tc != UNCOMMITTED) {
            throw new IllegalArgumentException("Already committed or aborted: " + this);
        }
        _tc = -timestamp;
    }

    void abort() {
        if (_tc != UNCOMMITTED) {
            throw new IllegalArgumentException("Already committed or aborted: " + this);
        }
        _tc = ABORTED;
    }

    void complete() {
        if (_tc > 0) {
            throw new IllegalStateException("Transaction not ready to complete: " + this);
        }
        if (_tc < 0 && _tc != ABORTED) {
            _tc = -_tc;
        }
        _notified = true;
    }

    /**
     * <p>
     * If the associated transaction is in the process of committing, this
     * method will wait until the commit is either finished or aborted. (The
     * latter will only happen when SSI is implemented.) For now this method
     * simply waits for another thread to complete a very fast process.
     * </p>
     * 
     * @return the commit status of the associated transaction.
     * @throws InterruptedException
     * @throws TimeoutException
     */
    long getSettledTc() throws InterruptedException, TimeoutException {
        long tc = _tc;
        while (tc != Long.MIN_VALUE && tc < 0) {
            if (wwLock(TransactionIndex.VERY_LONG_TIMEOUT)) {
                tc = _tc;
                wwUnlock();
            } else {
                throw new TimeoutException();
            }
        }
        return tc;
    }

    /**
     * Increment the count of MVV modifications made by the associated
     * transaction. This is done each time a transaction modifies a value. The
     * counter is used to determine when all values modified by an aborted
     * transaction have been pruned.
     * 
     * @return the updated count
     */
    int incrementMvvCount() {
        return _mvvCount.incrementAndGet();
    }

    /**
     * Decrement the count of MVV modifications made by the associated
     * transaction. This is done each time a version is removed by pruning. When
     * the count becomes zero, then if the associated transaction aborted its
     * TransactionStatus can be removed from the abort set.
     * 
     * @return the updated count
     */
    int decrementMvvCount() {
        int count = _mvvCount.decrementAndGet();
        _ta = _bucket.getTimestampAllocator().getCurrentTimestamp();
        return count;
    }

    /**
     * @return The count of MVV modifications made by the associated
     *         transaction.
     */
    int getMvvCount() {
        return _mvvCount.get();
    }

    /**
     * Sets the MVV modification count. In recovery, this is initially set to
     * Integer.MAX_VALUE to prevent pruning code from removing aborted
     * transactions from the abort set prematurely. Note that we do not attempt
     * to recover an accurate count after a crash.
     */
    void setMvvCount(final int count) {
        _mvvCount.set(count);
    }

    /**
     * <p>
     * Acquire a lock on this TransactionStatus. This supports the
     * {@link TransactionIndex#wwDependency(long, long, long)} method. While a
     * transaction is running this lock is in a locked state. The lock is
     * acquired when the transaction is registered (see
     * {@link TransactionIndex#registerTransaction(Transaction)} and released
     * once the transaction is either committed or aborted.
     * </p>
     * <p>
     * The <code>wwDependency</code> method also attempts to acquire, and then
     * immediately release this lock. This stalls the thread calling
     * wwDependency until the commit/abort status of the current transaction is
     * known.
     * 
     * @param timeout
     * @return
     * @throws InterruptedException
     */
    boolean wwLock(final long timeout) throws InterruptedException {
        return _wwLock.tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Release the lock acquired by {@link #wwLock(long)}.
     */
    void wwUnlock() {
        _wwLock.unlock();
    }

    /**
     * Initialize this <code>TransactionStatus</code> instance for a new
     * transaction.
     * 
     * @param ts
     * @throws InterruptedException
     * @throws TimeoutException
     */
    void initialize(final long ts) throws InterruptedException, TimeoutException {
        _ts = ts;
        _tc = UNCOMMITTED;
        _next = null;
        _mvvCount.set(0);
        _notified = false;
    }

    @Override
    public String toString() {
        return String.format("<ts=%d tc=%s mvv=%d>", _ts, tcString(_tc), _mvvCount.get());
    }

    static String tcString(final long ts) {
        if (ts == ABORTED) {
            return "ABORTED";
        } else if (ts == UNCOMMITTED) {
            return "UNCOMMITTED";
        } else {
            return String.format("%,d", ts);
        }
    }
}
