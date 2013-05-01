/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.persistit.Accumulator.Delta;

class TransactionStatus {

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
     * Distinguished synthetic timestamp signifying that a thread waiting for a
     * stable result timed out.
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
     * Semaphore used to manage ww dependencies. An attempt to update an MVV
     * that already contains a value version from a concurrently executing
     * transaction must wait for that other transaction to commit or abort. The
     * protocol uses a Semaphore rather than a ReentrantLock because it may be
     * acquired in one thread but released in another.
     */
    private final Semaphore _wwLock = new Semaphore(1);
    /**
     * Pointer to next member of singly-linked list.
     */
    private TransactionStatus _next;

    /**
     * Pointer to TransactionStatus on which we intend to claim a permit. (For
     * deadlock detection.)
     */
    private volatile TransactionStatus _depends;

    /**
     * Pointer to first member of a list of Delta instances contributed through
     * the associated transaction.
     */
    private volatile Delta _delta;

    /**
     * Indicates whether the transaction has called
     * {@link TransactionIndexBucket#notifyCompleted(long)}. Until then the
     * <code>TransactionStatus</code> may not be placed on the free list.
     */
    private volatile boolean _notified;

    TransactionStatus(final TransactionIndexBucket bucket) {
        _bucket = bucket;
    }

    /**
     * Constructs a partial copy. Used only in diagnostic code.
     * 
     * @param status
     */
    TransactionStatus(final TransactionStatus status) {
        this._bucket = status._bucket;
        this._mvvCount = status._mvvCount;
        this._notified = status._notified;
        this._ta = status._ta;
        this._tc = status._tc;
        this._ts = status._ts;
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
     * @return The next TransactionStatus on dependency linked list, or
     *         <code>null</code> if there is none
     */
    TransactionStatus getDepends() {
        return _depends;
    }

    /**
     * Link another TransactionStatus this one depends on.
     * 
     * @param next
     *            the TransactionStatus to link
     */
    void setDepends(final TransactionStatus depends) {
        _depends = depends;
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
     * <code>TransactionStatus</code> in a state indicating commit processing is
     * underway. The {@link #commit(long)} method completes the process. Note
     * that until we implement SSI this method is unnecessary, but is included
     * so that unit tests can test the interim state.
     * 
     * @param timestamp
     */
    void commit(final long timestamp) {
        if (timestamp < _ts || timestamp == UNCOMMITTED) {
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

    void complete(final long timestamp) {
        if (_tc > 0 || -_tc > timestamp && timestamp != ABORTED) {
            throw new IllegalStateException("Transaction not ready to complete: " + this);
        }
        if (_tc < 0 && _tc != ABORTED) {
            _tc = timestamp;
        }
        _notified = true;
    }

    void completeAndUnlock(final long timestamp) {
        complete(timestamp);
        wwUnlock();
    }

    Delta getDelta() {
        return _delta;
    }

    void addDelta(final Delta delta) {
        delta.setNext(_delta);
        _delta = delta;
    }

    Delta takeDelta() {
        final Delta delta = _delta;
        _delta = null;
        return delta;
    }

    long accumulate(final long value, final Accumulator accumulator, final int step) {
        long result = value;
        for (Delta delta = _delta; delta != null; delta = delta.getNext()) {
            if (delta.getAccumulator() == accumulator && delta.getStep() < step) {
                result = accumulator.applyValue(result, delta.getValue());
            }
        }
        return result;
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
        _ta = Long.MAX_VALUE;
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
        assert _tc == ABORTED : "can only decrement MVVs for an aborted transaction";
        final int count = _mvvCount.decrementAndGet();
        assert count >= 0 : "mvvCount is negative";
        if (count == 0) {
            _ta = _bucket.getTimestampAllocator().getCurrentTimestamp();
        }
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
     * Block briefly until another thread transiently holding the wwLock
     * vacates.
     * 
     * @param timeout
     *            in milliseconds
     * @throws InterruptedException
     */
    void briefLock(final long timeout) throws InterruptedException {
        boolean locked = false;
        try {
            locked = wwLock(timeout);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            if (locked) {
                wwUnlock();
            }
        }
    }

    /**
     * <p>
     * Acquire a permit on this TransactionStatus. This supports the
     * {@link TransactionIndex#wwDependency(long, long, long)} method. The
     * permit is acquired when the transaction is registered (see
     * {@link TransactionIndex#registerTransaction(Transaction)} and released
     * once the transaction is either committed or aborted.
     * </p>
     * <p>
     * The <code>wwDependency</code> method also attempts to acquire, and then
     * immediately release this permit. This stalls the thread calling
     * wwDependency until the commit/abort status of the current transaction is
     * known.
     * 
     * @param timeout
     * @return
     * @throws InterruptedException
     */
    boolean wwLock(final long timeout) throws InterruptedException {
        return _wwLock.tryAcquire(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Release the permit acquired by {@link #wwLock(long)}.
     */
    void wwUnlock() {
        _wwLock.release();
    }

    /**
     * Indicate whether this TransactionStatus has been locked. Tested by assert
     * statements in various places.
     * 
     * @return true if a thread has acquired a claim on this TransactionStatus.
     */
    boolean isLocked() {
        return _wwLock.availablePermits() == 0;
    }

    /**
     * Initialize this <code>TransactionStatus</code> instance for a new
     * transaction.
     * 
     * @param ts
     *            Start time of this status.
     */
    void initialize(final long ts) {
        _ts = ts;
        _tc = UNCOMMITTED;
        _ta = PRIMORDIAL;
        _next = null;
        _delta = null;
        _mvvCount.set(0);
        _notified = false;
    }

    /**
     * Initialize this <code>TransactionStatus</code> instance for an artificial
     * transaction known to be aborted. The initial state is aborted, infinite
     * MVV count, and notified.
     * 
     * @param ts
     *            Start time of this status.
     */
    void initializeAsAborted(final long ts) {
        initialize(ts);
        abort();
        setMvvCount(Integer.MAX_VALUE);
        _notified = true;
    }

    @Override
    public String toString() {
        return String.format("<ts=%,d tc=%s mvv=%,d>", _ts, tcString(_tc), _mvvCount.get());
    }

    static String versionString(final long version) {
        final StringBuilder sb = new StringBuilder();
        sb.append(String.format("%,d", TransactionIndex.vh2ts(version)));
        final int step = TransactionIndex.vh2step(version);
        if (step > 0) {
            sb.append(String.format("#%02d", step));
        }
        return sb.toString();
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
