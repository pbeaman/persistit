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

import static com.persistit.TransactionStatus.ABORTED;
import static com.persistit.TransactionStatus.UNCOMMITTED;

import java.util.concurrent.locks.ReentrantLock;

import com.persistit.Accumulator.Delta;
import com.persistit.exception.RetryException;

/**
 * <p>
 * Representation of one {@link TransactionIndex} hash table bucket. This class
 * contains much of the important logic for managing active and recently
 * committed or aborted transactions. It contains four singly-linked lists of
 * {@link TransactionStatus} objects:
 * <dl>
 * <dt>current</dt>
 * <dd>List of transactions having start timestamps greater than or equal to the
 * <i>floor</i> (defined below). <code>TransactionStatus</code> objects leave
 * this list when the floor is raised. This is done by the {@link #reduce()}
 * method.</dt>
 * <dt>aborted</dt>
 * <dd>List of aborted transactions having start times less than the floor.
 * <code>TransactionStatus</code> objects leave this list once their MvvCount
 * becomes zero, meaning that no MVV instances in the database containing
 * updates from that transaction still exist.</dd>
 * <dt>longRunning</dt>
 * <dd>List of active transactions having start times less than the floor which
 * have neither committed nor aborted. The {@link #reduce()} method moves long
 * running transactions to this list so that it can increase the floor.</dd>
 * <dt>free</dt>
 * <dd>List of currently unused <code>TransactionStatus</code> instances that
 * can be allocated and reused for new transactions.</dd>
 * </dl>
 * </p>
 * <p>
 * The <i>floor</i> value is a timestamp that divides transactions on the
 * current list from those moved to the longRunning, aborted or free list. The
 * status of any transaction started after the current value of floor is denoted
 * by a <code>TransactionStatus</code> object on the current list. For any
 * transaction started before the current floor, the transaction is known to
 * have committed unless a <code>TransactionStatus</code> object is found on
 * either the aborted or longRunning lists. These are intended as exceptional
 * cases, and their lists are expected to remain relatively short. The
 * <code>TransactionStatus</code> for a committed transaction (the usual case)
 * is simply moved to the free list once the floor is raised.
 * </p>
 * 
 * @author peter
 * 
 */
class TransactionIndexBucket {
    /**
     * The owner of the hash table that contains this bucket
     */
    final TransactionIndex _transactionIndex;
    /**
     * Index of this bucket in the TransactionIndex hash table
     */
    final int _hashIndex;

    /**
     * Floor timestamp. A transaction that started before this time is usually
     * committed, in which case there no longer exists a TransactionStatus
     * object for it. There are two other cases: the transaction is still
     * running, in which its TransactionStatus has been moved to the
     * long_running list, or it aborted, in which case its TransactionStatus has
     * moved to the aborted list.
     */
    volatile long _floor = Long.MAX_VALUE;
    /**
     * Singly-linked list of TransactionStatus instances for transactions
     * currently running or recently either committed or aborted. A
     * TransactionStatus enters this list when the corresponding transaction is
     * registered. It leaves this list after the _floor value has become larger
     * than its start timestamp, going to (a) the _free list (because the
     * transaction committed), the _aborted list (because it aborted) or the
     * _longRunning list because it is still running.
     */
    volatile TransactionStatus _current;
    /**
     * Current member count
     */
    int _currentCount;
    /**
     * Singly-linked list of TransactionStatus instances for aborted
     * transactions. A TransactionStatus enters this list after the transaction
     * has aborted; it leaves this list when all of the MVV versions created by
     * the transaction have been pruned.
     */
    volatile TransactionStatus _aborted;
    /**
     * Aborted member count
     */
    int _abortedCount;
    /**
     * Singly-linked list of TransactionStatus instances for transactions that
     * started before the floor value. A TransactionStatus enters this list when
     * its transaction has been running for a "long" time, meaning that the
     * current list has a large number of TransactionStatus instances that could
     * be removed by increasing the floor value.
     */
    volatile TransactionStatus _longRunning;
    /**
     * Long running member count
     */
    int _longRunningCount;
    /**
     * Singly-linked list of TransactionStatus instances that no longer
     * represent concurrent or recent transactions. These are recycled when a
     * new transaction is registered.
     */
    volatile TransactionStatus _free;
    /**
     * Free member count
     */
    int _freeCount;

    /**
     * Number of allocated TransactionStatus instances that were released for
     * garbage collection due to full free list.
     */
    int _droppedCount;

    /**
     * Last floor value received for all active transactions. Obtained from the
     * current ActiveTransactionCache.
     */
    long _activeTransactionFloor;
    /**
     * Lock used to prevent multi-threaded access to the lists in this
     * structure. Fair to prevent barging.
     */
    ReentrantLock _lock = new ReentrantLock(true);
    /**
     * Singly-linked list of Delta objects available for reuse
     */
    Delta _freeDeltaList;
    /**
     * Count of Delta instances on the free Delta list
     */
    int _freeDeltaCount;

    TransactionIndexBucket(final TransactionIndex transactionIndex, final int hashIndex) {
        _transactionIndex = transactionIndex;
        _hashIndex = hashIndex;
    }

    void lock() {
        _lock.lock();
    }

    void unlock() {
        _lock.unlock();
    }

    TransactionStatus allocateTransactionStatus() throws InterruptedException {
        assert _lock.isHeldByCurrentThread();
        final TransactionStatus status = _free;
        if (status != null) {
            if (status.isLocked()) {
                status.briefLock(Persistit.SHORT_DELAY);
            }
            assert !status.isLocked();
            _free = status.getNext();
            _freeCount--;
            status.setNext(null);
            return status;
        } else {
            return new TransactionStatus(this);
        }
    }

    void addCurrent(final TransactionStatus status) {
        assert _lock.isHeldByCurrentThread();
        status.setNext(_current);
        if (status.getTs() < _floor) {
            _floor = status.getTs();
        }
        _current = status;
        _currentCount++;
    }

    void addAborted(final TransactionStatus status) {
        assert _lock.isHeldByCurrentThread();
        status.setNext(_aborted);
        _aborted = status;
        _abortedCount++;
    }

    int getIndex() {
        return _hashIndex;
    }

    TransactionStatus getCurrent() {
        return _current;
    }

    TransactionStatus getAborted() {
        return _aborted;
    }

    TransactionStatus getLongRunning() {
        return _longRunning;
    }

    long getFloor() {
        return _floor;
    }

    int getCurrentCount() {
        return _currentCount;
    }

    int getLongRunningCount() {
        return _longRunningCount;
    }

    int getAbortedCount() {
        return _abortedCount;
    }

    int getFreeCount() {
        return _freeCount;
    }

    int getDroppedCount() {
        return _droppedCount;
    }

    TimestampAllocator getTimestampAllocator() {
        return _transactionIndex.getTimestampAllocator();
    }

    boolean hasFloorMoved() {
        return _activeTransactionFloor != _transactionIndex.getActiveTransactionFloor();
    }

    void notifyCompleted(final TransactionStatus status, final long timestamp) {
        assert _lock.isHeldByCurrentThread();
        final long ts = status.getTs();
        if (ts >= getFloor()) {
            for (TransactionStatus s = getCurrent(); s != null; s = s.getNext()) {
                if (s == status) {
                    status.completeAndUnlock(timestamp);
                    if (s.getTs() == getFloor() || hasFloorMoved()) {
                        reduce();
                    }
                    return;
                }
            }
        } else {
            TransactionStatus previous = null;
            for (TransactionStatus s = getLongRunning(); s != null; s = s.getNext()) {
                if (s == status) {
                    final TransactionStatus next = s.getNext();
                    assert s.getTc() != UNCOMMITTED;
                    status.completeAndUnlock(timestamp);
                    boolean moved = false;
                    if (s.getTc() == ABORTED) {
                        aggregate(s, false);
                        s.setNext(_aborted);
                        _aborted = s;
                        _abortedCount++;
                        moved = true;
                    } else {
                        /*
                         * The activeTransactionFloor can only increase after
                         * the relevant transactions have been notified.
                         * Therefore any transaction now being notified must
                         * either be ABORTED or have a tc greater than or equal
                         * to the floor.
                         */
                        assert s.getTc() >= _activeTransactionFloor;
                    }
                    if (moved) {
                        if (previous == null) {
                            _longRunning = next;
                        } else {
                            previous.setNext(next);
                        }
                        _longRunningCount--;
                    }
                    return;
                }
                previous = s;
            }
        }
        throw new IllegalStateException("No such transaction  " + status);
    }

    /**
     * <p>
     * Reduce the size of the {@link #_current} list by moving the oldest
     * <code>TransactionStatus</code> to the {@link #_aborted} list or the
     * {@link #_longRunning} list, and then increasing the {@link #_floor} to
     * reflect the start timestamp of the next oldest remaining transaction. If
     * this method determines that the next oldest transaction is also aborted,
     * it repeats the process and disposes of that transaction as well.
     * </p>
     * <p>
     * The {@link #clean()} method moves <code>TransactionStatus</code>
     * instances from the <code>_aborted</code> and <code>_longRunning</code>
     * lists back to the {@link #_free} list.
     * </p>
     */
    void reduce() {
        assert _lock.isHeldByCurrentThread();
        final boolean hasMoved = hasFloorMoved();
        _activeTransactionFloor = _transactionIndex.getActiveTransactionFloor();
        boolean more = true;
        while (more) {
            more = false;
            TransactionStatus previous = null;
            long newFloor = Long.MAX_VALUE;
            for (TransactionStatus status = _current; status != null;) {
                final TransactionStatus next = status.getNext();
                assert status.getTs() >= _floor;
                /*
                 * Is this TransactionStatus aborted and notified?
                 */
                final boolean aborted = isAborted(status);
                final boolean committed = isCommitted(status);

                if (status.getTs() == _floor) {
                    /*
                     * Essential invariant: the TransactionStatus is added to
                     * its new list before being removed from _current.
                     * Concurrently executing threads can read the _current,
                     * _aborted and _longRunning members outside of locks and
                     * therefore _aborted or _longRunning needs to be updated
                     * before _current becomes null.
                     */
                    boolean moved = false;
                    /*
                     * Move the TransactionStatus somewhere if any of these
                     * conditions hold
                     */
                    if (committed && isObsolete(status)) {
                        aggregate(status, true);
                        /*
                         * committed
                         */
                        free(status);
                        moved = true;
                    } else if (aborted) {
                        aggregate(status, false);
                        /*
                         * aborted
                         */
                        status.setNext(_aborted);
                        _aborted = status;
                        _abortedCount++;
                        moved = true;
                    } else if (_currentCount > _transactionIndex.getLongRunningThreshold()) {
                        /**
                         * long-running
                         */
                        status.setNext(_longRunning);
                        _longRunning = status;
                        _longRunningCount++;
                        moved = true;
                    }

                    if (moved) {
                        /*
                         * Unlink the TransactionStatus from the current list.
                         */
                        if (previous != null) {
                            previous.setNext(next);
                        } else {
                            _current = next;
                        }
                        _currentCount--;
                        status = next;
                        more = true;
                    } else {
                        newFloor = _floor;
                        previous = status;
                    }
                } else {
                    if (status.getTs() < newFloor) {
                        newFloor = status.getTs();
                    }
                    previous = status;
                }
                status = next;
            }
            _floor = newFloor;
        }
        if (hasMoved) {
            cleanup(_activeTransactionFloor);
        }
    }

    /**
     * Move <code>TransactionStatus</code> instances from the
     * <code>_aborted</code> and <code>_longRunning</code> lists back to the
     * {@link #_free} list when there are no longer any active transactions that
     * need them. This method is called periodically whenever the
     * {@link #reduce()} method detects that the ActiveTransactionCache has been
     * updated. It is also called by
     * <code>TransactionIndex{@link #cleanup(long)}</code> for unit tests.
     * <p>
     * An aborted <code>TransactionStatus</code> can be freed once its MVV count
     * becomes zero indicating that no versions written by that transaction
     * remain in the database <i>and</i> it is no longer concurrent with any
     * active transactions. This second condition is important because a
     * concurrent transaction may be in the process of reading an MVV while this
     * method is executing; the <code>TransactionStatus</code> must be retained
     * until that transaction (and any other transactions that might also read
     * the same MVV) has seen that the transaction aborted.
     * </p>
     * <p>
     * A long-running <code>TransactionStatus</code> can be removed once it has
     * committed or aborted and there exists no other possibly concurrent active
     * transaction.
     * </p>
     * 
     * @param activeTransactionFloor
     *            Lower bound of start timestamps of all currently executing
     *            transactions.
     */
    void cleanup(final long activeTransactionFloor) {
        assert _lock.isHeldByCurrentThread();
        TransactionStatus previous;

        /*
         * Remove every aborted transaction whose mvvCount became zero before
         * the start of any active transaction.
         */
        previous = null;
        for (TransactionStatus status = _aborted; status != null;) {
            final TransactionStatus next = status.getNext();
            assert status.getTc() == ABORTED;
            if (status.getMvvCount() == 0 && status.getTa() < activeTransactionFloor && status.isNotified()) {
                aggregate(status, false);
                if (previous == null) {
                    _aborted = next;
                } else {
                    previous.setNext(next);
                }
                _abortedCount--;
                free(status);
            } else {
                previous = status;
            }
            status = next;
        }
        /*
         * Remove every long-running transaction whose commit timestamp is no
         * longer concurrent with any active transaction.
         */
        previous = null;
        for (TransactionStatus status = _longRunning; status != null;) {
            final TransactionStatus next = status.getNext();
            if (status.isNotified() && isCommitted(status) && isObsolete(status)) {
                aggregate(status, true);
                if (previous == null) {
                    _longRunning = next;
                } else {
                    previous.setNext(next);
                }
                _longRunningCount--;
                free(status);
            } else {
                previous = status;
            }
            status = next;
        }
    }

    int resetMVVCounts(final long timestamp) {
        int count = 0;
        for (TransactionStatus status = _current; status != null;) {
            if (status.getTc() == ABORTED && status.getTs() < timestamp && status.getMvvCount() > 0) {
                status.setMvvCount(0);
                count++;

            }
            status = status.getNext();
        }
        for (TransactionStatus status = _aborted; status != null;) {
            assert status.getTc() == ABORTED;
            if (status.getTs() < timestamp && status.getMvvCount() > 0) {
                status.setMvvCount(0);
                count++;

            }
            status = status.getNext();
        }
        return count;
    }

    boolean isEmpty() {
        return _abortedCount + _currentCount + _longRunningCount == 0;
    }

    private boolean isAborted(final TransactionStatus status) {
        return status.getTc() == ABORTED && status.isNotified();
    }

    private boolean isCommitted(final TransactionStatus status) {
        return status.getTc() > 0 && status.getTc() != UNCOMMITTED && status.isNotified();
    }

    private boolean isObsolete(final TransactionStatus status) {
        return status.getTc() < _activeTransactionFloor;
    }

    private void aggregate(final TransactionStatus status, final boolean committed) {
        assert _lock.isHeldByCurrentThread();

        for (Delta delta = status.takeDelta(); delta != null; delta = freeDelta(delta)) {
            if (committed) {
                delta.getAccumulator().aggregate(_hashIndex, delta);
            }
            delta.setAccumulator(null);
        }
    }

    /**
     * Compute and return the snapshot value of an Accumulator
     * 
     * @throws RetryException
     *             if a TransactionStatus started but had not finished
     *             committing.
     * @throws InterruptedException
     */
    long getAccumulatorSnapshot(final Accumulator accumulator, final long timestamp, final int step)
            throws RetryException, InterruptedException {
        assert _lock.isHeldByCurrentThread();
        long value = accumulator.getBucketValue(_hashIndex);
        value = accumulatorSnapshotHelper(_current, accumulator, timestamp, step, value);
        value = accumulatorSnapshotHelper(_longRunning, accumulator, timestamp, step, value);
        return value;
    }

    private long accumulatorSnapshotHelper(final TransactionStatus start, final Accumulator accumulator,
            final long timestamp, final int step, final long initialValue) throws RetryException, InterruptedException {
        long value = initialValue;
        for (TransactionStatus status = start; status != null; status = status.getNext()) {
            final long tc = status.getTc();
            if (status.getTs() == timestamp) {
                for (Delta delta = status.getDelta(); delta != null; delta = delta.getNext()) {
                    if (delta.getAccumulator() == accumulator && (delta.getStep() <= step)) {
                        value = accumulator.applyValue(value, delta.getValue());
                    }
                }
            } else if (tc > 0 && tc != UNCOMMITTED && tc < timestamp) {
                for (Delta delta = status.getDelta(); delta != null; delta = delta.getNext()) {
                    if (delta.getAccumulator() == accumulator) {
                        value = accumulator.applyValue(value, delta.getValue());
                    }
                }
            } else if (tc < 0 && tc != ABORTED && -tc < timestamp) {
                status.briefLock(TransactionIndex.SHORT_TIMEOUT);
                _transactionIndex.incrementAccumulatorSnapshotRetryCounter();
                throw RetryException.SINGLE;
            }
        }
        return value;
    }

    void checkpointAccumulatorSnapshots(final long timestamp) throws RetryException, InterruptedException {
        assert _lock.isHeldByCurrentThread();
        accumulatorCheckpointHelper(_current, timestamp);
        accumulatorCheckpointHelper(_longRunning, timestamp);
    }

    private void accumulatorCheckpointHelper(final TransactionStatus start, final long timestamp)
            throws RetryException, InterruptedException {
        for (TransactionStatus status = start; status != null; status = status.getNext()) {
            final long tc = status.getTc();
            if (tc > 0 && tc != UNCOMMITTED && tc < timestamp) {
                for (Delta delta = status.getDelta(); delta != null; delta = delta.getNext()) {
                    final Accumulator accumulator = delta.getAccumulator();
                    final long newValue = accumulator.applyValue(accumulator.getCheckpointTemp(), delta.getValue());
                    accumulator.setCheckpointTemp(newValue);
                }
            } else if (tc < 0 && tc != ABORTED && -tc < timestamp) {
                status.briefLock(TransactionIndex.SHORT_TIMEOUT);
                _transactionIndex.incrementAccumulatorCheckpointRetryCounter();
                throw RetryException.SINGLE;
            }
        }
    }

    private void free(final TransactionStatus status) {
        assert _lock.isHeldByCurrentThread();

        if (_freeCount < _transactionIndex.getMaxFreeListSize()) {
            status.setNext(_free);
            _free = status;
            _freeCount++;
        } else {
            _droppedCount++;
        }
    }

    private Delta freeDelta(final Delta delta) {
        final Delta next = delta.getNext();
        /*
         * If the free Delta list is already full then simply drop this one and
         * let it be garbage collected
         */
        if (_freeDeltaCount < _transactionIndex.getMaxFreeDeltaListSize()) {
            delta.setNext(_freeDeltaList);
            _freeDeltaList = delta;
            _freeDeltaCount++;
        }
        return next;
    }

    Delta allocateDelta() {
        final Delta delta = _freeDeltaList;
        if (delta != null) {
            _freeDeltaList = delta.getNext();
            _freeDeltaCount--;
            return delta;
        } else {
            return new Delta();
        }
    }

    @Override
    public String toString() {
        return String.format("<floor=%s current=[%s]\n    aborted=[%s]\n    long=[%s]\n    free=[%s]>",
                TransactionIndex.minMaxString(_floor), listString(_current), listString(_aborted),
                listString(_longRunning), listString(_free));
    }

    String listString(final TransactionStatus list) {
        final StringBuilder sb = new StringBuilder();
        for (TransactionStatus status = list; status != null; status = status.getNext()) {
            if (sb.length() != 0) {
                sb.append(",");
            }
            sb.append(status);
        }
        return sb.toString();
    }
}
