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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sketch of an object that could sit at the root of a TransactionIndex hash
 * table bucket.
 * 
 * @author peter
 * 
 */
public class TransactionIndexBucket {
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
    volatile int _maxFreeListSize;
    /**
     * Floor timestamp. A transaction that started before this time is usually
     * committed, in which case there no longer exists a TransactionStatus
     * object for it. There are two other cases: the transaction is still
     * running, in which its TransactionStatus has been moved to the
     * long_running list, or it aborted, in which case its TransactionStatus has
     * moved to the aborted list.
     */
    volatile long _floor;
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

    long _highestTimestamp;
    /**
     * Lock used to prevent multi-threaded access to the lists in this
     * structure.
     */
    ReentrantLock _lock = new ReentrantLock();

    void lock() {
        _lock.lock();
    }

    void unlock() {
        _lock.unlock();
    }

    TransactionStatus allocateTransactionStatus() {
        assert _lock.isHeldByCurrentThread();
        TransactionStatus status = _free;
        if (status != null) {
            _free = status.getNext();
            _freeCount--;
            status.setNext(null);
            return status;
        } else {
            return new TransactionStatus();
        }
    }

    void releaseTransactionStatus(final TransactionStatus status) {
        assert _lock.isHeldByCurrentThread();
        status.setNext(_free);
        _free = status;
        _freeCount++;
    }

    void addCurrent(final TransactionStatus status) {
        assert _lock.isHeldByCurrentThread();
        status.setNext(_current);
        _current = status;
        _currentCount++;
        _highestTimestamp = status.getTs();
        if (_currentCount > _longRunningThreshold) {
            reduce();
        }
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

    /**
     * Traverse TransactionStatus instances on the current list and set the
     * floor value to the smallest start timestamp of any of them. See
     * {@link #reduce()}.
     */
    void recomputeFloor() {
        assert _lock.isHeldByCurrentThread();
        long newFloor = _highestTimestamp + 1;
        for (TransactionStatus status = _current; status != null; status = status.getNext()) {
            if (status.getTs() < newFloor) {
                newFloor = status.getTs();
            }
        }
        _floor = newFloor;
    }

    /**
     * Reduce the size of the {@link #_current} list by moving the oldest
     * transaction to the {@link #_aborted} list, {@link #_longRunning} list or
     * {@link #_free} list, and then increasing the {@link #_floor} to reflect
     * the start timestamp of the next oldest remaining transaction. If this
     * method determines that the next oldest transaction is either committed or
     * aborted, it repeats the process and disposes of that transaction as well.
     */
    void reduce() {
        assert _lock.isHeldByCurrentThread();
        long newFloor = _highestTimestamp + 1;
        boolean more = true;
        while (more) {
            more = false;
            TransactionStatus previous = null;
            for (TransactionStatus status = _current; status != null; status = status.getNext()) {
                assert status.getTs() >= _floor;
                final boolean aborted = status.getTc() == ABORTED;
                final boolean uncommitted = status.getTc() == UNCOMMITTED;
                final boolean committed = status.getTc() > 0 && !uncommitted;
                if (status.getTs() == _floor) {
                    if (aborted || committed || uncommitted && _currentCount > _maxFreeListSize) {
                        /*
                         * Essential invariant: the TransactionStatus is added
                         * to its new list before being removed from _current.
                         * Concurrently executing threads can read the _current,
                         * _aborted and _longRunning members outside of locks.
                         * (Currently they do not traverse the linked lists,
                         * however.)
                         */
                        final TransactionStatus next = status.getNext();
                        /*
                         * An aborted transaction only needs to be retained if
                         * there are outstanding MVV values it modified.
                         * Otherwise the status can be freed.
                         */
                        if (aborted && status.getMvvCount() > 0) {
                            status.setNext(_aborted);
                            _aborted.setNext(status);
                            _abortedCount++;
                        } else if (uncommitted) {
                            status.setNext(_longRunning);
                            _longRunning = status;
                            _longRunningCount++;
                        } else if (_freeCount < _maxFreeListSize) {
                            status.setNext(_free);
                            _free = status;
                            _freeCount++;
                        } else {
                            /**
                             * Simply let this TransactionStatus go away and be
                             * garbage collected.
                             */
                        }
                        /*
                         * Unlike the TransactionStatus from the current list.
                         */
                        if (previous != null) {
                            previous.setNext(next);
                        } else {
                            _current.setNext(next);
                        }
                        _currentCount--;
                    }
                } else if (status.getTs() < newFloor) {
                    newFloor = status.getTs();
                    more = aborted || committed;
                }
                previous = status;
            }
            _floor = newFloor;
        }
    }
}
