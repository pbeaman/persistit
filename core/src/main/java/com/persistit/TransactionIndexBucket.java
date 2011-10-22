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

import java.util.concurrent.locks.ReentrantLock;

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
 * cases, and are expected to remain relatively short. The
 * <code>TransactionStatus</code> for a committed transaction (the usual case)
 * is simply moved to the free list once the floor is raised.
 * </p>
 * 
 * @author peter
 * 
 */
public class TransactionIndexBucket {
    /**
     * The owner of the hash table that contains this bucket
     */
    final TransactionIndex _transactionIndex;

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
     * Lock used to prevent multi-threaded access to the lists in this
     * structure.
     */
    ReentrantLock _lock = new ReentrantLock();

    TransactionIndexBucket(final TransactionIndex index) {
        _transactionIndex = index;
    }

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
            return new TransactionStatus(this);
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
        if (status.getTs() < _floor) {
            _floor = status.getTs();
        }
        _current = status;
        _currentCount++;
        if (_currentCount > _transactionIndex.getLongRunningThreshold()) {
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

    int getDroppedCount() {
        return _droppedCount;
    }

    TimestampAllocator getTimestampAllocator() {
        return _transactionIndex.getTimestampAllocator();
    }

    TransactionStatus notifyCompleted(final long ts) {
        assert _lock.isHeldByCurrentThread();
        if (ts >= getFloor()) {
            for (TransactionStatus status = getCurrent(); status != null; status = status.getNext()) {
                if (status.getTs() == ts) {
                    status.complete();
                    if (ts == getFloor()) {
                        reduce();
                    }
                    return status;
                }
            }
        }
        TransactionStatus previous = null;
        for (TransactionStatus status = getLongRunning(); status != null; status = status.getNext()) {
            if (status.getTs() == ts) {
                TransactionStatus next = status.getNext();
                assert status.getTc() != UNCOMMITTED;
                status.complete();
                if (status.getTc() == ABORTED) {
                    status.setNext(_aborted);
                    _aborted = status;
                    _abortedCount++;
                } else if (_freeCount < _transactionIndex.getMaxFreeListSize()) {
                    status.setNext(_free);
                    _free = status;
                    _freeCount++;
                } else {
                    _droppedCount++;
                }
                if (previous == null) {
                    _longRunning = next;
                } else {
                    previous.setNext(next);
                }
                _longRunningCount--;
                return status;
            }
            previous = status;
        }
        throw new IllegalStateException("No such transaction  " + ts);
    }

    /**
     * Remove obsolete <code>TransactionStatus</code> instances from the aborted
     * list.
     * 
     * @return
     */
    void pruned() {
        assert _lock.isHeldByCurrentThread();
        TransactionStatus previous = null;
        for (TransactionStatus status = _aborted; status != null;) {
            final TransactionStatus next = status.getNext();
            if (status.getMvvCount() == 0 && _transactionIndex.getActiveTransactionFloor() > status.getTa()) {
                if (previous == null) {
                    _aborted = status.getNext();
                } else {
                    previous.setNext(status.getNext());
                }
                _abortedCount--;
                if (_freeCount < _transactionIndex.getMaxFreeListSize()) {
                    status.setNext(_free);
                    _free = status;
                    _freeCount++;
                }
            } else {
                previous = status;
            }
            status = next;
        }
    }

    /**
     * Traverse TransactionStatus instances on the current list and set the
     * floor value to the smallest start timestamp of any of them. See
     * {@link #reduce()}.
     */
    void recomputeFloor() {
        assert _lock.isHeldByCurrentThread();
        long newFloor = Long.MAX_VALUE;
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
        long newFloor = Long.MAX_VALUE;
        boolean more = true;
        while (more) {
            more = false;
            TransactionStatus previous = null;
            for (TransactionStatus status = _current; status != null;) {
                final TransactionStatus next = status.getNext();
                assert status.getTs() >= _floor;

                /*
                 * Is this TransactionStatus aborted and notified?
                 */
                final boolean aborted = status.getTc() == ABORTED && status.isNotified();
                /*
                 * Is this TransactionStatus eligible to be freed? It is only if
                 * it fully committed and there are no longer any concurrent
                 * transactions.
                 */
                boolean eligible = status.getTc() > 0 && status.getTc() != UNCOMMITTED && status.isNotified()
                        && status.getTc() < _transactionIndex.getActiveTransactionFloor();

                if (status.getTs() == _floor) {
                    /*
                     * Move the TransactionStatus somewhere if any of these
                     * conditions hold
                     */
                    if (aborted || eligible || _currentCount > _transactionIndex.getLongRunningThreshold()) {
                        /*
                         * Essential invariant: the TransactionStatus is added
                         * to its new list before being removed from _current.
                         * Concurrently executing threads can read the _current,
                         * _aborted and _longRunning members outside of locks.
                         * (Currently they do not traverse the linked lists,
                         * however.)
                         */
                        if (aborted) {
                            /*
                             * An aborted transaction only needs to be retained
                             * if there are outstanding MVV values it modified
                             * or it is concurrent with other active
                             * transactions. Otherwise the status can be freed.
                             */
                            if (status.getMvvCount() > 0
                                    || status.getTa() > _transactionIndex.getActiveTransactionFloor()) {
                                status.setNext(_aborted);
                                _aborted = status;
                                _abortedCount++;
                            } else {
                                eligible = true;
                            }
                        } else if (!eligible) {
                            /*
                             * If not aborted and not eligible to be freed, then
                             * move the TransactionStatus to the longRunning
                             * list.
                             */
                            status.setNext(_longRunning);
                            _longRunning = status;
                            _longRunningCount++;
                        }
                        /*
                         * If this TransactionStatus can be freed then add it to
                         * the free list unless the free list is already full.
                         * (This could happen if there is a temporary condition
                         * in which more than maxFreeListSize transactions are
                         * currently active for this bucket.)
                         */
                        if (eligible) {
                            if (_freeCount < _transactionIndex.getMaxFreeListSize()) {
                                status.setNext(_free);
                                _free = status;
                                _freeCount++;
                            } else {
                                _droppedCount++;
                            }
                        } else {
                            /*
                             * Simply let this TransactionStatus go away and be
                             * garbage collected.
                             */
                        }
                        /*
                         * Unlink the TransactionStatus from the current list.
                         */
                        if (previous != null) {
                            previous.setNext(next);
                        } else {
                            _current = next;
                            if (next == null) {
                                _floor = Long.MAX_VALUE;
                                more = false;
                            }
                        }
                        _currentCount--;
                        status = next;
                    } else {
                        newFloor = _floor;
                        previous = status;
                    }
                } else if (status.getTs() < newFloor) {
                    newFloor = status.getTs();
                    more = aborted || eligible;
                    previous = status;
                }
                status = next;
            }
            _floor = newFloor;
        }
    }

    @Override
    public String toString() {
        return String.format("<floor=%s current=[%s] aborted=[%s] long=[%s] free=[%s]>", floorString(_floor),
                listString(_current), listString(_aborted), listString(_longRunning), listString(_free));
    }

    String floorString(final long floor) {
        return floor == Long.MAX_VALUE ? "MAX" : String.format("%,d", floor);
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
