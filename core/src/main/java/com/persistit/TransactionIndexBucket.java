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
     * Singly-linked list of TransactionStatus instances for aborted transactions.
     * A TransactionStatus enters this list after the transaction has aborted; it
     * leaves this list when all of the MVV versions created by the transaction
     * have been pruned.
     */
    volatile TransactionStatus _aborted;
    /**
     * Singly-linked list of TransactionStatus instances for transactions that started
     * before the floor value.  A TransactionStatus enters this list when its
     * transaction has been running for a "long" time, meaning that the current list
     * has a large number of TransactionStatus instances that could be removed by increasing
     * the floor value.
     */
    volatile TransactionStatus _longRunning;
    /**
     * Singly-linked list of TransactionStatus instances that no longer represent
     * concurrent or recent transactions.  These are recycled when a new transaction
     * is registered.
     */
    volatile TransactionStatus _free;
    /**
     * Lock used to prevent multi-threaded access to the lists in this structure.
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
    }
    
    void addCurrent(final TransactionStatus status) {
        status.setNext(_current);
        _current = status;
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
    
}
