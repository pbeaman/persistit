package com.persistit.newtxn;
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


import java.util.ArrayList;
import java.util.List;

import com.persistit.Transaction;
import com.persistit.exception.TimeoutException;

public interface TxnIndex {

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
     * @throws TimeoutException
     *             if the timeout interval is exceeded
     * @throws InterruptedException
     *             if the waiting thread is interrupted
     * @param timeout
     *            Time in milliseconds to wait. If the <code>to</code>
     *            transaction has neither committed nor aborted within this time
     *            interval then a TimeoutException is thrown.
     * @return the commit status
     */
    long commitStatus(long versionHandle, long ts, long timeout) throws TimeoutException, InterruptedException;

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
    long registerTransaction(Transaction txn);

    /**
     * Notify the TransactionIndex that the specified Transaction has committed
     * or aborted. This method allows the TransactionIndex to awaken any threads
     * waiting for resolution of commit status or a write-write dependency.
     * 
     * @param txn
     */
    void notifyCompleted(Transaction txn);

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
    boolean hasConcurrentTransaction(long ts1, long ts2);

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
    boolean wwDependency(long from, long to, long timeout) throws TimeoutException, InterruptedException;

    static TxnIndex index = new TxnIndex() {

        @Override
        public long commitStatus(long versionHandle, long ts, long timeout) {
            return 0;
        }

        @Override
        public long registerTransaction(Transaction txn) {
            return 0;
        }

        @Override
        public void notifyCompleted(Transaction txn) {
        }

        @Override
        public boolean hasConcurrentTransaction(long ts1, long ts2) {
            return false;
        }

        @Override
        public boolean wwDependency(long from, long to, long timeout) {
            return false;
        }

    };

    static List<Long> versions = new ArrayList<Long>(0);

    final static long ABORTED = -1;
    final static long CONCURRENT = Long.MAX_VALUE;

    // Pseudo-code for reading a value version from a multi-version value
    class Reader {

        long myValue(long ts, long timeout) throws TimeoutException, InterruptedException {
            // Note: in reverse order, most recently appended first
            for (long versionHandle : versions) {
                long status = index.commitStatus(versionHandle, ts, timeout);
                if (status != ABORTED && status != CONCURRENT) {
                    return versionHandle;
                }
            }
            return -1;
        }
    }

    // Pseudo-code for pruning value versions from a multi-version value
    class Pruner {

        void prune(long timeout) throws TimeoutException, InterruptedException {
            long vcommit = -1;
            int versionCount = 0;

            for (long versionHandle : versions) {
                long status = index.commitStatus(versionHandle, Long.MAX_VALUE, timeout);
                if (status == ABORTED) {
                    // remove versionHandle version
                } else if (status != CONCURRENT) {
                    if (vcommit != -1) {
                        if (!index.hasConcurrentTransaction(vcommit, versionHandle)) {
                            // remove vcommit version
                        }
                        vcommit = versionHandle;
                        versionCount++;
                    }
                } else {
                    versionCount = Integer.MAX_VALUE;
                    break;
                }
            }
            if (versionCount == 0) {
                // write anti-value in pvalue form.
            } else if (versionCount == 1) {
                // write version in pvalue form
            } else {
                // retain multi-value form
            }
        }
    }
}
