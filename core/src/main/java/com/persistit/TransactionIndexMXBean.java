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

public interface TransactionIndexMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=TransactionIndex";

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
    public abstract long getActiveTransactionFloor();

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
    public abstract long getActiveTransactionCeiling();

    /**
     * Count of active transactions measured when
     * {@link #updateActiveTransactionCache()} was last called. The count may
     * have changed to due new transactions starting or existing transactions
     * committing since that invocation, and therefore the value returned by
     * this method is an estimate.
     * 
     * @return the count
     */
    public abstract long getActiveTransactionCount();

    /**
     * Refresh the ActiveTransactionCache. This method walks the hashTable to
     * update the non-current ActiveTransactionCache instance and then makes it
     * current. This method will block until it can acquire an exclusive lock on
     * _atCacheLock. Therefore there can never be more than one thread
     * performing this method at a time.
     */
    public abstract void updateActiveTransactionCache();

    /**
     * Invoke the {@link TransactionIndexBucket#cleanup()} method on each bucket
     * to remove all obsolete long-running and aborted
     * <code>TransactionStatus</code> instances. This is useful to generate a
     * canonical state for unit tests. Cleanup logic is normally called during
     * the {@link TransactionIndexBucket#reduce()} process.
     */
    public abstract void cleanup();

    /**
     * @return The approximate count of TransactionStatus instances on the
     *         current list.
     */
    public abstract int getCurrentCount();

    /**
     * @return The approximate count of TransactionStatus instances on the
     *         long-running list.
     */
    public abstract int getLongRunningCount();

    /**
     * @return The approximate count of TransactionStatus instances on the
     *         aborted list.
     */
    public abstract int getAbortedCount();

    /**
     * @return The approximate count of TransactionStatus instances on the free
     *         list.
     */
    public abstract int getFreeCount();

    /**
     * @return The approximate count of TransactionStatus instances that have
     *         been dropped.
     */
    public abstract int getDroppedCount();

}