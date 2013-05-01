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

package com.persistit.mxbeans;

import javax.management.MXBean;

/**
 * Management interface to the <code>TransactionIndex</code>. Statistics
 * provided here are generally only useful for diagnostic purposes.
 * 
 */
@MXBean
public interface TransactionIndexMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=TransactionIndex";

    /**
     * Timestamp known to be less than or equal to the start timestamp of any
     * currently executing transaction. This value is computed by
     * <code>updateActiveTransactionCache</code> and is therefore less the
     * timestamp of any currently executing transaction at the instant that
     * method was called. It is guaranteed that no running transaction has a
     * lower start timestamp.
     * 
     * @return Lower bound on start timestamps of currently active transactions.
     */
    public abstract long getActiveTransactionFloor();

    /**
     * Timestamp recorded at the start of the last invocation of
     * <code>updateActiveTransactionCache</code>. Any transaction newer than
     * this ceiling is currently considered active even if it has already
     * committed or aborted.
     * 
     * @return Upper bound on timestamps for which
     *         {@link com.persistit.TransactionIndex#hasConcurrentTransaction(long, long)
     *         hasConcurrentTransaction} returns accurate information.
     */
    public abstract long getActiveTransactionCeiling();

    /**
     * Count of active transactions measured when
     * <code>updateActiveTransactionCache</code> was last called. The count may
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
     * current. This method will acquires an exclusive lock on the
     * ActiveTransactionCache.
     */
    public abstract void updateActiveTransactionCache();

    /**
     * Remove all obsolete long-running and aborted
     * <code>TransactionStatus</code> instances. This is useful to generate a
     * canonical state for unit tests. Cleanup logic is normally invoke
     * automatically within a running system..
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
