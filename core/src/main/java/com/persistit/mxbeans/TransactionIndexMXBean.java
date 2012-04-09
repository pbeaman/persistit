/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit.mxbeans;

public interface TransactionIndexMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=TransactionIndex";

    /**
     * Timestamp known to be less than or equal to the start timestamp of any
     * currently executing transaction. This value is computed by
     * <code>updateActiveTransactionCache<code> and is therefore less the
     * timestamp of any currently executing transaction at the instant that
     * method was called. It is guaranteed that no running transaction
     * has a lower start timestamp.
     * 
     * @return Lower bound on start timestamps of currently active transactions.
     */
    public abstract long getActiveTransactionFloor();

    /**
     * Timestamp recorded at the start of the last invocation of
     * <code>updateActiveTransactionCache<code>. Any transaction newer than 
     * this ceiling is currently considered active even if it has
     * already committed or aborted.
     * 
     * @return Upper bound on timestamps for which
     *         {@link #hasConcurrentTransaction(long, long)} returns accurate
     *         information.
     */
    public abstract long getActiveTransactionCeiling();

    /**
     * Count of active transactions measured when
     * <code>updateActiveTransactionCache<code> was last called. The count may
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