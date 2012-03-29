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