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

import javax.management.MXBean;

@MXBean
public interface CleanupManagerMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=CleanupManager";

    /**
     * @return the number of <code>CleanupAction</code>s enqueued since
     *         Persistit started.
     */
    @Description("The number of CleanupAction items enqueud since Persistit started")
    public long getAcceptedCount();

    /**
     * @return the number of <code>CleanupAction</code>s rejected due to a full
     *         CleanupManager queue since Persistit started.
     */
    @Description("The number of CleanupAction items rejected due to a full queue since Persistit started")
    public long getRefusedCount();

    /**
     * @return the number of <code>CleanupAction</code>s successfully completed
     *         since Persistit started.
     */
    @Description("The number of CleanupAction items successfully completed since Persistit started")
    public long getPerformedCount();

    /**
     * @return the number of <code>CleanupAction</code>s that failed due to
     *         errors since Persistit started.
     */
    @Description("The number of CleanupAction items that failed due to errors since Persistit started")
    public long getErrorCount();

    /**
     * @return the number of <code>CleanupAction</code>s currently enqueued.
     */
    @Description("The number ofCleanupAction items currently enqueued")
    public long getEnqueuedCount();

    /**
     * If there are any enqueued <code>CleanupAction</code>s, take some of them
     * from the queue and attempt to complete them.
     */
    @Description("Attempt to complete some enqueued CleanupAction items")
    public void poll() throws Exception;

    /**
     * Clear the queue of all pending <code>CleanupAction</code>. This should be
     * done only with knowledge and care.
     */
    @Description("Clear the queue of all pending CleanupAction items")
    public void clear();

    /**
     * @return The interval in milliseconds between cleanup polling actions
     */
    @Description("The interval in milliseconds between cleanup polling actions")
    public long getPollInterval();

    /**
     * Set the interval in milliseconds between cleanup polling actions
     * 
     * @param interval
     *            the interval
     */
    @Description("The interval in milliseconds between cleanup polling actions")
    public void setPollInterval(final long interval);

    /**
     * @return The minimum interval in milliseconds between attempts to enqueue
     *         a page needing to be pruned.
     */
    @Description("The minimum interval in milliseconds between attempts to enqueue a page needing to be pruned")
    public long getMinimumPruningDelay();

    /**
     * Set the minimum interval in milliseconds between attempts to enqueue a
     * page needing to be pruned.
     * 
     * @param delay
     */
    @Description("The minimum interval in milliseconds between attempts to enqueue a page needing to be pruned")
    public void setMinimumPruningDelay(long delay);

}
