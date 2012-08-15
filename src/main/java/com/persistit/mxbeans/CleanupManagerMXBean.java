/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.mxbeans;

import javax.management.MXBean;

/**
 * Management information about the CLEANUP_MANAGER, a thread that performs
 * background pruning and other cleanup tasks.
 */
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
