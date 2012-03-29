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

import java.io.IOException;
import java.util.Collections;

import javax.management.MXBean;

import com.persistit.CleanupManager.CleanupAction;
import com.persistit.exception.PersistitException;

@MXBean
public interface CleanupManagerMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=CleanupManager";

    /**
     * @return the number of {@link CleanupAction}s successfully submitted since
     *         Persistit started.
     */
    public long getAcceptedCount();

    /**
     * @return the number of {@link CleanupActions}s rejected due to a full
     *         queue since Persistit started.
     */
    public long getRefusedCount();

    /**
     * @return the number of {@link CleanupActions}s successfully completed
     *         since Persistit started.
     */
    public long getPerformedCount();

    /**
     * @return the number of {@link CleanupActions}s that failed since Persistit
     *         started.
     */
    public long getErrorCount();

    /**
     * @return the number of {@link CleanupActions}s currently enqueued.
     */
    public long getEnqueuedCount();

    /**
     * If there are any enqueued {@link CleanupActions}s, take some of them from
     * the queue and attempt to complete them.
     */
    public void poll() throws Exception;

    /**
     * Clear the queue of all pending {@link CleanupAction}. This should be done
     * only with knowledge and care.
     */
    public void clear();
    
    public long getPollInterval();
    
    public void setPollInterval(final long interval);
    
    /**
     * @return The minimum interval in milliseconds between attempts to enqueue a page needing to be
     * pruned to the {@link CleanupManager}.
     */
    public long getMinimumPruningDelay();
    
    /**
     * Set the minimum interval in milliseconds between attempts to enqueue a page needing to be
     * pruned to the {@link CleanupManager}.
     * @param delay
     */
    public void setMinimumPruningDelay(long delay);

}
