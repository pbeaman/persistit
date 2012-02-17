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
