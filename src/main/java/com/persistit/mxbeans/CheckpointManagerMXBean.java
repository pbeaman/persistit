/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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
public interface CheckpointManagerMXBean {

    public final static long DEFAULT_CHECKPOINT_INTERVAL_S = 120;

    public final static long MINIMUM_CHECKPOINT_INTERVAL_S = 10;

    public final static long MAXIMUM_CHECKPOINT_INTERVAL_S = 3600;

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=CheckpointManager";

    @Description("Checkpoint most recently proposed")
    String getProposedCheckpoint();

    @Description("Checkpoint polling interval in seconds")
    long getCheckpointInterval();

    @Description("Checkpoint polling interval in seconds")
    void setCheckpointInterval(long interval);

    @Description("Number of proposed checkpoints that have not been confirmed")
    int getOutstandingCheckpointCount();

    @Description("Report of proposed checkpoints that have not been confirmed")
    String outstandingCheckpointReport();

}
