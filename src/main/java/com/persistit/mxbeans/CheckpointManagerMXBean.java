/**
 * Copyright 2012 Akiban Technologies, Inc.
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
