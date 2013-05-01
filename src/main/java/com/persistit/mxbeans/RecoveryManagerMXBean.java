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
 * Management interface for the <code>RecoveryManager</code>. Recovery is
 * performed during the {@link com.persistit.Persistit#initialize} method.
 * Recovery after a graceful shutdown is typically very fast, and the elements
 * of this class represent its final state. However, recovery after a crash
 * potentially requires a large number of transactions to be replayed, and this
 * class can be used to observer the process.
 */
@MXBean
public interface RecoveryManagerMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=RecoveryManager";

    public final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;

    /**
     * Number of transactions to apply per progress log message
     */
    final static int APPLY_TRANSACTION_LOG_COUNT = 1000;

    public String getJournalFilePath();

    public int getCommittedCount();

    public int getUncommittedCount();

    public int getAppliedTransactionCount();

    public int getErrorCount();

    public long getLastValidCheckpointTimestamp();

    public long getLastValidCheckpointAddress();

    public String getRecoveryEndedException();

    public long getRecoveryEndedAddress();

    public long getKeystoneAddress();

    public long getBaseAddress();

    public long getBlockSize();

    public long getJournalCreatedTime();

    public int getTransactionMapSize();

    public int getPageMapSize();

}
