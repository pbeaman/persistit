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
