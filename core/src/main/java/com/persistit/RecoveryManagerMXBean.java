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

import javax.management.MXBean;

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
