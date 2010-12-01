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
