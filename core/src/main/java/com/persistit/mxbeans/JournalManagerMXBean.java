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

import com.persistit.exception.PersistitException;

@MXBean
public interface JournalManagerMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=JournalManager";

    /**
     * Version number for the journal file format defined in this edition. Will
     * change if the journal file format changes.
     */
    final static int VERSION = 2;

    /**
     * Default size of one journal file (10E9).
     */
    final static long DEFAULT_BLOCK_SIZE = 1000000000L;

    /**
     * Minimum permitted journal file size.
     */
    final static long MINIMUM_BLOCK_SIZE = 10000000L;

    /**
     * Maximum permitted journal file size.
     */
    final static long MAXIMUM_BLOCK_SIZE = 100000000000L;

    /**
     * Size at which a completely obsolete journal file can be eliminated.
     */
    final static long ROLLOVER_THRESHOLD = 4 * 1024 * 1024;

    /**
     * Default, minimum and maximum size of journal write buffer.
     */
    final static int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;
    final static int MINIMUM_BUFFER_SIZE = 65536;
    final static int MAXIMUM_BUFFER_SIZE = DEFAULT_BUFFER_SIZE * 10;

    /**
     * Default size of journal read buffer.
     */
    final static int DEFAULT_COPY_BUFFER_SIZE = 16 * 1024 * 1024;

    /**
     * Default time interval (in milliseconds) between calls to the
     * FileChannel.force() method.
     */
    final static long DEFAULT_FLUSH_INTERVAL = 100;

    /**
     * Default time interval (in milliseconds) between calls to the journal
     * copier method.
     */
    final static long DEFAULT_COPIER_INTERVAL = 10000;

    /**
     * Default value for maximum pages to be copied per cycle.
     */
    final static int DEFAULT_COPIES_PER_CYCLE = 1000;

    /**
     * Default time interval (in milliseconds) for logging repetitive I/O
     * exceptions on attempts to write to the journal. Prevents excessively
     * verbose log on repeated failures.
     */
    final static long DEFAULT_LOG_REPEAT_INTERVAL = 60000L;
    final static long MINIMUM_LOG_REPEAT_INTERVAL = 1000L;
    final static long MAXIMUM_LOG_REPEAT_INTERVAL = Long.MAX_VALUE;
    /**
     * Default threshold time in milliseconds for JournalManager
     * flush operations. If a flush operation takes longer than
     * this time, a WARNING message is written to the log.
     */
    final static long DEFAULT_SLOW_IO_ALERT_THRESHOLD = 2000L;
    final static long MINIMUM_SLOW_ALERT_THRESHOLD = 100L;
    final static long MAXIMUM_SLOW_ALERT_THRESHOLD = Long.MAX_VALUE;
    /**
     * Format expression defining the name of a journal file.
     */
    final static String PATH_FORMAT = "%s.%012d";

    /**
     * Default setting for number of pages in the page map before the urgency of
     * copying starts to increase.
     */
    final static int DEFAULT_PAGE_MAP_SIZE_BASE = 250000;

    final static int MAXIMUM_CONCURRENT_TRANSACTIONS = 10000;

    @Description("Number of unique pages currently stored in the journal")
    int getPageMapSize();

    @Description("Address of first record in the journal required for recovery")
    long getBaseAddress();

    @Description("Address of next record to be written")
    long getCurrentAddress();

    @Description("Maximum size of one journal file")
    long getBlockSize();

    @Description("True if copying of pages from the journal to their destination volumes is disabled")
    boolean isAppendOnly();

    @Description("True if copy-fast mode has be enabled")
    boolean isCopyingFast();
   
    @Description("True if copying of pages from the journal to their destination volumes is disabled")
    void setAppendOnly(boolean appendOnly);

    @Description("True if copy-fast mode has be enabled")
    void setCopyingFast(boolean fast);

    @Description("Interval between data flush cycles in milliseconds")
    long getFlushInterval();

    @Description("Interval between data flush cycles in milliseconds")
    void setFlushInterval(long flushInterval);

    @Description("Interval between page copying cycles")
    long getCopierInterval();

    @Description("Interval between page copying cycles")
    void setCopierInterval(long copierInterval);

    @Description("True if the journal has been closed")
    boolean isClosed();

    @Description("True if the JOURNAL_COPIER thread is currently active")
    boolean isCopying();

    @Description("File path where journal files are written")
    String getJournalFilePath();

    @Description("Total number of page images read from the journal")
    long getReadPageCount();

    @Description("Total number of page images written to the journal")
    long getJournaledPageCount();

    @Description("Total number of page images copied from the journal to their destination volumes")
    long getCopiedPageCount();

    @Description("System time when journal was first created")
    long getJournalCreatedTime();

    @Description("Timestamp value when the most recently valid checkpoint was created")
    long getLastValidCheckpointTimestamp();
    
    @Description("Current timestamp value")
    long getCurrentTimestamp();

    @Description("True to enable pruning of rolled-back transactions")
    void setRollbackPruningEnabled(boolean rollbackPruning);

    @Description("True to enable pruning of rolled-back transactions")
    boolean isRollbackPruningEnabled();

    @Description("Degree of urgency for copying pages: 0-10")
    int urgency();

    @Description("Flush all pending journal records to durable storage")
    void force() throws PersistitException;

    @Description("Perform accelerated page copying")
    void copyBack() throws Exception;

    @Description("String value of last Exception encountered by the JOURNAL_COPIER thread")
    String getLastCopierException();

    @Description("String value of last Exception encountered by the JOURNAL_FLUSHER thread")
    String getLastFlusherException();

    @Description("Interval between proposed checkpoints in milliseconds")
    long getCheckpointInterval();

    @Description("System time when the most recently valid checkpoint was created")
    long getLastValidCheckpointTimeMillis();
    
    @Description("Total number of transaction commit records written to the journal")
    long getTotalCompletedCommits();
    
    @Description("Total aggregate time spent waiting for durable commits in milliseconds")
    long getCommitCompletionWaitTime();

    @Description("Threshold in  milliseconds for warnings of long duration flush cycles")
    long getSlowIoAlertThreshold();

    @Description("Threshold in  milliseconds for warnings of long duration flush cycles")
    void setSlowIoAlertThreshold(long slowIoAlertThreshold);

}
