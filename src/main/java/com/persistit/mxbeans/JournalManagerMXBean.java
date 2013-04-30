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

import com.persistit.exception.PersistitException;

/**
 * Management interface to the <code>JournalManager</code>, including journal
 * file names and file positions. Of particular interest are
 * {@link #getCurrentAddress()} and {@link #getBaseAddress()}. The difference
 * between these represents the degree to which JOURNAL_COPIER has fallen behind
 * recent updates.
 * 
 * @author peter
 * 
 */
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
    final static long DEFAULT_FLUSH_INTERVAL_MS = 100;

    /**
     * Default time interval (in milliseconds) between calls to the journal
     * copier method.
     */
    final static long DEFAULT_COPIER_INTERVAL_MS = 10000;
    /**
     * Default journal file count at which transactions are throttled to allow
     * copier to catch up.
     */
    final static int DEFAULT_URGENT_FILE_COUNT_THRESHOLD = 15;
    final static int MINIMUM_URGENT_FILE_COUNT_THRESHOLD = 5;
    final static int MAXIMUM_URGENT_FILE_COUNT_THRESHOLD = 100;
    /**
     * Default value for maximum pages to be copied per cycle.
     */
    final static int DEFAULT_COPIES_PER_CYCLE = 1000;

    /**
     * Default time interval (in milliseconds) for logging repetitive I/O
     * exceptions on attempts to write to the journal. Prevents excessively
     * verbose log on repeated failures.
     */
    final static long DEFAULT_LOG_REPEAT_INTERVAL_MS = 60000L;
    final static long MINIMUM_LOG_REPEAT_INTERVAL_MS = 1000L;
    final static long MAXIMUM_LOG_REPEAT_INTERVAL_MS = Long.MAX_VALUE;
    /**
     * Default threshold time in milliseconds for JournalManager flush
     * operations. If a flush operation takes longer than this time, a WARNING
     * message is written to the log.
     */
    final static long DEFAULT_SLOW_IO_ALERT_THRESHOLD_MS = 2000L;
    final static long MINIMUM_SLOW_ALERT_THRESHOLD_MS = 100L;
    final static long MAXIMUM_SLOW_ALERT_THRESHOLD_MS = Long.MAX_VALUE;

    /**
     * File name appended when journal path specifies only a directory
     */
    final static String DEFAULT_JOURNAL_FILE_NAME = "persistit_journal";
    /**
     * Format expression defining the name of a journal file.
     */
    final static String PATH_FORMAT = "%s.%012d";

    final static int MAXIMUM_CONCURRENT_TRANSACTIONS = 10000;

    @Description("Number of transaction map items in the live map")
    int getLiveTransactionMapSize();

    @Description("Number of unique pages currently stored in the journal")
    int getPageMapSize();

    @Description("Number of unique page versions currently stored in the journal")
    int getPageListSize();

    @Description("Address of first record in the journal required for recovery")
    long getBaseAddress();

    @Description("Address of next record to be written")
    long getCurrentAddress();

    @Description("Maximum size of one journal file")
    long getBlockSize();

    @Description("True if copying of pages from the journal to their destination volumes is disabled")
    boolean isAppendOnly();

    @Description("True to allow journal to lose pages from missing volumes")
    boolean isIgnoreMissingVolumes();

    @Description("True if copy-fast mode has been enabled")
    boolean isCopyingFast();

    @Description("True if copying of pages from the journal to their destination volumes is disabled")
    void setAppendOnly(boolean appendOnly);

    @Description("True to allow journal to lose pages from missing volumes")
    void setIgnoreMissingVolumes(boolean ignore);

    @Description("True if copy-fast mode has been enabled")
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

    @Description("Total number of page images pages dropped from the journal due the existence of newer versions")
    long getDroppedPageCount();

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

    @Description("True to enable pruning when writing pages to journal")
    void setWritePagePruningEnabled(boolean rollbackPruning);

    @Description("True to enable pruning when writing pages to journal")
    boolean isWritePagePruningEnabled();

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

    @Description("Journal file count threshold for throttling transactions")
    int getUrgentFileCountThreshold();

    @Description("Journal file count threshold for throttling transactions")
    void setUrgentFileCountThreshold(int threshold);
}
