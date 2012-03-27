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

import com.persistit.exception.PersistitIOException;

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

    int getPageMapSize();

    long getBaseAddress();

    long getCurrentAddress();

    long getBlockSize();

    boolean isAppendOnly();

    boolean isCopyingFast();

    void setAppendOnly(boolean appendOnly);

    void setCopyingFast(boolean fast);

    long getFlushInterval();

    void setFlushInterval(long flushInterval);

    long getCopierInterval();

    void setCopierInterval(long copierInterval);

    boolean isClosed();

    boolean isCopying();

    String getJournalFilePath();

    long getReadPageCount();

    long getJournaledPageCount();

    long getCopiedPageCount();

    long getJournalCreatedTime();

    long getLastValidCheckpointTimestamp();
    
    long getCurrentTimestamp();

    void setRollbackPruningEnabled(boolean rollbackPruning);

    boolean isRollbackPruningEnabled();

    int urgency();

    void force() throws PersistitIOException;

    void copyBack() throws Exception;

    String getLastCopierException();

    String getLastFlusherException();

    long getCheckpointInterval();

    long getLastValidCheckpointTimeMillis();
    
    long getTotalCompletedCommits();
    
    long getCommitCompletionWaitTime();

    long getLogRepeatInterval();

    void setLogRepeatInterval(long logRepeatInterval);

    long getSlowIoAlertThreshold();

    void setSlowIoAlertThreshold(long slowIoAlertThreshold);

}
