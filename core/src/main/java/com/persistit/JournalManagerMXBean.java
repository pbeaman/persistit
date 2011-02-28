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

import java.util.regex.Pattern;

import javax.management.MXBean;

import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;

@MXBean
public interface JournalManagerMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=JournalManager";

    /**
     * Version number for the journal file format defined in this edition. Will
     * change if the journal file format changes.
     */
    final static int VERSION = 1;

    /**
     * Default size of one journal file (10^9).
     */
    public final static long DEFAULT_BLOCK_SIZE = 1000000000L;

    /**
     * Minimum permitted journal file size.
     */
    public final static long MINIMUM_BLOCK_SIZE = 10000000L;

    /**
     * Maximum permitted journal file size.
     */
    public final static long MAXIMUM_BLOCK_SIZE = 100000000000L;

    /**
     * Size at which a completely obsolete journal file can be eliminated.
     */
    public final static long ROLLOVER_THRESHOLD = 4 * 1024 * 1024;

    /**
     * Default size of journal write buffer.
     */
    public final static int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;

    /**
     * Default size of journal read buffer.
     */
    public final static int DEFAULT_READ_BUFFER_SIZE = 64 * 1024;

    /**
     * Default time interval (in milliseconds) between calls to the
     * FileChannel.force() method.
     */
    public final static long DEFAULT_FLUSH_INTERVAL = 100;

    /**
     * Default time interval (in milliseconds) between calls to the journal
     * copier method.
     */
    public final static long DEFAULT_COPIER_INTERVAL = 10000;

    /**
     * Default value for maximum pages to be copied per cycle.
     */
    public final static int DEFAULT_COPIES_PER_CYCLE = 1000;

    /**
     * Default time interval (in milliseconds) for logging repetitive I/O
     * exceptions on attempts to write to the journal. Prevents excessively
     * verbose log on repeated failures.
     */
    public final static long DEFAULT_LOG_REPEAT_INTERVAL = 60000000000L;

    /**
     * Format expression defining the name of a journal file.
     */
    final static String PATH_FORMAT = "%s.%012d";

    /**
     * REGEX expression that recognizes the name of a journal file.
     */
    final static Pattern PATH_PATTERN = Pattern.compile(".+\\.(\\d{12})");

    /**
     * Default setting for number of pages in the page map before the urgency of
     * copying starts to increase.
     */
    final static int DEFAULT_PAGE_MAP_SIZE_BASE = 250000;

    public int getPageMapSize();

    public long getBaseAddress();

    public long getCurrentAddress();

    public long getBlockSize();

    public boolean isAppendOnly();

    public boolean isCopyingFast();

    public void setAppendOnly(boolean appendOnly);

    public void setCopyingFast(boolean fast);

    public long getFlushInterval();

    public void setFlushInterval(long flushInterval);

    public long getCopierInterval();

    public void setCopierInterval(long copierInterval);

    public boolean isClosed();

    public boolean isCopying();

    public String getJournalFilePath();

    public long getJournaledPageCount();

    public long getCopiedPageCount();

    public long getJournalCreatedTime();

    public long getLastValidCheckpointTimestamp();

    public int urgency();

    public void force() throws PersistitIOException;

    public void copyBack(final long toTimestamp) throws PersistitException;
}
