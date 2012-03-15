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

import java.nio.channels.FileChannel;
import java.util.List;

import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;

/**
 * Accessors for package-private methods in com.persistit so that unit tests can
 * be in a different package.
 * 
 * @author peter
 * 
 */
public class TestShim {

    public final static int BUFFER_INDEX_PAGE_OVERHEAD = Buffer.INDEX_PAGE_OVERHEAD;

    public static void rollover(final JournalManager journalManager) throws PersistitIOException {
        journalManager.rollover();
    }

    public static int maxStorableKeySize(final int bufferSize) {
        return Key.maxStorableKeySize(bufferSize);
    }

    public static int maxStorableValueSize(final Exchange ex, final int keySize) {
        return ex.maxValueSize(keySize);
    }

    public static int maxKeys(final Volume volume) {
        return volume.getPool().getMaxKeys();
    }

    public static Buffer buffer(final Volume volume, final long page) throws Exception {
        return volume.getPool().get(volume, page, false, true);
    }

    public static TimestampAllocator timestampAllocator(final Persistit persistit) {
        return persistit.getTimestampAllocator();
    }

    public static CleanupManager cleanupManager(final Persistit persistit) {
        return persistit.getCleanupManager();
    }

    public static void closeTransaction(Transaction t) throws PersistitException {
        t.close();
    }

    public static FileChannel getVolumeChannel(Volume volume) throws PersistitException {
        return volume.getStorage().getChannel();
    }

    public static void ignoreMVCC(final boolean doIgnore, final Exchange ex) {
        ex.ignoreMVCCFetch(doIgnore);
    }

    public static long allocateCheckpointTimestamp(final Persistit persistit) {
        return persistit.getTimestampAllocator().allocateCheckpointTimestamp();
    }

    public static void prune(final Exchange ex) throws PersistitException {
        ex.prune();
    }

    public static void flushTransactionBuffer(final Transaction txn, final boolean chain) throws PersistitException {
        txn.flushTransactionBuffer(chain);
    }

    public static void setMinimumPruningDelay(final Persistit persistit, final long delay) {
        persistit.getCleanupManager().setMinimumPruningDelay(delay);
    }

    public static SessionId newSessionId() {
        return new SessionId();
    }

    public static void flushBuffers(final Persistit persistit, final long timestamp)
            throws PersistitInterruptedException {
        for (final BufferPool pool : persistit.getBufferPoolHashMap().values()) {
            pool.flush(timestamp);
        }
    }

    public static void copyPages(final JournalManager jman) throws Exception {
        jman.copyBack();
    }
    
    public static Exchange directoryExchange(final Volume volume) {
        return volume.getStructure().directoryExchange();
    }
    
    public static boolean isValueLongRecord(final Exchange ex) throws PersistitException {
        return ex.isValueLongRecord();
    }
    
    public static List<Long> getGarbageList(final Volume volume) throws PersistitException {
        return volume.getStructure().getGarbageList();
    }
}
