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

package com.persistit;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.persistit.exception.BufferSizeUnavailableException;
import com.persistit.exception.InvalidKeyException;
import com.persistit.exception.PersistitException;
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

    public static void rollover(final JournalManager journalManager) throws PersistitException {
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

    public static void closeTransaction(final Transaction t) throws PersistitException {
        t.close();
    }

    public static FileChannel getVolumeChannel(final Volume volume) throws PersistitException {
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

    public static Exchange directoryExchange(final Volume volume) throws BufferSizeUnavailableException {
        return volume.getStructure().directoryExchange();
    }

    public static boolean isValueLongRecord(final Exchange ex) throws PersistitException {
        return ex.isValueLongRecord();
    }

    public static void testValidForAppend(final Key key) {
        key.testValidForAppend();
    }

    public static void testValidForStoreAndFetch(final Key key, final int bufferSize) throws InvalidKeyException {
        key.testValidForStoreAndFetch(bufferSize);
    }

    public static void testValidForTraverse(final Key key) throws InvalidKeyException {
        key.testValidForTraverse();
    }

    public static void nudgeDeeper(final Key key) {
        key.nudgeDeeper();
    }

    public static void nudgeLeft(final Key key) {
        key.nudgeLeft();
    }

    public static void nudgeRight(final Key key) {
        key.nudgeRight();
    }

    public static Task parseTask(final Persistit persistit, final String line) throws Exception {
        return CLI.parseTask(persistit, line);
    }

    public static int storeVersion(final byte[] target, final int targetOffset, final int targetLength,
            final int targetLimit, final long versionHandle, final byte[] source, final int sourceOffset,
            final int sourceLength) {
        return MVV.storeVersion(target, targetOffset, targetLength, targetLimit, versionHandle, source, sourceOffset,
                sourceLength);
    }

    public static void setClassIndexTestIdFloor(final Persistit persistit, final int id) {
        persistit.getClassIndex().setTestIdFloor(id);
    }

    public static void clearAllClassIndexEntries(final Persistit persistit) throws PersistitException {
        persistit.getClassIndex().clearAllEntries();
    }

    public static ByteBuffer getTransactionBuffer(final Transaction txn) {
        return txn.getTransactionBuffer();
    }
}
