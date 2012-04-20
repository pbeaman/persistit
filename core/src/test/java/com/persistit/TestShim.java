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

package com.persistit;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
    
    public static ByteBuffer getTransactionBuffer(Transaction txn) {
        return txn.getTransactionBuffer();
    }
}
