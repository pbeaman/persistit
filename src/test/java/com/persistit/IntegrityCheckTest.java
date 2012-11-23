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

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;

import org.junit.Test;

import com.persistit.exception.PersistitException;

public class IntegrityCheckTest extends PersistitUnitTestCase {

    private final static int SIZE = 1000;

    private final String _volumeName = "persistit";

    @Test
    public void testSimplePrimordialTree() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "primordial", true);
        nonTransactionalStore(ex);

        final IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getDataByteCount() >= RED_FOX.length() * SIZE);
        assertTrue(icheck.getDataPageCount() > 0);
        assertEquals(0, icheck.getFaults().length);
        assertTrue(icheck.getIndexByteCount() > 0);
        assertTrue(icheck.getIndexPageCount() > 0);
        assertEquals(0, icheck.getLongRecordByteCount());
        assertEquals(0, icheck.getLongRecordPageCount());
        assertEquals(0, icheck.getMvvCount());
        assertEquals(0, icheck.getMvvOverhead());
        assertEquals(0, icheck.getMvvAntiValues());
    }

    @Test
    public void testSimpleMvvTree() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        disableBackgroundCleanup();

        transactionalStore(ex);

        final IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getDataByteCount() >= RED_FOX.length() * SIZE);
        assertTrue(icheck.getDataPageCount() > 0);
        assertEquals(0, icheck.getFaults().length);
        assertTrue(icheck.getIndexByteCount() > 0);
        assertTrue(icheck.getIndexPageCount() > 0);
        assertEquals(0, icheck.getLongRecordByteCount());
        assertEquals(0, icheck.getLongRecordPageCount());
        assertEquals(SIZE, icheck.getMvvCount());
        assertTrue(icheck.getMvvOverhead() > 0);
        assertEquals(SIZE / 2, icheck.getMvvAntiValues());
    }

    @Test
    public void testBrokenKeySequence() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        disableBackgroundCleanup();

        transactionalStore(ex);

        corrupt1(ex);
        final IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getFaults().length > 0);

    }

    @Test
    public void testBrokenMVVs() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        disableBackgroundCleanup();
        transactionalStore(ex);
        corrupt2(ex);
        final IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getFaults().length > 0);
    }

    @Test
    public void testIndexFixHoles() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        final CleanupManager cm = _persistit.getCleanupManager();
        transactionalStore(ex);
        corrupt3(ex);
        IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertEquals(0, icheck.getFaults().length);
        final long holeCount = icheck.getIndexHoleCount();
        assertTrue(holeCount > 0);

        assertEquals(0, cm.getAcceptedCount());
        assertEquals(0, cm.getPerformedCount());

        icheck = icheck();
        icheck.setFixHolesEnabled(true);
        icheck.checkTree(ex.getTree());
        assertEquals(holeCount, cm.getAcceptedCount());
        waitForCleanupManager(cm);
        assertEquals(cm.getPerformedCount(), holeCount);

        icheck = icheck();
        icheck.checkTree(ex.getTree());

        assertEquals(0, icheck.getFaults().length);
        assertEquals(0, icheck.getIndexHoleCount());
    }

    @Test
    public void testPrune() throws Exception {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        final CleanupManager cm = _persistit.getCleanupManager();
        transactionalStore(ex);
        _persistit.getTransactionIndex().updateActiveTransactionCache();

        IntegrityCheck icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getMvvCount() > 0);
        assertEquals(0, icheck.getPrunedPagesCount());

        icheck = icheck();
        icheck.setPruneEnabled(true);
        icheck.checkTree(ex.getTree());
        assertTrue(icheck.getMvvCount() > 0);
        assertTrue(icheck.getPrunedPagesCount() > 0);

        waitForCleanupManager(cm);

        icheck = icheck();
        icheck.checkTree(ex.getTree());
        assertEquals(0, icheck.getMvvCount());
    }

    @Test
    public void testPruneRemovesAbortedTransactionStatus() throws Exception {
        _persistit.getJournalManager().setRollbackPruningEnabled(false);
        _persistit.getJournalManager().setWritePagePruningEnabled(false);

        for (int i = 0; i < 10; i++) {
            final Exchange ex = _persistit.getExchange(_volumeName, "mvv" + i, true);
            final Transaction txn = ex.getTransaction();
            txn.begin();
            try {
                transactionalStore(ex);
                if ((i % 2) == 0) {
                    txn.rollback();
                } else {
                    txn.commit();
                }
                if (i == 5) {
                    _persistit.checkpoint();
                }
            } finally {
                txn.end();
            }
        }
        final Configuration config = _persistit.getConfiguration();
        _persistit.crash();
        _persistit = new Persistit();
        _persistit.setConfiguration(config);
        _persistit.getRecoveryManager().setRecoveryDisabledForTestMode(true);
        _persistit.getJournalManager().setRollbackPruningEnabled(false);
        _persistit.initialize();
        disableBackgroundCleanup();

        for (int i = 0; i < 10; i++) {
            final Exchange ex = _persistit.getExchange(_volumeName, "mvv" + i, true).append(Key.BEFORE);
            int count = 0;
            while (ex.next()) {
                count++;
            }
            if ((i % 2) == 1 && i <= 5) {
                assertEquals(SIZE / 2, count);
            } else {
                assertEquals(0, count);
            }
        }
        assertTrue(_persistit.getTransactionIndex().getAbortedCount() > 0);

        final IntegrityCheck icheck = IntegrityCheck.icheck("*", false, false, false, false, true, true, false);
        icheck.setPersistit(_persistit);
        icheck.setMessageWriter(new PrintWriter(System.out));

        icheck.runTask();
        assertTrue(icheck.getPrunedPagesCount() > 0);
        _persistit.getTransactionIndex().cleanup();
        assertEquals(0, _persistit.getTransactionIndex().getAbortedCount());
    }

    @Test
    public void testCorruptGarbageChain() throws PersistitException {
        final Exchange ex = _persistit.getExchange(_volumeName, "mvv", true);
        nonTransactionalStore(ex);
        corrupt4(ex);
        final IntegrityCheck icheck = icheck();
        icheck.checkVolume(ex.getVolume());
        assertTrue(icheck.getFaults().length > 0);
    }

    private String key(final int i) {
        return String.format("%05d%s", i, RED_FOX);
    }

    private void nonTransactionalStore(final Exchange ex) throws PersistitException {
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < SIZE; i++) {
            ex.to(key(i));
            ex.store();
        }
        for (int i = 1; i < SIZE; i += 2) {
            ex.to(key(i));
            ex.remove();
        }
    }

    private void waitForCleanupManager(final CleanupManager cm) throws InterruptedException {
        for (int wait = 0; wait < 60 && cm.getEnqueuedCount() > 0; wait++) {
            Thread.sleep(1000);
        }
        assertEquals(0, cm.getEnqueuedCount());
    }

    private void transactionalStore(final Exchange ex) throws PersistitException {
        final Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        for (int i = 0; i < SIZE; i++) {
            ex.to(key(i));
            ex.store();
        }
        txn.commit();
        txn.end();
        txn.begin();
        for (int i = 1; i < SIZE; i += 2) {
            ex.to(key(i));
            ex.remove();
        }
        txn.commit();
        txn.end();

    }

    /**
     * Corrupts the first key on some page by incrementing its second byte
     * 
     * @param ex
     * @throws PersistitException
     */
    private void corrupt1(final Exchange ex) throws PersistitException {
        final Key key = ex.getKey();
        ex.clear().to(key(500));
        final Buffer copy = ex.fetchBufferCopy(0);
        final Buffer buffer = ex.getBufferPool().get(ex.getVolume(), copy.getPageAddress(), true, true);
        buffer.nextKey(key, buffer.toKeyBlock(0));
        assertTrue(key.getEncodedSize() > 1);
        final int t = (int) (buffer.at(buffer.toKeyBlock(0)) >>> 32);
        buffer.getBytes()[t - key.getEncodedSize() + 1]++;
        buffer.setDirtyAtTimestamp(_persistit.getTimestampAllocator().updateTimestamp());
        buffer.release();
    }

    /**
     * Corrupts some MVV values on a page by damaging a version length field
     * 
     * @param ex
     * @throws PersistitException
     */
    private void corrupt2(final Exchange ex) throws PersistitException {
        ex.ignoreMVCCFetch(true);
        ex.clear().to(key(500));
        int corrupted = 0;
        while (corrupted < 10 && ex.next()) {
            final byte[] bytes = ex.getValue().getEncodedBytes();
            final int length = ex.getValue().getEncodedSize();
            if (MVV.isArrayMVV(bytes, 0, length)) {
                bytes[9]++;
                ex.store();
                corrupted++;
            }
        }
        ex.ignoreMVCCFetch(false);
    }

    /**
     * Corrupts an index page to create index holes by removing some keys
     * 
     * @param ex
     * @throws PersistitException
     */
    private void corrupt3(final Exchange ex) throws PersistitException {
        final Key key = ex.getKey();
        ex.clear().to(key(500));
        final Buffer copy = ex.fetchBufferCopy(1);
        assertNotNull(copy);
        assertTrue(copy.isIndexPage());
        final Buffer buffer = ex.getBufferPool().get(ex.getVolume(), copy.getPageAddress(), true, true);
        buffer.nextKey(key, buffer.toKeyBlock(0));
        buffer.removeKeys(36, 52, ex.getKey());
        buffer.setDirtyAtTimestamp(_persistit.getTimestampAllocator().updateTimestamp());
        buffer.release();
    }

    /**
     * Corrupts garbage chain by adding a live data chain to it
     * 
     * @throw PersistitException
     */
    private void corrupt4(final Exchange ex) throws PersistitException {
        final Key key = ex.getKey();
        ex.clear().to(key(500));
        final Buffer copy = ex.fetchBufferCopy(0);
        final Buffer buffer = ex.getBufferPool().get(ex.getVolume(), copy.getPageAddress(), true, true);
        ex.getVolume().getStructure().deallocateGarbageChain(buffer.getPageAddress(), buffer.getRightSibling());
        buffer.release();
    }

    private IntegrityCheck icheck() {
        final IntegrityCheck icheck = new IntegrityCheck(_persistit);
        icheck.setMessageLogVerbosity(Task.LOG_VERBOSE);
        icheck.setMessageWriter(new PrintWriter(System.out));
        return icheck;
    }
}
