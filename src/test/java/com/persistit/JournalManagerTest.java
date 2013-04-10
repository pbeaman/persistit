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

import static com.persistit.unit.ConcurrentUtil.createThread;
import static com.persistit.unit.ConcurrentUtil.startAndJoinAssertSuccess;
import static com.persistit.util.SequencerConstants.PAGE_MAP_READ_INVALIDATE_B;
import static com.persistit.util.SequencerConstants.PAGE_MAP_READ_INVALIDATE_C;
import static com.persistit.util.SequencerConstants.PAGE_MAP_READ_INVALIDATE_SCHEDULE;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.sequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import com.persistit.Accumulator.SumAccumulator;
import com.persistit.CheckpointManager.Checkpoint;
import com.persistit.JournalManager.PageNode;
import com.persistit.TransactionPlayer.TransactionPlayerListener;
import com.persistit.exception.PersistitException;
import com.persistit.unit.ConcurrentUtil.ThrowingRunnable;
import com.persistit.unit.UnitTestProperties;
import com.persistit.util.Util;

public class JournalManagerTest extends PersistitUnitTestCase {

    /*
     * This class needs to be in com.persistit rather than com.persistit.unit
     * because it uses some package-private methods in Persistit.
     */

    private final String _volumeName = "persistit";

    @Test
    public void testJournalRecords() throws Exception {
        store1();
        _persistit.flush();
        final Transaction txn = _persistit.getTransaction();

        final Volume volume = _persistit.getVolume(_volumeName);
        volume.resetHandle();

        final JournalManager jman = new JournalManager(_persistit);
        final String path = UnitTestProperties.DATA_PATH + "/JournalManagerTest_journal_";
        jman.init(null, path, 100 * 1000 * 1000);
        final BufferPool pool = _persistit.getBufferPool(16384);
        final long pages = Math.min(1000, volume.getStorage().getNextAvailablePage() - 1);
        for (int i = 0; i < 1000; i++) {
            final Buffer buffer = pool.get(volume, i % pages, true, true);
            if ((i % 400) == 0) {
                jman.rollover();
            }
            buffer.setDirtyAtTimestamp(_persistit.getTimestampAllocator().updateTimestamp());
            jman.writePageToJournal(buffer);
            buffer.clearDirty();
            buffer.releaseTouched();
        }

        final Checkpoint checkpoint1 = _persistit.getCheckpointManager().createCheckpoint();
        jman.writeCheckpointToJournal(checkpoint1);
        final Exchange exchange = _persistit.getExchange(_volumeName, "JournalManagerTest1", false);
        volume.getTree("JournalManagerTest1", false).resetHandle();

        assertTrue(exchange.next(true));
        final long[] timestamps = new long[100];

        for (int i = 0; i < 100; i++) {
            assertTrue(exchange.next(true));
            final int treeHandle = jman.handleForTree(exchange.getTree());
            timestamps[i] = _persistit.getTimestampAllocator().updateTimestamp();

            txn.writeStoreRecordToJournal(treeHandle, exchange.getKey(), exchange.getValue());
            if (i % 50 == 0) {
                jman.rollover();
            }
            jman.writeTransactionToJournal(txn.getTransactionBuffer(), timestamps[i], i % 4 == 1 ? timestamps[i] + 1
                    : 0, 0);
        }
        jman.rollover();
        exchange.clear().append(Key.BEFORE);
        int commitCount = 0;
        long noPagesAfterThis = jman.getCurrentAddress();

        Checkpoint checkpoint2 = null;

        for (int i = 0; i < 100; i++) {
            assertTrue(exchange.next(true));
            final int treeHandle = jman.handleForTree(exchange.getTree());
            timestamps[i] = _persistit.getTimestampAllocator().updateTimestamp();
            txn.writeDeleteRecordToJournal(treeHandle, exchange.getKey(), exchange.getKey());
            if (i == 66) {
                jman.rollover();
            }
            if (i == 50) {
                checkpoint2 = _persistit.getCheckpointManager().createCheckpoint();
                jman.writeCheckpointToJournal(checkpoint2);
                noPagesAfterThis = jman.getCurrentAddress();
                commitCount = 0;
            }
            jman.writeTransactionToJournal(txn.getTransactionBuffer(), timestamps[i], i % 4 == 3 ? timestamps[i] + 1
                    : 0, 0);
            if (i % 4 == 3) {
                commitCount++;
            }
        }

        store1();

        /**
         * These pages will have timestamps larger than the last valid
         * checkpoint and therefore should not be represented in the recovered
         * page map.
         */
        for (int i = 0; i < 1000; i++) {
            final Buffer buffer = pool.get(volume, i % pages, false, true);
            if ((i % 400) == 0) {
                jman.rollover();
            }
            if (buffer.getTimestamp() > checkpoint2.getTimestamp()) {
                jman.writePageToJournal(buffer);
            }
            buffer.releaseTouched();
        }
        jman.close();
        volume.resetHandle();

        final RecoveryManager rman = new RecoveryManager(_persistit);
        rman.init(path);

        rman.buildRecoveryPlan();
        assertTrue(rman.getKeystoneAddress() != -1);
        assertEquals(checkpoint2.getTimestamp(), rman.getLastValidCheckpoint().getTimestamp());
        final Map<PageNode, PageNode> pageMap = new HashMap<PageNode, PageNode>();
        final Map<PageNode, PageNode> branchMap = new HashMap<PageNode, PageNode>();

        rman.collectRecoveredPages(pageMap, branchMap);
        assertEquals(pages, pageMap.size());

        for (final PageNode pn : pageMap.values()) {
            assertTrue(pn.getJournalAddress() <= noPagesAfterThis);
        }

        final Set<Long> recoveryTimestamps = new HashSet<Long>();
        final TransactionPlayerListener actor = new TransactionPlayerListener() {

            @Override
            public void store(final long address, final long timestamp, final Exchange exchange)
                    throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void removeKeyRange(final long address, final long timestamp, final Exchange exchange,
                    final Key from, final Key to) throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void removeTree(final long address, final long timestamp, final Exchange exchange)
                    throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void startRecovery(final long address, final long timestamp) throws PersistitException {
            }

            @Override
            public void startTransaction(final long address, final long startTimestamp, final long commitTimestamp)
                    throws PersistitException {
            }

            @Override
            public void endTransaction(final long address, final long timestamp) throws PersistitException {
            }

            @Override
            public void endRecovery(final long address, final long timestamp) throws PersistitException {
            }

            @Override
            public void delta(final long address, final long timestamp, final Tree tree, final int index,
                    final int accumulatorTypeOrdinal, final long value) throws PersistitException {
            }

            @Override
            public boolean requiresLongRecordConversion() {
                return true;
            }

        };
        rman.applyAllRecoveredTransactions(actor, rman.getDefaultRollbackListener());
        assertEquals(commitCount, recoveryTimestamps.size());

    }

    @Test
    public void testRollover() throws Exception {
        store1();
        _persistit.flush();
        final Volume volume = _persistit.getVolume(_volumeName);
        volume.resetHandle();
        final JournalManager jman = new JournalManager(_persistit);
        final String path = UnitTestProperties.DATA_PATH + "/JournalManagerTest_journal_";
        jman.init(null, path, 100 * 1000 * 1000);
        final BufferPool pool = _persistit.getBufferPool(16384);
        final long pages = Math.min(1000, volume.getStorage().getNextAvailablePage() - 1);
        for (int i = 0; jman.getCurrentAddress() < 300 * 1000 * 1000; i++) {
            final Buffer buffer = pool.get(volume, i % pages, true, true);
            buffer.setDirtyAtTimestamp(_persistit.getTimestampAllocator().updateTimestamp());
            buffer.save();
            jman.writePageToJournal(buffer);
            buffer.releaseTouched();
        }
        final Checkpoint checkpoint1 = _persistit.getCheckpointManager().createCheckpoint();
        jman.writeCheckpointToJournal(checkpoint1);
        _persistit.checkpoint();
        _persistit.close();
        _persistit = new Persistit(_config);
        _persistit.getJournalManager().setAppendOnly(true);
        final RecoveryManager rman = new RecoveryManager(_persistit);
        rman.init(path);
        assertTrue(rman.analyze());
    }

    @Test
    public void testRollover768048() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        final Exchange exchange = _persistit.getExchange(_volumeName, "JournalManagerTest1", true);
        final JournalManager jman = new JournalManager(_persistit);
        final String path = UnitTestProperties.DATA_PATH + "/JournalManagerTest_journal_";
        jman.init(null, path, 100 * 1000 * 1000);
        exchange.clear().append("key");
        final StringBuilder sb = new StringBuilder(1000000);
        for (int i = 0; sb.length() < 1000; i++) {
            sb.append(String.format("%018d  ", i));
        }
        final String kilo = sb.toString();
        exchange.getValue().put(kilo);
        final int overhead = JournalRecord.SR.OVERHEAD + exchange.getKey().getEncodedSize() + JournalRecord.JE.OVERHEAD
                + 1;
        long timestamp = 0;
        long addressBeforeRollover = -1;
        long addressAfterRollover = -1;

        while (jman.getCurrentAddress() < 300 * 1000 * 1000) {
            timestamp = _persistit.getTimestampAllocator().updateTimestamp();
            final long remaining = jman.getBlockSize() - (jman.getCurrentAddress() % jman.getBlockSize()) - 1;
            if (remaining == JournalRecord.JE.OVERHEAD) {
                addressBeforeRollover = jman.getCurrentAddress();
            }
            final long size = remaining - overhead;
            if (size > 0 && size < sb.length()) {
                exchange.getValue().put(kilo.substring(0, (int) size));
            } else {
                exchange.getValue().put(kilo);
            }

            txn.writeStoreRecordToJournal(12345, exchange.getKey(), exchange.getValue());
            jman.writeTransactionToJournal(txn.getTransactionBuffer(), timestamp, timestamp + 1, 0);
            if (remaining == JournalRecord.JE.OVERHEAD) {
                addressAfterRollover = jman.getCurrentAddress();
                assertTrue(addressAfterRollover - addressBeforeRollover < 2000);
            }
        }
    }

    @Test
    public void testRollback() throws Exception {
        // Allow test to control when pruning will happen
        _persistit.getJournalManager().setRollbackPruningEnabled(false);
        _persistit.getJournalManager().setWritePagePruningEnabled(false);
        final Transaction txn = _persistit.getTransaction();
        for (int i = 0; i < 10; i++) {
            txn.begin();
            store1();
            txn.rollback();
            txn.end();
        }
        assertEquals(50000, countKeys(false));
        assertEquals(0, countKeys(true));
        _persistit.getJournalManager().pruneObsoleteTransactions(true);
        assertTrue(countKeys(false) < 50000);
        final CleanupManager cm = _persistit.getCleanupManager();
        assertTrue(cm.getAcceptedCount() > 0);
        final long start = System.currentTimeMillis();
        while (cm.getPerformedCount() < cm.getAcceptedCount()) {
            if (System.currentTimeMillis() > start + 30000) {
                fail("Pruning not done in 30 seconds");
            }
            Util.sleep(100);
        }
        assertEquals(0, countKeys(false));
    }

    @Test
    public void testRollbackEventually() throws Exception {

        final Transaction txn = _persistit.getTransaction();
        for (int i = 0; i < 10; i++) {
            txn.begin();
            store1();
            txn.rollback();
            txn.end();
        }

        final long start = System.currentTimeMillis();
        long elapsed = 0;
        while (countKeys(false) > 0) {
            Util.sleep(1000);
            elapsed = System.currentTimeMillis() - start;
            if (elapsed > 60000) {
                break;
            }
        }
        assertTrue(elapsed < 60000);
    }

    @Test
    public void testRollbackLongRecords() throws Exception {
        // Allow test to control when pruning will happen
        _persistit.getJournalManager().setRollbackPruningEnabled(false);
        _persistit.getJournalManager().setWritePagePruningEnabled(false);

        final Volume volume = _persistit.getVolume(_volumeName);
        final Transaction txn = _persistit.getTransaction();
        for (int i = 0; i < 10; i++) {
            txn.begin();
            store2();
            txn.rollback();
            txn.end();
        }
        _persistit.getJournalManager().pruneObsoleteTransactions(true);
        assertEquals(0, countKeys(false));
        final IntegrityCheck icheck = new IntegrityCheck(_persistit);
        icheck.checkVolume(volume);
        final long totalPages = volume.getStorage().getNextAvailablePage();
        final long dataPages = icheck.getDataPageCount();
        final long indexPages = icheck.getIndexPageCount();
        final long longPages = icheck.getLongRecordPageCount();
        final long garbagePages = icheck.getGarbagePageCount();
        assertEquals(totalPages, dataPages + indexPages + longPages + garbagePages);
        assertEquals(0, longPages);
        assertTrue(garbagePages > 0);
    }

    @Test
    public void testTransactionMapSpanningJournalWriteBuffer() throws Exception {
        _persistit.getJournalManager().setWriteBufferSize(JournalManager.MINIMUM_BUFFER_SIZE);
        final Transaction txn = _persistit.getTransaction();
        SumAccumulator acc = _persistit.getVolume("persistit").getTree("JournalManagerTest", true)
                .getSumAccumulator(0);
        /*
         * Load up a sizable live transaction map
         */
        for (int i = 0; i < 25000; i++) {
            txn.begin();
            acc.increment();
            txn.commit();
            txn.end();
        }
        _persistit.getJournalManager().rollover();
        _persistit.close();
        _persistit = new Persistit(_config);

        acc = _persistit.getVolume("persistit").getTree("JournalManagerTest", true)
                .getSumAccumulator(0);
        assertEquals("Accumulator value is incorrect", 25000, acc.getLiveValue());

    }

    @Test
    public void testCleanupPageList() throws Exception {
        /*
         * Remove from the left end
         */
        {
            final List<PageNode> source = testCleanupPageListSource(10);
            for (int i = 0; i < 4; i++) {
                source.get(i).invalidate();
            }
            testCleanupPageListHelper(source);
        }

        /*
         * Remove from the right end
         */
        {
            final List<PageNode> source = testCleanupPageListSource(10);
            for (int i = 10; --i >= 7;) {
                source.get(i).invalidate();
            }
            testCleanupPageListHelper(source);
        }

        /*
         * Remove from the middle
         */
        {
            final List<PageNode> source = testCleanupPageListSource(10);
            for (int i = 2; i < 8; i++) {
                source.get(i).invalidate();
            }
            testCleanupPageListHelper(source);
        }

        /*
         * Randomly invalidated PageNodes
         */
        {
            final int SIZE = 1000000;
            final Random random = new Random(1);
            final List<PageNode> source = testCleanupPageListSource(SIZE);
            int next = -1;
            for (int index = 0; index < SIZE; index++) {
                if (index < next) {
                    source.get(index).invalidate();
                } else {
                    index += random.nextInt(5);
                    next = random.nextInt(5) + index;
                }
            }
            testCleanupPageListHelper(source);
        }
    }

    @Test
    public void missingVolumePageHandling() throws Exception {
        Volume volume1 = new Volume(_config.volumeSpecification("${datapath}/missing1,create,"
                + "pageSize:16384,initialPages:1,extensionPages:1,maximumPages:25000"));
        volume1.open(_persistit);
        Volume volume2 = new Volume(_config.volumeSpecification("${datapath}/missing2,create,"
                + "pageSize:16384,initialPages:1,extensionPages:1,maximumPages:25000"));
        volume2.open(_persistit);
        _persistit.getExchange(volume1, "test1", true);
        _persistit.getExchange(volume2, "test2", true);
        _persistit.close();
        new File(volume1.getPath()).delete();
        new File(volume2.getPath()).delete();
        volume1 = null;
        volume2 = null;
        _persistit = new Persistit(_config);
        final AlertMonitor am = _persistit.getAlertMonitor();
        assertTrue("Startup with missing volumes should have generated alerts",
                am.getHistory(AlertMonitor.MISSING_VOLUME_CATEGORY) != null);

        _persistit.getJournalManager().setIgnoreMissingVolumes(true);
        // Should add more alerts
        _persistit.copyBackPages();
        final int alertCount1 = am.getHistory(AlertMonitor.MISSING_VOLUME_CATEGORY).getCount();
        _persistit.copyBackPages();
        final int alertCount2 = am.getHistory(AlertMonitor.MISSING_VOLUME_CATEGORY).getCount();
        assertEquals("No more alerts after setting ignoreMissingVolumes", alertCount1, alertCount2);
    }

    @Test
    public void missingVolumeTransactionHandlingNotIgnored() throws Exception {
        Volume volume = new Volume(_config.volumeSpecification("${datapath}/missing1,create,"
                + "pageSize:16384,initialPages:1,extensionPages:1,maximumPages:25000"));
        volume.open(_persistit);
        final Exchange ex = _persistit.getExchange(volume, "test1", true);
        final Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        ex.to(0).store();
        txn.commit();
        txn.end();
        _persistit.getJournalManager().flush();
        _persistit.crash();
        new File(volume.getPath()).delete();
        volume = null;
        _persistit = new Persistit(_config);
        assertTrue("Should have failed updates during recovery", _persistit.getRecoveryManager().getPlayer()
                .getFailedUpdates() > 0);
    }

    @Test
    public void missingVolumeTransactionHandlingIgnored() throws Exception {
        Volume volume = new Volume(_config.volumeSpecification("${datapath}/missing1,create,"
                + "pageSize:16384,initialPages:1,extensionPages:1,maximumPages:25000"));
        volume.open(_persistit);
        final Exchange ex = _persistit.getExchange(volume, "test1", true);
        final Transaction txn = ex.getTransaction();
        txn.begin();
        ex.getValue().put(RED_FOX);
        ex.to(0).store();
        txn.commit();
        txn.end();
        _persistit.getJournalManager().flush();
        _persistit.crash();
        new File(volume.getPath()).delete();
        volume = null;
        _config.setIgnoreMissingVolumes(true);
        _persistit = new Persistit();
        _persistit.getJournalManager().setIgnoreMissingVolumes(true);
        _persistit.setConfiguration(_config);
        _persistit.initialize();
        final AlertMonitor am = _persistit.getAlertMonitor();
        assertTrue("Startup with missing volumes should have generated alerts",
                am.getHistory(AlertMonitor.MISSING_VOLUME_CATEGORY) != null);
        assertTrue("Should have failed updates during recovery", _persistit.getRecoveryManager().getPlayer()
                .getIgnoredUpdates() > 0);
    }

    private List<PageNode> testCleanupPageListSource(final int size) {
        final List<PageNode> source = new ArrayList<PageNode>(size);
        for (int index = 0; index < size; index++) {
            source.add(new PageNode(0, index, index * 10, index));
        }
        return source;
    }

    private void testCleanupPageListHelper(final List<PageNode> source) throws Exception {
        final List<PageNode> cleaned = new LinkedList<PageNode>(source);
        for (final Iterator<PageNode> iterator = cleaned.iterator(); iterator.hasNext();) {
            if (iterator.next().isInvalid()) {
                iterator.remove();
            }
        }
        final JournalManager jman = new JournalManager(_persistit);
        jman.unitTestInjectPageList(source);
        final int removed = jman.cleanupPageList();
        assertTrue(jman.unitTestPageListEquals(cleaned));
        assertEquals("Removed count is wrong", source.size() - cleaned.size(), removed);
        assertTrue("Invalidated no page nodes", source.size() > cleaned.size());
    }

    @Test
    public void copyBackPagesLeavesOneJournal() throws Exception {
        final int BATCH_SIZE = 1000;
        final JournalManager jman = _persistit.getJournalManager();

        int total = 0;
        for (long curSize = 0; curSize < JournalManager.ROLLOVER_THRESHOLD;) {
            final Exchange ex = _persistit.getExchange(UnitTestProperties.VOLUME_NAME, "JournalManagerTest", true);
            final Transaction txn = _persistit.getTransaction();
            final SumAccumulator accum = ex.getTree().getSumAccumulator(0);
            txn.begin();
            for (int j = 0; j < BATCH_SIZE; ++j) {
                ex.clear().append(total + j);
                ex.getValue().put(j);
                ex.store();
                accum.increment();
            }
            txn.commit();
            txn.end();
            total += BATCH_SIZE;
            curSize = jman.getCurrentJournalSize();
        }

        _persistit.copyBackPages();
        assertEquals("File count after copyBack", 1, jman.getJournalFileCount());
        final long curSize = jman.getCurrentJournalSize();
        assertTrue("Size is less than ROLLOVER after copyBack: " + curSize, curSize < JournalManager.ROLLOVER_THRESHOLD);
    }

    @Test
    public void concurrentReadAndInvalidationOfPageNodes() throws Exception {
        final int COUNT = 5000;
        final String TREE_NAME = "JournalManagerTest1";
        /*
         * Test sequence points in the JOURNAL_COPIER path, don't want to hit
         * unintentionally hit them.
         */
        _persistit.getJournalManager().setCopierInterval(50000);
        /*
         * Insert enough to dirty a few pages
         */
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        final Exchange ex = _persistit.getExchange(_volumeName, TREE_NAME, true);
        for (int i = 0; i < COUNT; ++i) {
            ex.to(i);
            ex.getValue().put(RED_FOX);
            ex.store();
        }
        txn.commit();
        txn.end();
        /*
         * Thread will read over everything that is inserted, hopefully going to
         * the journal for each required page.
         */
        final Thread thread1 = createThread("READ_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws PersistitException {
                final Transaction txn = _persistit.getTransaction();
                txn.begin();
                final Exchange ex = _persistit.getExchange(_volumeName, TREE_NAME, false);
                ex.to(Key.BEFORE);
                int count = 0;
                while (ex.next(true)) {
                    ++count;
                }
                assertEquals("Traversed count", COUNT, count);
                txn.commit();
            }
        });
        /*
         * Thread will copy pages out of the journal and into the volume,
         * hopefully invalidating pageMap entries during the cleanupForCopy.
         */
        final Thread thread2 = createThread("COPY_BACK_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                sequence(PAGE_MAP_READ_INVALIDATE_B);
                _persistit.getJournalManager().copyBack();
                sequence(PAGE_MAP_READ_INVALIDATE_C);
            }
        });
        /*
         * Make sure all pages are in journal
         */
        _persistit.flush();
        _persistit.checkpoint();
        /*
         * Invalidate so next read must go from disk and check if in journal
         */
        final Volume v = _persistit.getVolume(_volumeName);
        final BufferPool bp = _persistit.getBufferPool(v.getPageSize());
        bp.invalidate(v);
        /*
         * Enable sequencing and run threads
         */
        enableSequencer(true);
        addSchedules(PAGE_MAP_READ_INVALIDATE_SCHEDULE);
        startAndJoinAssertSuccess(25000, thread1, thread2);
        disableSequencer();
    }

    private int countKeys(final boolean mvcc) throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "JournalManagerTest1", false);
        exchange.ignoreMVCCFetch(!mvcc);
        int count1 = 0;
        final int count2 = 0;
        exchange.clear().append(Key.BEFORE);
        while (exchange.next()) {
            count1++;
        }
        // No longer valid because CleanupManager may prune while the loop is
        // running
        // exchange.clear().append(Key.AFTER);
        // while (exchange.previous()) {
        // count2++;
        // }
        // assertEquals(count1, count2);
        return count1;
    }

    private void store1() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "JournalManagerTest1", true);
        exchange.removeAll();
        final StringBuilder sb = new StringBuilder();

        for (int i = 1; i <= 50000; i++) {
            sb.setLength(0);
            sb.append((char) (i / 20 + 64));
            sb.append((char) (i % 20 + 64));
            exchange.clear().append(sb);
            exchange.getValue().put("Record #" + i);
            exchange.store();
        }
    }

    private void store2() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "JournalManagerTest1", true);
        exchange.removeAll();
        final StringBuilder sb = new StringBuilder();
        while (sb.length() < 50000) {
            sb.append(RED_FOX);
        }
        exchange.getValue().put(sb);
        for (int i = 1; i <= 5; i++) {
            exchange.to(i).store();
        }
    }

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

}
