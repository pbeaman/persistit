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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.persistit.JournalManager.PageNode;
import com.persistit.JournalManager.TreeDescriptor;
import com.persistit.JournalManager.VolumeDescriptor;
import com.persistit.RecoveryManager.RecoveryListener;
import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

public class RecoveryTest extends PersistitUnitTestCase {
    /*
     * This class needs to be in com.persistit because of some package-private
     * methods used in controlling the test.
     */

    private String _volumeName = "persistit";

    public void testRecoveryRebuildsPageMap() throws Exception {
        _persistit.getJournalManager().setAppendOnly(true);
        store1();
        _persistit.close();
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(saveProperties);
        JournalManager logMan = _persistit.getJournalManager();
        assertTrue(logMan.getPageMapSize() + logMan.getCopiedPageCount() > 0);
        fetch1a();
        fetch1b();
    }

    public void testCopierCleansUpJournals() throws Exception {
        store1();
        JournalManager jman = _persistit.getJournalManager();
        assertTrue(jman.getPageMapSize() > 0);
        _persistit.flush();
        _persistit.checkpoint();
        jman.copyBack();
        assertEquals(0, jman.getPageMapSize());
        _persistit.close();
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(saveProperties);
        jman = _persistit.getJournalManager();
        assertEquals(0, jman.getPageMapSize());
        fetch1a();
        fetch1b();
    }

    public void testRecoverCommittedTransactions() throws Exception {
        // create 10 transactions on the journal
        _persistit.getJournalManager().setAppendOnly(true);
        store2();
        _persistit.getJournalManager().flush();
        _persistit.crash();
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.getJournalManager().setAppendOnly(true);
        final RecoveryManager plan = _persistit.getRecoveryManager();
        plan.setRecoveryDisabledForTestMode(true);
        _persistit.initialize(saveProperties);
        assertEquals(15, plan.getCommittedCount());
        plan.setRecoveryDisabledForTestMode(false);
        final Set<Long> recoveryTimestamps = new HashSet<Long>();
        final RecoveryListener actor = new RecoveryListener() {

            @Override
            public void store(final long address, final long timestamp,
                    Exchange exchange) throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void removeKeyRange(final long address,
                    final long timestamp, Exchange exchange, Key from, Key to)
                    throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void removeTree(final long address, final long timestamp,
                    Exchange exchange) throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void startRecovery(long address, long timestamp)
                    throws PersistitException {
            }

            @Override
            public void startTransaction(long address, long timestamp)
                    throws PersistitException {
            }

            @Override
            public void endTransaction(long address, long timestamp)
                    throws PersistitException {
            }

            @Override
            public void endRecovery(long address, long timestamp)
                    throws PersistitException {
            }

        };
        plan.applyAllCommittedTransactions(actor);
        assertEquals(15, recoveryTimestamps.size());
    }

    public void testLongRecordTransactionRecovery() throws Exception {
        // create 10 transactions on the journal having long records.
        _persistit.getJournalManager().setAppendOnly(true);
        store3();
        fetch3();
        _persistit.getJournalManager().flush();
        _persistit.crash();
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.getJournalManager().setAppendOnly(true);
        final RecoveryManager rman = _persistit.getRecoveryManager();
        rman.setRecoveryDisabledForTestMode(true);
        _persistit.initialize(saveProperties);
        assertTrue(rman.getCommittedCount() > 0);
        rman.setRecoveryDisabledForTestMode(false);
        rman.applyAllCommittedTransactions(rman.getDefaultRecoveryListener());
        fetch3();
    }

    public void testRolloverDoesntDeleteLiveTransactions() throws Exception {
        JournalManager jman = _persistit.getJournalManager();
        final long blockSize = jman.getBlockSize();
        assertEquals(0, jman.getBaseAddress() / blockSize);

        store1();

        jman.rollover();
        _persistit.checkpoint();
        jman.copyBack();
        assertEquals(0, jman.getPageMapSize());

        final Transaction txn = _persistit.getTransaction();

        txn.begin();
        store1();
        jman.setUnitTestNeverCloseTransactionId(txn.getId());
        txn.commit();
        txn.end();
        jman.setUnitTestNeverCloseTransactionId(Long.MIN_VALUE);

        jman.rollover();
        _persistit.checkpoint();
        jman.copyBack();
        //
        // Because JournalManager thinks there's an open transaction
        // (due to the call to setUnitTestNeverCloseTransactionId method)
        // it should preserve the journal file containing the TS record
        // for the transaction.
        //
        assertEquals(2, jman.getBaseAddress() / blockSize);
        assertEquals(0, jman.getPageMapSize());

        txn.begin();
        store1();
        txn.commit();
        txn.end();

        // Using the same transaction resets the transaction status;
        // after the commit() call above, JournalManager should not
        // have any open transactions. Therefore rollover should
        // delete the earlier files after copyBack.
        //
        jman.rollover();
        _persistit.checkpoint();
        jman.copyBack();

        assertEquals(3, jman.getBaseAddress() / blockSize);
        assertEquals(0, jman.getPageMapSize());

        fetch1a();
        fetch1b();
    }

    // Verifies that a sequence of insert and remove transactions results
    // in the correct state after recovery. Tests fix for bug 719319.
    public void testRecoveredTransactionsAreCorrect() throws Exception {
        SortedSet<String> keys = new TreeSet<String>();
        Exchange[] exchanges = new Exchange[5];
        for (int index = 0; index < 5; index++) {
            final Exchange ex = _persistit.getExchange("persistit",
                    "RecoveryTest_" + index, true);
            ex.removeAll();
            exchanges[index] = ex;
        }
        for (int index = 0; index < exchanges.length; index++) {
            Exchange ex = exchanges[index];
            for (int a = 0; a < 5; a++) {
                ex.clear().append(a);
                ex.getValue().put(String.format("index=%d a=%d", index, a));
                tStore(ex, keys);
                for (int b = 0; b < 5; b++) {
                    ex.clear().append(a).append(b);
                    ex.getValue().put(
                            String.format("index=%d a=%d b=%d", index, a, b));
                    tStore(ex, keys);
                }
            }
        }
        for (int index = 0; index < exchanges.length; index++) {
            Exchange ex = exchanges[index];
            Key.Direction direction = index == 0 ? Key.EQ
                    : index == 1 ? Key.GTEQ : Key.GT;
            for (int a = 0; a < 5; a++) {
                ex.clear().append(a);
                if (a % 2 == 0) {
                    tRemove(ex, keys, direction);
                }
                if (a % 3 == 0) {
                    for (int b = 0; b < 5; b++) {
                        ex.clear().append(a).append(b);
                        tRemove(ex, keys, direction);
                    }
                }
            }
        }
        tDeleteTree(exchanges[4], keys);

        // Now crash Persistit
        exchanges = null;
        _persistit.getJournalManager().flush();
        _persistit.crash();
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(saveProperties);
        final Volume volume = _persistit.getVolume("persistit");

        for (int index = 0; index < 5; index++) {
            final Tree tree = volume.getTree("RecoveryTest_" + index, false);
            assertEquals(index == 4, tree == null);
            if (index != 4) {
                final Exchange ex = new Exchange(tree);
                ex.clear();
                while (ex.next(true)) {
                    final String ks = keyString(ex);
                    assertTrue(keys.remove(ks));
                }
            }
        }
        assertTrue(keys.isEmpty());
    }

    public void testLargePageMap() throws Exception {
        final VolumeDescriptor vd = new VolumeDescriptor("foo", 123);
        final Map<Integer, VolumeDescriptor> volumeMap = new TreeMap<Integer, VolumeDescriptor>();
        volumeMap.put(1, vd);
        // sorted to make reading hex dumps easier
        final Map<PageNode, PageNode> pageMap = new TreeMap<PageNode, PageNode>();
        for (long pageAddr = 0; pageAddr < 100000; pageAddr++) {
            PageNode lastPageNode = new PageNode(1, pageAddr, pageAddr * 100, 0);
            for (long ts = 1; ts < 10; ts++) {
                PageNode pn = new PageNode(1, pageAddr, pageAddr * 100 + ts
                        * 10, ts * 100);
                pn.setPrevious(lastPageNode);
                lastPageNode = pn;
            }
            pageMap.put(lastPageNode, lastPageNode);
        }
        JournalManager jman = new JournalManager(_persistit);
        final String path = UnitTestProperties.DATA_PATH
                + "/RecoveryManagerTest_journal_";
        jman.unitTestInjectVolumes(volumeMap);
        jman.unitTestInjectPageMap(pageMap);
        // Note: moved call to init after the unitTestInject calls
        // because init now starts a the journal file.
        jman.init(null, path, 100000000);
        jman.writeCheckpointToJournal(new Checkpoint(500, 12345));
        jman.close();
        final RecoveryManager rman = new RecoveryManager(_persistit);
        rman.init(path);
        rman.buildRecoveryPlan();
        assertTrue(rman.getKeystoneAddress() != -1);
        final Map<PageNode, PageNode> pageMapCopy = new TreeMap<PageNode, PageNode>();
        final Map<PageNode, PageNode> branchMapCopy = new TreeMap<PageNode, PageNode>();
        rman.collectRecoveredPages(pageMapCopy, branchMapCopy);
        assertEquals(pageMap.size(), pageMapCopy.size());
        PageNode key = new PageNode(1, 42, -1, -1);
        PageNode pn = pageMapCopy.get(key);
        int count = 0;
        while (pn != null) {
            assertTrue(pn.getTimestamp() <= 500);
            count++;
            pn = pn.getPrevious();
        }
        assertEquals(1, count);
    }

    public void testVolumeMetadataValid() throws Exception {
        // create a junk volume to make sure the internal handle count is bumped
        // up
        VolumeDescriptor vd = new VolumeDescriptor("foo", 123);
        int volumeHandle = _persistit.getJournalManager().handleForVolume(vd);
        // retrieve the value of the handle counter before crashing
        int initialHandleValue = _persistit.getJournalManager()
                .getHandleCount();
        _persistit.close();
        Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(saveProperties);
        // verify the value of the handle counter after recovery is
        // still valid.
        assertTrue(_persistit.getJournalManager().getHandleCount() > initialHandleValue);

        // create a junk tree to make sure the internal handle count is bumped
        // up
        TreeDescriptor td = new TreeDescriptor(volumeHandle, "gray");
        _persistit.getJournalManager().handleForTree(td);
        int updatedHandleValue = _persistit.getJournalManager()
                .getHandleCount();
        _persistit.close();
        saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(saveProperties);
        // verify the value of the handle counter after recovery is
        // still valid.
        assertTrue(_persistit.getJournalManager().getHandleCount() > updatedHandleValue);
    }

    public void testIndexHoles() throws Exception {
        _persistit.getJournalManager().setAppendOnly(true);
        Transaction transaction = _persistit.getTransaction();
        StringBuilder sb = new StringBuilder();
        while (sb.length() < 1000) {
            sb.append("The quick brown fox jumped over the lazy red dog");
        }

        String s = sb.toString();
        for (int cycle = 0; cycle < 2; cycle++) {
            for (int i = 1000; i < 2000; i++) {
                final Exchange exchange = _persistit.getExchange("persistit",
                        "RecoveryTest" + i, true);
                transaction.begin();
                try {
                    exchange.getValue().put(s);
                    for (int j = 0; j < 20; j++) {
                        exchange.to(j).store();
                    }
                    transaction.commit();
                } finally {
                    transaction.end();
                }
            }

            for (int j = 0; j < 20; j++) {
                for (int i = 1000; i < 2000; i++) {
                    final Exchange exchange = _persistit.getExchange(
                            "persistit", "RecoveryTest" + i, true);
                    transaction.begin();
                    try {
                        exchange.to(j).remove();
                        transaction.commit();
                    } finally {
                        transaction.end();
                    }
                }
            }

            for (int i = 1000; i < 2000; i += 2) {
                transaction.begin();
                try {
                    final Exchange exchange = _persistit.getExchange(
                            "persistit", "RecoveryTest" + i, true);
                    exchange.removeTree();
                    transaction.commit();
                } finally {
                    transaction.end();
                }
            }
        }
        _persistit.crash();

        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.getJournalManager().setAppendOnly(true);
        _persistit.initialize(saveProperties);

        final Volume volume = _persistit.getVolume("persistit");
        long page = volume.getDirectoryTree().getRootPageAddr();
        Buffer buffer = _persistit.getBufferPool(volume.getPageSize())
                .getBufferCopy(volume, page);
        assertEquals(0, buffer.getRightSibling());

        for (final String treeName : volume.getTreeNames()) {
            final Tree tree = volume.getTree(treeName, false);
            page = tree.getRootPageAddr();
            buffer = _persistit.getBufferPool(volume.getPageSize())
                    .getBufferCopy(volume, page);
            assertEquals(0, buffer.getRightSibling());
        }
    }

    private void store1() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName,
                "SimpleTest1", true);
        exchange.removeAll();
        final StringBuilder sb = new StringBuilder();

        for (int i = 1; i < 50000; i++) {
            sb.setLength(0);
            sb.append((char) (i / 20 + 64));
            sb.append((char) (i % 20 + 64));
            exchange.clear().append(sb);
            exchange.getValue().put("Record #" + i);
            exchange.store();
        }
    }

    private void fetch1a() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName,
                "SimpleTest1", false);
        final StringBuilder sb = new StringBuilder();

        for (int i = 1; i < 50000; i++) {
            sb.setLength(0);
            sb.append((char) (i / 20 + 64));
            sb.append((char) (i % 20 + 64));
            exchange.clear().append(sb);
            exchange.fetch();
            assertTrue(exchange.getValue().isDefined());
            assertEquals("Record #" + i, exchange.getValue().getString());
        }

    }

    private void fetch1b() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName,
                "SimpleTest1", false);
        final StringBuilder sb = new StringBuilder();
        for (int i = 1; i < 400; i++) {
            sb.setLength(0);
            sb.append((char) (i % 20 + 64));
            sb.append((char) (i / 20 + 64));
            exchange.clear().append(sb);
            exchange.fetch();
            final int k = (i / 20) + (i % 20) * 20;
            assertEquals(exchange.getValue().getString(), "Record #" + k);
        }
    }

    private void store2() throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "RecoveryTest",
                true);
        ex.removeAll();
        for (int j = 0; j++ < 10;) {

            final Transaction txn = ex.getTransaction();

            txn.begin();
            try {
                for (int i = 0; i < 10; i++) {
                    ex.getValue().put("String value #" + i + " for test1");
                    ex.clear().append("test1").append(j).append(i).store();
                }
                for (int i = 3; i < 10; i += 3) {
                    ex.clear().append("test1").append(j).append(i)
                            .remove(Key.GTEQ);
                }
                txn.commit();
            } finally {
                txn.end();
            }
        }

        for (int j = 1; j < 10; j += 2) {
            final Transaction txn = ex.getTransaction();
            txn.begin();
            try {
                ex.clear().append("test1").append(j).remove(Key.GTEQ);
                txn.commit();
            } finally {
                txn.end();
            }
        }
    }

    private void store3() throws PersistitException {
        final Exchange ex = _persistit.getExchange("persistit", "RecoveryTest",
                true);
        ex.removeAll();
        for (int j = 0; j++ < 5;) {
            final StringBuilder sb = new StringBuilder(500000);
            for (int i = 0; i < 100000; i++) {
                sb.append("abcde");
            }

            final Transaction txn = ex.getTransaction();

            txn.begin();
            try {
                for (int i = 0; i < 10; i++) {
                    sb.replace(0, 3, " " + i + " ");
                    ex.getValue().put(sb.toString());
                    ex.clear().append("test1").append(j).append(i).store();
                }
                for (int i = 3; i < 10; i += 3) {
                    ex.clear().append("test1").append(j).append(i)
                            .remove(Key.GTEQ);
                }
                txn.commit();
            } finally {
                txn.end();
            }
        }

        for (int j = 1; j < 10; j += 2) {
            final Transaction txn = ex.getTransaction();
            txn.begin();
            try {
                ex.clear().append("test1").append(j).remove(Key.GTEQ);
                txn.commit();
            } finally {
                txn.end();
            }
        }
    }

    private boolean shouldBeDefined(final int j, final int i) {
        if (j % 2 != 0 || j < 1 || j > 5) {
            return false;
        }
        if (i < 0 || i > 9) {
            return false;
        }
        if (i % 3 == 0 && i > 0) {
            return false;
        }
        return true;
    }

    private void fetch3() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "RecoveryTest",
                true);
        for (int j = 0; j++ < 5;) {
            final StringBuilder sb = new StringBuilder(500000);

            for (int i = 0; i < 100000; i++) {
                sb.append("abcde");
            }

            for (int i = 0; i < 10; i++) {
                ex.clear().append("test1").append(j).append(i).fetch();
                if (shouldBeDefined(j, i)) {
                    sb.replace(0, 3, " " + i + " ");
                    assertEquals(sb.toString(), ex.getValue().getString());
                } else {
                    assertTrue(!ex.getValue().isDefined());
                }
            }
        }

    }

    private String keyString(final Exchange ex) {
        final String s = ex.getKey().toString();
        return ex.getTree().getName() + "_" + s.substring(1, s.length() - 1);
    }

    private void tStore(final Exchange ex, final SortedSet<String> keys)
            throws PersistitException {
        final Transaction txn = ex.getTransaction();
        int retries = 10;
        for (;;) {
            try {
                txn.begin();
                ex.store();
                txn.commit();
                String ks = keyString(ex);
                assertTrue(keys.add(ks));
                break;
            } catch (RollbackException e) {
                if (--retries < 0) {
                    throw new TransactionFailedException();
                }
            } finally {
                txn.end();
            }
        }
    }

    private void tRemove(final Exchange ex, final SortedSet<String> keys,
            final Key.Direction direction) throws PersistitException {
        final Transaction txn = ex.getTransaction();
        int retries = 10;
        for (;;) {
            try {
                txn.begin();
                ex.remove(direction);
                txn.commit();
                final String ks = keyString(ex);
                for (final Iterator<String> it = keys.iterator(); it.hasNext();) {
                    final String candidate = it.next();
                    if ((direction == Key.EQ || direction == Key.GTEQ)
                            && candidate.equals(ks)) {
                        it.remove();
                    } else if ((direction == Key.GTEQ || direction == Key.GT)
                            && candidate.startsWith(ks)
                            && !candidate.equals(ks)) {
                        it.remove();
                    }
                }
                break;
            } catch (RollbackException e) {
                if (--retries < 0) {
                    throw new TransactionFailedException();
                }
            } finally {
                txn.end();
            }
        }
    }

    private void tDeleteTree(final Exchange ex, final SortedSet<String> keys)
            throws PersistitException {
        final Transaction txn = ex.getTransaction();
        int retries = 10;
        for (;;) {
            try {
                txn.begin();
                ex.removeTree();
                ex.clear();
                final String ks = keyString(ex);
                for (final Iterator<String> it = keys.iterator(); it.hasNext();) {
                    final String candidate = it.next();
                    if (candidate.startsWith(ks)) {
                        it.remove();
                    }
                }
                txn.commit();
                break;
            } catch (RollbackException e) {
                if (--retries < 0) {
                    throw new TransactionFailedException();
                }
            } finally {
                txn.end();
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        new RecoveryTest().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        testRecoveryRebuildsPageMap();
        testCopierCleansUpJournals();
        testRecoverCommittedTransactions();
        // test4();
    }
}
