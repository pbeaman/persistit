/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Aug 24, 2004
 */
package com.persistit;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import com.persistit.JournalManager.PageNode;
import com.persistit.JournalManager.VolumeDescriptor;
import com.persistit.RecoveryManager.RecoveredTransactionActor;
import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

public class RecoveryTest extends PersistitUnitTestCase {
    /*
     * This class needs to be in com.persistit because of some package-private
     * methods used in controlling the test.
     */

    private static String[] _args;

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
        jman.copyBack(Long.MAX_VALUE);
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
        final RecoveredTransactionActor actor = new RecoveredTransactionActor() {

            @Override
            public void store(final long address, final long timestamp,
                    Exchange exchange) throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void removeKeyRange(final long address,
                    final long timestamp, Exchange exchange)
                    throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void removeTree(final long address, final long timestamp,
                    Exchange exchange) throws PersistitException {
                recoveryTimestamps.add(timestamp);
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
        rman.applyAllCommittedTransactions(rman
                .getDefaultRecoveredTransactionActor());
        fetch3();
    }

    public void testRolloverDoesntDeleteLiveTransactions() throws Exception {
        JournalManager jman = _persistit.getJournalManager();
        final long blockSize = jman.getBlockSize();
        assertEquals(0, jman.getBaseAddress() / blockSize);

        store1();

        jman.rollover();
        _persistit.checkpoint();
        jman.copyBack(Long.MAX_VALUE);
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
        jman.copyBack(Long.MAX_VALUE);
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
        jman.copyBack(Long.MAX_VALUE);

        assertEquals(3, jman.getBaseAddress() / blockSize);
        assertEquals(0, jman.getPageMapSize());

        fetch1a();
        fetch1b();
    }

    public void testLargePageMap() throws Exception {
        JournalManager jman = new JournalManager(_persistit);
        final String path = UnitTestProperties.DATA_PATH
                + "/RecoveryManagerTest_journal_";
        jman.init(null, path, 100000000);
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
        jman.unitTestInjectVolumes(volumeMap);
        jman.unitTestInjectPageMap(pageMap);
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

    public static void main(final String[] args) throws Exception {
        _args = args;
        new RecoveryTest().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        testRecoveryRebuildsPageMap();
        testCopierCleansUpJournals();
        testRecoverCommittedTransactions();
        // test4();
    }
}
