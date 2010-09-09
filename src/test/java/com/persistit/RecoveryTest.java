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

import java.util.Properties;

import org.junit.Ignore;

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class RecoveryTest extends PersistitUnitTestCase {
    //
    // This class needs to be in com.persistit because Persistit#getLogManager()
    // is package-private.
    //
    private static String[] _args = new String[0];

    private String _volumeName = "persistit";

    public void testRecoveryRebuildsPageMap() throws Exception {
        store1();
        _persistit.close();
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(saveProperties);
        JournalManager logMan = _persistit.getJournalManager();
        assertTrue(logMan.getPageMapSize() > 0);
        fetch1a();
        fetch1b();
    }

    public void testCopierCleansUpJournals() throws Exception {
        store1();
        JournalManager logMan = _persistit.getJournalManager();
        assertTrue(logMan.getPageMapSize() > 0);
        _persistit.checkpoint();
        logMan.copyBack(Long.MAX_VALUE);
        _persistit.close();
        assertEquals(0, logMan.getPageMapSize());
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(saveProperties);
        logMan = _persistit.getJournalManager();
        assertEquals(0, logMan.getPageMapSize());
        fetch1a();
        fetch1b();
    }

    public void testRecoverCommittedTransactions() throws Exception {
        // create 10 transactions on the journal
        _persistit.getJournalManager().setCopyingSuspended(true);
        store2();
        _persistit.crash();
        final Properties saveProperties = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.getJournalManager().setCopyingSuspended(true);
        final RecoveryPlan plan = _persistit.getJournalManager()
                .getRecoveryPlan();
        plan.setRecoveryDisabledForTestMode(true);
        _persistit.initialize(saveProperties);
        assertEquals(15, plan.getCommittedCount());
        plan.setRecoveryDisabledForTestMode(false);
        plan.applyAllCommittedTransactions();
        System.out.println("done");
    }
    
    // TODO: Fix long-record transactions
//    public void testLongRecordTransactionRecovery() throws Exception {
//        // create 10 transactions on the journal
//        _persistit.getJournalManager().setCopyingSuspended(true);
//        store3();
//        _persistit.crash();
//        final Properties saveProperties = _persistit.getProperties();
//        _persistit = new Persistit();
//        _persistit.getJournalManager().setCopyingSuspended(true);
//        final RecoveryPlan plan = _persistit.getJournalManager()
//                .getRecoveryPlan();
//        plan.setRecoveryDisabledForTestMode(true);
//        _persistit.initialize(saveProperties);
//        assertEquals(15, plan.getCommittedCount());
//        plan.setRecoveryDisabledForTestMode(false);
//        plan.applyAllCommittedTransactions();
//        System.out.println("done");
//    }
    
    public void testRolloverDoesntDeleteLiveTransactions() throws Exception {
        store1();
        JournalManager logMan = _persistit.getJournalManager();
        assertTrue(logMan.getPageMapSize() > 0);
        logMan.rollover();
        _persistit.checkpoint();
        logMan.copyBack(Long.MAX_VALUE);

        assertEquals(2, logMan.getFirstGeneration());
        assertEquals(0, logMan.getPageMapSize());
        
        final Transaction txn = _persistit.getTransaction();
        
        txn.begin();
        store1();
        logMan.setUnitTestNeverCloseTransactionId(txn.getId());
        txn.commit();
        txn.end();
        logMan.setUnitTestNeverCloseTransactionId(Long.MIN_VALUE);
        
        logMan.rollover();
        _persistit.checkpoint();
        logMan.copyBack(Long.MAX_VALUE);
        // because JournalManager thinks there's an open transaction
        // (due to the call to setUnitTesNeverCloseTransactionId method)
        // it should preserve the journal file containing the TS record
        // for the transaction.
        assertEquals(2, logMan.getFirstGeneration());
        assertEquals(0, logMan.getPageMapSize());
        
        txn.begin();
        store1();
        txn.commit();
        txn.end();

        // Using the same transaction resets the transaction status; 
        // after the commit() call above, JournalManager should not
        // have any open transactions.  Therefore rollover should 
        // delete the earlier files after copyBack.
        //
        logMan.rollover();
        _persistit.checkpoint();
        logMan.copyBack(Long.MAX_VALUE);
        
        assertEquals(4, logMan.getFirstGeneration());
        assertEquals(0, logMan.getPageMapSize());
        
        fetch1a();
        fetch1b(); 
    }

    private void store1() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName,
                "SimpleTest1", true);
        exchange.removeAll();
        final StringBuffer sb = new StringBuffer();

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
        final StringBuffer sb = new StringBuffer();

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
        final StringBuffer sb = new StringBuffer();
        for (int i = 1; i < 400; i++) {
            sb.setLength(0);
            sb.append((char) (i % 20 + 64));
            sb.append((char) (i / 20 + 64));
            exchange.clear().append(sb);
            exchange.fetch();
            final int k = (i / 20) + (i % 20) * 20;
            assertEquals(exchange.getValue().getString(), "Record #" + k);
        }

        System.out.println("- done");
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
                    ex.clear().append("test1").append(j).append(i).remove(
                            Key.GTEQ);
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
        ex.getValue().setMaximumSize(8000000);
        for (int j = 0; j++ < 5;) {
            final StringBuilder sb = new StringBuilder(5000000);
            for (int i = 0; i < 1000000; i++) {
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
                    ex.clear().append("test1").append(j).append(i).remove(
                            Key.GTEQ);
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


    public static void main(final String[] args) throws Exception {
        _args = args;
        new RecoveryTest().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        testRecoveryRebuildsPageMap();
        testCopierCleansUpJournals();
        testRecoverCommittedTransactions();
//        test4();
    }
}
