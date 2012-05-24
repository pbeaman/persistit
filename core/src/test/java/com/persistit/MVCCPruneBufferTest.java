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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.util.Util;

public class MVCCPruneBufferTest extends MVCCTestBase {
    
    @Test
    public void testPrunePrimordialAntiValues() throws PersistitException {
        trx1.begin();
      
        ex1.getValue().put(RED_FOX);
        for (int i = 0; i < 5000; i++) {
            ex1.to(i).store();
        }
        for (int i = 2; i < 5000; i += 2) {
            ex1.to(i).remove();
        }
        ex1.to(2500);
        Buffer buffer1 = ex1.fetchBufferCopy(0);
        int mvvCount = buffer1.getMvvCount();
        assertTrue(mvvCount > 0);
        int available = buffer1.getAvailableSize();
        int keys = buffer1.getKeyCount();
        buffer1.claim(true);
        buffer1.pruneMvvValues(null);
        assertEquals(keys, buffer1.getKeyCount());
        assertTrue(buffer1.getMvvCount() > 0);
        assertEquals(available, buffer1.getAvailableSize());

        trx1.commit();
        trx1.end();

        _persistit.getTransactionIndex().updateActiveTransactionCache();

        buffer1.pruneMvvValues(null);
        assertEquals(0, _persistit.getCleanupManager().getAcceptedCount());
        assertTrue("Pruning should have removed primordial Anti-values", keys > buffer1.getKeyCount());
        // mvvCount is 1 because there is still a leading primordial AntiValue
        assertEquals("Pruning should leave mvvCount==1", 1, buffer1.getMvvCount());
        assertTrue("Pruning should liberate space", buffer1.getAvailableSize() > available);
    }

    @Test
    public void testPruneCleanup() throws Exception {
        final CleanupManager cm = _persistit.getCleanupManager();
        trx1.begin();

        ex1.getValue().put(RED_FOX);
        for (int i = 0; i < 5000; i++) {
            ex1.to(i).store();
        }
        ex1.removeAll();
        ex1.to(2500);
        Buffer buffer1 = ex1.fetchBufferCopy(0);
        int mvvCount = buffer1.getMvvCount();
        assertTrue(mvvCount > 0);
        int available = buffer1.getAvailableSize();
        int keys = buffer1.getKeyCount();
        buffer1.claim(true);
        buffer1.pruneMvvValues(null);
        buffer1.release();
        assertEquals(keys, buffer1.getKeyCount());
        assertTrue(buffer1.getMvvCount() > 0);
        assertEquals(available, buffer1.getAvailableSize());
        trx1.commit();
        trx1.end();

        _persistit.getTransactionIndex().updateActiveTransactionCache();
        ex1.ignoreMVCCFetch(true);
        int antiValueCount1 = 0;
        ex1.to(Key.BEFORE);
        while (ex1.next()) {
            antiValueCount1++;
        }
        assertTrue(antiValueCount1 > 0);

        /*
         * Prune without enqueuing edge key AntiValues
         */
        final long pageCount = ex1.getVolume().getStorage().getNextAvailablePage();
        for (long page = 1; page < pageCount; page++) {
            final Buffer buffer = ex1.getBufferPool().get(ex1.getVolume(), page, true, true);
            try {
                buffer.pruneMvvValues(null);
            } finally {
                buffer.release();
            }
        }
        int antiValueCount2 = 0;
        ex1.to(Key.BEFORE);
        while (ex1.next()) {
            antiValueCount2++;
        }

        assertTrue(antiValueCount2 > 0);
        assertTrue(antiValueCount2 < antiValueCount1);

        cm.setPollInterval(-1);
        /*
         * Prune with enqueuing edge key AntiValues
         */
        for (long page = 1; page < pageCount; page++) {
            final Buffer buffer = ex1.getBufferPool().get(ex1.getVolume(), page, true, true);
            try {
                buffer.pruneMvvValues(ex1.getTree());
            } finally {
                buffer.release();
            }
        }
        
        assertTrue(cm.getAcceptedCount() > 0);
        cm.poll();
        assertEquals("Should have performed all actions", cm.getAcceptedCount(), cm.getPerformedCount());

        int antiValueCount3 = 0;
        ex1.to(Key.BEFORE);
        while (ex1.next()) {
            antiValueCount3++;
        }

        assertEquals(0, antiValueCount3);
    }
    
    @Test
    public void testPruneLongRecordsSimple() throws Exception {
        _persistit.getCleanupManager().setPollInterval(Long.MAX_VALUE);
        trx1.begin();
        storeLongMVV(ex1, "x");
        trx1.commit();
        trx1.end();
        _persistit.getTransactionIndex().cleanup();
        ex1.prune();
        assertTrue("Should no longer be an MVV", !ex1.isValueLongMVV());
    }

    @Test
    public void testPruneLongRecordsWithRollback() throws Exception {
        _persistit.getCleanupManager().setPollInterval(Long.MAX_VALUE);
        /*
         * Start a concurrent transaction to prevent pruning during the store operations.
         */
        final Exchange ex0 = createUniqueExchange();
        ex0.getTransaction().begin();
        
        trx2.begin();
        storeLongMVV(ex2, "x");
        trx2.commit();
        trx2.end();
        trx1.begin();
        storeLongMVV(ex1, "x");
        trx1.flushTransactionBuffer(true);
        trx1.rollback();
        trx1.end();
        ex0.getTransaction().commit();
        ex0.getTransaction().end();
        _persistit.getTransactionIndex().cleanup();
        ex1.prune();
        assertTrue("Should no longer be an MVV", !ex1.isValueLongMVV());
    }

    @Test
    public void testComplexPruning() throws Exception {
        _persistit.getCheckpointManager().setCheckpointIntervalNanos(5000);
        final Thread[] threads = new Thread[500];
        for (int cycle = 0; cycle < threads.length; cycle++) {
            Thread.sleep(50);
            final int myCycle = cycle;
            threads[cycle] = new Thread(new Runnable() {
                public void run() {
                    Transaction txn = _persistit.getTransaction();
                    try {
                        txn.begin();
                        switch (myCycle % 3) {
                        case 0:
                            storeNewVersion(myCycle);
                            break;
                        case 1:
                            removeKeys(myCycle);
                            break;
                        case 2:
                            countKeys();
                            break;
                        }
                        if ((myCycle % 7) < 3) {
                            txn.rollback();
                        } else {
                            txn.commit();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        txn.end();
                    }
                }
            });
            threads[cycle].start();

        }
        for (int cycle = 0; cycle < threads.length; cycle++) {
            threads[cycle].join();
        }
    }

    @Test
    public void testDeadlock() throws Exception {
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int cycle = 0; cycle < 200; cycle++) {
                    if ((cycle % 10) == 0) {
                        System.out.println("cycle " + cycle);
                    }
                    Transaction txn = _persistit.getTransaction();
                    try {
                        txn.begin();
                        storeNewVersion(cycle);
                        Util.sleep(10);
                        txn.rollback();
                        System.gc();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        txn.end();
                    }
                }
            }
        });
        thread.start();
        while (thread.isAlive()) {
            _persistit.checkpoint();
            Util.sleep(100);
        }
    }

    private void storeNewVersion(final int cycle) throws Exception {
        final Exchange exchange = _persistit.getExchange(TEST_VOLUME_NAME, String.format("%s%04d", TEST_TREE_NAME,
                cycle), true);
        exchange.getValue().put(String.format("%s%04d", RED_FOX, cycle));
        for (int i = 1; i <= 100; i++) {
            exchange.to(i).store();
        }
    }

    private void removeKeys(final int cycle) throws Exception {
        final Exchange exchange = _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
        for (int i = (cycle % 2) + 1; i <= 100; i += 2) {
            exchange.to(i).remove();
        }

    }

    private int countKeys() throws Exception {
        Thread.sleep(2000);
        final Exchange exchange = _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
        exchange.clear().append(Key.BEFORE);
        int count = 0;
        while (exchange.next()) {
            count++;
        }
        return count;
    }
    
    public static void main(final String[] args) throws Exception {
        int repeat = 100;
        if (args.length > 0) {
            repeat = Integer.parseInt(args[0]);
        }
        for (int i = 1; i <=repeat; i++) {
            MVCCPruneBufferTest test = new MVCCPruneBufferTest();
            System.out.println("Cycle " + i);
            test.setUp();
            test.testPruneCleanup();
            test.tearDown();
        }
    }
}
