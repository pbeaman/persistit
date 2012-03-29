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

import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.persistit.TransactionPlayer.TransactionPlayerListener;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

public class AccumulatorRecoveryTest extends PersistitUnitTestCase {
    final static int ROW_COUNT_ACCUMULATOR_INDEX = 17;
    /*
     * This class needs to be in com.persistit because of some package-private
     * methods used in controlling the test.
     */
    private String journalSize = "100000000";
    private Random random = new Random();
    final AtomicBoolean running = new AtomicBoolean();
    final AtomicLong counter = new AtomicLong();

    @Override
    protected Properties getProperties(final boolean cleanup) {
        final Properties properties = UnitTestProperties.getBiggerProperties(cleanup);
        properties.setProperty("journalsize", journalSize);
        return properties;
    }

    @Test
    public void testRecoverCommittedTransactions() throws Exception {
        _persistit.getJournalManager().setAppendOnly(true);
        final Exchange ex = _persistit.getExchange("persistit", "RecoveryTest", true);
        final Accumulator rowCount = ex.getTree().getAccumulator(Accumulator.Type.SUM, 0);
        for (int j = 0; j++ < 10;) {

            final Transaction txn = ex.getTransaction();

            txn.begin();
            try {
                for (int i = 0; i < 10; i++) {
                    ex.getValue().put("String value #" + i + " for test1");
                    ex.clear().append("test1").append(j).append(i).store();
                    rowCount.update(1, txn);
                }
                for (int i = 3; i < 10; i += 3) {
                    ex.clear().append("test1").append(j).append(i).remove(Key.GTEQ);
                    rowCount.update(-1, txn);
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
                boolean removed = ex.clear().append("test1").append(j).remove(Key.GTEQ);
                if (removed) {
                    rowCount.update(-7, txn);
                }
                txn.commit();
            } finally {
                txn.end();
            }
        }

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
        final AtomicLong recoveredRowCount = new AtomicLong();
        final AtomicLong expectedRowCount = new AtomicLong();
        final TransactionPlayerListener commitListener = new TransactionPlayerListener() {

            @Override
            public void store(final long address, final long timestamp, Exchange exchange) throws PersistitException {
                recoveryTimestamps.add(timestamp);
                expectedRowCount.incrementAndGet();
            }

            @Override
            public void removeKeyRange(final long address, final long timestamp, Exchange exchange, Key from, Key to)
                    throws PersistitException {
                recoveryTimestamps.add(timestamp);
                expectedRowCount.addAndGet(from.getDepth() == 2 ? -7 : -1); // because
                                                                            // there
                                                                            // are
                                                                            // 7
                                                                            // rows
                                                                            // being
                                                                            // deleted
                                                                            // by
                                                                            // each
                                                                            // range
                                                                            // delete
                                                                            // operation
            }

            @Override
            public void removeTree(final long address, final long timestamp, Exchange exchange)
                    throws PersistitException {
                recoveryTimestamps.add(timestamp);
            }

            @Override
            public void startRecovery(long address, long timestamp) throws PersistitException {
            }

            @Override
            public void startTransaction(long address, long startTmestamp, long commitTimestamp)
                    throws PersistitException {
            }

            @Override
            public void endTransaction(long address, long timestamp) throws PersistitException {
            }

            @Override
            public void endRecovery(long address, long timestamp) throws PersistitException {
            }

            @Override
            public void delta(long address, long timestamp, Tree tree, int index, int accumulatorTypeOrdinal, long value)
                    throws PersistitException {
                recoveredRowCount.addAndGet(value);
            }

        };
        plan.applyAllRecoveredTransactions(commitListener, plan.getDefaultRollbackListener());
        assertEquals(15, recoveryTimestamps.size());
        assertEquals(expectedRowCount.get(), recoveredRowCount.get());
    }

    /**
     * Insert "rows" within transactions in concurrent threads. Crash Persistit.
     * Verify that accumulators match stored data.
     */
    @Test
    public void testAccumulatorRecovery1() throws Exception {

        running.set(true);
        counter.set(0);
        // Make sure the helper methods work
        accumulateRows(10000);
        long accumulated = verifyRowCount();
        assertEquals(counter.get(), accumulated);
    }

    @Test
    public void testAccumulatorRecovery2() throws Exception {
        // Make sure the helper methods work in concurrent transactions
        counter.set(0);
        running.set(true);
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    try {
                        accumulateRows(1000);
                        verifyRowCount();
                    } catch (Exception e) {
                        System.out.println("Thread " + index + " failed");
                        e.printStackTrace();
                    }
                }
            });
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        assertEquals(counter.get(), verifyRowCount());
    }

    @Test
    public void testAccumulatorRecovery3() throws Exception {
        // Crash Persistit while transactions are being executed, then
        // verify the accumulator status
        counter.set(0);
        running.set(true);

        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    for (int j = 0; j < 10; j++) {
                        try {
                            accumulateRows(1000000);
                            verifyRowCount();
                        } catch (Exception e) {
                            System.out.println("Thread " + index + " failed");
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        Thread.sleep(10000);
        _persistit.checkpoint();
        Thread.sleep(5000);
        running.set(false);
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        
        _persistit.crash();
        
        final Properties props = _persistit.getProperties();
        _persistit = new Persistit();
        _persistit.initialize(props);

        verifyRowCount();
    }

    private void accumulateRows(int max) throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "AccumulatorRecoveryTest", true);
        final Transaction txn = _persistit.getTransaction();
        int count = 0;
        Exception exception = null;
        while (running.get() && count++ < max) {
            int key = random.nextInt(1000000);
            int op = random.nextInt(100);
            int retryCount = 0;
            while (true) {
                txn.begin();
                try {
                    boolean exists = exchange.to(key).isValueDefined();
                    int update = 0;
                    if (op < 80) { // TODO - when constant is 80, test causes
                                   // corruption during recovery
                        if (!exists) {
                            exchange.getValue().put(RED_FOX);
                            exchange.store();
                            Accumulator rowCount = exchange.getTree().getAccumulator(Accumulator.Type.SUM,
                                    ROW_COUNT_ACCUMULATOR_INDEX);
                            rowCount.update(1, txn);
                            update = 1;
                        }
                    } else {
                        if (exists) {
                            exchange.remove();
                            Accumulator rowCount = exchange.getTree().getAccumulator(Accumulator.Type.SUM,
                                    ROW_COUNT_ACCUMULATOR_INDEX);
                            rowCount.update(-1, txn);
                            update = -1;
                        }
                    }
                    txn.commit();
                    counter.addAndGet(update);
                    break;
                } catch (RollbackException re) {
                    retryCount++;
                    assertTrue(retryCount < 5);
                    System.out.println("(Acceptable) rollback in " + Thread.currentThread().getName());
                } catch (Exception e) {
                    exception = e;
                    throw e;
                } finally {
                    txn.end();
                }
            }
        }
    }

    private long verifyRowCount() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "AccumulatorRecoveryTest", false);
        final Transaction txn = _persistit.getTransaction();
        Exception exception = null;
        txn.begin();
        try {
            Accumulator rowCount = exchange.getTree().getAccumulator(Accumulator.Type.SUM, ROW_COUNT_ACCUMULATOR_INDEX);
            final long accumulated = rowCount.getSnapshotValue(txn);
            long counted = 0;
            exchange.to(Key.BEFORE);
            while (exchange.next()) {
                counted++;
                assertFalse(exchange.getValue().isAntiValue());
            }
            long accumulated2 = rowCount.getSnapshotValue(txn);
            if (accumulated != counted || accumulated != accumulated2) {
                synchronized (this) {
                    System.out.printf("%s accumulated=%,d accumulated2=%,d counted=%,d\n", Thread.currentThread()
                            .getName(), accumulated, accumulated2, counted);
                }
                assertEquals(accumulated, accumulated2);
                assertEquals(accumulated, counted);
            }
            txn.commit();
            return counted;
        } catch (Exception e) {
            exception = e;
            throw e;
        } finally {
            txn.end();
        }
    }
}
