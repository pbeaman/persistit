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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.exception.PersistitException;
import com.persistit.unit.UnitTestProperties;
import org.junit.Test;

import com.persistit.Accumulator.Type;
import com.persistit.exception.TimeoutException;
import com.persistit.unit.PersistitUnitTestCase;

public class AccumulatorTest extends PersistitUnitTestCase {

    private final TimestampAllocator _tsa = new TimestampAllocator();

    @Test
    public void testBasicMethodsOneBucket() throws Exception {
        final TransactionIndex ti = new TransactionIndex(_tsa, 1);
        Accumulator acc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        TransactionStatus status = ti.registerTransaction();
        acc.update(1, status, 0);
        assertEquals(1, acc.getLiveValue());
        assertEquals(1, acc.getSnapshotValue(1, 0));
        assertEquals(1, acc.getSnapshotValue(1, 1));
        acc.update(1, status, 1);
        assertEquals(1, acc.getSnapshotValue(1, 0));
        assertEquals(1, acc.getSnapshotValue(1, 1));
        assertEquals(2, acc.getSnapshotValue(1, 2));
        status.commit(_tsa.updateTimestamp());
        assertEquals(0, acc.getSnapshotValue(_tsa.getCurrentTimestamp(), 0));
        ti.notifyCompleted(status, _tsa.getCurrentTimestamp());
        assertEquals(0, acc.getSnapshotValue(status.getTs() + 1, 0));
        assertEquals(2, acc.getSnapshotValue(status.getTc() + 1, 0));
    }

    @Test
    public void testBasicMethodsMultipleBuckets() throws Exception {
        final TransactionIndex ti = new TransactionIndex(_tsa, 1000);
        Accumulator countAcc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        Accumulator sumAcc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        Accumulator minAcc = Accumulator.accumulator(Accumulator.Type.MIN, null, 0, 0, ti);
        Accumulator maxAcc = Accumulator.accumulator(Accumulator.Type.MAX, null, 0, 0, ti);
        Accumulator seqAcc = Accumulator.accumulator(Accumulator.Type.SEQ, null, 0, 0, ti);
        for (int count = 0; count < 100000; count++) {
            TransactionStatus status = ti.registerTransaction();
            assertEquals(count, countAcc.getLiveValue());
            assertEquals(count * 3, seqAcc.getLiveValue());
            countAcc.update(1, status, 0);
            sumAcc.update(count, status, 0);
            minAcc.update(-1000 - (count % 17), status, 0);
            maxAcc.update(1000 + (count % 17), status, 0);
            seqAcc.update(3, status, 0);
            status.commit(_tsa.updateTimestamp());
            ti.notifyCompleted(status, _tsa.getCurrentTimestamp());
            if ((count % 1000) == 0) {
                ti.updateActiveTransactionCache();
            }
        }
        long after = _tsa.updateTimestamp();
        assertEquals(100000, countAcc.getSnapshotValue(after, 0));
        assertEquals(-1016, minAcc.getSnapshotValue(after, 0));
        assertEquals(1016, maxAcc.getSnapshotValue(after, 0));
        assertEquals(sumAcc.getLiveValue(), sumAcc.getSnapshotValue(after, 0));
        assertEquals(seqAcc.getLiveValue(), seqAcc.getSnapshotValue(after, 0));

        assertEquals(0, countAcc.getCheckpointValue());
        assertEquals(0, sumAcc.getCheckpointValue());
        assertEquals(0, minAcc.getCheckpointValue());
        assertEquals(0, maxAcc.getCheckpointValue());
        assertEquals(0, seqAcc.getCheckpointValue());

        ti.checkpointAccumulatorSnapshots(_tsa.getCurrentTimestamp(), Arrays.asList(new Accumulator[] { countAcc,
                sumAcc, minAcc, maxAcc, seqAcc }));

        assertEquals(countAcc.getLiveValue(), countAcc.getCheckpointValue());
        assertEquals(sumAcc.getLiveValue(), sumAcc.getCheckpointValue());
        assertEquals(minAcc.getLiveValue(), minAcc.getCheckpointValue());
        assertEquals(maxAcc.getLiveValue(), maxAcc.getCheckpointValue());
        assertEquals(seqAcc.getLiveValue(), seqAcc.getCheckpointValue());

    }

    @Test
    public void testBasicIsolation() throws Exception {
        final SessionId s1 = new SessionId();
        final SessionId s2 = new SessionId();
        int expectedErrors = 0;
        _persistit.setSessionId(s1);
        final Transaction txn1 = _persistit.getTransaction();
        _persistit.setSessionId(s2);
        final Transaction txn2 = _persistit.getTransaction();
        final Tree tree = _persistit.getVolume("persistit").getTree("AccumulatorTest", true);
        final Accumulator acc = tree.getAccumulator(Accumulator.Type.SUM, 0);
        assertTrue(txn1 != txn2);
        txn2.begin();
        try {
            acc.getSnapshotValue(txn1);
            fail("Should have thrown an exceptio");
        } catch (IllegalStateException e) {
            expectedErrors++;
            // expected
        }
        assertEquals(1, expectedErrors);
        txn1.begin();
        assertEquals(0, acc.getSnapshotValue(txn1));
        assertEquals(0, acc.getSnapshotValue(txn2));
        acc.update(1, txn1);
        assertEquals(0, acc.getSnapshotValue(txn2));
        acc.update(1, txn2);
        assertEquals(1, acc.getSnapshotValue(txn1));
        assertEquals(1, acc.getSnapshotValue(txn2));
        txn1.commit();
        txn1.end();
        assertEquals(acc.getSnapshotValue(txn2), 1);
        txn2.commit();
        txn2.end();
        txn1.begin();
        assertEquals(2, acc.getSnapshotValue(txn1));
        txn1.commit();
        txn1.end();
    }

    @Test
    public void testBasicIsolation2() throws Exception {
        final Thread[] threads = new Thread[25];
        final Random random = new Random(1);
        final TransactionIndex ti = _persistit.getTransactionIndex();
        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    long end = System.currentTimeMillis() + 60000;
                    int cleanRun = 0;
                    while (System.currentTimeMillis() < end) {
                        try {
                            final Exchange ex = _persistit.getExchange("persistit", "AccumulatorTest", true);
                            final Transaction txn = ex.getTransaction();
                            txn.begin();
                            try {
                                final Accumulator acc = ex.getTree().getAccumulator(Accumulator.Type.SUM, 0);
                                final long floor1 = ti.getActiveTransactionFloor();
                                final long v1 = acc.getSnapshotValue(txn);
                                Thread.sleep(random.nextInt(2));
                                final long v2 = acc.getSnapshotValue(txn);
                                final long floor2 = ti.getActiveTransactionFloor();
                                acc.update(1, txn);
                                final long v3 = acc.getSnapshotValue(txn);
                                final long v4 = acc.getSnapshotValue(txn);
                                final long v5 = acc.getSnapshotValue(txn);

                                if (v1 != v2 || v2 + 1 != v3 || v3 != v4 || v4 != v5) {
                                    System.out.printf("Thread #%d v1=%,10d v2=%,10d v3=%,10d v4=%,10d v5=%,10d "
                                            + "floor1=%,10d floor2=%,10d cleanRun=%,10d %s\n", index, v1, v2, v3, v4,
                                            v5, floor1, floor2, cleanRun, floor1 == floor2 ? " ***" : "");
                                    cleanRun = 0;
                                } else {
                                    if (floor1 != floor2) {
                                        cleanRun++;
                                    }
                                }
                                txn.commit();
                            } finally {
                                txn.end();
                            }
                        } catch (Exception e) {
                            System.out.println("Exception in thread #" + index);
                            e.printStackTrace();
                            break;
                        }
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
    }

    /**
     * Run a bunch of concurrent pseudo-transactions with random pauses between
     * update, commit and notifyCompleted. Each pseudo-transaction increments an
     * Accumulator. In foreground thread periodically compute and check for
     * sanity the Accumulator's snapshot value. Conclude by verifying total.
     * 
     */
    @Test
    public void testAggregationRetry() throws Exception {
        final long time = 5000;
        final TransactionIndex ti = new TransactionIndex(_tsa, 256);
        final AtomicLong before = new AtomicLong();
        final AtomicLong after = new AtomicLong();
        final AtomicLong pauseTime = new AtomicLong();
        final Accumulator acc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        final List<Accumulator> accumulators = new ArrayList<Accumulator>();
        accumulators.add(acc);
        final long stopTime = System.currentTimeMillis() + time;
        final Random random = new Random();
        final Thread[] threads = new Thread[50];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    while (System.currentTimeMillis() < stopTime) {
                        final TransactionStatus status;
                        try {
                            status = ti.registerTransaction();
                            acc.update(1, status, 0);
                            before.incrementAndGet();
                            if (random.nextInt(100) < 1) {
                                Thread.sleep(1);
                                pauseTime.incrementAndGet();
                            }
                            status.commit(_tsa.getCurrentTimestamp());
                            if (random.nextInt(100) < 5) {
                                Thread.sleep(1);
                                pauseTime.incrementAndGet();
                            }
                            ti.notifyCompleted(status, _tsa.updateTimestamp());
                            after.incrementAndGet();
                        } catch (TimeoutException e) {
                            e.printStackTrace();
                            break;
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            });
        }
        for (final Thread thread : threads) {
            thread.start();
        }
        long elapsedNanos = 0;
        int calls = 0;
        for (int i = 0; System.currentTimeMillis() < stopTime; i++) {
            Thread.sleep(1);
            ti.updateActiveTransactionCache();
                ti.checkpointAccumulatorSnapshots(_tsa.getCurrentTimestamp(), accumulators);
                long low = after.get();
                long timestamp = _tsa.updateTimestamp();
                elapsedNanos -= System.nanoTime();
                long value = acc.getSnapshotValue(timestamp, 0);
                elapsedNanos += System.nanoTime();
                calls++;
                long high = before.get();
                assertTrue(low <= value);
                assertTrue(value <= high);
        }
        for (final Thread thread : threads) {
            thread.join();
        }
        assertEquals(after.get(), acc.getSnapshotValue(_tsa.updateTimestamp(), 0));
        //
        // Verify that retries were created
        //
        assertTrue("At least one RetryException should have been thrown", ti
                .incrementAccumulatorCheckpointRetryCounter() > 1);
        assertTrue("At least one RetryException should have been thrown",
                ti.incrementAccumulatorSnapshotRetryCounter() > 1);
        final long workTime = (threads.length * time) - pauseTime.get();
        if (workTime > 0) {
            System.out.printf("Count per ms = %,d  Nanos per call=%,d", after.get() / workTime, elapsedNanos / calls);
        }
    }

    @Test
    public void testCheckpointSaveToValue() throws Exception {
        final Value value = new Value((Persistit) null);
        final TransactionIndex ti = new TransactionIndex(_tsa, 5000);
        Accumulator sumAcc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        TransactionStatus status = ti.registerTransaction();
        sumAcc.update(18, status, 0);
        status.commit(_tsa.getCurrentTimestamp());
        ti.notifyCompleted(status, _tsa.updateTimestamp());
        ti.checkpointAccumulatorSnapshots(_tsa.updateTimestamp(), Arrays.asList(new Accumulator[] { sumAcc }));
        assertEquals(18, sumAcc.getCheckpointValue());
        value.put(sumAcc);
        Object object = value.get();
        assertTrue(object instanceof AccumulatorState);
        AccumulatorState as = (AccumulatorState) object;
        assertEquals(18, as.getValue());
        assertEquals(sumAcc.getType(), as.getType());
        assertEquals(0, as.getIndex());
        assertEquals("", as.getTreeName());
    }

    @Test
    public void testCheckpointSaveAccumulators() throws Exception {
        int count = 1000;
        for (int retry = 0; retry < 10; retry++) {
            System.out.printf("Retry %,5d\n", retry);
            final Transaction txn = _persistit.getTransaction();
            final String treeName = String.format("AccumulatorTest%2d", retry);
            final Exchange exchange = _persistit.getExchange("persistit", treeName, true);
            final Accumulator rowCount = exchange.getTree().getAccumulator(Type.SUM, 0);
            final Accumulator sequence = exchange.getTree().getAccumulator(Type.SEQ, 1);

            for (int i = 0; i < count; i++) {
                txn.begin();
                try {
                    exchange.clear().append(sequence.update(17, txn));
                    exchange.getValue().put(RED_FOX);
                    exchange.store();
                    rowCount.update(1, txn);
                    txn.commit();
                } finally {
                    txn.end();
                }
            }

            assertEquals(count, rowCount.getLiveValue());
            assertEquals(count * 17, sequence.getLiveValue());

            _persistit.checkpoint();

            AccumulatorState as;

            as = Accumulator.getAccumulatorState(exchange.getTree(), 0);
            assertEquals(treeName, as.getTreeName());
            assertEquals(Type.SUM, as.getType());
            assertEquals(0, as.getIndex());
            assertEquals(count, as.getValue());

            as = Accumulator.getAccumulatorState(exchange.getTree(), 1);
            assertEquals(treeName, as.getTreeName());
            assertEquals(Type.SEQ, as.getType());
            assertEquals(1, as.getIndex());
            assertEquals(count * 17, as.getValue());
        }
    }

    /*
     * bug979332:
     * If a tree that has had accumulator activity is removed, a checkpoint
     * occurs, and that same tree is recreated the accumulators would get
     * initialized with the old, stale values.
     *
     * This was because the map in Persistit was not informed of the remove
     * so the checkpoint proceeded to save data it didn't need to.
     */
    public void testRecreateAccumulatorAfterCheckpoint() throws PersistitException {
        final int PASS_COUNT = 2;
        final int ROW_COUNT = 10;
        final int ACCUM_INDEX = 0;
        final String TEST_VOLUME_NAME = UnitTestProperties.VOLUME_NAME;
        final String TEST_TREE_NAME = "AccumulatorTest";
        final Accumulator.Type ACCUM_TYPE = Accumulator.Type.SUM;

        assertNotNull("Initial checkpoint successful (not null)", _persistit.checkpoint());

        for (int pass = 1; pass <= PASS_COUNT; ++pass) {
            Volume vol = _persistit.getVolume(TEST_VOLUME_NAME);
            assertNull("Tree should not exist, pass"+pass, vol.getTree(TEST_TREE_NAME, false));

            Exchange ex = _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
            Accumulator accum = ex.getTree().getAccumulator(ACCUM_TYPE, ACCUM_INDEX);
            Transaction txn = _persistit.getTransaction();

            txn.begin();
            assertEquals("Initial accumulator value, pass"+pass, 0, accum.getSnapshotValue(txn));
            txn.commit();
            txn.end();

            for(int row = 0; row < ROW_COUNT; ++row) {
                txn.begin();
                ex.clear().append(row);
                accum.update(1, txn);
                txn.commit();
                txn.end();
            }

            txn.begin();
            txn.commit();
            assertEquals("Accumulator after pass inserts, pass"+pass, ROW_COUNT, accum.getSnapshotValue(txn));
            txn.end();

            ex.removeTree();
            assertNotNull("Checkpoint after removeTree successful, pass"+pass, _persistit.checkpoint());
        }
    }
}
