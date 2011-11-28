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

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.exception.TimeoutException;
import com.persistit.unit.PersistitUnitTestCase;

public class AccumulatorTest extends PersistitUnitTestCase {

    private final TimestampAllocator _tsa = new TimestampAllocator();

    public void testBasicMethodsOneBucket() throws Exception {
        final TransactionIndex ti = new TransactionIndex(_tsa, 1);
        Accumulator acc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        TransactionStatus status = ti.registerTransaction();
        acc.update(1, status, 0);
        assertEquals(1, acc.getLiveValue());
        assertEquals(0, acc.getSnapshotValue(1, 0));
        status.commit(_tsa.updateTimestamp());
        assertEquals(0, acc.getSnapshotValue(_tsa.getCurrentTimestamp(), 0));
        ti.notifyCompleted(status);
        assertEquals(0, acc.getSnapshotValue(status.getTs(), 0));
        assertEquals(1, acc.getSnapshotValue(status.getTc() + 1, 0));
    }

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
            ti.notifyCompleted(status);
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
    }

    /**
     * Run a bunch of concurrent pseudo-transactions with random pauses between
     * update, commit and notifyCompleted. Each pseudo-transaction increments an
     * Accumulator. In foreground thread periodically compute and check for
     * sanity the Accumulator's snapshot value. Conclude by verifying total.
     * 
     */
    public void testAggregationRetry() throws Exception {
        final long time = 10000;
        final TransactionIndex ti = new TransactionIndex(_tsa, 5000);
        final AtomicLong before = new AtomicLong();
        final AtomicLong after = new AtomicLong();
        final AtomicLong pauseTime = new AtomicLong();
        final Accumulator acc = Accumulator.accumulator(Accumulator.Type.SUM, null, 0, 0, ti);
        final long stopTime = System.currentTimeMillis() + time;
        final Random random = new Random();
        final Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    while (System.currentTimeMillis() < stopTime) {
                        final TransactionStatus status;
                        try {
                            status = ti.registerTransaction();
                            acc.update(1, status, 0);
                            before.incrementAndGet();
                            if (random.nextInt(100) < 2) {
                                Thread.sleep(1);
                                pauseTime.incrementAndGet();
                            }
                            status.commit(_tsa.updateTimestamp());
                            if (random.nextInt(100) < 2) {
                                Thread.sleep(1);
                                pauseTime.incrementAndGet();
                            }
                            ti.notifyCompleted(status);
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
        for (int i = 0; System.currentTimeMillis() < stopTime; i++) {
            Thread.sleep(5);
            ti.updateActiveTransactionCache();
            if ((i % 10) == 0) {
                long low = after.get();
                long timestamp = _tsa.updateTimestamp();
                long value = acc.getSnapshotValue(timestamp, 0);
                long high = before.get();
                assertTrue(low <= value);
                assertTrue(value <= high);
            }
        }
        for (final Thread thread : threads) {
            thread.join();
        }
        assertEquals(after.get(), acc.getSnapshotValue(_tsa.updateTimestamp(), 0));
        final long workTime = (threads.length * time) - pauseTime.get();
        if (workTime > 0) {
            System.out.printf("Count per ms = %,d", after.get() / workTime);
        }
    }
}
