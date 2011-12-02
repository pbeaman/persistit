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
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.persistit.RecoveryManager.RecoveryListener;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class AccumulatorRecoveryTest extends PersistitUnitTestCase {
    final static int ROW_COUNT_ACCUMULATOR_INDEX = 17;
    /*
     * This class needs to be in com.persistit because of some package-private
     * methods used in controlling the test.
     */
    private String journalSize = "10000000";
    private Random random = new Random();
    final AtomicBoolean running = new AtomicBoolean();
    final AtomicLong counter = new AtomicLong();

    @Override
    protected Properties getProperties(final boolean cleanup) {
        final Properties properties = super.getProperties(cleanup);
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
        final RecoveryListener commitListener = new RecoveryListener() {

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
        plan.applyAllCommittedTransactions(commitListener, plan.getDefaultRollbackListener());
        assertEquals(15, recoveryTimestamps.size());
        assertEquals(expectedRowCount.get(), recoveredRowCount.get());
    }

    /**
     * Insert "rows" within transactions in concurrent threads. Crash Persistit.
     * Verify that accumulators match stored data.
     */
    @Test
    public void testAccumulatorRecovery() throws Exception {

        running.set(true);
        counter.set(0);
        // Make sure the helper methods work
        accumulateRows(10000);
        long accumulated = verifyRowCount();
        assertEquals(counter.get(), accumulated);

        // Make sure the helper methods work in concurrent transactions
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    try {
                        for (int j = 0; j < 10; j++) {
                            accumulateRows(100);
                            verifyRowCount();
                        }
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

    private void accumulateRows(int max) throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "AccumulatorRecoveryTest", true);
        final Transaction txn = _persistit.getTransaction();
        int count = 0;
        while (running.get() && count++ < max) {
            int key = random.nextInt(1000000);
            int op = random.nextInt(100);
            txn.begin();
            try {
                boolean exists = exchange.to(key).isValueDefined();
                int update = 0;
                if (op < 80) {
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
            } finally {
                txn.end();
            }
        }
    }

    private long verifyRowCount() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "AccumulatorRecoveryTest", false);
        final Transaction txn = _persistit.getTransaction();
        txn.begin();
        try {
            Accumulator rowCount = exchange.getTree().getAccumulator(Accumulator.Type.SUM, ROW_COUNT_ACCUMULATOR_INDEX);
            final long accumulated = rowCount.getSnapshotValue(txn);
            long counted = 0;
            exchange.to(Key.BEFORE);
            while (exchange.next()) {
                counted++;
            }
            long accumulated2 = rowCount.getSnapshotValue(txn);
            if (accumulated != counted || accumulated != accumulated2) {
                synchronized (this) {
                    System.out.printf("%s accumulated=%,d accumulated2=%,d counted=%,d\n", Thread.currentThread()
                            .getName(), accumulated, accumulated2, counted);
                }
            }
            txn.commit();
            return counted;
        } finally {
            txn.end();
        }
    }
}
