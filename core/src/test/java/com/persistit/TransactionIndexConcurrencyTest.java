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

import static com.persistit.TransactionStatus.ABORTED;
import static com.persistit.TransactionStatus.TIMED_OUT;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.junit.Test;

import com.persistit.exception.TimeoutException;

public class TransactionIndexConcurrencyTest extends TestCase {
    final static int HASH_TABLE_SIZE = 1000;
    final static int MVV_COUNT = 20000;
    final static int ITERATIONS = 50000;
    final static int THREAD_COUNT = 20;
    final static int SEED = 1;

    final static Random RANDOM = new Random(SEED);
    final TimestampAllocator tsa = new TimestampAllocator();
    final TransactionIndex ti = new TransactionIndex(tsa, HASH_TABLE_SIZE);
    final MVV[] mvvs = new MVV[MVV_COUNT];
    final AtomicLong commits = new AtomicLong();
    final AtomicLong aborts = new AtomicLong();

    static class MVV {
        List<Long> versionHandles = new ArrayList<Long>();
    }

    public TransactionIndexConcurrencyTest() {
        super();
        for (int i = 0; i < MVV_COUNT; i++) {
            mvvs[i] = new MVV();
        }
    }

    static class TestRunnable implements Runnable {

        public void run() {

        }

    }

    static class Txn {
        static int counter = 0;
        int id = ++counter;

        TransactionStatus status;
    }

    @Test
    public void testSingleThreaded() throws Exception {
        final Txn txn = new Txn();
        for (int i = 0; i < ITERATIONS; i++) {
            runTransaction(txn);
            if ((i % 100) == 99) {
                ti.updateActiveTransactionCache();
            }
        }

        for (int i = 0; i < MVV_COUNT; i++) {
            prune(mvvs[i]);
            assertTrue(mvvs[i].versionHandles.isEmpty());
        }
    }

    @Test
    public void testConcurrentOperations() throws Exception {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
               ti.updateActiveTransactionCache();
            }
            
        }, 100, 100);
        final Thread threads[] = new Thread[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    final Txn txn = new Txn();
                    try {
                        for (int i = 0; i < ITERATIONS; i++) {
                            runTransaction(txn);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, String.format("Test%03d", i));
            threads[i] = thread;
        }
        for (int i = 0; i < THREAD_COUNT; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREAD_COUNT; i++) {
            threads[i].join();
        }
        timer.cancel();
        ti.updateActiveTransactionCache();
        System.out.printf("Commits=%,d  Aborts=%,d\n", commits.get(), aborts.get());
        for (int i = 0; i < MVV_COUNT; i++) {
            prune(mvvs[i]);
            assertTrue(mvvs[i].versionHandles.isEmpty());
        }
    }

    private void runTransaction(final Txn txn) throws Exception {
        txn.status = ti.registerTransaction();
        final long ts = txn.status.getTs();
        sometimesSleep(5);
        int vcount = RANDOM.nextInt(4);
        boolean okay = true;
        for (int i = 0; okay && i < vcount; i++) {
            int mvvIndex = RANDOM.nextInt(MVV_COUNT);
            MVV mvv = mvvs[mvvIndex];
            boolean retry = true;
            int index = 0;
            while (retry && okay) {
                long versionHandle = 0;
                retry = false;
                sometimesSleep(1);
                synchronized (mvv) {
                    prune(mvv);
                    for (; index < mvv.versionHandles.size(); index++) {
                        long vh = mvv.versionHandles.get(index);
                        long tc = ti.wwDependency(vh, ts, 0);
                        if (tc == TIMED_OUT) {
                            versionHandle = vh;
                            retry = true;
                            break;
                        } else if (tc > 0) {
                            okay = false;
                            break;
                        } else {

                        }
                    }
                    if (okay && !retry) {
                        mvv.versionHandles.add(TransactionIndex.ts2vh(ts));
                    }
                }
                if (retry) {
                    long tc = ti.wwDependency(versionHandle, ts, 60000);
                    if (tc == TIMED_OUT) {
                        throw new TimeoutException();
                    }
                    if (tc > 0) {
                        okay = false;
                    }
                }
            }
        }
        sometimesSleep(5);
        long tc = tsa.updateTimestamp();
        if (okay) {
            txn.status.commit(tc);
            commits.incrementAndGet();
        } else {
            txn.status.abort();
            aborts.incrementAndGet();
        }
        sometimesSleep(1);
        ti.notifyCompleted(txn.status);
    }

    void prune(final MVV mvv) throws TimeoutException, InterruptedException {
        long ts0 = 0;
        for (int index = 0; index < mvv.versionHandles.size(); index++) {
            long vh = mvv.versionHandles.get(index);
            long tc = ti.commitStatus(vh, Long.MAX_VALUE, 0);
            if (tc == ABORTED) {
                // remove if aborted
                mvv.versionHandles.remove(index);
                index--;
            } else if (tc > 0 && !ti.hasConcurrentTransaction(ts0, tc)) {
                // remove if primordial - simulation does not need to keep it
                mvv.versionHandles.remove(index);
                index--;
            }
        }
    }

    void sometimesSleep(final int pctProbability) throws InterruptedException {
        if (RANDOM.nextInt(100) < pctProbability) {
            Thread.sleep(1);
        }
    }
}
