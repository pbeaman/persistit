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

import static com.persistit.TransactionStatus.ABORTED;
import static com.persistit.TransactionStatus.TIMED_OUT;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.junit.Test;

import com.persistit.exception.TimeoutException;
import com.persistit.util.ArgParser;

public class TransactionIndexConcurrencyTest extends TestCase {
    final static int HASH_TABLE_SIZE = 1000;
    final static int MVV_COUNT = 20000;
    final static int ITERATIONS = 50000;
    final static int THREAD_COUNT = 10;
    final static int SEED = 1;
    final static Random RANDOM = new Random(SEED);

    final TimestampAllocator tsa = new TimestampAllocator();
    final TransactionIndex ti;
    final MVV[] mvvs;
    final AtomicLong commits = new AtomicLong();
    final AtomicLong aborts = new AtomicLong();
    final AtomicLong timeouts = new AtomicLong();

    static int hashTableSize = HASH_TABLE_SIZE;
    static int mvvCount = MVV_COUNT;
    static int iterations = ITERATIONS;
    static int threadCount = THREAD_COUNT;
    static boolean sleep = false;

    static class MVV {
        List<Long> versionHandles = new ArrayList<Long>();
    }

    public TransactionIndexConcurrencyTest() {
        ti = new TransactionIndex(tsa, hashTableSize);
        mvvs = new MVV[mvvCount];
        for (int i = 0; i < mvvCount; i++) {
            mvvs[i] = new MVV();
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
        for (int i = 0; i < iterations; i++) {
            runTransaction(txn);
            if ((i % 100) == 99) {
                ti.updateActiveTransactionCache();
            }
        }

        for (int i = 0; i < mvvCount; i++) {
            prune(mvvs[i]);
            assertTrue(mvvs[i].versionHandles.isEmpty());
        }
    }

    @Test
    public void testConcurrentOperations() throws Exception {
        final long start = System.currentTimeMillis();
        final AtomicLong reported = new AtomicLong(start);
        Timer timer = new Timer();
        final AtomicInteger errCount = new AtomicInteger();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                ti.updateActiveTransactionCache();
                final long now = System.currentTimeMillis();
                if (now - reported.get() > 60000) {
                    reported.addAndGet(60000);
                    report(now - start);
                }
            }

        }, 10, 10);
        final Thread threads[] = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    final Txn txn = new Txn();
                    try {
                        for (int i = 0; i < iterations; i++) {
                            runTransaction(txn);
                        }
                    } catch (Exception e) {
                        errCount.incrementAndGet();
                        e.printStackTrace();
                    }
                }
            }, String.format("Test%03d", i));
            threads[i] = thread;
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        timer.cancel();
        
        final long end = System.currentTimeMillis();
        System.out.printf("\nBefore cleanup:\n");
        report(end - start);
        
        ti.cleanup();
        System.out.printf("\nAfter cleanup:\n");
        report(end - start);
        //
        // Verify that all mvv's were cleaned up
        //
        for (int i = 0; i < mvvCount; i++) {
            prune(mvvs[i]);
            assertTrue(mvvs[i].versionHandles.isEmpty());
        }
        assertEquals(0, errCount.get());
        assertTrue(aborts.get() > 0);
        assertTrue(commits.get() > 0);
        assertTrue(timeouts.get() > 0);
    }

    private void report(final long elapsed) {
        System.out.printf("%,8dms:  Commits=%,d Aborts=%,d\nCurrentCount=%,d  AbortedCount=%,d  "
                + "LongRunningCount=%,d  FreeCount=%,d\natCache=%s\n\n", elapsed, commits.get(), aborts.get(), ti
                .getCurrentCount(), ti.getAbortedCount(), ti.getLongRunningCount(), ti.getFreeCount(), ti
                .getActiveTransactionCache());

    }

    private void runTransaction(final Txn txn) throws Exception {
        txn.status = ti.registerTransaction();
        final long ts = txn.status.getTs();
        sometimesSleep(5);
        int vcount = RANDOM.nextInt(4);
        boolean okay = true;
        for (int i = 0; okay && i < vcount; i++) {
            int mvvIndex = RANDOM.nextInt(mvvCount);
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
                        long tc = ti.wwDependency(vh, txn.status, 0);
                        if (tc == TIMED_OUT) {
                            timeouts.incrementAndGet();
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
                        txn.status.incrementMvvCount();
                    }
                }
                if (retry) {
                    long tc = ti.wwDependency(versionHandle, txn.status, 300000);
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
        if (okay) {
            txn.status.commit(tsa.getCurrentTimestamp());
            commits.incrementAndGet();
        } else {
            txn.status.abort();
            aborts.incrementAndGet();
        }
        sometimesSleep(1);
        long tc = tsa.updateTimestamp();
        ti.notifyCompleted(txn.status, tc);
    }

    void prune(final MVV mvv) throws TimeoutException, InterruptedException {
        long ts0 = 0;
        for (int index = 0; index < mvv.versionHandles.size(); index++) {
            long vh = mvv.versionHandles.get(index);
            long tc = ti.commitStatus(vh, Long.MAX_VALUE, 0);
            if (tc == ABORTED) {
                // remove if aborted
                mvv.versionHandles.remove(index);
                ti.decrementMvvCount(vh);
                index--;
            } else if (tc > 0 && !ti.hasConcurrentTransaction(ts0, tc)) {
                // remove if primordial - simulation does not need to keep it
                mvv.versionHandles.remove(index);
                index--;
            }
        }
    }

    void sometimesSleep(final int pctProbability) throws InterruptedException {
        if (sleep) {
            if (RANDOM.nextInt(100) < pctProbability) {
                Thread.sleep(1);
            }
            if (RANDOM.nextInt(10000) < pctProbability) {
                Thread.sleep(100);
            }
            if (RANDOM.nextInt(1000000) < pctProbability) {
                Thread.sleep(10000);
                System.out.println("Sleep 10 seconds");
            }
            if (RANDOM.nextInt(100000000) < pctProbability) {
                Thread.sleep(100000);
                System.out.println("Sleep 100 seconds");
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final ArgParser ap = new ArgParser("TransactionIndexConcurrencyTest", args, new String[] {
                "iterations|int:20000:0:1000000000|Transaction iterations per thread",
                "threads|int:20:1:1000|Thread count", "mvvCount|int:1:1:1000000000|Number of MVV buckets",
                "hashCount|int:1000:1:100000000|Hash table size", "_flag|s|Enable sleep intervals" });

        iterations = ap.getIntValue("iterations");
        threadCount = ap.getIntValue("threads");
        hashTableSize = ap.getIntValue("hashCount");
        mvvCount = ap.getIntValue("mvvCount");
        sleep = ap.isFlag('s');
        
        final TransactionIndexConcurrencyTest test = new TransactionIndexConcurrencyTest();
        test.testConcurrentOperations();
    }
}
