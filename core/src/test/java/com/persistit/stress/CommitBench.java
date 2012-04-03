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

package com.persistit.stress;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.exception.RollbackException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;
import com.persistit.util.ArgParser;

/**
 * Synthetic benchmark of Commit durability processing. This class tests
 * performance of the transaction commit method under the SOFT, HARD and GROUP
 * policies for varying numbers of threads. Transaction deliberately (a) do not
 * read any pages and (b) create a limited numbers of dirty pages. The purpose
 * is to isolate and study just the I/O required to commit transactions.
 * 
 * @author peter
 * 
 */
public class CommitBench extends PersistitUnitTestCase {
    /*
     * Want about 1000 pages worth of data (no evictions). Each record is about
     * 50 bytes, so 1000 * 16384 * 60% / 50 ~= 200000.
     */

    private final int RECORDS = 200000;
    private final int RECORDS_PER_TXN = 10;
    private final String[] ARG_TEMPLATE = new String[] { "threads|int:1:1000|Number of threads",
            "duration|int:10:86400|Duration of test in seconds",
            "policy|String:HARD|Commit policy: SOFT, HARD or GROUP", };

    volatile long stopTime;
    AtomicInteger commitCount = new AtomicInteger();
    AtomicInteger rollbackCount = new AtomicInteger();

    @Override
    protected Properties getProperties(boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    public void bench(final String[] args) throws Exception {
        final ArgParser ap = new ArgParser("CommitBench", args, ARG_TEMPLATE);
        int threadCount = ap.getIntValue("threads");
        int duration = ap.getIntValue("duration");
        String policy = ap.getStringValue("policy");
        _persistit.setDefaultTransactionCommitPolicy(CommitPolicy.valueOf(policy));
        TransactionRun[] runs = new TransactionRun[threadCount];
        Thread[] threads = new Thread[threadCount];
        for (int index = 0; index < threadCount; index++) {
            runs[index] = new TransactionRun(index, threadCount);
            threads[index] = new Thread(runs[index]);
        }
        System.out.printf("Starting %,d threads for %,d seconds with policy %s\n", threadCount, duration, policy);
        final long startTime = System.currentTimeMillis();
        stopTime = startTime + (duration * 1000);
        for (int index = 0; index < threadCount; index++) {
            threads[index].start();
        }
        for (int index = 0; index < threadCount; index++) {
            threads[index].join();
        }
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.printf("Joined %,d threads after %,d seconds\n", threadCount, (elapsed / 1000));
        long committed = commitCount.get();
        long rate = (committed * 60000) / elapsed;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (final TransactionRun run : runs) {
            min = Math.min(min, run.countPerThread);
            max = Math.max(max, run.countPerThread);
        }
        System.out.printf("%,d commited at rate %,d per minute with maximum difference %,d; %,d rollbacks\n",
                committed, rate, max - min, rollbackCount.get());
    }

    private class TransactionRun implements Runnable {
        final int threadId;
        final int threadCount;
        Transaction txn;
        Exchange exchange;
        final Random random = new Random();
        int countPerThread = 0;

        TransactionRun(int index, int count) {
            threadId = index;
            threadCount = count;
        }

        public void run() {
            try {
                if (txn == null) {
                    exchange = _persistit.getExchange("persistit", "CommitBench", true);
                    txn = exchange.getTransaction();
                }
                while (System.currentTimeMillis() < stopTime) {
                    try {
                        txn.begin();
                        int key = (random.nextInt(RECORDS / threadCount / RECORDS_PER_TXN) * threadCount + threadId)
                                * RECORDS_PER_TXN;
                        exchange.getValue().put(
                                RED_FOX + " " + Thread.currentThread().getName() + " " + ++countPerThread);
                        for (int index = 0; index < RECORDS_PER_TXN; index++) {
                            exchange.to((key + index) % RECORDS).store();
                        }
                        txn.commit();
                        commitCount.incrementAndGet();
                    } catch (RollbackException e) {
                        rollbackCount.incrementAndGet();
                    } finally {
                        txn.end();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final CommitBench bench = new CommitBench();
        try {
            bench.setUp();
            bench.bench(args);
        } finally {
            bench.tearDown();
        }
    }
}
