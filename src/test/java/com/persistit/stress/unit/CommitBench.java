/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit.stress.unit;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.persistit.Exchange;
import com.persistit.PersistitUnitTestCase;
import com.persistit.Transaction;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.exception.RollbackException;
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

    final static Pattern PATH_PATTERN = Pattern.compile("(.+)\\.(\\d{12})");

    /*
     * Want about 1000 pages worth of data (no evictions). Each record is about
     * 50 bytes, so 1000 * 16384 * 60% / 50 ~= 200000.
     */

    private final int RECORDS = 200000;
    private final int RECORDS_PER_TXN = 10;
    private final String[] ARG_TEMPLATE = new String[] { "threads|int:1:1:1000|Number of threads",
            "duration|int:10:10:86400|Duration of test in seconds",
            "policy|String:HARD|Commit policy: SOFT, HARD or GROUP", "datapath|String|Datapath property",
            "_flag|P|Reuse journal file" };

    volatile long stopTime;
    final ArgParser ap;
    AtomicInteger commitCount = new AtomicInteger();
    AtomicInteger rollbackCount = new AtomicInteger();

    CommitBench(final String[] args) {
        ap = new ArgParser("CommitBench", args, ARG_TEMPLATE).strict();
    }

    @Override
    protected Properties getProperties(final boolean cleanup) {
        final Properties p = UnitTestProperties.getBiggerProperties(false);
        if (ap.isSpecified("datapath")) {
            p.setProperty("datapath", ap.getStringValue("datapath"));
        }
        /*
         * Custom data directory cleanup - leaving the journal file behind if
         */
        final String path = p.getProperty("datapath");
        final File dir = new File(path);
        assert dir.isDirectory() : "Data path does not specify a directory: " + path;
        final File[] files = dir.listFiles();
        for (final File file : files) {
            if (ap.isFlag('P') && PATH_PATTERN.matcher(file.getName()).matches()) {
                try {
                    /*
                     * Damage the file so that there's no keystone checkpoint
                     */
                    final RandomAccessFile raf = new RandomAccessFile(file, "rws");
                    raf.seek(0);
                    raf.write(new byte[256]);
                    raf.close();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                if (file.isDirectory()) {
                    UnitTestProperties.cleanUpDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        return p;
    }

    public void bench() throws Exception {
        final int threadCount = ap.getIntValue("threads");
        final int duration = ap.getIntValue("duration");
        final String policy = ap.getStringValue("policy");
        _persistit.setDefaultTransactionCommitPolicy(CommitPolicy.valueOf(policy));
        final TransactionRun[] runs = new TransactionRun[threadCount];
        final Thread[] threads = new Thread[threadCount];
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
        final long elapsed = System.currentTimeMillis() - startTime;
        System.out.printf("Joined %,d threads after %,d seconds\n", threadCount, (elapsed / 1000));
        final long committed = commitCount.get();
        final long rate = (committed * 60000) / elapsed;
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

        TransactionRun(final int index, final int count) {
            threadId = index;
            threadCount = count;
        }

        @Override
        public void run() {
            try {
                if (txn == null) {
                    exchange = _persistit.getExchange("persistit", "CommitBench", true);
                    txn = exchange.getTransaction();
                }
                while (System.currentTimeMillis() < stopTime) {
                    try {
                        txn.begin();
                        final int key = (random.nextInt(RECORDS / threadCount / RECORDS_PER_TXN) * threadCount + threadId)
                                * RECORDS_PER_TXN;
                        exchange.getValue().put(
                                RED_FOX + " " + Thread.currentThread().getName() + " " + ++countPerThread);
                        for (int index = 0; index < RECORDS_PER_TXN; index++) {
                            exchange.to((key + index) % RECORDS).store();
                        }
                        txn.commit();
                        commitCount.incrementAndGet();
                    } catch (final RollbackException e) {
                        rollbackCount.incrementAndGet();
                    } finally {
                        txn.end();
                    }
                }
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final CommitBench bench = new CommitBench(args);
        try {
            bench.setUp();
            bench.bench();
        } finally {
            bench.tearDown();
        }
    }
}
