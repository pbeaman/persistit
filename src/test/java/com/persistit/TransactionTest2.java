/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.persistit.Transaction.CommitPolicy;
import com.persistit.exception.PersistitClosedException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PersistitInterruptedException;
import com.persistit.exception.RollbackException;

/**
 * Demonstrates the use of Persistit Transactions. This demo runs multiple
 * threads that transfer "money" between accounts. At all times the sum of all
 * balances should remain unchanged.
 * 
 * @version 1.0
 */
public class TransactionTest2 extends PersistitUnitTestCase {

    final static Object LOCK = new Object();

    final static CommitPolicy policy = CommitPolicy.SOFT;

    final static long TIMEOUT = 20000; // 20 seconds

    static int _threadCount = 8;
    static int _iterationsPerThread = 25000;
    static int _accounts = 5000;

    static AtomicInteger _retriedTransactionCount = new AtomicInteger();
    static AtomicInteger _completedTransactionCount = new AtomicInteger();
    static AtomicInteger _failedTransactionCount = new AtomicInteger();
    static AtomicInteger _strandedThreads = new AtomicInteger();

    static int _threadCounter = 0;

    int _threadIndex = 0;

    public static void main(final String[] args) throws Exception {
        try {
            if (args.length > 0) {
                _threadCount = Integer.parseInt(args[0]);
            }
            if (args.length > 1) {
                _iterationsPerThread = Integer.parseInt(args[1]);
            }
            if (args.length > 2) {
                _accounts = Integer.parseInt(args[2]);
            }
            new TransactionTest2().initAndRunTest();
        } catch (final NumberFormatException e) {
            usage();
        }
    }

    static void usage() {
        final PrintStream o = System.out;
        o.println("Demonstrates Persistit transactions.  This program");
        o.println("transfers random amounts of \"money\" between ");
        o.println("randomly chosen accounts.  The parameters specify ");
        o.println("how many threads, how many iterations per threads, ");
        o.println("and the total number of accounts to receive these");
        o.println("transfers.  Usage:");
        o.println();
        o.println("java SimpleTransaction <nthreads> <itersPerThread> <accounts>");
        o.println();
    }

    @Override
    public String toString() {
        return Thread.currentThread().getName();
    }

    @Test
    public void transactions() throws Exception {
        //
        // An Exchange for the Tree containing account "balances".
        //
        final Exchange accountEx = _persistit.getExchange("persistit", "account", true);
        accountEx.removeAll();
        //
        // Get the starting "balance", that is, the sum of the amounts in
        // each account.
        //
        System.out.println("Computing balance");
        final int startingBalance = balance(accountEx);
        System.out.println("Starting balance is " + startingBalance);
        //
        // Create the threads
        //
        final Thread[] threadArray = new Thread[_threadCount];
        for (int index = 0; index < _threadCount; index++) {
            threadArray[index] = new Thread(new Runnable() {
                @Override
                public void run() {
                    runIt();
                }
            }, "TransactionThread_" + index);

        }
        //
        // Start them all and measure the time until the last thread ends
        //
        long time = System.currentTimeMillis();
        System.out.println("Starting transaction threads");

        for (int index = 0; index < _threadCount; index++) {
            threadArray[index].start();
        }

        System.out.println("Waiting for threads to end");
        for (int index = 0; index < _threadCount; index++) {
            threadArray[index].join();
        }
        //
        // All done
        //
        time = System.currentTimeMillis() - time;
        System.out.println("All threads ended at " + time + " ms");
        System.out.println("Completed transactions: " + _completedTransactionCount);
        System.out.println("Failed transactions: " + _failedTransactionCount);
        System.out.println("Retried transactions: " + _retriedTransactionCount);

        final int endingBalance = balance(accountEx);
        System.out.print("Ending balance is " + endingBalance + " which ");
        System.out.println(endingBalance == startingBalance ? "AGREES" : "DISAGREES");
        assertEquals(startingBalance, endingBalance);
    }

    @Test
    public void transactionsWithInterrupts() throws Exception {
        final TransactionIndex ti = _persistit.getTransactionIndex();
        final Exchange accountEx = _persistit.getExchange("persistit", "account", true);
        accountEx.removeAll();
        int index = 0;

        System.out.println("Computing balance");
        final int startingBalance = balance(accountEx);
        System.out.println("Starting balance is " + startingBalance);

        final long expires = System.currentTimeMillis() + TIMEOUT;

        final List<Thread> threads = new ArrayList<Thread>();
        while (System.currentTimeMillis() < expires) {
            //
            // Remove any Thread instances that died (due to interrupt)
            //
            for (final Iterator<Thread> iter = threads.iterator(); iter.hasNext();) {
                if (!iter.next().isAlive()) {
                    iter.remove();
                }
            }
            //
            // Top up the set of running threads
            //
            while (threads.size() < _threadCount) {
                final Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        runIt();
                    }
                }, "TransactionThread_" + ++index);
                threads.add(thread);
                thread.start();
            }

            final Thread victim = threads.get(index % threads.size());
            victim.interrupt();
            Thread.sleep(50);
        }
        //
        // Now interrupt all remaining threads and wait for them to die
        //
        for (final Thread thread : threads) {
            thread.interrupt();
        }
        for (final Thread thread : threads) {
            thread.join();
        }
        //
        // Just to be sure this has been done after all threads died
        //
        ti.updateActiveTransactionCache();

        //
        System.out.println("Completed transactions: " + _completedTransactionCount);
        System.out.println("Failed transactions: " + _failedTransactionCount);
        System.out.println("Retried transactions: " + _retriedTransactionCount);

        System.out.printf("\nCurrentCount=%,d  AbortedCount=%,d  "
                + "LongRunningCount=%,d  FreeCount=%,d\natCache=%s\n", ti.getCurrentCount(), ti.getAbortedCount(),
                ti.getLongRunningCount(), ti.getFreeCount(), ti.getActiveTransactionCache());

        final int endingBalance = balance(accountEx);
        System.out.print("Ending balance is " + endingBalance + " which ");
        System.out.println(endingBalance == startingBalance ? "AGREES" : "DISAGREES");
        assertEquals("Starting and ending balance don't agree", startingBalance, endingBalance);
        assertTrue("ATC has very old transaction",
                ti.getActiveTransactionCeiling() - ti.getActiveTransactionFloor() < 10000);
    }

    @Test(expected = IllegalStateException.class)
    public void illegalStateExceptionOnRollback() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        txn.rollback();
        txn.begin();
    }

    @Test(expected = IllegalStateException.class)
    public void illegalStateExceptionOnCommit() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        txn.commit();
        txn.begin();
    }

    @Test
    public void transactionsConcurrentWithPersistitClose() throws Exception {
        new Thread(new Runnable() {
            public void run() {
                try {
                    transactions();
                } catch (PersistitClosedException e) {
                    // expected sometimes
                    System.out.println(e);
                } catch (InterruptedException e) {
                    // expected sometimes
                    System.out.println(e);
                } catch (PersistitInterruptedException e) {
                    // expected sometimes
                    System.out.println(e);
                } catch (Exception e) {
                    fail(e.toString());
                }
            }
        }).start();
        /*
         * Let the threads crank up
         */
        Thread.sleep(1000);
        _persistit.close();
        assertEquals("All threads should have exited correctly", 0, _strandedThreads.get());
    }

    public void runIt() {
        _strandedThreads.incrementAndGet();
        try {
            final Exchange accountEx = _persistit.getExchange("persistit", "account", true);
            //
            final Random random = new Random();
            for (int iterations = 1; iterations <= _iterationsPerThread; iterations++) {
                final int accountNo1 = random.nextInt(_accounts);
                // int accountNo2 = random.nextInt(_accounts - 1);
                // if (accountNo2 == accountNo1) accountNo2++;
                final int accountNo2 = random.nextInt(_accounts);

                final int delta = random.nextInt(10000);
                // final int delta = 1;

                transfer(accountEx, accountNo1, accountNo2, delta);
                _completedTransactionCount.incrementAndGet();

                if (iterations % 25000 == 0) {
                    System.out.println(this + " has finished " + iterations + " iterations");
                    System.out.flush();
                }
            }
        } catch (final PersistitInterruptedException exception) {
            _strandedThreads.decrementAndGet();
            // expected
        } catch (final PersistitClosedException exception) {
            _strandedThreads.decrementAndGet();
            // expected
        } catch (final PersistitIOException exception) {
            if (InterruptedIOException.class.equals(exception.getCause().getClass())) {
                _strandedThreads.decrementAndGet();
                // expected
            } else {
                exception.printStackTrace();
                _failedTransactionCount.incrementAndGet();
            }
        } catch (final Exception exception) {
            exception.printStackTrace();
            _failedTransactionCount.incrementAndGet();
        }
    }

    void transfer(final Exchange ex, final int accountNo1, final int accountNo2, final int delta)
            throws PersistitException {
        //
        // A Transaction object is actually a Transaction context. You invoke
        // begin() and commit() on it to create and commit a transaction scope.
        //
        final Transaction txn = ex.getTransaction();
        int remainingAttempts = 500;
        boolean done = false;

        while (!done) {
            //
            // Start the scope of a transaction
            //
            txn.begin();
            try {

                ex.clear().append(accountNo1).fetch();
                final int balance1 = ex.getValue().isDefined() ? ex.getValue().getInt() : 0;
                ex.getValue().put(balance1 - delta);
                ex.store();

                assertEquals(1, txn.getTransactionStatus().getMvvCount());

                ex.clear().append(accountNo2).fetch();
                final int balance2 = ex.getValue().isDefined() ? ex.getValue().getInt() : 0;
                ex.getValue().put(balance2 + delta);
                ex.store();

                assertEquals(accountNo1 == accountNo2 ? 1 : 2, txn.getTransactionStatus().getMvvCount());
                //
                // Commit the transaction
                //
                txn.commit(policy);
                //
                // Done.
                //
                done = true;
            } catch (final RollbackException rollbackException) {
                _retriedTransactionCount.incrementAndGet();
                if (--remainingAttempts <= 0) {
                    throw rollbackException;
                }
            } finally {
                txn.end();
            }
        }
    }

    //
    // To illustrate another way to use the Transaction API, this
    // method uses a TransactionRunnable to perform its logic. The
    // TransactionRunnable (see the Balancer class, below) encapsulates
    // all the logic to be performed within the transaction scope.
    // The Transaction run() method handles provides the transaction scope
    // and handles retries appropriately.
    //
    static int balance(final Exchange ex) throws PersistitException {
        final Transaction txn = ex.getTransaction();
        final Balancer balancer = new Balancer(ex);
        txn.run(balancer);
        return balancer.getTotal();
    }

    //
    // A TransactionRunnable that encapsulates the logic to be performed within
    // a transaction.
    //
    static class Balancer implements TransactionRunnable {
        int _total = 0;
        Exchange _ex;

        Balancer(final Exchange ex) {
            _ex = ex;
        }

        @Override
        public void runTransaction() throws PersistitException {
            _ex.clear().append(Key.BEFORE);
            _total = 0;
            while (_ex.next()) {
                final int balance = _ex.getValue().getInt();
                _total += balance;
            }
        }

        int getTotal() {
            return _total;
        }
    }

    static final int RETRIES = 20;

    void transferZZZ(final Exchange exchange, final int accountNo1, final int accountNo2, final int delta)
            throws PersistitException {
        //
        // A Transaction object is actually a Transaction context. You invoke
        // begin() and commit() on it to create and commit a transaction scope.
        //
        final Transaction txn = exchange.getTransaction();
        //
        // Because Persistit schedules transactions optimistically,
        // applications must always handle rollbacks. This simple
        // example simply retries the transaction up to RETRIES times.
        //
        for (int attempt = 0; attempt < RETRIES; attempt++) {
            //
            // Begin the scope of a transaction
            //
            txn.begin();
            try {
                //
                // Fetch the account balance for accountNo1
                //
                exchange.clear().append(accountNo1).fetch();
                final int balance1 = exchange.getValue().getInt();
                //
                // Update and store the account balance for accountNo1
                //
                exchange.getValue().put(balance1 - delta);
                exchange.store();
                //
                // Fetch the account balance for accountNo2
                //
                exchange.clear().append(accountNo2).fetch();
                final int balance2 = exchange.getValue().getInt();
                //
                // Update and store the account balance for accountNo2
                //
                exchange.getValue().put(balance2 + delta);
                exchange.store();
                //
                // Commit the transaction. By default this is a memory
                // commit which is fast but not durable. Use
                // txn.commit(true)
                // to force transaction updates to volume files.
                //
                txn.commit(policy);
                //
                // Done.
                //
                break;
            } catch (final RollbackException rollbackException) {
                // Any special logic to handle rollbacks goes here.
            } finally {
                txn.end();
            }
        }
        throw new RollbackException();
    }

}
