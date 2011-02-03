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

package com.persistit.unit;

import java.io.PrintStream;
import java.util.Random;

import org.junit.Ignore;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.TransactionRunnable;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

/**
 * Demonstrates the use of Persisit Transactions. This demo runs multiple
 * threads that transfer "money" between accounts. At all times the sum of all
 * balances should remain unchanged.
 * 
 * @version 1.0
 */
public class TransactionTest2 extends PersistitUnitTestCase {

    final static Object LOCK = new Object();

    static int _threads = 5;
    static int _iterationsPerThread = 50000;
    static int _accounts = 1000;

    static int _retriedTransactionCount = 0;
    static int _completedTransactionCount = 0;
    static int _failedTransactionCount = 0;

    static int _threadCounter = 0;

    int _threadIndex = 0;

    public static void main(final String[] args) throws Exception {
        try {
            if (args.length > 0) {
                _threads = Integer.parseInt(args[0]);
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

    @Override
    public void runAllTests() throws Exception {
        test1();
    }

    // Temporarily ignored for Persistit 2.1
    @Ignore
    public void test1() throws Exception {
        return;
        //
        // //
        // // An Exchange for the Tree containing account "balances".
        // //
        // final Exchange accountEx =
        // _persistit.getExchange("persistit", "account", true);
        // accountEx.removeAll();
        // //
        // // Get the starting "balance", that is, the sum of the amounts in
        // // each account.
        // //
        // System.out.println("Computing balance");
        // final int startingBalance = balance(accountEx);
        // System.out.println("Starting balance is " + startingBalance);
        // //
        // // Create the threads
        // //
        // final Thread[] threadArray = new Thread[_threads];
        // for (int index = 0; index < _threads; index++) {
        // threadArray[index] = new Thread(new Runnable() {
        // public void run() {
        // runIt();
        // }
        // }, "TransactionThread_" + index);
        //
        // }
        // //
        // // Start them all and measure the time until the last thread ends
        // //
        // long time = System.currentTimeMillis();
        // System.out.println("Starting transaction threads");
        //
        // for (int index = 0; index < _threads; index++) {
        // threadArray[index].start();
        // }
        //
        // System.out.println("Waiting for threads to end");
        // for (int index = 0; index < _threads; index++) {
        // threadArray[index].join();
        // }
        // //
        // // All done
        // //
        // time = System.currentTimeMillis() - time;
        // System.out.println("All threads ended at " + time + " ms");
        // System.out.println("Completed transactions: "
        // + _completedTransactionCount);
        // System.out.println("Failed transactions: " +
        // _failedTransactionCount);
        // System.out.println("Retried transactions: " +
        // _retriedTransactionCount);
        //
        // final int endingBalance = balance(accountEx);
        // System.out.print("Ending balance is " + endingBalance + " which ");
        // System.out.println(endingBalance == startingBalance ? "AGREES"
        // : "DISAGREES");
        // assertEquals(startingBalance, endingBalance);
    }

    public void runIt() {
        try {
            final Exchange accountEx = _persistit.getExchange("persistit",
                    "account", true);
            //
            final Random random = new Random();
            for (int iterations = 0; iterations < _iterationsPerThread; iterations++) {
                final int accountNo1 = random.nextInt(_accounts);
                // int accountNo2 = random.nextInt(_accounts - 1);
                // if (accountNo2 == accountNo1) accountNo2++;
                final int accountNo2 = random.nextInt(_accounts);

                // int delta = random.nextInt(10000);
                final int delta = 1;

                transfer(accountEx, accountNo1, accountNo2, delta);
                synchronized (LOCK) {
                    _completedTransactionCount++;
                }

                if (iterations % 25000 == 0) {
                    System.out.println(this + " has finished " + iterations
                            + " iterations");
                    System.out.flush();
                }
            }
        } catch (final Exception exception) {
            exception.printStackTrace();
            synchronized (LOCK) {
                _failedTransactionCount++;
            }
        }
    }

    void transfer(final Exchange ex, final int accountNo1,
            final int accountNo2, final int delta) throws PersistitException {
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
                final int balance1 = ex.getValue().isDefined() ? ex.getValue()
                        .getInt() : 0;
                ex.getValue().put(balance1 - delta);
                ex.store();

                ex.clear().append(accountNo2).fetch();
                final int balance2 = ex.getValue().isDefined() ? ex.getValue()
                        .getInt() : 0;
                ex.getValue().put(balance2 + delta);
                ex.store();

                //
                // Commit the transaction
                //
                txn.commit();
                //
                // Done.
                //
                done = true;
            } catch (final RollbackException rollbackException) {
                synchronized (LOCK) {
                    _retriedTransactionCount++;
                }
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

    void transferZZZ(final Exchange exchange, final int accountNo1,
            final int accountNo2, final int delta) throws PersistitException {
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
                txn.commit();
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
