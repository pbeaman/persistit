/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

import java.io.PrintStream;
import java.util.Random;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.TransactionRunnable;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;

/**
 * Demonstrates the use of Persisit Transactions. This demo runs multiple
 * threads that transfer "money" between accounts. At all times the sum of all
 * balances should remain unchanged.
 * 
 * @version 1.0
 */
public class SimpleTransaction implements Runnable {
    final static Object LOCK = new Object();

    static int _threadCount = 0;

    static int _rolledBackTransactionCount = 0;
    static int _committedTransactionCount = 0;
    static int _failedTransactionCount = 0;

    int _threadIndex = 0;
    int _iterationsPerThread;
    int _accounts;

    final static Persistit persistit = new Persistit();

    public static void main(String[] args) throws Exception {
        try {
            int threads = 10;
            int iterationsPerThread = 100000;
            int accounts = 10000;

            if (args.length > 0)
                threads = Integer.parseInt(args[0]);
            if (args.length > 1)
                iterationsPerThread = Integer.parseInt(args[1]);
            if (args.length > 2)
                accounts = Integer.parseInt(args[2]);
            //
            // Read configuration from persistit.properties file.
            //
            persistit.initialize();
            //
            // An Exchange for the Tree containing account "balances".
            //
            Exchange accountEx = new Exchange(persistit, "txndemo", "account", true);
            //
            // Get the starting "balance", that is, the sum of the amounts in
            // each account.
            //
            System.out.println("Computing balance");
            int startingBalance = balance(accountEx);
            System.out.println("Starting balance is " + startingBalance);
            //
            // Create the threads
            //
            Thread[] threadArray = new Thread[threads];
            for (int index = 0; index < threads; index++) {
                threadArray[index] = new Thread(new SimpleTransaction(iterationsPerThread, accounts));
            }
            //
            // Start them all and measure the time until the last thread ends
            //
            long time = System.currentTimeMillis();
            System.out.println("Starting transaction threads");

            for (int index = 0; index < threads; index++) {
                threadArray[index].start();
            }

            System.out.println("Waiting for threads to end");
            for (int index = 0; index < threads; index++) {
                threadArray[index].join();
            }
            //
            // All done
            //
            time = System.currentTimeMillis() - time;
            System.out.println("All threads ended at " + time + " ms");
            System.out.println("Completed transactions: " + _committedTransactionCount);
            System.out.println("Failed transactions: " + _failedTransactionCount);
            System.out.println("Retried transactions: " + _rolledBackTransactionCount);
            System.out.println("Average completed transactions rate: " + (_committedTransactionCount * 1000 / time)
                    + " per second");

            int endingBalance = balance(accountEx);
            System.out.print("Ending balance is " + endingBalance + " which ");
            System.out.println(endingBalance == startingBalance ? "AGREES" : "DISAGREES");
        } catch (NumberFormatException e) {
            usage();
        } finally {
            //
            // Always close Persistit.
            //
            persistit.close();
        }
    }

    static void usage() {
        PrintStream o = System.out;
        o.println("Demonstrates Persistit transactions.  This program");
        o.println("transfers random amounts of \"money\" between ");
        o.println("randomly chosen \"accounts.\"  The parameters specify ");
        o.println("how many threads, how many iterations per threads, ");
        o.println("and the total number of accounts to receive these");
        o.println("transfers.  Usage:");
        o.println();
        o.println("java SimpleTransaction <nthreads> <itersPerThread> <accounts>");
        o.println();
    }

    SimpleTransaction(int iterations, int accounts) {
        _iterationsPerThread = iterations;
        _accounts = accounts;

        synchronized (LOCK) {
            _threadIndex = _threadCount++;
        }
    }

    public String toString() {
        return "SimpleTransaction #" + _threadIndex;
    }

    public void run() {
        try {
            Exchange accountEx = new Exchange(persistit, "txndemo", "account", true);

            Random random = new Random();

            for (int iterations = 0; iterations < _iterationsPerThread; iterations++) {
                // Choose the first random "account" number.
                int accountNo1 = random.nextInt(_accounts);

                // Choose a different second random "account" number.
                int accountNo2 = random.nextInt(_accounts - 1);
                if (accountNo2 >= accountNo1)
                    accountNo2++;

                // Choose a random amount of "money"
                int delta = random.nextInt(10000);
                transfer(accountEx, accountNo1, accountNo2, delta);
                if (iterations % 25000 == 0) {
                    System.out.println(this + " has finished " + iterations + " iterations");
                    System.out.flush();
                }
            }
            Transaction txn = accountEx.getTransaction();
            //
            // Copy per-Transaction counters to global counters.
            //
            _committedTransactionCount += txn.getCommittedTransactionCount();
            _rolledBackTransactionCount += txn.getRolledBackTransactionCount();
        } catch (Exception exception) {
            _failedTransactionCount++;
            exception.printStackTrace();
        }
    }

    void transfer(Exchange ex, int accountNo1, int accountNo2, int delta) throws PersistitException {
        // A Transaction object is a context in which a transaction scope
        // begins, commits and ends.
        //
        Transaction txn = ex.getTransaction();
        int remainingAttempts = 10;
        //
        // Retry until successful commit or failure
        //
        for (;;) {
            // Start the scope of a transaction.
            //
            txn.begin();
            try {
                // Debit accountNo1.
                //
                ex.clear().append(accountNo1).fetch();
                int balance1 = ex.getValue().isDefined() ? ex.getValue().getInt() : 0;
                ex.getValue().put(balance1 - delta);
                ex.store();
                //
                // Credit accountNo2.
                //
                ex.clear().append(accountNo2).fetch();
                int balance2 = ex.getValue().isDefined() ? ex.getValue().getInt() : 0;
                ex.getValue().put(balance2 + delta);
                ex.store();
                //
                // Commit the transaction and finish the loop.
                //
                txn.commit();
                break;
            } catch (RollbackException rollbackException) {
                if (--remainingAttempts <= 0) {
                    throw new TransactionFailedException();
                }
            } finally {
                // Every begin() must have a matching call to end().
                //
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
        Transaction txn = ex.getTransaction();
        Balancer balancer = new Balancer(ex);
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

        Balancer(Exchange ex) {
            _ex = ex;
        }

        public void runTransaction() throws PersistitException {
            _ex.clear().append(Key.BEFORE);
            //
            // Must reset the total here because due to optimistic scheduling
            // this code may retry. Only Persistit database updates are reset
            // on a rollback, not fields and variables.
            //
            _total = 0;
            while (_ex.next()) {
                int balance = _ex.getValue().getInt();
                _total += balance;
            }
        }

        int getTotal() {
            return _total;
        }
    }
}

