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

package com.persistit.stress;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.exception.RollbackException;
import com.persistit.test.TestResult;
import com.persistit.util.ArgParser;

/**
 * This class tests recovery after a non-graceful shutdown. To test, run this
 * class, then stop it by: (a) normal shutdown, (b) kill -9, or (c) power-off,
 * (d) pulling a disk from its bay.
 * 
 * The test writes a sequence of transactions in pseudo-random but predictable
 * order. As soon as each transaction commits, a "ticket" is written to stdout.
 * The stream of tickets can be redirected either to a file (for (a) and (b)) or
 * to a terminal session on second computer (for (c) and (d)) so that after
 * recovering the victim Persistit instance, a nearly complete set of
 * transaction tickets is available. Note that because of network latency and/or
 * buffering the ticket stream will be nearly complete but may be a fraction of
 * a second behind what actually happened on the victim.
 * 
 * Tickets are produced by incrementing an AtomicLong object. The AtomicLong is
 * static so that multiple threads running this test acquire strictly unique,
 * increasing ticket values.
 * 
 * After recovery, the ticket stream is run through the verify method of this
 * class to confirm that every transaction whose ticket was record is actually
 * present in the database. This imposes
 * 
 * This class offers a framework for testing different sizes and types of
 * transactions. Each transaction type registers itself with the transaction
 * scheduler.
 * 
 * This class performs randomized transactions, but according to predictable
 * schedule. As each transaction is committed, this class emits a ticket to
 * stdout. A driver program records the stream of tickets and then feeds them to
 * a verification step to ensure that every transaction this class said it has
 * committed is actually represented in the database.
 * 
 * 
 * @author peter
 * 
 */
public class StressRecovery extends StressBase {

    private final static String SHORT_DESCRIPTION = "Issues a series of transactions with external logging";

    private final static String LONG_DESCRIPTION = "   Execute transactions in single- or multi-threaded pattern: \r\n"
            + "    write progress to stdout.  This can be recorded, and played back to ensure that the"
            + "    resulting database after recovery contains all the committed transactions \r\n";

    private final static String[] ARGS_TEMPLATE = { "size|int:30:1:20000|Maximum size of each data value",
            "verify|String:|Path name of ticket list to verify",
            "latency|long:0:0:60000|Maximum acceptable fault latency" };

    private final static LFSRTicketSequence ticketSequence = new LFSRTicketSequence();

    private final ArrayList<TransactionType> registry = new ArrayList<TransactionType>();

    int _size;
    int _splay;
    boolean _verifyMode;
    String _verifyPath;
    long _maxLatency;
    BufferedReader _verifyReader;

    static class LFSRTicketSequence {
        AtomicLong _bits = new AtomicLong(1);

        long incrementAndGet() {
            for (;;) {
                long current = _bits.get();
                long next = (current >>> 1) ^ (-(current & 1) & 0xD800000000000000L);
                if (_bits.compareAndSet(current, next)) {
                    return next & Long.MAX_VALUE;
                }
            }
        }
    }

    public interface TransactionType {
        /**
         * Given a ticketId, perform a transaction which can later be verified.
         * If this method returns without throwing an Exception, then the
         * transaction must be committed.
         * 
         * @param ticketId
         * @throws Exception
         */
        void performTransaction(final long ticketId) throws Exception;

        /**
         * Given a ticketId, verify that the transaction previously performed by
         * {@link #performTransaction(long)} is present in the database.
         * 
         * @param ticketId
         * @throws Exception
         */
        void verifyTransaction(final long ticketId) throws Exception;
    }

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public void setUp() throws Exception {
        _ap = new ArgParser("com.persistit.StressRecovery", _args, ARGS_TEMPLATE);
        _size = _ap.getIntValue("size");
        _maxLatency = _ap.getLongValue("latency") * 1000000l;
        _verifyPath = _ap.getStringValue("verify");
        if (_verifyPath != null && !_verifyPath.isEmpty()) {
            _verifyMode = true;
            _verifyReader = new BufferedReader(new FileReader(_verifyPath));
        }
        _dotGranularity = 10000;

        super.setUp(!_verifyMode);
        try {
            // Exchange with shared Tree
            _exs = getPersistit().getExchange("persistit", "shared", true);
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
        // registry.add(new SimpleTransactionType());
        registry.add(new IndexSimulationTransactionType());
    }

    @Override
    public void executeTest() throws IOException {
        if (_verifyMode) {
            verifyTicketStream();
        } else {
            executeTicketStream();
        }
    }

    /**
     * Perform first part of test. This method generates tickets and executes
     * the associated transactions. Runs "forever" because it is intended to be
     * interrupted by a shutdown or crash.
     */
    private void executeTicketStream() {
        final long zero = System.nanoTime();
        for (int _count = 1;; _count++) {
            final long ticketId = ticketSequence.incrementAndGet();
            final TransactionType tt = registry.get((int) (ticketId % registry.size()));
            final long start = System.nanoTime();
            try {
                tt.performTransaction(ticketId);
                final long now = System.nanoTime();
                emit(ticketId, start - zero, now - start);
            } catch (Exception e) {
                emit(ticketId, start - zero, -1);
                printStackTrace(e);
            }
        }
    }

    /**
     * Verify that all (or almost all) transactions in the ticket stream were
     * applied to the database. The "almost" case is when transactions were not
     * hard-committed during the first phase. In this case verification passes
     * if some transactions near the end of the ticket stream were not
     * recovered. "Near" is determined by elapsed system time, and is intended
     * to be no more than a few hundred milliseconds. The value is specified by
     * the <code>latency</code> value specified in command-line parameters.
     */
    private void verifyTicketStream() {
        long firstFault = Long.MAX_VALUE;
        long firstFaultTicket = 0;
        int successAfterFailureCount = 0;
        long last = Long.MIN_VALUE;
        int faults = 0;
        for (int _count = 1;; _count++) {
            String line = "~not read~";
            long ticketId = -1;
            long start = -1;
            long elapsed = -1;
            try {
                line = _verifyReader.readLine();
                if (line == null) {
                    break;
                }
                if (line.isEmpty() || !Character.isDigit(line.charAt(0))) {
                    continue;
                }
                final String[] s = line.split(",");
                if (s.length != 3) {
                    continue;
                }
                ticketId = Long.parseLong(s[0]);
                start = Long.parseLong(s[1]);
                elapsed = Long.parseLong(s[2]);
                last = Math.max(last, start + elapsed);
            } catch (Exception e) {
                fail(e + " while reading line " + _count + " of " + _verifyPath + ": " + line);
            }
            if (elapsed >= 0) {
                try {
                    TransactionType tt = registry.get((int) (ticketId % registry.size()));
                    tt.verifyTransaction(ticketId);
                    if (start + elapsed > firstFault) {
                        successAfterFailureCount++;
                    }
                } catch (Exception e) {
                    firstFault = Math.min(firstFault, start + elapsed);
                    firstFaultTicket = ticketId;
                    faults++;
                }
            }
        }

        if (faults > 0) {
            if (last - firstFault < _maxLatency) {
                String msg = String
                        .format("There were %,d faults. Last one occurred %,dms before crash: "
                                + "PASS because acceptable latency setting is %,dms First-failed ticketId=%,d laterSuccess=%,d.",
                                faults, (last - firstFault) / 1000000l, _maxLatency / 1000000l, firstFaultTicket,
                                successAfterFailureCount);
                println(msg);
                _result = new TestResult(true, msg);
            } else {
                _result = new TestResult(
                        false,
                        String.format(
                                "There were %,d faults. Last one occurred %,dms before crash: "
                                        + "FAIL because acceptable latency setting is %,dms First-failed ticketId=%,d laterSuccess=%,d.",
                                faults, (last - firstFault) / 1000000l, _maxLatency / 1000000l, firstFaultTicket,
                                successAfterFailureCount));
            }
        }
    }

    private synchronized static void emit(final long ticketId, final long start, final long elapsed) {
        System.out.println(ticketId + "," + start + "," + elapsed);
        System.out.flush();
    }

    int keyInteger(final int counter) {
        int keyInteger = (counter * _splay) % _total;
        if (keyInteger < 0) {
            keyInteger += _total;
        }
        return keyInteger;
    }

    class SimpleTransactionType implements TransactionType {
        private static final int SCALE = 10000;

        @Override
        public void performTransaction(long ticketId) throws Exception {
            Transaction txn = _persistit.getTransaction();
            for (;;) {
                txn.begin();
                try {
                    _exs.getValue().putString("ticket " + ticketId + " value");
                    _exs.clear().append(ticketId % SCALE).append(ticketId / SCALE);
                    _exs.store();
                    txn.commit();
                    break;
                } catch (RollbackException e) {
                    continue;
                } finally {
                    txn.end();
                }
            }
        }

        @Override
        public void verifyTransaction(long ticketId) throws Exception {
            _exs.clear().append(ticketId % SCALE).append(ticketId / SCALE);
            _exs.fetch();
            check(ticketId, _exs, "ticket " + ticketId + " value");
        }
    }

    class IndexSimulationTransactionType implements TransactionType {

        final String[] FRAGMENTS = { "now", "is", "the", "time", "for", "a", "quick", "brown", "fox", "to", "come",
                "aid", "some", "party" };

        @Override
        public void performTransaction(long ticketId) throws Exception {
            final StringBuilder sb = new StringBuilder(String.format("%,15d", ticketId));
            final int size = (int) ((ticketId * 17) % 876);
            for (int i = 0; i < size; i++) {
                sb.append('-');
            }
            Transaction txn = _persistit.getTransaction();
            for (;;) {
                txn.begin();
                try {
                    _exs.getValue().putString(sb);
                    long t = ticketId;
                    _exs.clear();
                    while (t != 0) {
                        _exs.getKey().append(FRAGMENTS[(int) (t % FRAGMENTS.length)]);
                        t /= FRAGMENTS.length;
                    }
                    _exs.store();

                    _exs.getValue().putString("$$$");
                    _exs.clear().append(1);
                    t = ticketId / 11;
                    while (t != 0) {
                        _exs.getKey().append(FRAGMENTS[(int) (t % FRAGMENTS.length)]);
                        t /= FRAGMENTS.length;
                    }
                    _exs.store();

                    _exs.clear().append(2);
                    t = ticketId / 13;
                    while (t != 0) {
                        _exs.getKey().append(FRAGMENTS[(int) (t % FRAGMENTS.length)]);
                        t /= FRAGMENTS.length;
                    }
                    _exs.store();

                    txn.commit();
                    break;
                } catch (RollbackException e) {
                    continue;
                } finally {
                    txn.end();
                }
            }
        }

        @Override
        public void verifyTransaction(long ticketId) throws Exception {

            long t = ticketId;
            _exs.clear();
            while (t != 0) {
                _exs.getKey().append(FRAGMENTS[(int) (t % FRAGMENTS.length)]);
                t /= FRAGMENTS.length;
            }
            _exs.fetch();
            check(ticketId, _exs, String.format("%,15d", ticketId));

            _exs.clear().append(1);
            t = ticketId / 11;
            while (t != 0) {
                _exs.getKey().append(FRAGMENTS[(int) (t % FRAGMENTS.length)]);
                t /= FRAGMENTS.length;
            }
            _exs.fetch();
            check(ticketId, _exs, "$$$");

            _exs.clear().append(2);
            t = ticketId / 13;
            while (t != 0) {
                _exs.getKey().append(FRAGMENTS[(int) (t % FRAGMENTS.length)]);
                t /= FRAGMENTS.length;
            }
            _exs.fetch();
            check(ticketId, _exs, "$$$");

        }
    }

    private void check(final long ticketId, final Exchange ex, final String expected) throws Exception {
        ex.fetch();
        if (!ex.getValue().isDefined()) {
            throw new RuntimeException("Ticket " + ticketId + " missing value at " + ex.getKey());
        }
        final String s = ex.getValue().getString();
        if (!s.startsWith(expected)) {
            throw new RuntimeException("Ticket " + ticketId + " incorrect value at " + ex.getKey());
        }
    }

    public static void main(final String[] args) {
        final StressRecovery test = new StressRecovery();
        test.runStandalone(args);
    }

}
