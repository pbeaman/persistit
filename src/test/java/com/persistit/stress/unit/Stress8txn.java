/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.stress.unit;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.TransactionRunnable;
import com.persistit.exception.PersistitException;
import com.persistit.util.ArgParser;
import com.persistit.util.Debug;

/**
 * @version 1.0
 */
public class Stress8txn extends StressBase {

    public Stress8txn(String argsString) {
        super(argsString);
    }

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Repetitions",
            "count|int:100:0:100000|Number of iterations per cycle",
            "size|int:1000:1:100000000|Number of 'C' accounts", "seed|int:1:1:20000|Random seed", };

    static boolean _consistencyCheckDone;
    int _size;
    int _seed;

    int _mvvReports;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress8txn", _args, ARGS_TEMPLATE);
        _total = _ap.getIntValue("count");
        _repeatTotal = _ap.getIntValue("repeat");
        _size = _ap.getIntValue("size");
        _seed = _ap.getIntValue("seed");
        seed(_seed);

        try {
            _exs = getPersistit().getExchange("persistit", "shared", true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    /**
     * <p>
     * Implements tests with "accounts" to be updated transactionally There is a
     * hierarchy of accounts categories A, B and C. A accounts contain B
     * accounts which contain C accounts. At all times, the sums of C accounts
     * must match the total in their containing B account, and so on. The
     * overall sum of every account must always be 0. Operations are:
     * <ol>
     * <li>"transfer" (add/subtract) an amount from a C account to another C
     * account within the same B.</li>
     * <li>"transfer" (add/subtract) an amount from a C account to a C account
     * in a different B account, resulting in changes to B and possibly A
     * account totals.</li>
     * <li>Consistency check - determining that the sub-accounts total to the
     * containing account total.</li>
     * </ol>
     * </p>
     * <p>
     * As a wrinkle, a few "account" totals are represented by strings of a
     * length that represents the account total, rather than by an integer. This
     * is to test long record management during transactions.
     * </p>
     * <p>
     * The expected result is that each consistency check will match, no matter
     * what. This includes the result of abruptly stopping and restarting the
     * JVM. The first thread starting this test performs a consistency check
     * across the entire database to make sure that the result of any recovery
     * operation is correct.
     * </p>
     */
    @Override
    public void executeTest() {
        synchronized (Stress8txn.class) {
            if (!_consistencyCheckDone) {
                _consistencyCheckDone = true;
                try {
                    if (totalConsistencyCheck()) {
                        System.out.println("Consistency check completed successfully");
                    }
                } catch (final PersistitException pe) {
                    fail(pe);
                }
            }
        }

        final Transaction txn = _exs.getTransaction();
        final Operation[] ops = new Operation[6];
        ops[0] = new Operation0();
        ops[1] = new Operation1();
        ops[2] = new Operation2();
        ops[3] = new Operation3();
        ops[4] = new Operation4();
        ops[5] = new Operation5();

        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {

            for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                try {
                    final int selector = select();
                    final Operation op = ops[selector];
                    final int acct1 = random(0, _size);
                    final int acct2 = random(0, _size);
                    op.setup(acct1, acct2);
                    final int passes = txn.run(op, 100, 5, CommitPolicy.SOFT);
                    Debug.$assert1.t(passes <= 90);
                } catch (final Exception pe) {
                    fail(pe);
                }
            }
        }

        try {
            _exs.clear().append("stress8txn");
            while (_exs.next(true)) {
                addWork(1);

                if ((_exs.getValue().isType(String.class)) && (getAccountValue(_exs) > 8000)) {
                    // System.out.println("len=" + getAccountValue(_exs) +
                    // " Key=" + _exs.getKey().toString());
                }
            }
        } catch (final PersistitException pe) {
            fail(pe);
        }
    }

    private int select() {
        final int r = random(0, 1000);
        if (r < 500) {
            return 0;
        }
        if (r < 800) {
            return 1;
        }
        if (r < 900) {
            return 2;
        }
        if (r < 950) {
            return 3;
        }
        if (r < 990) {
            return 4;
        }
        return 5;
    }

    private abstract class Operation implements TransactionRunnable {
        int _a1, _b1, _c1, _a2, _b2, _c2;

        void setup(final int acct1, final int acct2) {
            _a1 = (acct1 / 25);
            _b1 = (acct1 / 5) % 5;
            _c1 = (acct1 % 5);

            _a2 = (acct2 / 25);
            _b2 = (acct2 / 5) % 5;
            _c2 = (acct2 % 5);
        }

    }

    private class Operation0 extends Operation {
        /**
         * Transfers from one C account to another within the same B
         */
        @Override
        public void runTransaction() throws PersistitException {
            final int delta = random(-1000, 1000);
            if (_c1 != _c2) {
                _exs.clear().append("stress8txn").append(_a1).append(_b1).append(_c1).fetch();
                putAccountValue(_exs, getAccountValue(_exs) + delta, _c1 == 1);
                _exs.store();
                addWork(1);

                _exs.clear().append("stress8txn").append(_a1).append(_b1).append(_c2).fetch();
                putAccountValue(_exs, getAccountValue(_exs) - delta, _c2 == 1);
                _exs.store();
                addWork(1);

            }
        }
    }

    private class Operation1 extends Operation {
        /*
         * Transfers from one C account to another in possibly a different B
         * account
         */
        @Override
        public void runTransaction() throws PersistitException {
            final int delta = random(-1000, 1000);
            if ((_c1 != _c2) || (_b1 != _b2) || (_a1 != _a2)) {
                _exs.clear().append("stress8txn").append(_a1).append(_b1).append(_c1).fetch();
                putAccountValue(_exs, getAccountValue(_exs) + delta, _c1 == 1);
                _exs.store();
                addWork(1);

                _exs.clear().append("stress8txn").append(_a2).append(_b2).append(_c2).fetch();
                putAccountValue(_exs, getAccountValue(_exs) - delta, _c2 == 1);
                _exs.store();
                addWork(1);

            }

            if ((_b1 != _b2) || (_a1 != _a2)) {
                _exs.clear().append("stress8txn").append(_a1).append(_b1).fetch();
                putAccountValue(_exs, getAccountValue(_exs) + delta, _b1 == 1);
                _exs.store();
                addWork(1);


                _exs.clear().append("stress8txn").append(_a2).append(_b2).fetch();
                putAccountValue(_exs, getAccountValue(_exs) - delta, _b1 == 1);
                _exs.store();
                addWork(1);

            }

            if (_a1 != _a2) {
                _exs.clear().append("stress8txn").append(_a1).fetch();
                putAccountValue(_exs, getAccountValue(_exs) + delta, _a1 == 1);
                _exs.store();
                addWork(1);


                _exs.clear().append("stress8txn").append(_a2).fetch();
                putAccountValue(_exs, getAccountValue(_exs) - delta, _a1 == 1);
                _exs.store();
                addWork(1);

            }
        }
    }

    private class Operation2 extends Operation {
        /**
         * Perform consistency check across a B account
         */
        @Override
        public void runTransaction() throws PersistitException {
            _exs.clear().append("stress8txn").append(_a1).append(_b1).fetch();
            addWork(1);

            final int valueB = getAccountValue(_exs);
            final int totalC = accountTotal(_exs);
            if (valueB != totalC) {
                fail("totalC=" + totalC + " valueB=" + valueB + " at " + _exs);
            }
        }
    }

    private class Operation3 extends Operation {
        /**
         * Perform consistency check across an A account
         */
        @Override
        public void runTransaction() throws PersistitException {
            _exs.clear().append("stress8txn").append(_a1).fetch();
            addWork(1);

            final int valueA = getAccountValue(_exs);
            final int totalB = accountTotal(_exs);
            if (valueA != totalB) {
                fail("totalB=" + totalB + " valueA=" + valueA + " at " + _exs);
            }
        }
    }

    private class Operation4 extends Operation {
        /**
         * Perform consistency check across all A accounts
         */
        @Override
        public void runTransaction() throws PersistitException {
            _exs.clear().append("stress8txn");
            final int totalA = accountTotal(_exs);
            if (totalA != 0) {
                fail("totalA=" + totalA + " at " + _exs);
            }
        }
    }

    private class Operation5 extends Operation {
        /**
         * Perform consistency check across an A account
         */
        @Override
        public void runTransaction() throws PersistitException {
            totalConsistencyCheck();
            if ((_mvvReports++ % 1000) == 0) {
                System.out.println("Consistency check passed");
            }
        }
    }

    private int accountTotal(final Exchange ex) throws PersistitException {
        int total = 0;
        ex.append(Key.BEFORE);
        while (ex.next()) {
            addWork(1);

            int value = getAccountValue(ex);
            total += value;
        }
        ex.cut();
        return total;
    }

    private boolean totalConsistencyCheck() throws PersistitException {
        int totalA = 0;
        final Exchange exa = new Exchange(_exs);
        final Exchange exb = new Exchange(_exs);
        final Exchange exc = new Exchange(_exs);

        int countA = 0;
        exa.clear().append("stress8txn").append(Key.BEFORE);
        while (exa.next()) {
            addWork(1);

            countA++;
            exa.fetch();
            final int valueA = getAccountValue(exa);
            final int valueAA = getAccountValue(exa);
            Debug.$assert1.t(valueA == valueAA);
            totalA += valueA;
            int totalB = 0;
            exa.getKey().copyTo(exb.getKey());
            exb.append(Key.BEFORE);
            int countB = 0;
            while (exb.next()) {
                countB++;
                exb.fetch();
                addWork(1);

                final int valueB = getAccountValue(exb);
                final int valueBB = getAccountValue(exb);
                Debug.$assert1.t(valueB == valueBB);

                totalB += valueB;
                int totalC = 0;
                exb.getKey().copyTo(exc.getKey());
                exc.append(Key.BEFORE);
                int countC = 0;
                while (exc.next()) {
                    addWork(1);

                    countC++;
                    Key key1 = new Key(exc.getKey());
                    final int valueC = getAccountValue(exc);
                    exc.fetch();
                    addWork(1);

                    Key key2 = new Key(exc.getKey());

                    final int valueCC = getAccountValue(exc);

                    Debug.$assert1.t(valueC == valueCC);
                    totalC += valueC;
                }
                if (totalC != valueB) {
                    int totalC1 = 0;
                    int countC1 = 0;
                    while (exc.next()) {
                        countC1++;
                        Key key1 = new Key(exc.getKey());
                        final int valueC1 = getAccountValue(exc);
                        exc.fetch();
                        addWork(1);

                        Key key2 = new Key(exc.getKey());

                        final int valueCC1 = getAccountValue(exc);

                        Debug.$assert1.t(valueC1 == valueCC1);
                        totalC1 += valueC1;
                    }
                    fail("totalC=" + totalC + " valueB=" + valueB + " at " + exb);
                    return false;
                }
            }
            if (totalB != valueA) {
                fail("totalB=" + totalB + " valueA=" + valueA + " at " + exa);
                return false;
            }
        }
        if (totalA != 0) {
            fail("totalA=" + totalA + " at " + exa);
            return false;
        }
        return true;
    }

    private int getAccountValue(final Exchange ex) {
        if (!ex.getValue().isDefined()) {
            return 0;
        }
        try {
            if (ex.getValue().isType(String.class)) {
                _sb.setLength(0);
                ex.getValue().getString(_sb);
                return _sb.length();
            } else {
                return ex.getValue().getInt();
            }
        } catch (final NullPointerException npe) {
            npe.printStackTrace();
            try {
                Thread.sleep(10000);
            } catch (final InterruptedException ie) {
            }
            throw npe;
        }
    }

    private void putAccountValue(final Exchange ex, final int value, final boolean string) {
        if ((value > 0) && (value < 1000) && ((random(0, 100) == 0) || string)) {
            _sb.setLength(0);
            int i = 0;
            for (i = 100; i < value; i += 100) {
                _sb.append("......... ......... ......... ......... ......... "
                        + "......... ......... ......... .........           ");
                int k = i;
                for (int j = 1; (k != 0) && (j < 10); j++) {
                    _sb.setCharAt(i - j, (char) ('0' + (k % 10)));
                    k /= 10;
                }
            }
            for (i = i - 100; i < value; i++) {
                _sb.append(".");
            }
            if (_sb.length() != value) {
                throw new RuntimeException("oops");
            }
            ex.getValue().putString(_sb);
        } else {
            ex.getValue().put(value);
        }
    }

}
