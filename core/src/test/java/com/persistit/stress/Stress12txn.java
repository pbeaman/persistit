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

import java.util.concurrent.atomic.AtomicLong;

import com.persistit.Transaction;
import com.persistit.Value;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;
import com.persistit.test.TestResult;
import com.persistit.util.ArgParser;

public class Stress12txn extends StressBase {

    private final static String SHORT_DESCRIPTION = "Transactional counter with isolation test";

    private final static String LONG_DESCRIPTION = "   Transaction counter with isolation test\r\n"
            + "    Increments one of several values in a B-Tree and keeps a copy in memory.\r\n"
            + "    Verifies the in-memory copy with the transactionally stored value.\r\n";

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Number of cycles",
            "count|int:1000:1:100000000|Number Repetitions per cycle",
            "size|int:1:1:100000|Number of nodes to populate", "seed|int:1:1:20000|Random seed", };

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    static boolean _initializedOnce = false;
    int _size;
    int _seed;

    static AtomicLong[] _counters = new AtomicLong[100000];

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress1", _args, ARGS_TEMPLATE);
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _size = _ap.getIntValue("size");
        _dotGranularity = 10000;
        _seed = _ap.getIntValue("seed");
        seed(_seed);

        try {
            // Exchange with Thread-private Tree
            _exs = getPersistit().getExchange("persistit", "shared", true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    @Override
    public void executeTest() {
        synchronized (Stress12txn.class) {
            if (!_initializedOnce) {
                _initializedOnce = true;
                try {
                    _exs.removeAll();
                    for (int index = 0; index < _size; index++) {
                        _counters[index] = new AtomicLong();
                    }
                } catch (Exception e) {
                    _result = new TestResult(false, e);
                    forceStop();
                }
            }
        }
        final Transaction txn = _exs.getTransaction();
        final Value value = _exs.getValue();
        verboseln();
        verboseln();
        verboseln("Starting test cycle " + _repeat + " at " + tsString());

        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                int retries = 100;
                try {
                    while (true) {
                        try {
                            txn.begin();
                            final int keyInteger = _random.nextInt(_size);
                            _exs.clear().append(keyInteger).fetch();
                            final long currentValue = getCount(_exs.getValue());
                            putCount(_exs.getValue(), currentValue + 1);
                            _exs.store();
                            txn.commit();
                            break;
                        } catch (final RollbackException re) {
                            if (--retries < 0) {
                                throw new TransactionFailedException();
                            }
                        } finally {
                            txn.end();
                        }
                    }
                } catch (final Exception e) {
                    handleThrowable(e);
                }
            }
        }

        verboseln();
        verbose("done");
    }

    private void putCount(final Value value, final long v) {
        if ((v > 0) && (v < 100000) && ((random(0, 10) == 0))) {
            _sb.setLength(0);
            int i = 0;
            for (i = 100; i < v; i += 100) {
                _sb.append("......... ......... ......... ......... ......... "
                        + "......... ......... ......... .........           ");
                int k = i;
                for (int j = 1; (k != 0) && (j < 10); j++) {
                    _sb.setCharAt(i - j, (char) ('0' + (k % 10)));
                    k /= 10;
                }
            }
            for (i = i - 100; i < v; i++) {
                _sb.append(".");
            }
            if (_sb.length() != v) {
                throw new RuntimeException("oops");
            }
            value.putString(_sb);
        } else {
            value.put(v);
        }
    }

    private long getCount(final Value value) {
        if (!value.isDefined()) {
            return 0;
        }
        try {
            if (value.isType(String.class)) {
                value.getString(_sb);
                return _sb.length();
            } else {
                return value.getLong();
            }
        } catch (final NullPointerException npe) {
            printStackTrace(npe);
            try {
                Thread.sleep(10000);
            } catch (final InterruptedException ie) {
            }
            throw npe;
        }
    }

    public static void main(final String[] args) {
        final Stress12txn test = new Stress12txn();
        test.runStandalone(args);
    }
}
