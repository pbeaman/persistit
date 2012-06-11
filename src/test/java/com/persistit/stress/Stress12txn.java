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

package com.persistit.stress;

import java.util.concurrent.atomic.AtomicLong;

import com.persistit.Transaction;
import com.persistit.Value;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;
import com.persistit.suite.TestResult;
import com.persistit.util.ArgParser;

public class Stress12txn extends StressBase {

    private final static String SHORT_DESCRIPTION = "Transactional counter with isolation test";

    private final static String LONG_DESCRIPTION = "   Transaction counter with isolation test\r\n"
            + "    Increments one of several values in a B-Tree and keeps a copy in memory.\r\n"
            + "    Verifies the in-memory copy with the transactionally stored value.\r\n";

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Number of cycles",
            "count|int:1000:1:100000000|Number Repetitions per cycle",
            "size|int:1:1:100000|Number of nodes to populate", "seed|int:1:1:20000|Random seed", };

    public Stress12txn(String argsString) {
        super(argsString);
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
                    addWork(1);

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

        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {
            for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                int retries = 100;
                try {
                    while (true) {
                        try {
                            txn.begin();
                            addWork(1);

                            final int keyInteger = _random.nextInt(_size);
                            _exs.clear().append(keyInteger).fetch();
                            addWork(1);

                            final long currentValue = getCount(_exs.getValue());
                            putCount(_exs.getValue(), currentValue + 1);
                            _exs.store();
                            addWork(1);

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
                _sb.setLength(0);
                value.getString(_sb);
                return _sb.length();
            } else {
                return value.getLong();
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

}
