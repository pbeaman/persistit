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

import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.TransactionRunnable;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.util.ArgParser;

public class Stress1txn extends StressBase {


    private final static String[] ARGS_TEMPLATE = { "op|String:wrtd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions", "count|int:10000:0:1000000000|Number of nodes to populate",
            "size|int:30:1:5000000|Size of each data value", "splay|int:1:1:1000|Splay", };

    int _size;
    int _splay;
    String _opflags;

    public Stress1txn(String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress1", _args, ARGS_TEMPLATE).strict();
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    @Override
    public void executeTest() throws Exception {
        final Value value1 = _ex.getValue();
        final Value value2 = new Value(getPersistit());
        final Transaction txn = getPersistit().getTransaction();

        setPhase("@");
        try {
            txn.begin();
            _ex.clear().remove(Key.GTEQ);
            addWork(1);

            txn.commit();
        } catch (final Exception e) {
            handleThrowable(e);
        } finally {
            txn.end();
        }
        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                try {
                    txn.begin();
                    for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                        final int keyInteger = keyInteger(_count);
                        _ex.clear().append(keyInteger);
                        setupTestValue(_ex, _count, _size);
                        _ex.store();
                        addWork(1);

                    }
                    txn.commit();
                } catch (final Exception e) {
                    handleThrowable(e);
                    break;
                } finally {
                    txn.end();
                }
            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);
                    _ex.clear().append(keyInteger);
                    setupTestValue(_ex, _count, _size);
                    try {
                        txn.run(new TransactionRunnable() {
                            @Override
                            public void runTransaction() throws PersistitException {

                                // fetch to a different Value object so we can
                                // compare
                                // with the original.
                                _ex.fetch(value2);
                                addWork(1);

                                compareValues(value1, value2);
                            }
                        });
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
            }

            if (_opflags.indexOf('t') >= 0) {
                setPhase("t");
                try {
                    txn.begin();
                    _ex.clear().append(Integer.MIN_VALUE);
                    for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                        addWork(1);

                        if (!_ex.next()) {
                            break;
                        }
                    }
                    if (_count != _total && !isStopped()) {
                        fail("Traverse count=" + _count + " out of " + _total
                                + " repetition=" + _repeat + " in thread=" + _threadIndex);
                        break;
                    }
                    txn.commit();
                } catch (final Exception e) {
                    handleThrowable(e);
                } finally {
                    txn.end();
                }
            }

            if (_opflags.indexOf('d') >= 0) {
                setPhase("d");
                try {
                    txn.begin();
                    for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                        final int keyInteger = keyInteger(_count);
                        _ex.clear().append(keyInteger);
                        _ex.remove();
                        addWork(1);

                    }
                    txn.commit();
                } catch (final Exception e) {
                    handleThrowable(e);
                } finally {
                    txn.end();
                }
            }

            if ((_opflags.indexOf('h') > 0) && !isStopped()) {
                setPhase("h");
                try {
                    Thread.sleep(1000);
                } catch (final Exception e) {
                }
            }

        }
    }

    int keyInteger(final int counter) {
        int keyInteger = (counter * _splay) % _total;
        if (keyInteger < 0) {
            keyInteger += _total;
        }
        return keyInteger;
    }

}
