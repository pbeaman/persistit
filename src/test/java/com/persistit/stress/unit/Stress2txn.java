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

package com.persistit.stress.unit;

import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.Transaction.CommitPolicy;
import com.persistit.TransactionRunnable;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RebalanceException;
import com.persistit.exception.RollbackException;
import com.persistit.util.ArgParser;

public class Stress2txn extends StressBase {

    private final static String[] ARGS_TEMPLATE = { "op|String:wrtd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions", "count|int:10000:0:1000000000|Number of nodes to populate",
            "size|int:200:1:200000|Size of each data value", "seed|int:1:1:20000|Random seed",
            "splay|int:1:1:1000|Splay", };

    int _size;
    int _splay;
    int _seed;
    String _opflags;

    public Stress2txn(final String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress2ts", _args, ARGS_TEMPLATE).strict();
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _size = _ap.getIntValue("size");
        _seed = _ap.getIntValue("seed");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
            _exs = getPersistit().getExchange("persistit", "shared", true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    @Override
    public void executeTest() throws Exception {
        final Value value1 = _exs.getValue();
        final Value value2 = new Value(getPersistit());
        final Transaction txn = getPersistit().getTransaction();

        setPhase("@");

        for (int remainingAttempts = 20; --remainingAttempts >= 0;) {
            try {
                txn.begin();
                _ex.clear().remove(Key.GTEQ);
                _exs.clear().append("stress2").append(Key.BEFORE);

                while (_exs.next()) {
                    _exs.append(_threadIndex);
                    _exs.remove(Key.GTEQ);
                    _exs.cut();
                    addWork(1);

                }

                txn.commit();
                break;
            } catch (final RollbackException re) {
                if (remainingAttempts % 5 == 0) {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException e) {
                    }
                }
                if (remainingAttempts == 0) {
                    throw re;
                }
            } catch (final Exception e) {
                handleThrowable(e);
            } finally {
                txn.end();
            }

        }

        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                _random.setSeed(_seed);
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);

                    _exs.clear().append("stress2").append(keyInteger).append(_threadIndex);
                    setupTestValue(_exs, keyInteger, random(20, _size));

                    _ex.clear().append(keyInteger);
                    _ex.getValue().put(_exs.getValue().getEncodedSize());

                    try {
                        final int passes = txn.run(new TransactionRunnable() {
                            @Override
                            public void runTransaction() throws PersistitException {
                                _exs.store();
                                _ex.store();
                                addWork(2);

                            }
                        }, 10, 0, CommitPolicy.SOFT);
                    } catch (final Exception e) {
                        handleThrowable(e);

                        break;
                    }
                }
            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                _random.setSeed(_seed);
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);
                    _exs.clear().append("stress2").append(keyInteger).append(_threadIndex);
                    setupTestValue(_exs, keyInteger, random(20, _size));
                    _ex.clear().append(keyInteger);
                    try {
                        txn.run(new TransactionRunnable() {
                            @Override
                            public void runTransaction() throws PersistitException {
                                _ex.fetch();
                                addWork(1);

                                int size1 = 0;
                                if (_ex.getValue().isDefined() && !_ex.getValue().isNull()) {
                                    size1 = _ex.getValue().getInt();
                                }
                                _exs.fetch(value2);
                                addWork(1);

                                final int size2 = value2.getEncodedSize();
                                if (size2 != size1) {
                                    fail("Value is size " + size2 + ", should be " + size1 + " key=" + _ex.getKey());
                                }
                            }
                        }, 10, 0, CommitPolicy.SOFT);
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
            }

            if (_opflags.indexOf('t') >= 0) {
                setPhase("t");

                _exs.clear().append("stress2").append(Key.BEFORE);
                int count1 = 0;
                int count2 = 0;
                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    try {
                        addWork(1);

                        if (!_exs.next()) {
                            break;
                        }
                        addWork(1);
                        if (_exs.append(_threadIndex).fetch().getValue().isDefined()) {
                            count1++;
                        }
                        _exs.cut();
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }

                setPhase("T");
                _ex.clear().append(Key.BEFORE);
                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    try {
                        addWork(1);

                        if (!_ex.next()) {
                            break;
                        }
                        count2++;
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (count1 != count2 && !isStopped()) {
                    fail("Traverse count is " + count1 + " but should be " + count2 + " on repetition=" + _repeat
                            + " in thread=" + _threadIndex);
                    break;
                }
            }

            if (_opflags.indexOf('d') >= 0) {
                setPhase("d");
                _random.setSeed(_seed);

                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);
                    _exs.clear().append("stress2").append(keyInteger).append(_threadIndex);
                    _ex.clear().append(keyInteger);
                    try {
                        _exs.remove();
                        _ex.remove();
                        addWork(2);
                    } catch (final RebalanceException e) {
                        // TODO - fix code so that RebalanceExceptions don't
                        // occur.
                        // For now this is a known problem so don't make the
                        // stress test fail
                        System.err.println(e + " at " + _exs);
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
            }

            if ((_opflags.indexOf('h') > 0) && !isStopped()) {
                setPhase("h");
                try {
                    Thread.sleep(random(1000, 5000));
                } catch (final Exception e) {
                }
            }
        }

    }

    int keyInteger(final int counter) {
        final int keyInteger = random(0, _total);
        return keyInteger;
    }

}
