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

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import com.persistit.ArgParser;
import com.persistit.PersistitMap;
import com.persistit.Value;
import com.persistit.test.TestResult;

public class PersistitMapStress1 extends StressBase {

    private final static String SHORT_DESCRIPTION = "PersistitMap test - simple write/read/delete/traverse loops";

    private final static String LONG_DESCRIPTION = "   Simple stress test that perform <repeat> iterations of the following: \r\n"
            + "    - insert <count> sequentially ascending keys \r\n"
            + "    - read and verify <count> sequentially ascending key/value pairs\r\n"
            + "    - traverse and count all keys using next() \r\n"
            + "    - delete <count> sequentially ascending keys \r\n"
            + "   Optional <splay> value allows variations in key sequence: \r\n";

    private final static String[] ARGS_TEMPLATE = {
            "op|String:Cwrtd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions",
            "count|int:10000:0:1000000000|Number of nodes to populate",
            "size|int:30:1:20000|Size of each data value",
            "splay|int:1:1:1000|Splay",
            "_flag|t|Timing test on TreeMap instead of PersistitMap", };

    int _size;
    int _splay;
    String _opflags;

    SortedMap _dm1;
    SortedMap _dm2;

    long _timeWrite;
    long _timeRead;
    long _timeIter;
    long _timeRemove;

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public void setUp() {
        _ap = new ArgParser("com.persistit.stress.PersistitMapStress2", _args,
                ARGS_TEMPLATE);
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _dotGranularity = 10000;

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit",
                    _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }

        _ex.to("dmtest").append(_rootName + _threadIndex);

        if (_ap.isFlag('t')) {
            _dm1 = new TreeMap();
            _dm2 = new PersistitMap(_ex);
        } else {
            _dm1 = new PersistitMap(_ex);
            _dm2 = new TreeMap();
        }
    }

    private long ts() {
        return System.currentTimeMillis();
    }

    @Override
    public void executeTest() {
        final Value value1 = _ex.getValue();
        final Value value2 = new Value(getPersistit());

        if (_opflags.indexOf('C') >= 0) {
            setPhase("C");
            try {
                _dm1.clear();
            } catch (final Exception e) {
                handleThrowable(e);
            }
            verboseln();
        }

        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            verboseln();
            verboseln("Starting cycle " + (_repeat + 1) + " of " + _repeatTotal);

            long ts = ts();
            long tt;

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final int keyInteger = keyInteger(_count);

                    setupTestValue(_ex, _count, _size);
                    try {
                        _dm1.put(new Integer(keyInteger), _sb1.toString());
                    } catch (final Exception e) {
                        handleThrowable(e);
                        break;
                    }
                }
                if (_dm1.size() != _total) {
                    _result = new TestResult(false, "PersistitMap.size()="
                            + _dm1.size() + " out of " + _total
                            + " repetition=" + _repeat + " in thread="
                            + _threadIndex);
                    println(_result);
                    forceStop();
                    break;
                }
                _timeWrite += (tt = ts()) - ts;
                ts = tt;

            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final int keyInteger = keyInteger(_count);
                    setupTestValue(_ex, _count, _size);
                    final String s1 = _sb1.toString();

                    try {
                        // fetch to a different Value object so we can compare
                        // with the original.
                        final String s2 = (String) _dm1.get(new Integer(
                                keyInteger));
                        compareStrings(s1, s2);
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                _timeRead += (tt = ts()) - ts;
                ts = tt;
            }

            if (_opflags.indexOf('t') >= 0) {
                setPhase("t");
                final Iterator itr = _dm1.keySet().iterator();

                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    dot();
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_count != _total) {
                    _result = new TestResult(false, "Traverse count=" + _count
                            + " out of " + _total + " repetition=" + _repeat
                            + " in thread=" + _threadIndex);
                    println(_result);
                    forceStop();
                    break;
                }
                _timeIter += (tt = ts()) - ts;
                ts = tt;
            }

            if (_opflags.indexOf('d') >= 0) {
                setPhase("d");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final int keyInteger = keyInteger(_count);
                    setupTestValue(_ex, _count, _size);
                    final String s1 = _sb1.toString();
                    try {
                        final String s2 = (String) _dm1.remove(new Integer(
                                keyInteger));
                        compareStrings(s1, s2);
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }

                //
                // Now verify that the interator has no members.
                //
                final Iterator itr = _dm1.keySet().iterator();

                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    dot();
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_count != 0) {
                    _result = new TestResult(false, "Traverse count=" + _count
                            + " when 0 were expected" + " repetition="
                            + _repeat + " in thread=" + _threadIndex);
                    println(_result);
                    forceStop();
                    break;
                }
                _timeRemove += (tt = ts()) - ts;
                ts = tt;
            }

            if (_opflags.indexOf('D') >= 0) {
                setPhase("D");
                //
                // Now verify that the interator has no members.
                //
                final Iterator itr = _dm1.keySet().iterator();

                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    dot();
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                        itr.remove();
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_dm1.size() != 0) {
                    _result = new TestResult(false, "PersistitMap.size()= "
                            + _dm1.size() + " when 0 were expected"
                            + " repetition=" + _repeat + " in thread="
                            + _threadIndex);
                    println(_result);
                    forceStop();
                    break;
                }
                _timeRemove += (tt = ts()) - ts;
                ts = tt;
            }

            if ((_opflags.indexOf('h') > 0) && !isStopped()) {
                setPhase("h");
                try {
                    Thread.sleep(1000);
                } catch (final Exception e) {
                }
            }

        }
        verboseln();
        verboseln(" timeWrite=" + _timeWrite + " timeRead=" + _timeRead
                + " timeIter=" + _timeIter + " timeRemove=" + _timeRemove
                + " total="
                + (_timeWrite + _timeRead + _timeIter + _timeRemove));
        verbose("done");
    }

    int keyInteger(final int counter) {
        int keyInteger = (counter * _splay) % _total;
        if (keyInteger < 0) {
            keyInteger += _total;
        }
        return keyInteger;
    }

    public static void main(final String[] args) {
        final PersistitMapStress1 test = new PersistitMapStress1();
        test.runStandalone(args);
    }
}
