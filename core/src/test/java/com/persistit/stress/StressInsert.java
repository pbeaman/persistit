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

import java.util.Random;

import com.persistit.Key;
import com.persistit.Value;
import com.persistit.test.TestResult;
import com.persistit.util.ArgParser;

public class StressInsert extends StressBase {

    private final static String SHORT_DESCRIPTION = "Random write/read/delete/traverse loops";

    private final static String LONG_DESCRIPTION = "   Random stress test that perform <repeat> iterations of the following: \r\n"
            + "    - insert <count> sequentially ascending keys \r\n"
            + "    - read and verify <count> sequentially ascending key/value pairs\r\n"
            + "    - traverse and count all keys using next() \r\n"
            + "    - delete <count> sequentially ascending keys \r\n"
            + "   Optional <splay> value allows variations in key sequence: \r\n";

    private final static String[] ARGS_TEMPLATE = { "op|String:wrtd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions", "count|int:10000:0:1000000000|Number of nodes to populate",
            "size|int:30:1:20000|Size of each data value", "splay|int:1:1:1000|Splay", };

    int _size;
    int _splay;
    String _opflags;
    Random _random = new Random();

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
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress1", _args, ARGS_TEMPLATE);
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _dotGranularity = 10000;

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    @Override
    public void executeTest() {
        final Value value1 = _ex.getValue();
        final Value value2 = new Value(getPersistit());

        setPhase("@");
        try {
            _ex.clear().remove(Key.GTEQ);
        } catch (final Exception e) {
            handleThrowable(e);
        }
        verboseln();
        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            verboseln();
            verboseln("Starting cycle " + (_repeat + 1) + " of " + _repeatTotal);

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                seed();
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final long keyInteger = keyInteger(_count);
                    _ex.clear().append(keyInteger);
                    setupTestValue(_ex, _count, _size);
                    try {
                        _ex.store();
                    } catch (final Exception e) {
                        handleThrowable(e);
                        break;
                    }
                }
            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                seed();
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final long keyInteger = keyInteger(_count);
                    _ex.clear().append(keyInteger);
                    setupTestValue(_ex, _count, _size);
                    try {
                        // fetch to a different Value object so we can compare
                        // with the original.
                        _ex.fetch(value2);
                        compareValues(value1, value2);
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
            }

            if (_opflags.indexOf('t') >= 0) {
                setPhase("t");
                seed();
                _ex.clear().append(Integer.MIN_VALUE);
                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    dot();
                    try {
                        if (!_ex.next()) {
                            break;
                        }
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_count != _total) {
                    _result = new TestResult(false, "Traverse count=" + _count + " out of " + _total + " repetition="
                            + _repeat + " in thread=" + _threadIndex);
                    verboseln(_result);
                    forceStop();
                    break;
                }
            }

            if (_opflags.indexOf('d') >= 0) {
                setPhase("d");
                seed();
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final long keyInteger = keyInteger(_count);
                    _ex.clear().append(keyInteger);
                    try {
                        _ex.remove();
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
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
        verboseln();
        verbose("done");
    }

    private void seed() {
        _random.setSeed(_threadIndex + 1);
    }

    long keyInteger(final int counter) {
        return _random.nextLong();
    }

    public static void main(final String[] args) {
        final StressInsert test = new StressInsert();
        test.runStandalone(args);
    }
}
