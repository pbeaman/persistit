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
import com.persistit.Value;
import com.persistit.util.ArgParser;

public class Stress1 extends StressBase {

    private final static String[] ARGS_TEMPLATE = { "op|String:wrtd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions", "count|int:10000:0:1000000000|Number of nodes to populate",
            "size|int:30:1:20000|Size of each data value", "splay|int:1:1:1000|Splay", };

    int _size;
    int _splay;
    String _opflags;

    public Stress1(final String argsString) {
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
    public void executeTest() {
        final Value value1 = _ex.getValue();
        final Value value2 = new Value(getPersistit());

        setPhase("@");
        try {
            _ex.clear().remove(Key.GTEQ);
            addWork(1);

        } catch (final Exception e) {
            handleThrowable(e);
        }
        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);
                    _ex.clear().append(keyInteger);
                    setupTestValue(_ex, _count, _size);
                    try {
                        _ex.store();
                        addWork(1);

                    } catch (final Exception e) {
                        handleThrowable(e);
                        break;
                    }
                }
            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);
                    _ex.clear().append(keyInteger);
                    setupTestValue(_ex, _count, _size);
                    try {
                        // fetch to a different Value object so we can compare
                        // with the original.
                        _ex.fetch(value2);
                        addWork(1);

                        compareValues(value1, value2);
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
            }

            if (_opflags.indexOf('t') >= 0) {
                setPhase("t");
                _ex.clear().append(Integer.MIN_VALUE);
                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    try {
                        addWork(1);
                        if (!_ex.next()) {
                            break;
                        }
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_count != _total && !isStopped()) {
                    fail("Traverse count=" + _count + " out of " + _total + " repetition=" + _repeat + " in thread="
                            + _threadIndex);
                    break;
                }
            }

            if (_opflags.indexOf('d') >= 0) {
                setPhase("d");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);
                    _ex.clear().append(keyInteger);
                    try {
                        _ex.remove();
                        addWork(1);

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
    }

    int keyInteger(final int counter) {
        int keyInteger = (counter * _splay) % _total;
        if (keyInteger < 0) {
            keyInteger += _total;
        }
        return keyInteger;
    }

}
