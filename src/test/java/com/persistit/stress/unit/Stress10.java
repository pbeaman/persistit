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
import com.persistit.util.ArgParser;

public class Stress10 extends StressBase {

    private final static String[] ARGS_TEMPLATE = { "op|String:wrtd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions", "count|int:100000:0:1000000000|Number of nodes to populate",
            "size|int:500:1:200000|Size of each data value", "seed|int:1:1:20000|Random seed", };

    int _size;
    int _splay;
    int _seed;
    String _opflags;

    public Stress10(final String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress10", _args, ARGS_TEMPLATE).strict();
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
    public void executeTest() {

        setPhase("@");
        try {
            _ex.clear().remove(Key.GTEQ);
            _exs.clear().append("stress10").append(Key.BEFORE);
            while (_exs.next()) {
                _exs.append(_threadIndex);
                _exs.remove(Key.GTEQ);
                _exs.cut();
                addWork(1);

            }
        } catch (final Exception e) {
            handleThrowable(e);
        }

        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {
            _random.setSeed(_seed);

            for (_count = 0; (_count < _total) && !isStopped(); _count++) {

                final int keyInteger = keyInteger(_count);
                int action = random(0, 10);

                // Strong bias toward inserting new data.
                action = action < 5 ? 0 : action < 7 ? 1 : action < 9 ? 2 : 3;

                switch (action) {

                case 0: {
                    // write a record

                    _exs.clear().append("stress10").append(keyInteger).append(_threadIndex);
                    setupTestValue(_exs, keyInteger, random(20, _size));

                    _ex.clear().append(keyInteger);
                    _ex.getValue().put(_exs.getValue().getEncodedSize());

                    try {
                        _exs.store();
                        _ex.store();
                        addWork(2);

                    } catch (final Exception e) {
                        handleThrowable(e);

                    }
                    break;
                }
                case 1: {
                    // fetch a record

                    _exs.clear().append("stress10").append(keyInteger).append(_threadIndex);
                    setupTestValue(_exs, keyInteger, random(20, _size));
                    _ex.clear().append(keyInteger);
                    try {
                        _ex.fetch();
                        addWork(1);

                        int size1 = 0;
                        if (_ex.getValue().isDefined() && !_ex.getValue().isNull()) {
                            size1 = _ex.getValue().getInt();
                        }
                        _exs.fetch();
                        addWork(1);

                        final int size2 = _exs.getValue().getEncodedSize();
                        if (size2 != size1) {
                            fail("Value is size " + size2 + ", should be " + size1 + " key=" + _ex.getKey());
                        }
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                    break;
                }
                case 2: {
                    // traverse up to 1000 records

                    _exs.clear().append("stress10").append(keyInteger);
                    for (int count = 0; (count < random(10, 1000)) && !isStopped(); count++) {
                        try {
                            addWork(1);
                            if (!_exs.next()) {
                                break;
                            }
                            final int curKeyInteger = _exs.getKey().indexTo(1).decodeInt();
                            _exs.append(_threadIndex).fetch().getValue();
                            addWork(1);

                            final int size2 = _exs.getValue().getEncodedSize();
                            _ex.clear().append(curKeyInteger).fetch();
                            addWork(1);

                            int size1 = 0;
                            if (_ex.getValue().isDefined()) {
                                size1 = _ex.getValue().getInt();
                            }
                            if (size2 != size1) {
                                fail("Value is size " + size2 + ", should be " + size1 + " key=" + _ex.getKey());
                            }
                            _exs.cut();
                        } catch (final Exception e) {
                            handleThrowable(e);
                        }
                    }
                    break;
                }
                case 3: {
                    // delete a record

                    _exs.clear().append("stress10").append(keyInteger).append(_threadIndex);
                    _ex.clear().append(keyInteger);
                    try {
                        _exs.remove();
                        _ex.remove();
                        addWork(2);

                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                    break;
                }
                }
            }
        }

    }

    int keyInteger(final int counter) {
        final int keyInteger = random(0, _total);
        return keyInteger;
    }
}
