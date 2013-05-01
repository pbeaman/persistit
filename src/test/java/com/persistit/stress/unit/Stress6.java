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

/**
 * Test to try all split and join locations and conditions. Plan: 1. For each of
 * several key and value sizes, create a tree with enough key/value pairs of
 * that size to yield 3-level tree 2. Between each key position on one or more
 * pages, insert one or more key/value pairs to force a split, then delete them
 * to cause a rejoin 3. Make sure the resulting tree is valid.
 * 
 */
public class Stress6 extends StressBase {

    public Stress6(final String argsString) {
        super(argsString);
    }

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Repetitions",
            "count|int:1000:0:1000000000|Number of nodes to populate", "size|int:500:1:1000|Max splitting value size", };

    int _size;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress6", _args, ARGS_TEMPLATE).strict();
        _total = _ap.getIntValue("count");
        _repeatTotal = _ap.getIntValue("repeat");
        _size = _ap.getIntValue("size");

        try {
            _exs = getPersistit().getExchange("persistit", "shared", true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    /**
     * Implements tests with long keys and values of borderline length
     */
    @Override
    public void executeTest() {
        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {
            try {
                _exs.getValue().putString("");
                final int keyLength = (_repeat % 2000) + 10;
                _sb1.setLength(0);
                _sb2.setLength(0);
                for (int i = 0; i < keyLength; i++) {
                    _sb1.append('x');
                }
                for (int i = 0; i < 500; i++) {
                    _sb2.append('x');
                }
                setPhase("@");
                _exs.clear().append("stress6").append(_threadIndex).remove(Key.GTEQ);
                addWork(1);

                setPhase("a");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    _exs.clear().append("stress6").append(_threadIndex).append(_count).append(_sb1);
                    _exs.store();
                    addWork(1);

                }
                setPhase("b");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    _exs.clear().append("stress6").append(_threadIndex).append(_count).append(_sb1);
                    _sb2.setLength(0);
                    for (int size = 0; (size < _size) && !isStopped(); size++) {
                        _sb2.append('y');
                        _exs.getValue().putString(_sb2);
                        _exs.store();
                        addWork(1);

                    }
                    for (int size = _size; --size >= 0;) {
                        _sb2.setLength(size);
                        _exs.getValue().putString(_sb2);
                        _exs.store();
                        addWork(1);

                    }
                }
                setPhase("c");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    _exs.clear().append("stress6").append(_threadIndex).append(_count).append(_sb1);
                    _exs.fetch();
                    addWork(1);

                    final Value value = _exs.getValue();
                    boolean okay = false;
                    if (value.isDefined()) {
                        _sb2.setLength(0);
                        value.getString(_sb2);
                        if (_sb2.length() == 0) {
                            okay = true;
                        }
                    }
                    if (!okay) {
                        fail("Value for key " + _exs.getKey() + " is not zero-length");
                        break;
                    }
                }
            } catch (final Exception de) {
                handleThrowable(de);
            }
        }
    }

}
