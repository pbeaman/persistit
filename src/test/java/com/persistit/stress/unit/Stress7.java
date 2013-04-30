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

/**
 * Test to try all split and join locations and conditions. Plan: 1. For each of
 * several key and value sizes, create a tree with enough key/value pairs of
 * that size to yield 3-level tree 2. Between each key position on one or more
 * pages, insert one or more key/value pairs to force a split, then delete them
 * to cause a rejoin 3. Make sure the resulting tree is valid.
 * 
 */
public class Stress7 extends StressBase {

    public Stress7(final String argsString) {
        super(argsString);
    }

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Repetitions",
            "count|int:1000:0:100000|Number of nodes to populate", "size|int:500:1:10000|Max splitting value size",
            "seed|int:1:1:20000|Random seed",

    };

    int _size;
    int _seed;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress7", _args, ARGS_TEMPLATE).strict();
        _total = _ap.getIntValue("count");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _size = _ap.getIntValue("size");
        _seed = _ap.getIntValue("seed");

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
                final int keyLength = (_repeat) + 10;
                _sb1.setLength(0);
                _sb2.setLength(0);
                for (int i = 0; i < keyLength; i++) {
                    _sb1.append('x');
                }
                for (int i = 0; i < 500; i++) {
                    _sb2.append('x');
                }

                setPhase("@");
                _exs.clear().append("stress7").append(_threadIndex).remove(Key.GTEQ);
                addWork(1);

                setPhase("a");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    _exs.clear().append("stress7").append(_threadIndex).append(_count).append(_sb1);
                    _exs.store();
                    addWork(1);

                }
                setPhase("b");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    _exs.clear().append("stress7").append(_threadIndex).append(_count).append(_sb1);
                    _sb2.setLength(0);
                    final int toSize = random(1, _size);
                    for (int size = 0; (size < toSize) && !isStopped(); size += 4) {
                        _sb2.append('y');
                        _exs.getValue().putString(_sb2);
                        _exs.store();
                        _exs.remove();
                        addWork(1);

                    }
                }
                setPhase("c");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    _exs.clear().append("stress7").append(_threadIndex).append(_count).append(_sb1);
                    _exs.fetch();
                    addWork(1);

                    if (_exs.getValue().isDefined()) {
                        fail("Value for key " + _exs.getKey() + " is defined but should not be");
                        break;
                    }
                }
            } catch (final Exception de) {
                handleThrowable(de);
            }
        }
    }

}
