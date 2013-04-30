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

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.util.ArgParser;

public class Stress5 extends StressBase {

    public Stress5(final String argsString) {
        super(argsString);
    }

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Repetitions",
            "count|int:10000:0:1000000000|Number of nodes to populate", "size|int:4029:10:10000000|Data record size",
            "splay0|int:12:1:1000|Splay 0", "splay1|int:3:1:1000|Splay 1", "splay2|int:7:1:1000|Splay 2", };

    int _splay0;
    int _splay1;
    int _splay2;
    int _size;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress5", _args, ARGS_TEMPLATE).strict();
        _total = _ap.getIntValue("count");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");

        _splay0 = _ap.getIntValue("splay0");
        _splay1 = _ap.getIntValue("splay1");
        _splay2 = _ap.getIntValue("splay2");
        _size = _ap.getIntValue("size");

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
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
        final Value value = new Value(getPersistit());

        final int baselineCount = 500;
        final int keyLength = 2041;
        final int maxDepth = keyLength - 10;
        final int minDepth = 20;

        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {
            try {

                setPhase("@");
                _ex.clear().remove(Key.GTEQ);
                addWork(1);

                setPhase("a");
                for (_count = 0; (_count < baselineCount) && !isStopped(); _count++) {
                    setupKey(_ex, keyLength, keyLength - 5, _count, _count, '5');
                    setupTestValue(_ex, _count, _size);
                    _ex.store();
                    addWork(1);

                }
                if (isStopped()) {
                    break;
                }
                setPhase("b");
                int depth;
                for (_count = 0, depth = maxDepth; (depth > minDepth) && !isStopped(); depth -= _splay1, _count++) {
                    setupKey(_ex, keyLength, depth, minDepth + (depth % _splay0), 55555 + depth, '5');
                    setupTestValue(_ex, 55555 + depth, _size);
                    _ex.store();
                    addWork(1);

                }
                if (isStopped()) {
                    break;
                }

                setPhase("c");
                for (_count = 0, depth = maxDepth; (depth > minDepth) && !isStopped(); depth -= _splay2, _count++) {
                    setupKey(_ex, keyLength, depth, minDepth + (depth % _splay0), 55555 - depth, '5');
                    setupTestValue(_ex, 55555 - depth, _size);
                    _ex.store();
                    addWork(1);

                }
                if (isStopped()) {
                    break;
                }

                setPhase("d");
                for (_count = 0, depth = maxDepth; (depth > minDepth) && !isStopped(); depth -= _splay1, _count++) {
                    setupKey(_ex, keyLength, depth, minDepth + (depth % _splay0), 55555 + depth, '5');
                    setupTestValue(_ex, 55555 + depth, _size);
                    _ex.fetch(value);
                    addWork(1);

                    compareValues(_ex.getValue(), value);
                    if (isStopped()) {
                        break;
                    }
                    _ex.remove();
                }
                if (isStopped()) {
                    break;
                }

                setPhase("e");
                for (_count = 0, depth = maxDepth; (depth > minDepth) && !isStopped(); depth -= _splay2, _count++) {
                    setupKey(_ex, keyLength, depth, minDepth + (depth % _splay0), 55555 - depth, '5');
                    setupTestValue(_ex, 55555 - depth, _size);
                    _ex.fetch(value);
                    addWork(1);

                    compareValues(_ex.getValue(), value);
                    if (isStopped()) {
                        break;
                    }
                    addWork(1);
                    if (!_ex.remove()) {
                        fail("Failed to remove depth=" + depth);
                        break;
                    }
                }
                if (isStopped()) {
                    break;
                }

                setPhase("f");
                for (_count = 0; _count < baselineCount; _count++) {
                    setupKey(_ex, keyLength, keyLength - 5, _count, _count, '5');
                    addWork(1);
                    if (!_ex.remove()) {
                        System.out.println("Failed to remove counter=" + _count);
                    }
                }

                setPhase("g");
                _ex.clear();
                addWork(1);

                if (_ex.traverse(Key.GT, true)) {
                    System.out.println("Tree is not empty");
                }

            } catch (final PersistitException de) {
                handleThrowable(de);
            }
        }
    }

    private void setupKey(final Exchange ex, final int length, final int depth, final int a, final int b,
            final char fill) {
        _sb1.setLength(0);
        for (int i = 0; i < length; i++) {
            _sb1.append(fill);
        }
        fillLong(b, depth, 5, true);
        ex.clear().append(a).append(_sb1);
    }

}
