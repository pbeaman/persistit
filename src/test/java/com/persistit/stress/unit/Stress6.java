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

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.stress.TestResult;
import com.persistit.util.ArgParser;

/**
 * Test to try all split and join locations and conditions. Plan: 1. For each of
 * several key and valuesizes, create a tree with enough key/value pairs of that
 * size to yield 3-level tree 2. Between each key position on one or more pages,
 * insert one or more key/value pairs to force a split, then delete them to
 * cause a rejoin 3. Make sure the resulting tree is valid.
 * 
 */
public class Stress6 extends StressBase {

    public Stress6(String argsString) {
        super(argsString);
    }

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Repetitions",
            "count|int:1000:0:1000000000|Number of nodes to populate", "size|int:500:1:1000|Max splitting value size", };

    int _size;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress6", _args, ARGS_TEMPLATE);
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
                        _result = new TestResult(false, "Value for key " + _exs.getKey() + " is not zero-length");
                        forceStop();
                        break;
                    }
                }
            } catch (final Exception de) {
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
