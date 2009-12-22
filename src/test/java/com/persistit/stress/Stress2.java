/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.persistit.stress;

import com.persistit.ArgParser;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.test.TestRunner;

public class Stress2 extends StressBase {

    private final static String SHORT_DESCRIPTION =
        "Random key and value size write/read/delete/traverse loops";

    private final static String LONG_DESCRIPTION =
        "   Simple stress test that repeats the following steps <repeat> times: \r\n"
            + "    - insert <count> random keys with random value length \r\n"
            + "    - read and verify <count> key/value pairs \r\n"
            + "    - traverse and count all keys using next() \r\n"
            + "    - delete <count> random keys\r\n";

    private final static String[] ARGS_TEMPLATE =
        { "op|String:wrtd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions",
            "count|int:10000:0:1000000000|Number of nodes to populate",
            "size|int:200:1:200000|Size of each data value",
            "seed|int:1:1:20000|Random seed", "splay|int:1:1:1000|Splay", };

    int _size;
    int _splay;
    int _seed;
    String _opflags;

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public void setupTest(final String[] args) {
        _ap = new ArgParser("com.persistit.Stress2", args, ARGS_TEMPLATE);
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _size = _ap.getIntValue("size");
        _seed = _ap.getIntValue("seed");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _dotGranularity = 10000;

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
            _exs = getPersistit().getExchange("persistit", "shared", true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    @Override
    public void runTest() {
        final Value value1 = _exs.getValue();
        final Value value2 = new Value(getPersistit());

        setPhase("@");
        try {
            _ex.clear().remove(Key.GTEQ);
            _exs.clear().append("stress2").append(Key.BEFORE);
            while (_exs.next()) {
                _exs.append(_threadIndex);
                _exs.remove(Key.GTEQ);
                _exs.cut();
            }
        } catch (final Exception e) {
            handleThrowable(e);
        }
        println();

        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            println();
            println("Starting cycle " + (_repeat + 1) + " of " + _repeatTotal);

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                _random.setSeed(_seed);
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final int keyInteger = keyInteger(_count);

                    _exs.clear().append("stress2").append(keyInteger).append(
                        _threadIndex);
                    setupTestValue(_exs, keyInteger, random(20, _size));

                    _ex.clear().append(keyInteger);
                    _ex.getValue().put(_exs.getValue().getEncodedSize());

                    try {
                        _exs.store();
                        _ex.store();
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
                    dot();
                    final int keyInteger = keyInteger(_count);
                    _exs.clear().append("stress2").append(keyInteger).append(
                        _threadIndex);
                    setupTestValue(_exs, keyInteger, random(20, _size));
                    _ex.clear().append(keyInteger);
                    try {
                        _ex.fetch();
                        int size1 = 0;
                        if (_ex.getValue().isDefined()
                            && !_ex.getValue().isNull()) {
                            size1 = _ex.getValue().getInt();
                        }
                        _exs.fetch(value2);
                        final int size2 = value2.getEncodedSize();
                        if (size2 != size1) {
                            _result =
                                new TestRunner.Result(false, "Value is size "
                                    + size2 + ", should be " + size1 + " key="
                                    + _ex.getKey());
                            println(_result);
                            forceStop();
                        }
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
                    dot();
                    try {
                        if (!_exs.next()) {
                            break;
                        }
                        if (_exs.append(_threadIndex).fetch().getValue()
                            .isDefined()) {
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
                    dot();
                    try {
                        if (!_ex.next()) {
                            break;
                        }
                        count2++;
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (count1 != count2) {
                    _result =
                        new TestRunner.Result(false, "Traverse count is "
                            + count1 + " but should be " + count2
                            + " on repetition=" + _repeat + " in thread="
                            + _threadIndex);

                    break;
                }
            }

            if (_opflags.indexOf('d') >= 0) {
                setPhase("d");
                _random.setSeed(_seed);

                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final int keyInteger = keyInteger(_count);
                    _exs.clear().append("stress2").append(keyInteger).append(
                        _threadIndex);
                    _ex.clear().append(keyInteger);
                    try {
                        _exs.remove();
                        _ex.remove();
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
        println();
        print("done");

    }

    int keyInteger(final int counter) {
        final int keyInteger = random(0, _total);
        return keyInteger;
    }

    public static void main(final String[] args) throws Exception {
        final Stress2 test = new Stress2();
        test.runStandalone(args);
    }
}
