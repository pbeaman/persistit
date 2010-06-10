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
 * 
 * Created on Mar 19, 2004
 */
package com.persistit.stress;

import com.persistit.ArgParser;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.test.PersistitTestResult;

/**
 * Test to try all split and join locations and conditions. Plan: 1. For each of
 * several key and valuesizes, create a tree with enough key/value pairs of that
 * size to yield 3-level tree 2. Between each key position on one or more pages,
 * insert one or more key/value pairs to force a split, then delete them to
 * cause a rejoin 3. Make sure the resulting tree is valid.
 * 
 */
public class Stress6 extends StressBase {
    private final static String SHORT_DESCRIPTION =
        "Exercise page splits and rejoin";

    private final static String LONG_DESCRIPTION =
        "   Inserts and deletes key/value pairs in a pattern that tests\r\n"
            + "   split and rejoin logic extensively.";

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    private final static String[] ARGS_TEMPLATE =
        { "repeat|int:1:0:1000000000|Repetitions",
            "count|int:1000:0:1000000000|Number of nodes to populate",
            "size|int:500:1:1000|Max splitting value size", };

    int _size;

    @Override
    public void setUp() throws Exception {
    	super.setUp();
        _ap = new ArgParser("com.persistit.Stress6", _args, ARGS_TEMPLATE);
        _total = _ap.getIntValue("count");
        _repeatTotal = _ap.getIntValue("repeat");
        _size = _ap.getIntValue("size");

        _dotGranularity = 100;

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
        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
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
                println();
                println();
                println("Starting test cycle " + _repeat + " at " + tsString());
                describeTest("Deleting all records");
                setPhase("@");
                _exs.clear().append("stress6").append(_threadIndex).remove(
                    Key.GTEQ);
                println();

                describeTest("Creating baseline records");
                setPhase("a");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    _exs.clear().append("stress6").append(_threadIndex).append(
                        _count).append(_sb1);
                    _exs.store();
                }
                println();

                describeTest("Splitting and joining");
                setPhase("b");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    _exs.clear().append("stress6").append(_threadIndex).append(
                        _count).append(_sb1);
                    _sb2.setLength(0);
                    for (int size = 0; (size < _size) && !isStopped(); size++) {
                        _sb2.append('y');
                        _exs.getValue().putString(_sb2);
                        _exs.store();
                    }
                    for (int size = _size; --size >= 0;) {
                        _sb2.setLength(size);
                        _exs.getValue().putString(_sb2);
                        _exs.store();
                    }
                }
                println();

                describeTest("Verifying");
                setPhase("c");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    _exs.clear().append("stress6").append(_threadIndex).append(
                        _count).append(_sb1);
                    _exs.fetch();
                    final Value value = _exs.getValue();
                    boolean okay = false;
                    if (value.isDefined()) {
                        value.getString(_sb2);
                        if (_sb2.length() == 0) {
                            okay = true;
                        }
                    }
                    if (!okay) {
                        _result =
                            new PersistitTestResult(false, "Value for key " + _exs.getKey()
                                + " is not zero-length");
                        forceStop();
                        break;
                    }
                }
                println();

            } catch (final Exception de) {
                handleThrowable(de);
            }
            println("Done at " + tsString());
        }
    }

    private void setupKey(final Exchange ex, final int length, final int depth,
        final int a, final int b, final char fill) {
        _sb1.setLength(0);
        for (int i = 0; i < length; i++) {
            _sb1.append(fill);
        }
        fillLong(b, depth, 5, true);
        ex.clear().append(a).append(_sb1);
    }

    private void describeTest(final String m) {
        print(m);
        print(": ");
        for (int i = m.length(); i < 52; i++) {
            print(" ");
        }
    }

    public static void main(final String[] args) throws Exception {
        new Stress6().runStandalone(args);
    }
}
