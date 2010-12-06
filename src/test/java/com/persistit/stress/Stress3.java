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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

import com.persistit.ArgParser;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.test.TestResult;

public class Stress3 extends StressBase {

    private final static String SHORT_DESCRIPTION = "Insert and index filename list from sample data file";

    private final static String LONG_DESCRIPTION = "   Stress test that indexes filenames from sample data file in \r\n"
            + "   various ways, including reverse spelling.  Uses the \r\n"
            + "   incrementValue() method.  Tests integrity of resulting \r\n"
            + "   data structures.  Performs random and whole-tree deletes \r\n";

    private final static String DEFAULT_DATA_FILE_NAME = "src/test/resources/testdata/test3_data.txt";

    private final static String[] ARGS_TEMPLATE = {
            "op|String:wrd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions",
            "count|int:10000:0:1000000000|Number of nodes to populate",
            "seed|int:1:1:20000|Random seed",
            "size|int:3000:1:20000|Maximum record size",
            "splay|int:1:1:1000|Splay",
            "datafile|String:" + DEFAULT_DATA_FILE_NAME, };

    static String[] _fileNames = null;
    static boolean _filesLoaded;

    int _splay;
    int _seed;
    int _size;
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
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress3", _args, ARGS_TEMPLATE);
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _seed = _ap.getIntValue("seed");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _dotGranularity = 1000;

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit",
                    _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }

        initialize(_ap.getStringValue("datafile"));
    }

    /**
     * Performed by only one thread.
     */
    public synchronized static void initialize(final String fileName) {
        if (_filesLoaded) {
            return;
        }
        try {
            final BufferedReader br = new BufferedReader(new FileReader(
                    fileName));
            final ArrayList list = new ArrayList(10000);
            for (;;) {
                final String line = br.readLine();
                if (line == null) {
                    break;
                }
                list.add(line);
            }
            _fileNames = (String[]) list.toArray(new String[0]);
        } catch (final IOException ioe) {
            _fileNames = new String[] { "foo", "bar" };
        }
        _filesLoaded = true;
    }

    /**
     * Performed once by each newly started thread.
     */
    @Override
    public void executeTest() {
        final int[] sizeArray = new int[_total * 40];

        final int[] randomSeq = new int[_total];
        final int[] randomUniqueSeq = new int[_total];
        _random.setSeed(_seed);

        BitSet bs = new BitSet();
        for (int counter = 0; counter < _total; counter++) {
            final int keyInteger = random(0, _fileNames.length);
            randomSeq[counter] = keyInteger;
            final boolean unique = !bs.get(keyInteger);
            if (unique) {
                bs.set(keyInteger);
            }
            randomUniqueSeq[counter] = unique ? keyInteger : -1;
        }
        bs = null;

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
                final Exchange ex1 = new Exchange(_ex);
                final Exchange ex2 = new Exchange(_ex);
                final Exchange ex3 = new Exchange(_ex);

                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    try {
                        final int keyInteger = randomSeq[_count];

                        final String s = _fileNames[keyInteger];

                        long atomic;

                        ex1.clear().append("byName").append(s).fetch();
                        if (!ex1.getValue().isDefined()
                                || ex1.getValue().isNull()) {
                            atomic = _ex.clear().append("counter")
                                    .incrementValue();
                            ex1.getValue().put(atomic);
                            ex1.store();
                        } else {
                            atomic = ex1.getValue().getLong();
                        }

                        setupTestValue(ex2, _count, random(30, _size));
                        sizeArray[(int) atomic] = ex2.getValue()
                                .getEncodedSize();
                        ex2.clear().append("byCounter").append(atomic).store();

                        _sb1.setLength(0);
                        _sb1.append(s);
                        _sb1.reverse();

                        ex3.getValue().put(atomic);
                        ex3.clear().append("byReversedName").append(_sb1)
                                .store();
                    } catch (final Throwable t) {
                        handleThrowable(t);
                    }
                }
            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                final Exchange ex1 = new Exchange(_ex);
                final Exchange ex2 = new Exchange(_ex);
                final Exchange ex3 = new Exchange(_ex);
                final Value value2 = new Value(getPersistit());
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    try {
                        final int keyInteger = randomSeq[_count];

                        final String s = _fileNames[keyInteger];

                        ex1.clear().append("byName").append(s).fetch();
                        if (!ex1.getValue().isDefined()
                                || ex1.getValue().isNull()) {
                            throw new RuntimeException("Expected filename <"
                                    + s + "> was not found - key="
                                    + ex1.getKey());
                        }
                        final long atomic = ex1.getValue().getLong();

                        setupTestValue(ex2, _count, random(30, _size));
                        ex2.clear().append("byCounter").append(atomic)
                                .fetch(value2);

                        if (!value2.isDefined() || value2.isNull()) {
                            throw new RuntimeException(
                                    "Expected value for byCounter " + atomic
                                            + " was not found - key="
                                            + ex2.getKey());
                        }
                        final int size2 = value2.getEncodedSize();
                        final int size1 = sizeArray[(int) atomic];
                        if (size1 != size2) {
                            throw new RuntimeException("Value is size " + size2
                                    + ", should be " + size1 + " key="
                                    + ex2.getKey());
                        }

                        _sb1.setLength(0);
                        _sb1.append(s);
                        _sb1.reverse();
                        ex3.clear().append("byReversedName").append(_sb1)
                                .fetch();
                        if (!ex3.getValue().isDefined()
                                || ex3.getValue().isNull()
                                || (ex3.getValue().getLong() != atomic)) {
                            throw new RuntimeException(
                                    "Missing or incorrect value "
                                            + ex3.getValue() + " should be "
                                            + atomic + " key=" + ex3.getKey());
                        }
                    } catch (final Throwable t) {
                        handleThrowable(t);
                    }
                }
            }

            if ((_opflags.indexOf('D') >= 0) && !isStopped()) {
                if (random(0, 4) == 0) {
                    setPhase("D");
                    try {
                        _ex.clear().append("byName").remove(Key.GTEQ);
                        _ex.clear().append("byCounter").remove(Key.GTEQ);
                        _ex.clear().append("byReversedName").remove(Key.GTEQ);
                        _ex.clear().append("counter").remove();
                        Arrays.fill(sizeArray, 0);
                        // We've deleted everything, so we might as well restart
                        continue;
                    } catch (final Throwable t) {
                        handleThrowable(t);
                    }
                }
            }

            if (_opflags.indexOf('d') >= 0) {
                if (random(0, 4) == 0) {
                    setPhase("d");

                    final Exchange ex1 = new Exchange(_ex);
                    final Exchange ex2 = new Exchange(_ex);
                    final Exchange ex3 = new Exchange(_ex);

                    for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                        dot();
                        try {
                            final int keyInteger = randomUniqueSeq[_count];
                            if (keyInteger < 0) {
                                continue;
                            }

                            final String s = _fileNames[keyInteger];

                            ex1.clear().append("byName").append(s).fetch();
                            if (!ex1.getValue().isDefined()
                                    || ex1.getValue().isNull()) {
                                _result = new TestResult(false,
                                        "Expected filename <" + s
                                                + "> was not found - key="
                                                + ex1.getKey() + " keyInteger="
                                                + keyInteger + " at counter="
                                                + _count);
                                println(_result);
                                forceStop();
                                break;
                            }
                            final long atomic = ex1.getValue().getLong();
                            ex1.remove();
                            ex2.clear().append("byCounter").append(atomic)
                                    .remove();
                            sizeArray[(int) atomic] = 0;
                            _sb1.setLength(0);
                            _sb1.append(s);
                            _sb1.reverse();
                            ex3.clear().append("byReversedName").append(_sb1)
                                    .remove();
                        } catch (final Throwable t) {
                            handleThrowable(t);
                        }
                    }
                }
            }
        }
        verboseln();
        verbose("done");

    }

    public static void main(final String[] args) {
        final Stress3 test = new Stress3();
        test.runStandalone(args);
    }
}
