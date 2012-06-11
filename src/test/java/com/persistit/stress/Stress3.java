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

package com.persistit.stress;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.suite.TestResult;
import com.persistit.util.ArgParser;

public class Stress3 extends StressBase {

    private final static String DEFAULT_PATH = "../../";

    private final static String[] ARGS_TEMPLATE = { "op|String:wrd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions", "count|int:10000:0:1000000000|Number of nodes to populate",
            "seed|int:1:1:20000|Random seed", "size|int:3000:1:20000|Maximum record size",
            "path|String:" + DEFAULT_PATH, };

    static String[] _fileNames = null;
    static boolean _filesLoaded;
    final static AtomicLong _counter = new AtomicLong(0);

    int _seed;
    int _size;
    String _opflags;

    public Stress3(String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress3", _args, ARGS_TEMPLATE);
        _opflags = _ap.getStringValue("op");
        _seed = _ap.getIntValue("seed");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _counter.set(0);

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }

        initialize(_ap.getStringValue("path"));
    }

    /**
     * Performed by only one thread.
     */
    public synchronized static void initialize(final String fileName) {
        if (_filesLoaded) {
            return;
        }
        final List<String> list = new ArrayList<String>(50000);
        try {
            initializeDir(new File(fileName), list);
            _fileNames = list.toArray(new String[0]);
        } catch (final Exception e) {
            _fileNames = new String[] { "foo", "bar" };
        }
        _filesLoaded = true;
    }

    private static void initializeDir(final File file, final List<String> list) {
        list.add(file.getPath());
        if (file.isDirectory()) {
            final File[] files = file.listFiles();
            for (final File child : files) {
                if (list.size() > 50000) {
                    break;
                }
                initializeDir(child, list);
            }
        }
    }

    /**
     * Performed once by each newly started thread.
     */
    @Override
    public void executeTest() {
        final int[] sizeArray = new int[_total * 100];

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

        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                final Exchange ex1 = new Exchange(_ex);
                final Exchange ex2 = new Exchange(_ex);
                final Exchange ex3 = new Exchange(_ex);

                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    try {
                        final int keyInteger = randomSeq[_count];

                        final String s = _fileNames[keyInteger];

                        long atomic;

                        ex1.clear().append("byName").append(s).fetch();
                        addWork(1);

                        if (!ex1.getValue().isDefined() || ex1.getValue().isNull()) {
                            atomic = _counter.incrementAndGet();
                            ex1.getValue().put(atomic);
                            ex1.store();
                            addWork(1);

                        } else {
                            atomic = ex1.getValue().getLong();
                        }

                        setupTestValue(ex2, _count, random(30, _size));
                        if (atomic < sizeArray.length) {
                            sizeArray[(int) atomic] = ex2.getValue().getEncodedSize();
                        }
                        ex2.clear().append("byCounter").append(atomic).store();
                        addWork(1);

                        _sb1.setLength(0);
                        _sb1.append(s);
                        _sb1.reverse();

                        ex3.getValue().put(atomic);
                        ex3.clear().append("byReversedName").append(_sb1).store();
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
                    try {
                        final int keyInteger = randomSeq[_count];

                        final String s = _fileNames[keyInteger];

                        ex1.clear().append("byName").append(s).fetch();
                        addWork(1);

                        if (!ex1.getValue().isDefined() || ex1.getValue().isNull()) {
                            throw new RuntimeException("Expected filename <" + s + "> was not found - key="
                                    + ex1.getKey());
                        }
                        final long atomic = ex1.getValue().getLong();

                        setupTestValue(ex2, _count, random(30, _size));
                        ex2.clear().append("byCounter").append(atomic).fetch(value2);
                        addWork(1);


                        if (!value2.isDefined() || value2.isNull()) {
                            throw new RuntimeException("Expected value for byCounter " + atomic
                                    + " was not found - key=" + ex2.getKey());
                        }
                        final int size2 = value2.getEncodedSize();
                        if (atomic < sizeArray.length) {
                            final int size1 = sizeArray[(int) atomic];
                            if (size1 != size2) {
                                throw new RuntimeException("Value is size " + size2 + ", should be " + size1 + " key="
                                        + ex2.getKey());
                            }
                        }

                        _sb1.setLength(0);
                        _sb1.append(s);
                        _sb1.reverse();
                        ex3.clear().append("byReversedName").append(_sb1).fetch();
                        addWork(1);

                        if (!ex3.getValue().isDefined() || ex3.getValue().isNull()
                                || (ex3.getValue().getLong() != atomic)) {
                            throw new RuntimeException("Missing or incorrect value " + ex3.getValue() + " should be "
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
                        addWork(4);

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
                        try {
                            final int keyInteger = randomUniqueSeq[_count];
                            if (keyInteger < 0) {
                                continue;
                            }

                            final String s = _fileNames[keyInteger];

                            ex1.clear().append("byName").append(s).fetch();
                            addWork(1);

                            if (!ex1.getValue().isDefined() || ex1.getValue().isNull()) {
                                _result = new TestResult(false, "Expected filename <" + s + "> was not found - key="
                                        + ex1.getKey() + " keyInteger=" + keyInteger + " at counter=" + _count);
                                forceStop();
                                break;
                            }
                            final long atomic = ex1.getValue().getLong();
                            ex1.remove();
                            addWork(1);

                            ex2.clear().append("byCounter").append(atomic).remove();
                            addWork(1);

                            if (atomic < sizeArray.length) {
                                sizeArray[(int) atomic] = 0;
                            }
                            _sb1.setLength(0);
                            _sb1.append(s);
                            _sb1.reverse();
                            ex3.clear().append("byReversedName").append(_sb1).remove();
                            addWork(1);

                        } catch (final Throwable t) {
                            handleThrowable(t);
                        }
                    }
                }
            }
        }
    }

}
