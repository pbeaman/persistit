/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.stress.unit;

import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.Value;
import com.persistit.exception.RollbackException;
import com.persistit.stress.TestResult;
import com.persistit.util.ArgParser;

public class Stress3txn extends StressBase {

    private final static String DEFAULT_DATA_FILE_NAME = "src/test/resources/test3_data.txt";

    private final static String[] ARGS_TEMPLATE = { "op|String:wrd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions", "count|int:10000:0:1000000000|Number of nodes to populate",
            "seed|int:1:1:20000|Random seed", "size|int:3000:1:20000|Maximum record size", "splay|int:1:1:1000|Splay",
            "datafile|String:" + DEFAULT_DATA_FILE_NAME, };

    static String[] _fileNames = null;
    static boolean _filesLoaded;
    final static AtomicLong _counter = new AtomicLong(0);

    int _splay;
    int _seed;
    int _size;
    String _opflags;

    public Stress3txn(String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress3txn", _args, ARGS_TEMPLATE).strict();
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _seed = _ap.getIntValue("seed");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
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
        Stress3.initialize(fileName);
        _fileNames = Stress3._fileNames;
        _filesLoaded = Stress3._filesLoaded;
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

        final Exchange ex1 = new Exchange(_ex);
        final Exchange ex2 = new Exchange(_ex);
        final Exchange ex3 = new Exchange(_ex);

        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {

            final Transaction txn = ex1.getTransaction();
            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");

                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    try {
                        final int keyInteger = randomSeq[_count];

                        final String s = _fileNames[keyInteger];

                        long atomic;

                        int retries = 100;

                        for (;;) {
                            txn.begin();
                            try {
                                ex1.clear().append("byName").append(s).fetch();
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
                                addWork(1);

                                txn.commit();
                                break;
                            } catch (final RollbackException rbe) {
                                if (--retries <= 0) {
                                    throw rbe;
                                }
                            } finally {
                                txn.end();
                            }
                        }

                    } catch (final Throwable t) {
                        handleThrowable(t);
                    }

                }
            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                final Value value2 = new Value(getPersistit());
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    try {
                        final int keyInteger = randomSeq[_count];
                        if (keyInteger < 0) {
                            continue;
                        }

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
                        ex1.clear().append("byName").remove(Key.GTEQ);
                        ex2.clear().append("byCounter").remove(Key.GTEQ);
                        ex3.clear().append("byReversedName").remove(Key.GTEQ);
                        ex1.clear().append("counter").remove();
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

                    for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                        try {
                            final int keyInteger = randomUniqueSeq[_count];
                            if (keyInteger < 0) {
                                continue;
                            }

                            int retries = 100;
                            for (;;) {
                                txn.begin();
                                try {
                                    final String s = _fileNames[keyInteger];

                                    ex1.clear().append("byName").append(s).fetch();
                                    addWork(1);

                                    if (!ex1.getValue().isDefined() || ex1.getValue().isNull()) {
                                        fail("Expected filename <" + s
                                                + "> was not found - key=" + ex1.getKey() + " keyInteger=" + keyInteger
                                                + " at counter=" + _count);
                                        break;
                                    }
                                    final long atomic = ex1.getValue().getLong();
                                    ex1.remove();
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

                                    txn.commit();
                                    break;
                                } catch (final RollbackException rbe) {
                                    if (--retries <= 0) {
                                        throw rbe;
                                    }
                                } finally {
                                    txn.end();
                                }
                            }
                        } catch (final Throwable t) {
                            handleThrowable(t);
                        }
                    }
                }
            }
        }
    }

}
