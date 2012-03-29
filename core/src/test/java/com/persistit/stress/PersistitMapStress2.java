/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit.stress;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import com.persistit.PersistitMap;
import com.persistit.Value;
import com.persistit.test.TestResult;
import com.persistit.util.ArgParser;

public class PersistitMapStress2 extends StressBase {

    private final static String SHORT_DESCRIPTION = "PersistitMap test - write/read/delete/traverse loops using serialized values";

    private final static String LONG_DESCRIPTION = "   Stress test that perform <repeat> iterations of the following: \r\n"
            + "    - insert <count> sequentially ascending keys \r\n"
            + "    - read and verify <count> sequentially ascending key/value pairs\r\n"
            + "    - traverse and count all keys using next() \r\n"
            + "    - delete <count> sequentially ascending keys \r\n"
            + "   Optional <splay> value allows variations in key sequence: \r\n";

    private final static String[] ARGS_TEMPLATE = { "op|String:Cwrtd|Operations to perform",
            "repeat|int:1:0:1000000000|Repetitions", "count|int:10000:0:1000000000|Number of nodes to populate",
            "size|int:30:1:20000|Size of each data value", "splay|int:1:1:1000|Splay",
            "_flag|t|Timing test on TreeMap instead of PersistitMap", };

    int _size;
    int _splay;
    String _opflags;

    SortedMap _dm1;
    SortedMap _dm2;

    long _timeWrite;
    long _timeRead;
    long _timeIter;
    long _timeRemove;

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public void setUp() {
        _ap = new ArgParser("com.persistit.stress.PersistitMapStress2", _args, ARGS_TEMPLATE);
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _dotGranularity = 10000;

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }

        _ex.to("dmtest").append(_rootName + _threadIndex);

        if (_ap.isFlag('t')) {
            _dm1 = new TreeMap();
            _dm2 = new PersistitMap(_ex);
        } else {
            _dm1 = new PersistitMap(_ex);
            _dm2 = new TreeMap();
        }
    }

    private long ts() {
        return System.currentTimeMillis();
    }

    @Override
    public void executeTest() {
        final Value value1 = _ex.getValue();
        final Value value2 = new Value(getPersistit());

        if (_opflags.indexOf('C') >= 0) {
            setPhase("C");
            try {
                _dm1.clear();
            } catch (final Exception e) {
                handleThrowable(e);
            }
            verboseln();
        }

        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            verboseln();
            verboseln("Starting cycle " + (_repeat + 1) + " of " + _repeatTotal);

            long ts = ts();
            long tt;

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final int keyInteger = keyInteger(_count);

                    final Object o1 = testObject(_count, _size);
                    try {
                        _dm1.put(new Integer(keyInteger), o1);
                    } catch (final Exception e) {
                        handleThrowable(e);
                        break;
                    }
                }
                if (_dm1.size() != _total) {
                    _result = new TestResult(false, "PersistitMap.size()=" + _dm1.size() + " out of " + _total
                            + " repetition=" + _repeat + " in thread=" + _threadIndex);
                    println(_result);
                    forceStop();
                    break;
                }
                _timeWrite += (tt = ts()) - ts;
                ts = tt;

            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final int keyInteger = keyInteger(_count);
                    final Object o1 = testObject(_count, _size);

                    try {
                        final Object o2 = _dm1.get(new Integer(keyInteger));
                        compareObjects(o1, o2);
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                _timeRead += (tt = ts()) - ts;
                ts = tt;
            }

            if (_opflags.indexOf('t') >= 0) {
                setPhase("t");
                final Iterator itr = _dm1.keySet().iterator();

                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    dot();
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_count != _total) {
                    _result = new TestResult(false, "Traverse count=" + _count + " out of " + _total + " repetition="
                            + _repeat + " in thread=" + _threadIndex);
                    println(_result);
                    forceStop();
                    break;
                }
                _timeIter += (tt = ts()) - ts;
                ts = tt;
            }

            if (_opflags.indexOf('d') >= 0) {
                setPhase("d");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    dot();
                    final int keyInteger = keyInteger(_count);
                    final Object o1 = testObject(_count, _size);
                    try {
                        final Object o2 = _dm1.remove(new Integer(keyInteger));
                        compareObjects(o1, o2);
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }

                //
                // Now verify that the interator has no members.
                //
                final Iterator itr = _dm1.keySet().iterator();

                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    dot();
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_count != 0) {
                    _result = new TestResult(false, "Traverse count=" + _count + " when 0 were expected"
                            + " repetition=" + _repeat + " in thread=" + _threadIndex);
                    println(_result);
                    forceStop();
                    break;
                }
                _timeRemove += (tt = ts()) - ts;
                ts = tt;
            }

            if (_opflags.indexOf('D') >= 0) {
                setPhase("D");
                //
                // Now verify that the interator has no members.
                //
                final Iterator itr = _dm1.keySet().iterator();

                for (_count = 0; (_count < (_total * 10)) && !isStopped(); _count++) {
                    dot();
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                        itr.remove();
                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_dm1.size() != 0) {
                    _result = new TestResult(false, "PersistitMap.size()= " + _dm1.size() + " when 0 were expected"
                            + " repetition=" + _repeat + " in thread=" + _threadIndex);
                    println(_result);
                    forceStop();
                    break;
                }
                _timeRemove += (tt = ts()) - ts;
                ts = tt;
            }

            if ((_opflags.indexOf('h') > 0) && !isStopped()) {
                setPhase("h");
                try {
                    Thread.sleep(1000);
                } catch (final Exception e) {
                }
            }

        }
        verboseln();
        verboseln(" timeWrite=" + _timeWrite + " timeRead=" + _timeRead + " timeIter=" + _timeIter + " timeRemove="
                + _timeRemove + " total=" + (_timeWrite + _timeRead + _timeIter + _timeRemove));
        verbose("done");
    }

    int keyInteger(final int counter) {
        int keyInteger = (counter * _splay) % _total;
        if (keyInteger < 0) {
            keyInteger += _total;
        }
        return keyInteger;
    }

    public Object testObject(final int count, final int size) {
        final ArrayList list = new ArrayList();
        for (int i = 0; i < size; i++) {
            list.add("ArrayList element " + i);
        }
        return list;
    }

    public boolean compareObjects(final Object o1, final Object o2) {
        if ((o1 == null) && (o2 == null)) {
            return true;
        }
        if ((o1 != null) && o1.equals(o2)) {
            return true;
        }
        println("Object o1 != o2:");
        println("  o1=" + o1);
        println("  o2=" + o2);
        return false;
    }

    public static void main(final String[] args) {
        final PersistitMapStress2 test = new PersistitMapStress2();
        test.runStandalone(args);
    }
}
