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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import com.persistit.PersistitMap;
import com.persistit.Value;
import com.persistit.stress.TestResult;
import com.persistit.util.ArgParser;

public class PersistitMapStress2 extends StressBase {

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
    
    public PersistitMapStress2(String argsString) {
        super(argsString);
    }
    
    @Override
    public void setUp() {
        _ap = new ArgParser("com.persistit.stress.PersistitMapStress2", _args, ARGS_TEMPLATE);
        _splay = _ap.getIntValue("splay");
        _opflags = _ap.getStringValue("op");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");

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
                addWork(1);

            } catch (final Exception e) {
                handleThrowable(e);
            }
        }

        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {

            long ts = ts();
            long tt;

            if (_opflags.indexOf('w') >= 0) {
                setPhase("w");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);

                    final Object o1 = testObject(_count, _size);
                    try {
                        _dm1.put(new Integer(keyInteger), o1);
                        addWork(1);

                    } catch (final Exception e) {
                        handleThrowable(e);
                        break;
                    }
                }
                if (_dm1.size() != _total) {
                    _result = new TestResult(false, "PersistitMap.size()=" + _dm1.size() + " out of " + _total
                            + " repetition=" + _repeat + " in thread=" + _threadIndex);
                    forceStop();
                    break;
                }
                _timeWrite += (tt = ts()) - ts;
                ts = tt;

            }

            if (_opflags.indexOf('r') >= 0) {
                setPhase("r");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);
                    final Object o1 = testObject(_count, _size);

                    try {
                        final Object o2 = _dm1.get(new Integer(keyInteger));
                        addWork(1);

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
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                        addWork(1);

                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_count != _total) {
                    _result = new TestResult(false, "Traverse count=" + _count + " out of " + _total + " repetition="
                            + _repeat + " in thread=" + _threadIndex);
                    forceStop();
                    break;
                }
                _timeIter += (tt = ts()) - ts;
                ts = tt;
            }

            if (_opflags.indexOf('d') >= 0) {
                setPhase("d");
                for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                    final int keyInteger = keyInteger(_count);
                    final Object o1 = testObject(_count, _size);
                    try {
                        final Object o2 = _dm1.remove(new Integer(keyInteger));
                        addWork(1);

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
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                        addWork(1);

                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_count != 0) {
                    _result = new TestResult(false, "Traverse count=" + _count + " when 0 were expected"
                            + " repetition=" + _repeat + " in thread=" + _threadIndex);
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
                    try {
                        if (!itr.hasNext()) {
                            break;
                        }
                        itr.next();
                        itr.remove();
                        addWork(1);

                    } catch (final Exception e) {
                        handleThrowable(e);
                    }
                }
                if (_dm1.size() != 0) {
                    _result = new TestResult(false, "PersistitMap.size()= " + _dm1.size() + " when 0 were expected"
                            + " repetition=" + _repeat + " in thread=" + _threadIndex);
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
        System.out.println("Object o1 != o2:");
        System.out.println("  o1=" + o1);
        System.out.println("  o2=" + o2);
        return false;
    }
}
