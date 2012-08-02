/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

import java.util.UUID;

import com.persistit.Key;
import com.persistit.util.ArgParser;

public class StressUUID extends StressBase {

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Number of major loops",
            "count|int:100000:0:1000000|Number of UUID keys to populate per major loop",
            "size|int:30:1:20000|Maximum size of each data value", };

    int _size;

    public StressUUID(String argsString) {
        super(argsString);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _ap = new ArgParser("com.persistit.Stress1", _args, ARGS_TEMPLATE).strict();
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit", _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    @Override
    public void executeTest() {

        setPhase("@");
        try {
            _ex.clear().remove(Key.GTEQ);
        } catch (final Exception e) {
            handleThrowable(e);
        }
        setPhase("w");
        for (_repeat = 0; (_repeat < _repeatTotal || isUntilStopped()) && !isStopped(); _repeat++) {
            final long start = System.nanoTime();
            for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                final UUID uuid = UUID.randomUUID();
                final String uuidString = uuid.toString();
                _ex.clear().append(uuidString.substring(0, 5)).append(uuidString.substring(5));
                setupTestValue(_ex, _count, _size);
                try {
                    _ex.store();
                    addWork(1);
                } catch (final Exception e) {
                    handleThrowable(e);
                    break;
                }
            }
            final long end = System.nanoTime();
        }
    }

}
