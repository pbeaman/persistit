/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.stress;

import java.util.UUID;

import com.persistit.ArgParser;
import com.persistit.Key;

public class StressUUID extends StressBase {

    private final static String SHORT_DESCRIPTION = "Insert a large number of short records using random keys";

    private final static String LONG_DESCRIPTION = "   Insert a large number of short records using random keys: \r\n";

    private final static String[] ARGS_TEMPLATE = {
            "repeat|int:1:0:1000000000|Number of major loops",
            "count|int:100000:0:1000000|Number of UUID keys to populate per major loop",
            "size|int:30:1:20000|Maximum size of each data value", };

    int _size;

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
        _ap = new ArgParser("com.persistit.Stress1", _args, ARGS_TEMPLATE);
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _dotGranularity = 10000;

        try {
            // Exchange with Thread-private Tree
            _ex = getPersistit().getExchange("persistit",
                    _rootName + _threadIndex, true);
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
        verboseln();
        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            verboseln();
            verbose("Starting cycle " + (_repeat + 1) + " of " + _repeatTotal
                    + "  ");
            final long start = System.nanoTime();
            for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                dot();
                final UUID uuid = UUID.randomUUID();
                final String uuidString = uuid.toString();
                _ex.clear().append(uuidString.substring(0, 5))
                        .append(uuidString.substring(5));
                setupTestValue(_ex, _count, _size);
                try {
                    _ex.store();
                } catch (final Exception e) {
                    handleThrowable(e);
                    break;
                }
            }
            final long end = System.nanoTime();
            verboseln();
            verboseln("  Ending cycle "
                    + (_repeat + 1)
                    + " of "
                    + _repeatTotal
                    + String.format(" cycle elapsed time=%,d msec",
                            (end - start) / 1000000));
        }
        verboseln();
        verbose("done");
        verboseln();
    }

    public static void main(final String[] args) {
        final StressUUID test = new StressUUID();
        test.runStandalone(args);
    }
}
