/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    public StressUUID(final String argsString) {
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
