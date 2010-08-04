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

import java.util.UUID;

import com.persistit.ArgParser;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.test.PersistitTestResult;

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
        println();
        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            println();
            print("Starting cycle " + (_repeat + 1) + " of " + _repeatTotal + "  ");
            final long start = System.nanoTime();
            for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                dot();
                final UUID uuid = UUID.randomUUID();
                final String uuidString = uuid.toString();
                _ex.clear().append(uuidString.substring(0, 5)).append(
                        uuidString.substring(5));
                setupTestValue(_ex, _count, _size);
                try {
                    _ex.store();
                } catch (final Exception e) {
                    handleThrowable(e);
                    break;
                }
            }
            final long end = System.nanoTime();
            println();
            println("  Ending cycle "
                    + (_repeat + 1)
                    + " of "
                    + _repeatTotal
                    + String.format(" cycle elapsed time=%,d msec",
                            (end - start) / 1000000));
        }
        println();
        print("done");
        println();
        _persistit.getJournalManager().setUrgentDemand(true);
        println("Allowing 30sec for copyBack");
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
        }
        _persistit.getJournalManager().setUrgentDemand(false);
    }

    public static void main(final String[] args) throws Exception {
        final StressUUID test = new StressUUID();
        test.runStandalone(args);
    }
}
