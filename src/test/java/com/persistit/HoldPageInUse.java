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
 * Created on Apr 6, 2004
 */
package com.persistit;

import com.persistit.exception.PersistitException;
import com.persistit.stress.StressBase;
import com.persistit.unit.UnitTestProperties;

/**
 * 
 */
public class HoldPageInUse extends StressBase {
    private final static String SHORT_DESCRIPTION =
        "Special-purpose: holds a page in-use for a period of time";

    private final static String LONG_DESCRIPTION =
        "This test is used to induce and test InUseExceptions";

    private final static String[] ARGS_TEMPLATE =
        {
            "repeat|int:1:0:1000000000|Repetitions",
            "delay|int:60:0:1000000000|Delay before claim (in sec)",
            "page|int:2:0:20000|Address of page to claim",
            "hold|int:300:1:100000000|Time to hold page for in each rep (in sec)", };

    int _delay;

    int _page;

    int _hold;

    @Override
    public String shortDescription() {
        return SHORT_DESCRIPTION;
    }

    @Override
    public String longDescription() {
        return LONG_DESCRIPTION;
    }

    @Override
    public void setupTest(final String[] args) {
        _ap = new ArgParser("com.persistit.Stress1", args, ARGS_TEMPLATE);
        _hold = _ap.getIntValue("hold");
        _page = _ap.getIntValue("page");
        _repeatTotal = _ap.getIntValue("repeat");
        _delay = _ap.getIntValue("delay");
        _dotGranularity = 10000;

        try {
            // Exchange with Thread-private Tree
            _ex =
                _persistit.getExchange("persistit", _rootName + _threadIndex,
                    true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    public void test1() throws PersistitException {
        System.out.print("test1 ");

        final Volume volume = _ex.getVolume();
        final BufferPool pool = volume.getPool();
        for (_repeat = 0; _repeat < _repeatTotal; _repeat++) {
            println("delay...");
            try {
                Thread.sleep(_delay * 1000);
            } catch (final InterruptedException ie) {
            }

            print("get page " + _page + " in volume " + volume);
            final Buffer buffer = pool.get(volume, _page, true, true);
            println(" - " + buffer);
            try {
                println("sleeping...");
                Thread.sleep(_hold * 1000);
            } catch (final InterruptedException ie) {
            } finally {
                println("releasing page " + buffer);
                buffer.release();
            }
        }
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        runTest(args);
    }

    public static void runTest(final String[] args) throws Exception {
        final HoldPageInUse tc = new HoldPageInUse();
        tc.runTest();
    }

    @Override
    public void runTest() {
        Exception exception = null;
        try {
            _persistit.initialize(UnitTestProperties.getBiggerProperties());
            test1();
        } catch (final Exception e) {
            exception = e;
            handleThrowable(e);
        } finally {
            try {
                _persistit.close();
            } catch (final PersistitException e) {
                if (exception == null) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
