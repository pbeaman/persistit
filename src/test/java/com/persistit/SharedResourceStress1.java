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

package com.persistit;

import com.persistit.exception.InUseException;
import com.persistit.exception.PersistitException;
import com.persistit.stress.StressBase;

public class SharedResourceStress1 extends StressBase {

    private final static String SHORT_DESCRIPTION =
        "SharedResource Stress Test";

    private final static String LONG_DESCRIPTION =
        "   Simple stress test in which multiple threads compete for"
            + "   SharedResource instances (Buffers).";

    private final static String[] ARGS_TEMPLATE =
        { "op|String:wrtd|Operations to perform",
            "size|int:10:1:10000|Number of resources (Buffers) to compete for",
            "repeat|int:1:0:1000000000|Repetitions",
            "seed|int:1:1:20000|Random seed",
            "pdelay|int:1:0:100|Probability of delay in any iteration",
            "pexclusive|int:25:0:100|Probability of exclusive claim",
            "count|int:10000:0:1000000000|Number of nodes to populate", };

    int _size;
    String _opflags;
    int _seed;
    int _pdelay;
    int _pexclusive;

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
        _opflags = _ap.getStringValue("op");
        _size = _ap.getIntValue("size");
        _repeatTotal = _ap.getIntValue("repeat");
        _total = _ap.getIntValue("count");
        _seed = _ap.getIntValue("seed");
        _pdelay = _ap.getIntValue("pdelay");
        _pexclusive = _ap.getIntValue("pexclusive");
        _dotGranularity = 10000;

        try {
            // Exchange with Thread-private Tree
            _ex = _persistit.getExchange("persistit", _rootName + _threadIndex, true);
        } catch (final Exception ex) {
            handleThrowable(ex);
        }
    }

    @Override
    public void runTest() {
        final BufferPool pool = _ex.getVolume().getPool();

        println();

        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            println();
            println("Starting cycle " + (_repeat + 1) + " of " + _repeatTotal);

            _random.setSeed(_seed);
            for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                dot();
                final int pageAddr = random(0, _size);
                final boolean exclusive = random(0, 100) < _pexclusive;
                final boolean delay = random(0, 100) < _pdelay;
                Buffer buffer0 = null;
                Buffer buffer1 = null;
                try {
                    buffer0 =
                        pool.get(_ex.getVolume(), 0, pageAddr == 0 ? exclusive
                            : false, false);
                    buffer1 =
                        pool.get(_ex.getVolume(), pageAddr, exclusive, false);
                    if (delay) {
                        Thread.sleep(1);
                    }
                } catch (final InUseException iue) {
                    handleThrowable(iue);
                } catch (final PersistitException pe) {
                    handleThrowable(pe);
                } catch (final InterruptedException ie) {
                    handleThrowable(ie);
                } finally {
                    if (buffer1 != null) {
                        pool.release(buffer1);
                    }
                    if (buffer0 != null) {
                        pool.release(buffer0);
                    }
                    buffer1 = null;
                    buffer0 = null;
                }
            }
        }

        println();
        print("done");
    }

    public static void main(final String[] args) throws Exception {
        final SharedResourceStress1 test = new SharedResourceStress1();
        test.runStandalone(args);
    }
}
