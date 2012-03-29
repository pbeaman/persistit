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

import java.util.UUID;

import com.persistit.Key;
import com.persistit.util.ArgParser;

public class StressUUID extends StressBase {

    private final static String SHORT_DESCRIPTION = "Insert a large number of short records using random keys";

    private final static String LONG_DESCRIPTION = "   Insert a large number of short records using random keys: \r\n";

    private final static String[] ARGS_TEMPLATE = { "repeat|int:1:0:1000000000|Number of major loops",
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
        verboseln();
        for (_repeat = 0; (_repeat < _repeatTotal) && !isStopped(); _repeat++) {
            verboseln();
            verbose("Starting cycle " + (_repeat + 1) + " of " + _repeatTotal + "  ");
            final long start = System.nanoTime();
            for (_count = 0; (_count < _total) && !isStopped(); _count++) {
                dot();
                final UUID uuid = UUID.randomUUID();
                final String uuidString = uuid.toString();
                _ex.clear().append(uuidString.substring(0, 5)).append(uuidString.substring(5));
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
            verboseln("  Ending cycle " + (_repeat + 1) + " of " + _repeatTotal
                    + String.format(" cycle elapsed time=%,d msec", (end - start) / 1000000));
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
