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

package com.persistit.unit;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

public class WarmUpPerfTest extends PersistitUnitTestCase {
    private static final int SEED = 42;
    private static final int TREE_COUNT = 5;
    private static final int WRITE_COUNT = 100 * 1024 * 1024;
    private static final int MAX_VALUE_LEN = 1024;

    private Random _keyRandom;
    private Random _valueRandom;
    private Exchange[] _exchanges;
    private Properties _savedProperties;


    @Before
    public void setUp() throws PersistitException {
        _keyRandom = new Random();
        _valueRandom = new Random();
        _exchanges = new Exchange[TREE_COUNT];
        for(int i = 0; i < TREE_COUNT; ++i) {
            String treeName = "tree_" + i;
            _exchanges[i] = _persistit.getExchange(UnitTestProperties.VOLUME_NAME, treeName, true);
        }
    }

    @After
    public void tearDown() {
        _keyRandom = null;
        _valueRandom = null;
        for(Exchange ex : _exchanges) {
            _persistit.releaseExchange(ex);
        }
        _exchanges = null;
    }

    private double doLoad() throws PersistitException {
        // print starting load
        // save start time
        double duration = 0;

        final String LONG_STR = createString(MAX_VALUE_LEN);
        _keyRandom.setSeed(SEED);
        _valueRandom.setSeed(SEED);

        for(int i = 0; i < WRITE_COUNT; ++i) {
            int index = _keyRandom.nextInt(TREE_COUNT);
            Exchange ex = _exchanges[index];

            int id = _keyRandom.nextInt(WRITE_COUNT);
            int strLen = _valueRandom.nextInt(MAX_VALUE_LEN);
            ex.clear().append(id).getValue().put(LONG_STR.substring(0, strLen));
            ex.store();
        }

        // save end time, compute duration
        // print end load
        return duration;
    }

    private double doRandomFetches() throws PersistitException {
        // print starting load
        // save start time
        double duration = 0;

        int emptyFetches = 0;
        _keyRandom.setSeed(SEED);

        for(int i = 0; i < WRITE_COUNT; ++i) {
            int index = _keyRandom.nextInt(TREE_COUNT);
            Exchange ex = _exchanges[index];

            int id = _keyRandom.nextInt(WRITE_COUNT);
            ex.clear().append(id);
            ex.fetch();

            if(!ex.getValue().isDefined()) {
                ++emptyFetches;
            }
        }

        if(emptyFetches == 0) {
            // warn empty fetches
        }

        // save end time, compute duration
        // print end load
        return duration;
    }

    private double doSequentialFetches() throws PersistitException {
        // print starting load
        // save start time
        double duration = 0;

        int[] keysPerEx = new int[TREE_COUNT];

        for(int i = 0; i < TREE_COUNT; ++i) {
            Exchange ex = _exchanges[i];

            int count = 0;
            ex.clear().append(Key.BEFORE);
            while(ex.hasNext()) {
                ++count;
            }

            keysPerEx[i] = count;
        }

        int totalKeys = 0;
        for(int count : keysPerEx) {
            totalKeys += count;
        }

        // if(totalKeys ~= WRITE_COUNT)
        //   warn too few keys

        // save end time, compute duration
        // print end load
        return duration;
    }

    private double doShutdown() throws PersistitException {
        // print starting load
        // save start time
        double duration = 0;

        // See:  crashWithoutFlushAndRestoreProperties();
        assert _savedProperties == null;
        _savedProperties = _persistit.getProperties();
        _persistit.crash();

        // save end time, compute duration
        // print end load
        return duration;
    }

    private double doStartup() throws PersistitException {
        // print starting load
        // save start time
        double duration = 0;

        // See:  crashWithoutFlushAndRestoreProperties();
        assert _savedProperties != null;
        _persistit = new Persistit();
        _persistit.initialize(_savedProperties);
        _savedProperties = null;

        // save end time, compute duration
        // print end load
        return duration;
    }


    // Expectation: no perf change
    @Test
    public void loadOnly() throws PersistitException {
        doLoad();
    }

    // Expectation: no perf change
    @Test
    public void loadShutdown() throws PersistitException {
        doLoad();
        doShutdown();
    }

    // Expectation: no perf change
    @Test
    public void loadRandomFetch() throws PersistitException {
        doLoad();
        doRandomFetches();
    }

    // Expectation: no perf change
    @Test
    public void loadSequentialFetch() throws PersistitException {
        doLoad();
        doSequentialFetches();
    }

    // Expectation: lod same, shutdown same, startup "a little" slower with page cacher, and random fetches "much" faster with page cacher
    @Test
    public void loadShutdownStartupRandomFetch() throws PersistitException {
        doLoad();
        doShutdown();
        doStartup();
        doRandomFetches();
    }

    // Expectation: lod same, shutdown same, startup "a little" slower with page cacher, and random fetches "a little" faster with page cacher
    @Test
    public void loadShutdownStartupSequentialFetch() throws PersistitException {
        doLoad();
        doShutdown();
        doStartup();
        doRandomFetches();
    }
}
