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

package com.persistit;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

public class WarmUpPerfTest extends PersistitUnitTestCase {
    private static final int SEED = 42;
    private static final int TREE_COUNT = 5;
    private static final int WRITE_COUNT = 100 * 1024;
    private static final int MAX_VALUE_LEN = 1024;

    private Random _keyRandom;
    private Random _valueRandom;
    private Exchange[] _exchanges;
    private Properties _savedProperties;

    private void setupExchanges() throws PersistitException {
        _exchanges = new Exchange[TREE_COUNT];
        for(int i = 0; i < TREE_COUNT; ++i) {
            String treeName = "tree_" + i;
            _exchanges[i] = _persistit.getExchange(UnitTestProperties.VOLUME_NAME, treeName, true);
        }
    }
    
    @Override
    public void setUp() throws Exception {
        //WARMUP_ON = false;
        
        super.setUp();
        _keyRandom = new Random();
        _valueRandom = new Random();
        setupExchanges();
    }

    @Override
    public void tearDown() throws Exception {
        _keyRandom = null;
        _valueRandom = null;
        for(Exchange ex : _exchanges) {
            _persistit.releaseExchange(ex);
        }
        _exchanges = null;
        super.tearDown();
    }

    private double doLoad() throws PersistitException {
        System.out.println("Start doLoad...");
        long start = System.nanoTime();

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

        if (WARMUP_ON) populateWarmupFile();
        
        double duration = nanoToSec(System.nanoTime() - start);
        System.out.printf("doLoad took %fs%n", duration);
        return duration;
    }

    private double doRandomFetches() throws PersistitException {
        System.out.println("Start doRandomFetches...");
        long start = System.nanoTime();

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
            System.out.println("Empty fetch performed");
        }

        double duration = nanoToSec(System.nanoTime() - start);
        System.out.printf("doRandomFetches took %fs%n", duration);
        return duration;
    }

    private double doSequentialFetches() throws PersistitException {
        System.out.println("Start doSequentialFetches...");
        long start = System.nanoTime();

        int[] keysPerEx = new int[TREE_COUNT];

        for(int i = 0; i < TREE_COUNT; ++i) {
            Exchange ex = _exchanges[i];

            int count = 0;
            ex.clear().append(Key.BEFORE);
            while(ex.next()) {
                ++count;
            }

            keysPerEx[i] = count;
        }

        int totalKeys = 0;
        for(int count : keysPerEx) {
            totalKeys += count;
        }
        
        if (totalKeys != WRITE_COUNT) {
            System.out.println("Too few keys");
        }

        double duration = nanoToSec(System.nanoTime() - start);
        System.out.printf("doSequentialFetches took %fs%n", duration);
        return duration;
    }

    private double doShutdown() throws PersistitException {
        System.out.println("Start doShutdown...");
        long start = System.nanoTime();

        // See:  crashWithoutFlushAndRestoreProperties();
        assert _savedProperties == null;
        _savedProperties = _persistit.getProperties();
        _persistit.crash();

        double duration = nanoToSec(System.nanoTime() - start);
        System.out.printf("doShutdown took %fs%n", duration);
        return duration;
    }

    private double doStartup() throws PersistitException {
        System.out.println("Start doStartup...");
        long start = System.nanoTime();

        // See:  crashWithoutFlushAndRestoreProperties();
        assert _savedProperties != null;
        _persistit = new Persistit();
        _persistit.initialize(_savedProperties);
        _savedProperties = null;

        setupExchanges();
        
        double duration = nanoToSec(System.nanoTime() - start);
        System.out.printf("doStartup took %fs%n", duration);
        return duration;
    }
    
    private void populateWarmupFile() throws PersistitException {           
        for (BufferPool p: _persistit.getBufferPoolHashMap().values()) {
            p.populateWarmupFile();
        }
    }
    
    private double getPercent(double total, double num) {
        return num / total * 100;
    }
    
    private double nanoToSec(double nano) {
        return nano / 1000000000;
    }


    // Expectation: no perf change
    @Test
    public void loadOnly() throws PersistitException {
        double t = doLoad();
        System.out.printf("Total time %fs %n", nanoToSec(t));
    }

    // Expectation: no perf change
    @Test
    public void loadShutdown() throws PersistitException {
        double load = doLoad();
        double shutdown = doShutdown();
        double t = load + shutdown;
        System.out.printf("Total time %fs\t load took %f%% \t shutdown took %f%% %n",
                t, getPercent(t, load), getPercent(t, shutdown));
    }

    // Expectation: no perf change
    @Test
    public void loadRandomFetch() throws PersistitException {
        double load = doLoad();
        double random = doRandomFetches();
        double t = load + random;
        System.out.printf("Total time %fs\t load took %f%% \t random took %f%% %n",
                t, getPercent(t, load), getPercent(t, random));
    }

    // Expectation: no perf change
    @Test
    public void loadSequentialFetch() throws PersistitException {
        double load = doLoad();
        double seq = doSequentialFetches();
        double t = load + seq;
        System.out.printf("Total time %fs\t load took %f%% \t sequential took %f%% %n",
                t, getPercent(t, load), getPercent(t, seq));
    }

    // Expectation: lod same, shutdown same, startup "a little" slower with page cacher, and random fetches "much" faster with page cacher
    @Test
    public void loadShutdownStartupRandomFetch() throws PersistitException {
        double load = doLoad();
        double shutdown = doShutdown();
        double startup = doStartup();
        double random = doRandomFetches();
        double t = load + shutdown + startup + random;
        System.out.printf("Total time %fs\t load took %f%% \t shutdown took %f%% \t startup took %f%% \t "
                + "random took %f%% %n", t, getPercent(t, load), getPercent(t, shutdown),
                getPercent(t, startup), getPercent(t, random));
    }

    // Expectation: load same, shutdown same, startup "a little" slower with page cacher, and random fetches "a little" faster with page cacher
    @Test
    public void loadShutdownStartupSequentialFetch() throws PersistitException {
        double load = doLoad();
        double shutdown = doShutdown();
        double startup = doStartup();
        double seq = doSequentialFetches();
        double t = load + shutdown + startup + seq;
        System.out.printf("Total time %fs\t load took %f%% \t shutdown took %f%% \t startup took %f%% \t "
                + "sequential took %f%% %n", t, getPercent(t, load), getPercent(t, shutdown),
                getPercent(t, startup), getPercent(t, seq));
    }
    
    public static void main(String[] args) throws Exception {
        new WarmUpPerfTest().initAndRunTest();
    }
    
    @Override
    public void runAllTests() throws Exception {
        //loadOnly();
        //loadShutdown();
        loadRandomFetch();
        //loadSequentialFetch();
        //loadShutdownStartupRandomFetch();
        //loadShutdownStartupSequentialFetch();
    }
}
