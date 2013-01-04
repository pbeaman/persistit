/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import java.util.Random;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Tree;
import com.persistit.TreeBuilder;
import com.persistit.Value;
import com.persistit.stress.AbstractStressTest;
import com.persistit.util.ArgParser;
import com.persistit.util.Util;

/**
 * <p>
 * Simulate loading a large set (e.g., 500M) of records with large random keys.
 * Because straightforward insertion results in highly randomized page access
 * after the database size has exceed the amount of buffer pool memory space,
 * this demo creates smaller sorted sets of keys and then merges them to create
 * the final Tree in sequential order. As a side-effect, the final tree is also
 * physically coherent in that the logical and physical order of keys disk are
 * closely aligned.
 * </p>
 * <p>
 * This class can be run stand-alone through its static main method, or within
 * the stress test suite.
 * </p>
 * 
 * @author peter
 */
public class BigLoad extends AbstractStressTest {

    private static final Random RANDOM = new Random();
    private final TreeBuilder tb;
    
    private int totalRecords;

    public BigLoad(final TreeBuilder tb, final int totalRecords, final int buckets) {
        super("");
        this.totalRecords = totalRecords;
        this.tb = tb;
    }

    public long load(final Persistit db) throws Exception {
        final long startLoadTime = System.nanoTime();
        
        final Exchange resultExchange = db.getExchange("persistit", "sorted", true);
        System.out.printf("Loading %,d records\n", totalRecords);

        for (int i = 0; i < totalRecords; i++) {
            resultExchange.clear().append(randomKey());
            tb.store(resultExchange);
        }
        final long endLoadTime = System.nanoTime();
        System.out.printf("Loaded %,d records into %,d buckets in %,dms\n", totalRecords, tb.getSortVolumeCount(),
                (endLoadTime - startLoadTime) / Util.NS_PER_MS);
        return endLoadTime - startLoadTime;
    }

    public long merge(final Persistit db) throws Exception {
        final long  startMergeTime = System.nanoTime();
    
        System.out.printf("Merging %,d records into main database\n", totalRecords);

        tb.merge();
        final long endMergeTime = System.nanoTime();
        System.out.printf("Merged %,d records in %,dms\n", totalRecords, (endMergeTime - startMergeTime) / Util.NS_PER_MS);

        return endMergeTime - startMergeTime;
    }
    
    public long count(final Persistit db) throws Exception {
        final long startCountTime = System.nanoTime();
        System.out.printf("Counting keys in main database (100M keys per dot) ");
        final Exchange resultExchange = db.getExchange("persistit", "sorted", false);
        resultExchange.clear().append(Key.BEFORE);
        long count = 0;
        while (resultExchange.next()) {
            count++;
            if ((count % 100000000) == 0) {
                System.out.print(".");
                System.out.flush();
            }
        }
        final long endCountTime = System.nanoTime();
        System.out.printf("\nCounted %,d keys in the main database in %,dms\n", count, (endCountTime - startCountTime)
                / Util.NS_PER_MS);
        return endCountTime - startCountTime;
    }

    final StringBuilder sb = new StringBuilder(
            "00000000000000000000xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

    private String randomKey() {
        long n = RANDOM.nextLong() & Long.MAX_VALUE;
        for (int i = 20; --i >= 0;) {
            sb.setCharAt(i, (char) ((n % 10) + '0'));
            n /= 10;
        }
        return sb.toString();
    }

    /**
     * Arguments:
     * 
     * records - total records to load
     * 
     * buckets - number of subdivisions to create while loading
     * 
     * propertiesPath - path to properties file for Persistit initialization
     * 
     * @param args
     * @throws Exception
     */

    public static void main(final String[] args) throws Exception {
        final int records = args.length > 0 ? Integer.parseInt(args[0]) : 1000000;
        final int buckets = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        final Persistit db = new Persistit();
        if (args.length > 2) {
            db.setPropertiesFromFile(args[2]);
            db.initialize();
        } else {
            db.initialize();
        }
        final BigLoad bl = new BigLoad(new BigLoadTreeBuilder(db), records, buckets);
        try {
            bl.load(db);
            bl.merge(db);
            bl.clone();
        } finally {
            db.close();
        }
    }
    
    public static class BigLoadTreeBuilder extends TreeBuilder  {
        
        public BigLoadTreeBuilder(final Persistit db) {
            super(db);
        }
        
        @Override
        protected void reportSorted(final long count) {
            System.out.printf("Sorted %,15d records\n", count);
        }

        @Override
        protected void reportMerged(final long count) {
            System.out.printf("Merged %,15d records\n", count);
        }

        @Override
        protected boolean duplicateKeyDetected(final Tree tree, final Key key, final Value v1, final Value v2) {
            System.out.println("Duplicate  key detected: " + key);
            return true;
        }
    }

    /*
     * ----------------------------------------------------------------------
     * 
     * Stuff below this line is required to run within the stress test suite
     * 
     * ----------------------------------------------------------------------
     */

    public BigLoad(final TreeBuilder tb, final String argsString) {
        super(argsString);
        this.tb = tb;
    }

    private final static String[] ARGS_TEMPLATE = { "records|int:1000000:1:1000000000|Total records to create",
            "tmpdir|string:|Temporary volume path" };

    /**
     * Method to parse stress test arguments passed by the stress test suite.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        final ArgParser ap = new ArgParser("com.persistit.BigLoad", _args, ARGS_TEMPLATE).strict();
        totalRecords = ap.getIntValue("records");
        final String path = ap.getStringValue("tmpdir");
        if (path != null && !path.isEmpty()) {
            getPersistit().getConfiguration().setTmpVolDir(path);
        }
    }

    @Override
    protected void executeTest() throws Exception {
        load(getPersistit());
    }
}
