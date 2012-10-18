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
import java.util.SortedMap;
import java.util.TreeMap;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import com.persistit.stress.AbstractStressTest;
import com.persistit.util.ArgParser;
import com.persistit.util.Util;

/**
 * Simulate loading a large set (e.g., 500M) of records with large random keys.
 * Because straightforward insertion results in highly entropic page access
 * after the database size has exceed the amount of buffer pool memory space,
 * this demo creates smaller sorted sets of keys and then merges them to create
 * the final Tree in sequential order. As a side-effect, the final tree is also
 * physically coherent in that the logical and physical order of keys disk are
 * closely aligned.
 * 
 * @author peter
 */
public class BigLoad extends AbstractStressTest {

    private static final Random RANDOM = new Random();
    private static final StringBuilder SB = new StringBuilder();

    private int totalRecords;
    private int recordsPerBucket;

    static class ComparableExchangeHolder implements Comparable<ComparableExchangeHolder> {

        final Exchange exchange;

        ComparableExchangeHolder(final Exchange exchange) {
            this.exchange = exchange;
        }

        @Override
        public int compareTo(final ComparableExchangeHolder ceh) {
            final Key k1 = exchange.getKey();
            final Key k2 = ceh.exchange.getKey();
            return k1.compareTo(k2);
        }
    }

    public BigLoad(final String argsString) {
        super(argsString);
    }

    public BigLoad(final int totalRecords, final int buckets) {
        super("");
        this.totalRecords = totalRecords;
        this.recordsPerBucket = totalRecords / buckets;
    }

    public void load(final Persistit db) throws PersistitException {
        final long startLoadTime = System.nanoTime();
        final Volume sortVolume = db.createTemporaryVolume();
        final Exchange resultExchange = db.getExchange("persistit", "BigLoad", true);
        final int bucketCount = loadBuckets(db, sortVolume);
        mergeBuckets(db, bucketCount, sortVolume, resultExchange);
        sortVolume.close();
        final long endLoadTime = System.nanoTime();
        System.out.printf("Total time to load and sort %,d records is %,dms", totalRecords,
                (endLoadTime - startLoadTime) / Util.NS_PER_MS);
    }

    private int loadBuckets(final Persistit db, final Volume volume) throws PersistitException {
        long bucketStartTime = 0;
        int bucket = 0;
        Exchange ex = null;
        for (int i = 0; i < totalRecords; i++) {
            if ((i % recordsPerBucket) == 0) {
                final long now = System.nanoTime();
                if (i > 0) {
                    System.out.printf("Loaded bucket %,5d in %,12dms\n", bucket, (now - bucketStartTime)
                            / Util.NS_PER_MS);
                }
                bucketStartTime = now;
                bucket++;
                ex = db.getExchange(volume, "sort" + bucket, true);
            }
            ex.clear().append(randomKey());
            ex.store();
        }
        return bucket;
    }

    private void mergeBuckets(final Persistit db, final int bucketCount, final Volume sortVolume, final Exchange to)
            throws PersistitException {
        final long startLoadTime = System.nanoTime();
        int loaded = 0;

        final SortedMap<ComparableExchangeHolder, Integer> sortMap = new TreeMap<ComparableExchangeHolder, Integer>();
        /*
         * Load the sortMap using as keys the first key of each bucket.
         */
        for (int bucket = 1; bucket <= bucketCount; bucket++) {
            final Exchange ex = db.getExchange(sortVolume, "sort" + bucket, false);
            if (ex.append(Key.BEFORE).next()) {
                final Integer duplicate = sortMap.put(new ComparableExchangeHolder(ex), bucket);
                showDuplicate(duplicate, bucket, ex);
            }
        }

        while (!sortMap.isEmpty()) {
            final ComparableExchangeHolder ceh = sortMap.firstKey();
            final int bucket = sortMap.remove(ceh);
            SB.setLength(0);
            ceh.exchange.getKey().reset().decodeString(SB);
            if (ceh.exchange.next()) {
                final Integer duplicate = sortMap.put(ceh, bucket);
                showDuplicate(duplicate, bucket, ceh.exchange);
            }
            to.clear().append(SB);
            to.store();
            if ((++loaded % 10000000) == 0) {
                System.out.printf("Loaded %,12d records in %,12dms\n", loaded, (System.nanoTime() - startLoadTime)
                        / Util.NS_PER_MS);
            }
        }
        System.out.printf("Finished loading %,12d records in %,12dms\n", loaded, (System.nanoTime() - startLoadTime)
                / Util.NS_PER_MS);
    }

    private String randomKey() {
        return String.format("%020dxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                (RANDOM.nextLong() & Long.MAX_VALUE));
    }

    private void showDuplicate(final Integer bucket1, final int bucket2, final Exchange ex) {
        if (bucket1 != null) {
            System.out.printf("Duplicate key %s in buckets %,d and %,d\n", ex.getKey(), bucket1, bucket2);
        }
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
        final BigLoad bl = new BigLoad(records, buckets);
        final Persistit db = new Persistit();
        if (args.length > 2) {
            db.initialize(args[2]);
        } else {
            db.initialize();
        }
        try {
            bl.load(db);
        } finally {
            db.close();
        }
    }

    /*
     * Stuff required to run within the stress test suite
     */

    private final static String[] ARGS_TEMPLATE = { "records|int:1:1:1000000000|Total records to create",
            "buckets|int:1:1:1000000|Number of sort buckets,tmpdir|string:|Temporary volume path" };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final ArgParser ap = new ArgParser("com.persistit.BigLoad", _args, ARGS_TEMPLATE).strict();
        totalRecords = ap.getIntValue("records");
        recordsPerBucket = totalRecords / ap.getIntValue("buckets");
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
