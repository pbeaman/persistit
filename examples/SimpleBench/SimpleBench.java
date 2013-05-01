/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

import java.util.Properties;
import java.util.Random;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.Persistit;

/**
 * <p>
 * A simple demonstration of Persistit&trade;'s insert and retrieval and
 * performance. This class performs the following operations:
 * <ol>
 * <li>Initializes {@link com.persistit.Persistit} using stock properties file.</li>
 * <li>Creates an {@link com.persistit.Exchange} with which to store and fetch
 * data to and from a tree.</li>
 * <li>Removes any data formerly stored in the tree.</li>
 * <li>Inserts numerous String values, indexed by small integers.</li>
 * <li>Traverses all keys in forward sequential order.</li>
 * <li>Traverses all keys in reverse sequential order.</li>
 * <li>Modifies records by random key value</li>
 * <li>Lengthens all records </liL
 * <li>Fetches records by random key value.</li>
 * </ol>
 * By default, <tt>SimpleBench</tt> performs this cycle ten times, inserting
 * 500,000 records. You can specify different iteration and cycle counts on the
 * command line.
 * </p>
 * <p>
 * Note that if you run this program using a Sun HotSpot&trade; Server JVM, you
 * will observe a substantial performance improvement over multiple cycles as
 * the optimizing compiler fully analyzes and optimizes heavily-used code paths.
 * </p>
 * 
 * @version 1.0
 */
public class SimpleBench {
    public static void main(String[] args) throws Exception {
        w("");
        w("SimpleBench");
        w("");
        w("This simple benchmark demonstrates Persistit's ability to");
        w("modify its in-memory data structures fast. The buffer pool has");
        w("been carefully sized to ensure that once the buffers are");
        w("\"warmed up\", no disk I/O is needed to complete the store,");
        w("traverse, and fetch operations being performed.");
        w("");
        w("Although real applications do not perform entirely in this");
        w("manner, they typically do exhibit strong locality of reference");
        w("within the keyspace, which is why disk caching is effective,");
        w("and why it is important to examine in-memory performance.");
        w("");
        w("Truly meaningful performance comparisons can best be made by");
        w("measuring actual application performance.");
        w("");
        //
        // Initializes buffer pool, loads Volume(s)
        // and sets up PrewriteJournal.
        //
        System.out.println("Initializing Persistit");
        // Note: you can override datapath and logpath properties as follows:
        //
        // java -Dcom.persistit.datapath=... -Dcom.persistit.logpath=...
        // SimpleBench
        //
        Properties props = new Properties();
        props.setProperty("datapath", ".");
        props.setProperty("logpath", ".");
        props.setProperty("logfile", "${logpath}/sbdemo_${timestamp}.log");
        props.setProperty("buffer.count.16384", "8K");
        props.setProperty("volume.1", "${datapath}/sbdemo,create,pageSize:16K,"
                + "initialSize:50M,extensionSize:1M,maximumSize:10G");
        props.setProperty("journalpath", "${datapath}/sbdemo_journal");

        Persistit persistit = new Persistit();
        persistit.initialize(props);

        try {
            doBench(persistit, args);
        } finally {
            persistit.close();
        }
    }

    private static void w(String s) {
        System.out.println(s);
    }

    public static void doBench(Persistit persistit, String[] args) throws Exception {
        //
        // Source of random key values. Initialized with a constant seed
        // value so that results are reproducible.
        //
        Random random = new Random(1);
        //
        // An Exchange provides all the methods you use to manipulate data.
        // This constructs a Exchange in volume "sbdemo" and either opens
        // or creates within that volume a Tree called "BenchTree".
        //
        Exchange exchange = new Exchange(persistit, "sbdemo", "BenchTree", true);
        long time;

        int iterations = 1000000;
        int cycles = 10;

        if (args.length > 0)
            iterations = Integer.parseInt(args[0]);
        if (args.length > 1)
            cycles = Integer.parseInt(args[1]);

        long time0 = System.currentTimeMillis();
        for (int cycle = 1; cycle <= cycles; cycle++) {
            System.out.println();
            System.out.println();
            System.out.println("Starting cycle #" + cycle + " of " + cycles);
            System.out.println();
            //
            // Remove all records everything.
            //
            exchange.removeAll();
            //
            // Set up a stock 30-character value to store.
            //
            exchange.getValue().putString("ABCDEFGHIJABCDEFGHIJABCDEFGHIJ");
            //
            // INSERTION
            // Inserts numerous String values, indexed by small integers.
            //
            System.out.print("Inserting " + iterations + " records in forward sequential key order ");
            time = System.currentTimeMillis();
            for (int index = 0; index < iterations; index++) {
                exchange.to(index).store();
                if ((index % 50000) == 0)
                    System.out.print(".");
            }
            time = System.currentTimeMillis() - time;
            System.out.println(" -- took " + time + "ms");

            //
            // TRAVERSAL
            // Traverses all keys in forward-sequential order.
            //
            System.out.print("Traversing " + iterations + " keys in forward order ");
            exchange.to(Key.BEFORE);
            time = System.currentTimeMillis();
            for (int index = 0; exchange.next(); index++) {
                if ((index % 50000) == 0)
                    System.out.print(".");
            }
            time = System.currentTimeMillis() - time;
            System.out.println(" -- took " + time + "ms");

            //
            // TRAVERSAL
            // Traverses all keys in reverse-sequential order.
            //
            System.out.print("Traversing " + iterations + " keys in reverse order ");
            exchange.to(Key.AFTER);
            time = System.currentTimeMillis();
            for (int index = 0; exchange.previous(); index++) {
                if ((index % 50000) == 0)
                    System.out.print(".");
            }
            time = System.currentTimeMillis() - time;
            System.out.println(" -- took " + time + "ms");
            //
            // UPDATE
            // Updates numerous String values with equal length using random
            // keys.
            //
            exchange.getValue().putString("abcdefghijabcdefghijabcdefghij");

            System.out.print("Modifying " + iterations + " values by random key ");
            time = System.currentTimeMillis();
            for (int index = 0; index < iterations; index++) {
                if ((index % 50000) == 0)
                    System.out.print(".");
                int randomKeyInt = random.nextInt(iterations);
                exchange.to(randomKeyInt).store();
            }
            time = System.currentTimeMillis() - time;
            System.out.println(" -- took " + time + "ms");

            //
            // LENGTHEN
            // Lengthens all String values in forward-seqential key order.
            //
            exchange.getValue().putString("ABCDEFGHIJABCDEFGHIJABCDEFGHIJ0123456789");
            System.out.print("Lengthening " + iterations + " values by 10 chars in forward-sequential order");
            time = System.currentTimeMillis();
            for (int index = 0; index < iterations; index++) {
                if ((index % 50000) == 0)
                    System.out.print(".");
                exchange.to(index).store();
            }
            time = System.currentTimeMillis() - time;
            System.out.println(" -- took " + time + "ms");
            //
            // RETRIEVE ALL VALUES IN PSEUDO-RANDOM ORDER
            //
            StringBuilder sb = new StringBuilder();
            System.out.print("Retrieving " + iterations + " values in random order");
            time = System.currentTimeMillis();
            for (int index = 0; index < iterations; index++) {
                if ((index % 50000) == 0)
                    System.out.print(".");
                int randomKeyInt = random.nextInt(iterations);
                exchange.to(randomKeyInt).fetch();
                exchange.getValue().getString(sb);
                if (sb.length() != 40) {
                    throw new RuntimeException("Wrong value " + exchange.getValue() + " at " + randomKeyInt);
                }
                sb.setLength(0);
            }
            time = System.currentTimeMillis() - time;
            System.out.println(" -- took " + time + "ms");

            System.out.print("Flushing all pending updates to the OS");
            time = System.currentTimeMillis();
            persistit.flush();
            time = System.currentTimeMillis() - time;
            System.out.println(" -- took " + time + "ms");

            System.out.print("Synching all pending I/O to disk");
            time = System.currentTimeMillis();
            persistit.force();
            time = System.currentTimeMillis() - time;
            System.out.println(" -- took " + time + "ms");

        }
        time0 = System.currentTimeMillis() - time0;
        System.out.println("Total time: " + time0);

        persistit.close();
    }
}
