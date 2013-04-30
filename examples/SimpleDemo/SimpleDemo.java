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

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;

/**
 * <p>
 * A very simple class to demonstrate basic operations of Persistit&trade; This
 * class performs the following operations:
 * <ol>
 * <li>Initializes {@link com.persistit.Persistit} using a Properties file.</li>
 * <li>Creates a {@link com.persistit.Exchange} with which to store and fetch
 * data to and from a {@link com.persistit.Tree}.</li>
 * <li>Reads some names from the console and stores records in the <tt>Tree</tt>
 * indexed by those names.</li>
 * <li>Dispays the names in alphabetical order.</li>
 * <li>Closes Persistic to ensure all records have been written to disk.
 * </ol>
 * @version 1.0
 */
public class SimpleDemo {
    public static void main(String[] args) throws Exception {
        //
        // Initializes buffer pool, loads Volume(s)
        // and sets up the journal.
        //
        System.out.println("Initializing Persistit");
        Persistit persistit = new Persistit();
        persistit.initialize();
        //
        // A Exchange provides all the methods you use to
        // manipulate data. This constructs a Exchange in
        // volume "SimpleDemo" and either opens or creates
        // within that volume a Tree called "my_first_tree".
        //
        Exchange exchange = new Exchange(persistit, "sdemo", "my_first_tree", true);

        if (args.length > 0 && "-d".equals(args[0])) {
            //
            // Remove all elements.
            //
            exchange.removeAll();
        }

        System.out.println();
        System.out.println("Please enter some names -");
        System.out.println();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        boolean done = false;
        int count = 0;
        while (true) {
            System.out.print("Name: ");
            String name = reader.readLine();
            if (name == null || name.length() == 0)
                break;
            //
            // Set up the value to insert into the Tree.
            //
            exchange.getValue().put("Name # " + (++count));
            //
            // Set up the key and store the record. The to()
            // method replaces the final segment of the key with
            // the supplied value. If the key is empty then
            // it appends the segment.
            //
            // Note that all key manipulation methods return the
            // Exchange so that you can chain calls like this:
            //
            exchange.getKey().to(name);
            exchange.store();
        }

        System.out.println();
        System.out.println("Here are the names you entered: ");
        //
        // Set Exchange to an imaginary key that's to the left of all real
        // keys. This allows the next() operation to find the first key.
        //
        exchange.getKey().to(Key.BEFORE);
        //
        // The next() method advances to the next ascending key
        // segment (in this case, the next name). Returns false
        // if there is none. This method also atomically retrieves the
        // value associated with the key.
        //
        while (exchange.next()) {
            //
            // Recover the name from the Key.
            //
            String name = exchange.getKey().decodeString();
            //
            // Fetch the associated Value and decode it into
            // a String.
            //
            String value = exchange.getValue().getString();

            System.out.println(name + "  -->  " + value);
        }

        //
        // Flushes everything nicely to disk and releases all resources.
        //
        persistit.close();
    }
}
