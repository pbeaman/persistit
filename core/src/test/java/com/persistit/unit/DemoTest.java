/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.unit;

import java.io.IOException;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.util.Debug;

/**
 * Not really a test. This class creates a very small B-Tree so that you can
 * look at pages and do small tasks.
 * 
 * @author peter
 * 
 */
public class DemoTest extends PersistitUnitTestCase {

    private String volumeName = "persistit";
    private String treeName = "DemoTest";
    boolean all = true;

    @Test
    public void test1() throws Exception {
        //
        // Get an Exchange, creating tree as necessary
        final Exchange exchange = _persistit.getExchange(volumeName, treeName, true);
        //
        // Launch the CLI server so we can look at pages
        _persistit.getManagement().launch("cliserver port=9999");
        //
        // Make up some silly test data
        String value = "%,10d ";
        while (value.length() < 120) {
            value = value + RED_FOX + " ";
        }
        for (int index = 0; index < 1000; index++) {
            //
            // Pause for user interaction
            all = all || more();
            //
            // Create a Key value which, for interest, will insert rows non-sequentially
            exchange.getKey().to("key" + index);
            //
            // Format a silly value
            exchange.getValue().put(String.format(value, index));
            //
            // Tell user what's happening
            if (!all) {
                System.out.println("Storing " + exchange);
            }
            exchange.getTransaction().begin();
            //
            // Store it
            try {
                exchange.store();
                exchange.getTransaction().commit();
            } finally {
                exchange.getTransaction().end();
            }
        }
    }

    private boolean more() throws IOException {
        while (true) {
            System.out.printf("\nPress ENTER for one more key, or SPACE for the remaininder");
            int c = System.in.read();
            if (c == -1 || c == '\n') {
                return false;
            } else if (c == ' ') {
                return true;
            }
        }
        
    }
    @Override
    public void runAllTests() throws Exception {
        test1();
    }
    
    public static void main(final String[] args) throws Exception {
        DemoTest dt = new DemoTest();
        dt.setName("DemoTest");
        dt.setUp();
        dt.all = false;
        dt.test1();
        dt.tearDown();

    }

}
