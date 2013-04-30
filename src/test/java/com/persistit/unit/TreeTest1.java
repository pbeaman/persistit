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

package com.persistit.unit;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.PersistitUnitTestCase;

public class TreeTest1 extends PersistitUnitTestCase {

    @Test
    public void test1() throws Exception {
        System.out.print("test1 ");
        Exchange exchange = _persistit.getExchange("persistit", "NewTree", true);
        exchange.removeTree();
        // pause("Removed NewTree");
        exchange = _persistit.getExchange("persistit", "NewTree", true);
        exchange.getValue().put("This String is just here to fill up some space");
        for (int i = 1; i < 100000; i++) {
            exchange.clear().append(i);
            exchange.store();
        }
        // pause("NewTree with 100000 nodes");
        boolean hasKeys = exchange.clear().append(Key.BEFORE).next();
        assertTrue(hasKeys);

        exchange.removeTree();
        exchange = _persistit.getExchange("persistit", "NewTree", true);
        hasKeys = exchange.clear().append(Key.BEFORE).next();
        assertTrue(!hasKeys);
        exchange.removeTree();
        System.out.println("- done");
    }

    @Test
    public void test2() throws Exception {
        System.out.print("test2 ");
        final Exchange[] exchanges = new Exchange[1000];
        for (int counter = 0; counter < 1000; counter++) {
            exchanges[counter] = _persistit.getExchange("persistit", "TreeTest1_" + counter, true);
        }
        // pause("TreeTest1_0 through TreeTest1_999 created");

        for (int counter = 0; counter < 1000; counter++) {
            exchanges[counter].removeTree();
        }

        // pause("TreeTest1_0 through TreeTest1_999 removed");

        final String[] treeNames = exchanges[0].getVolume().getTreeNames();
        for (int index = 0; index < treeNames.length; index++) {
            assertTrue(!treeNames[index].startsWith("TreeTest1_"));
        }
        System.out.println("- done");
    }

    public static void pause(final String prompt) {
        System.out.print(prompt + "  Press ENTER to continue");
        System.out.flush();
        try {
            while (System.in.read() != '\r') {
            }
        } catch (final IOException ioe) {
        }
        System.out.println();
    }

    public static void main(final String[] args) throws Exception {
        new TreeTest1().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
        test2();
    }

}
