/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.unit;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;

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
