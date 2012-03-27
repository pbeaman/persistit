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

import java.io.IOException;

import com.persistit.Exchange;
import com.persistit.Key;

public class TreeTest1 extends PersistitUnitTestCase {

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
