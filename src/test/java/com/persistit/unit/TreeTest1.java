/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Aug 24, 2004
 */
package com.persistit.unit;

import java.io.IOException;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

public class TreeTest1 extends PersistitTestCase {
    
    public void test1() throws PersistitException {
        System.out.print("test1 ");
        Exchange exchange = _persistit.getExchange("persistit", "NewTree", true);
        exchange.removeTree();
        // pause("Removed NewTree");
        exchange = _persistit.getExchange("persistit", "NewTree", true);
        exchange.getValue().put(
            "This String is just here to fill up some space");
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

    public void test2() throws PersistitException {
        System.out.print("test2 ");
        final Exchange[] exchanges = new Exchange[1000];
        for (int counter = 0; counter < 1000; counter++) {
            exchanges[counter] =
                _persistit.getExchange("persistit", "TreeTest1_" + counter, true);
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

    public void runTest() throws Exception {
        test1();
        test2();
    }

}
