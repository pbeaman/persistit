/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

public class TreeTest2 extends PersistitUnitTestCase {

    final static int COUNT = 10000;

    @Test
    public void testLotsOfTrees() throws Exception {
        Volume volume = _persistit.getVolume("persistit");
        final Transaction txn = _persistit.getTransaction();
        assertEquals(1, volume.getStructure().directoryExchange().getTree().getRootPageAddr());
        for (int counter = 0; counter < COUNT; counter++) {
            txn.begin();
            final Exchange exchange = _persistit.getExchange("persistit", "TreeTest1_" + counter, true);
            for (int k = 0; k < 10; k++) {
                exchange.to(k).store();
            }
            txn.commit();
            txn.end();
        }
        assertTrue(1 != volume.getStructure().directoryExchange().getTree().getRootPageAddr());

        for (int counter = 0; counter < COUNT; counter++) {
            txn.begin();
            final Exchange exchange = _persistit.getExchange("persistit", "TreeTest1_" + counter, false);
            exchange.removeTree();
            txn.commit();
            txn.end();
        }

        _persistit.getJournalManager().flush();
        _persistit.crash();
        _persistit = new Persistit(_config);

        volume = _persistit.getVolume("persistit");
        assertTrue(1 != volume.getStructure().directoryExchange().getTree().getRootPageAddr());

        final String[] treeNames = volume.getTreeNames();
        for (final String t : treeNames) {
            assertTrue(!t.startsWith("TreeTest1"));
        }
    }

    @Test
    public void testSameTreeName() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        for (int counter = 0; counter < 100; counter++) {
            txn.begin();
            try {
                final Exchange exchange = _persistit.getExchange("persistit", "TreeTest1", true);
                exchange.getValue().put("The quick brown fox jumped over the lazy red dog");
                for (int k = 0; k < counter * 100; k++) {
                    exchange.to(k).store();
                }
                txn.commit();
            } finally {
                txn.end();
            }
            txn.begin();
            try {
                final Exchange exchange = _persistit.getExchange("persistit", "TreeTest1", true);
                exchange.removeTree();
                txn.commit();
            } finally {
                txn.end();
            }
        }
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
        new TreeTest2().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        testLotsOfTrees();
    }

}
