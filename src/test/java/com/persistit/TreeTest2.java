/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

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
