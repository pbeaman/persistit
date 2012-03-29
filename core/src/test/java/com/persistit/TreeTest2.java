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

package com.persistit;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class TreeTest2 extends PersistitUnitTestCase {

    final static int COUNT = 10000;

    @Test
    public void testLotsOfTrees() throws Exception {
        Volume volume = _persistit.getVolume("persistit");
        Transaction txn = _persistit.getTransaction();
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

        Properties properties = _persistit.getProperties();
        _persistit.getJournalManager().flush();
        _persistit.crash();
        _persistit = new Persistit();
        _persistit.initialize(properties);

        volume = _persistit.getVolume("persistit");
        assertTrue(1 != volume.getStructure().directoryExchange().getTree().getRootPageAddr());

        String[] treeNames = volume.getTreeNames();
        for (final String t : treeNames) {
            assertTrue(!t.startsWith("TreeTest1"));
        }
    }

    @Test
    public void testSameTreeName() throws Exception {
        Transaction txn = _persistit.getTransaction();
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
