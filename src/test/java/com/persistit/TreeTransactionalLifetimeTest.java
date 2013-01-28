/**
2 * Copyright © 2013 Akiban Technologies, Inc.  All rights reserved.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Semaphore;

import org.junit.Test;

import com.persistit.exception.PersistitException;

public class TreeTransactionalLifetimeTest extends PersistitUnitTestCase {
    final Semaphore semA = new Semaphore(0);
    final Semaphore semB = new Semaphore(0);
    final Semaphore semT = new Semaphore(0);
    final Semaphore semU = new Semaphore(0);

    /*
     * This class needs to be in com.persistit because of some package-private
     * methods used in controlling the test.
     */

    private Tree tree(final String name) throws PersistitException {
        return vstruc().getTree(name, false);
    }

    private Exchange exchange(final String name) throws PersistitException {
        return _persistit.getExchange("persistit", name, true);
    }

    private VolumeStructure vstruc() {
        return _persistit.getVolume("persistit").getStructure();
    }

    @Test
    public void simplePruning() throws Exception {

        final Transaction txn = _persistit.getTransaction();
        for (int i = 0; i < 2; i++) {
            txn.begin();
            final Exchange ex = exchange("ttlt");
            ex.getValue().put(RED_FOX);
            ex.to(1).store();
            ex.to(2).store();
            txn.rollback();
            txn.end();
        }
        // _persistit.cleanup();
        Thread.sleep(5000);
        assertEquals("There should be no tree", null, tree("ttlt"));
        assertTrue(vstruc().getGarbageRoot() != 0);
    }
    
    @Test
    public void createdTreeIsNotVisibleUntilCommit() throws Exception {
        Thread t = new Thread(new TExec() {
            void exec(final Transaction txn) throws Exception {
                final Exchange ex1 = exchange("ttlt");
                ex1.getValue().put(RED_FOX);
                ex1.to(1).store();
                semA.release();
                semB.acquire();
                txn.commit();
            }
        });
        t.start();
        semA.acquire();
        assertEquals(null, _persistit.getVolume("persistit").getTree("ttlt", false));
        semB.release();
        t.join();
        
        final Exchange ex = exchange("ttlt");
        assertTrue(ex.to(Key.BEFORE).next());
        assertEquals(1, ex.getKey().decodeInt());
    }
        

    @Test
    public void removeTreeIsNotVisibleUntilCommit() throws Exception {
        final Exchange ex = exchange("ttlt");
        ex.getValue().put(RED_FOX);
        ex.to(1).store();
        Thread t = new Thread(new TExec() {
            void exec(final Transaction txn) throws Exception {
                final Exchange ex1 = exchange("ttlt");
                ex1.removeTree();
                semA.release();
                semB.acquire();
                txn.commit();
            }
        });
        t.start();
        semA.acquire();
        assertEquals(ex.getTree(), ex.getVolume().getTree("ttlt", false));
        semB.release();
        t.join();
        assertNull(ex.getVolume().getTree("ttlt", false));
    }
    
    @Test
    public void removeCreateRemove() throws Exception {
        
        final Exchange ex = exchange("ttlt");
        ex.getValue().put(RED_FOX);
        ex.to(1).store();
        Thread t = new Thread(new TExec() {
            void exec(final Transaction txn) throws Exception {
                final Exchange ex1 = exchange("ttlt");
                ex1.removeTree();
                final Exchange ex2 = exchange("ttlt");
                ex2.getValue().put(RED_FOX);
                ex2.to(2).store();
                ex2.removeTree();
                final Exchange ex3 = exchange("ttlt");
                ex3.getValue().put(RED_FOX);
                ex3.to(3).store();
                final Exchange ex4 = exchange("ttlt");
                ex4.to(Key.BEFORE);
                assertTrue(ex4.next());
                assertEquals(3, ex4.getKey().decodeInt());
                assertTrue(!ex4.next());
                semA.release();
                semB.acquire();
                txn.rollback();
            }
        });
        t.start();
        semA.acquire();
        assertEquals(ex.getTree(), ex.getVolume().getTree("ttlt", false));
        semB.release();
        t.join();
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        _persistit.pruneTimelyResources();
        assertTrue(ex.to(Key.BEFORE).next());
        assertEquals(1, ex.getKey().decodeInt());
        assertTrue(!ex.next());
        assertTrue(ex.getVolume().getStructure().getGarbageRoot() != 0);
    }

    abstract class TExec implements Runnable {

        public void run() {
                try {
                    final Transaction txn = _persistit.getTransaction();
                    txn.begin();
                    try {
                        exec(txn);
                    } finally {
                        txn.end();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }

        abstract void exec(final Transaction txn) throws Exception;
    }
}
