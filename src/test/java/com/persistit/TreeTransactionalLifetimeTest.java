/**
 * Copyright 2013 Akiban Technologies, Inc.
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

import static com.persistit.unit.ConcurrentUtil.assertSuccess;
import static com.persistit.unit.ConcurrentUtil.createThread;
import static com.persistit.unit.ConcurrentUtil.join;
import static com.persistit.unit.ConcurrentUtil.start;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.ConcurrentUtil.ThrowingRunnable;

public class TreeTransactionalLifetimeTest extends PersistitUnitTestCase {
    final static int TIMEOUT_MS = 10000;

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
        _persistit.cleanup();
        assertEquals("There should be no tree", null, tree("ttlt"));
        assertTrue(vstruc().getGarbageRoot() != 0);
    }

    @Test
    public void createdTreeIsNotVisibleUntilCommit() throws Exception {
        final Thread t = createThread("ttlt", new TExec() {
            @Override
            void exec(final Transaction txn) throws Exception {
                final Exchange ex1 = exchange("ttlt");
                ex1.getValue().put(RED_FOX);
                ex1.to(1).store();
                semA.release();
                semB.acquire();
                txn.commit();
            }
        });
        final Map<Thread, Throwable> errors = start(t);
        semA.acquire();
        assertEquals(null, _persistit.getVolume("persistit").getTree("ttlt", false));
        semB.release();
        join(TIMEOUT_MS, errors, t);
        assertSuccess(errors);
        final Exchange ex = exchange("ttlt");
        assertTrue(ex.to(Key.BEFORE).next());
        assertEquals(1, ex.getKey().decodeInt());
    }

    @Test
    public void removeTreeIsNotVisibleUntilCommit() throws Exception {
        final Exchange ex = exchange("ttlt");
        ex.getValue().put(RED_FOX);
        ex.to(1).store();
        final Thread t = createThread("ttlt", new TExec() {
            @Override
            void exec(final Transaction txn) throws Exception {
                final Exchange ex1 = exchange("ttlt");
                ex1.removeTree();
                semA.release();
                semB.acquire();
                txn.commit();
            }
        });
        final Map<Thread, Throwable> errors = start(t);
        semA.acquire();
        assertEquals(ex.getTree(), ex.getVolume().getTree("ttlt", false));
        semB.release();
        join(TIMEOUT_MS, errors, t);
        assertNull(ex.getVolume().getTree("ttlt", false));
    }

    @Test
    public void removeCreateRemove() throws Exception {

        final Exchange ex = exchange("ttlt");
        ex.getValue().put(RED_FOX);
        ex.to(1).store();
        final Thread t = createThread("ttlt", new TExec() {
            @Override
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
        final Map<Thread, Throwable> errors = start(t);
        semA.acquire();
        assertEquals(ex.getTree(), ex.getVolume().getTree("ttlt", false));
        semB.release();
        join(TIMEOUT_MS, errors, t);
        _persistit.getTransactionIndex().updateActiveTransactionCache();
        _persistit.pruneTimelyResources();
        assertTrue(ex.to(Key.BEFORE).next());
        assertEquals(1, ex.getKey().decodeInt());
        assertTrue(!ex.next());
        assertTrue(ex.getVolume().getStructure().getGarbageRoot() != 0);
    }

    @Test
    public void createRemoveByStep() throws Exception {
        createRemoveByStepHelper("ttlt1", false, true, false, false, "0,1:,2:a=step2,3,4:b=step4", "0:b=step4");
        createRemoveByStepHelper("ttlt2", false, false, false, false, "0,1:,2:a=step2,3,4:b=step4", "0");
        createRemoveByStepHelper("ttlt3", true, true, false, false, "0:,1:,2:a=step2,3,4:b=step4", "0:b=step4");
        createRemoveByStepHelper("ttlt4", true, false, false, false, "0:,1:,2:a=step2,3,4:b=step4", "0:");

        createRemoveByStepHelper("ttlt5", false, true, false, true, "0,1:,2:a=step2,3,4:b=step4", "0:b=step4");
        createRemoveByStepHelper("ttlt6", false, false, false, true, "0,1:,2:a=step2,3,4:b=step4", "0");
        createRemoveByStepHelper("ttlt7", true, true, false, true, "0:,1:,2:a=step2,3,4:b=step4", "0:b=step4");
        createRemoveByStepHelper("ttlt8", true, false, false, true, "0:,1:,2:a=step2,3,4:b=step4", "0:");

        createRemoveByStepHelper("ttlt1cr", false, true, true, false, "0,1:,2:a=step2,3,4:b=step4", "0");
        createRemoveByStepHelper("ttlt2cr", false, false, true, false, "0,1:,2:a=step2,3,4:b=step4", "0");
        createRemoveByStepHelper("ttlt3cr", true, true, true, false, "0:,1:,2:a=step2,3,4:b=step4", "0:");
        createRemoveByStepHelper("ttlt4cr", true, false, false, false, "0:,1:,2:a=step2,3,4:b=step4", "0:");

        createRemoveByStepHelper("ttlt5cr", false, true, true, true, "0,1:,2:a=step2,3,4:b=step4", "0");
        createRemoveByStepHelper("ttlt6cr", false, false, true, true, "0,1:,2:a=step2,3,4:b=step4", "0");
        createRemoveByStepHelper("ttlt7cr", true, true, true, true, "0:,1:,2:a=step2,3,4:b=step4", "0:");
        createRemoveByStepHelper("ttlt8cr", true, false, true, true, "0:,1:,2:a=step2,3,4:b=step4", "0:");
    }

    private void createRemoveByStepHelper(final String treeName, final boolean primordial, final boolean commit,
            final boolean crash, final boolean restart, final String expected1, final String expected2)
            throws Exception {

        final Transaction txn = _persistit.getTransaction();
        final Volume volume = _persistit.getVolume("persistit");
        if (primordial) {
            volume.getTree(treeName, true);
        }
        txn.begin();
        try {
            txn.setStep(1);
            volume.getTree(treeName, true);

            txn.setStep(2);
            final Exchange ex1 = exchange(treeName);
            ex1.getValue().put("step2");
            ex1.to("a").store();

            txn.setStep(3);
            ex1.removeTree();

            txn.setStep(4);
            final Exchange ex2 = exchange(treeName);
            ex2.getValue().put("step4");
            ex2.to("b").store();

            assertEquals("Expected contents at steps", expected1, computeCreateRemoveState(treeName, 5));

            if (crash) {
                _persistit.checkpoint();
                _persistit.crash();
            } else {
                if (commit) {
                    txn.commit();
                } else {
                    txn.rollback();
                }
            }
        } finally {
            if (!crash) {
                txn.end();
            }
        }
        if (restart) {
            _persistit.close();
        }
        if (crash || restart) {
            _persistit = new Persistit(_config);
            _persistit.initialize();
        }
        assertEquals("Expected contents at steps", expected2, computeCreateRemoveState(treeName, 1));
    }

    private String computeCreateRemoveState(final String treeName, final int steps) throws PersistitException {
        final StringBuilder sb = new StringBuilder();
        for (int step = 0; step < steps; step++) {
            _persistit.getTransaction().setStep(step);
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(step);
            if (_persistit.getVolume("persistit").getTree(treeName, false) != null) {
                sb.append(":");
                final Exchange ex = exchange(treeName);
                ex.append(Key.BEFORE);
                while (ex.next()) {
                    sb.append(ex.getKey().decodeString()).append("=").append(ex.getValue().getString());
                }
            }
        }
        return sb.toString();
    }

    abstract class TExec extends ThrowingRunnable {

        @Override
        public void run() throws Exception {
            final Transaction txn = _persistit.getTransaction();
            txn.begin();
            try {
                exec(txn);
            } finally {
                txn.end();
            }
        }

        abstract void exec(final Transaction txn) throws Exception;
    }
}
