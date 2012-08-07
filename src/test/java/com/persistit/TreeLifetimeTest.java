/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import static com.persistit.util.SequencerConstants.TREE_CREATE_REMOVE_A;
import static com.persistit.util.SequencerConstants.TREE_CREATE_REMOVE_B;
import static com.persistit.util.SequencerConstants.TREE_CREATE_REMOVE_C;
import static com.persistit.util.SequencerConstants.TREE_CREATE_REMOVE_SCHEDULE;
import static com.persistit.util.ThreadSequencer.addSchedules;
import static com.persistit.util.ThreadSequencer.array;
import static com.persistit.util.ThreadSequencer.describeHistory;
import static com.persistit.util.ThreadSequencer.describePartialOrdering;
import static com.persistit.util.ThreadSequencer.disableSequencer;
import static com.persistit.util.ThreadSequencer.enableSequencer;
import static com.persistit.util.ThreadSequencer.historyMeetsPartialOrdering;
import static com.persistit.util.ThreadSequencer.out;
import static com.persistit.util.ThreadSequencer.rawSequenceHistoryCopy;
import static com.persistit.util.ThreadSequencer.sequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;

import com.persistit.JournalManager.TreeDescriptor;
import com.persistit.exception.PersistitException;
import com.persistit.exception.TreeNotFoundException;
import com.persistit.unit.UnitTestProperties;

public class TreeLifetimeTest extends PersistitUnitTestCase {
    private static final String TREE_NAME = "tree_one";
    final int A = TREE_CREATE_REMOVE_A;
    final int B = TREE_CREATE_REMOVE_B;
    final int C = TREE_CREATE_REMOVE_C;

    private Volume getVolume() {
        return _persistit.getVolume(UnitTestProperties.VOLUME_NAME);
    }

    private Exchange getExchange(boolean create) throws PersistitException {
        return _persistit.getExchange(getVolume(), TREE_NAME, create);
    }

    @Test
    public void testRemovedTreeGoesToGarbageChain() throws PersistitException {
        Transaction txn = _persistit.getTransaction();

        txn.begin();
        Exchange ex = getExchange(true);
        for (int i = 0; i < 5; ++i) {
            ex.clear().append(i).getValue().clear().put(i);
            ex.store();
        }
        txn.commit();
        txn.end();
        _persistit.releaseExchange(ex);
        ex = null;

        txn.begin();
        ex = getExchange(false);
        final long treeRoot = ex.getTree().getRootPageAddr();
        ex.removeTree();
        txn.commit();
        txn.end();
        _persistit.releaseExchange(ex);
        ex = null;

        final List<Long> garbage = getVolume().getStructure().getGarbageList();
        assertTrue("Expected tree root <" + treeRoot + "> in garbage list <" + garbage.toString() + ">", garbage
                .contains(treeRoot));
    }

    @Test
    public void testGetTreeWithoutCreateShouldCreate() throws PersistitException {
        Transaction txn = _persistit.getTransaction();

        txn.begin();
        try {
            getExchange(false);
            fail("Tree should not have existed!");
        } catch (TreeNotFoundException e) {
            // expected
        }

        final Volume volume = getVolume();

        // Check on disk
        List<String> treeNames = Arrays.asList(volume.getTreeNames());
        assertFalse("Tree <" + TREE_NAME + "> should not be in Volume list <" + treeNames + ">", treeNames
                .contains(TREE_NAME));

        // Check in-memory
        assertFalse("Volume should not know about tree", volume.getStructure().treeMapContainsName(TREE_NAME));
        assertEquals("Journal should not have handle for tree", -1, _persistit.getJournalManager().handleForTree(
                new TreeDescriptor(volume.getHandle(), TREE_NAME), false));

        txn.commit();
        txn.end();
    }

    @Test
    public void testCleanupManagerShouldNotInstantiateTrees() throws Exception {
        Exchange ex = getExchange(true);
        _persistit.releaseExchange(ex);

        assertNotNull("Tree should exist after create", getVolume().getTree(TREE_NAME, false));

        ex = getExchange(false);
        final int treeHandle = ex.getTree().getHandle();
        final long rootPage = ex.getTree().getRootPageAddr();
        ex.removeTree();
        _persistit.releaseExchange(ex);

        assertNull("Tree should not exist after remove", getVolume().getTree(TREE_NAME, false));

        CleanupManager cm = _persistit.getCleanupManager();
        boolean accepted = cm.offer(new CleanupManager.CleanupPruneAction(treeHandle, rootPage));
        assertTrue("CleanupPruneAction was accepted", accepted);
        cm.kick();
        while (cm.getEnqueuedCount() > 0) {
            Thread.sleep(50);
        }
        assertNull("Tree should not exist after cleanup action", getVolume().getTree(TREE_NAME, false));
    }

    @Test
    public void testReanimatedTreeCreateAndRemoveSynchronization() throws PersistitException, InterruptedException {
        enableSequencer(true);
        addSchedules(TREE_CREATE_REMOVE_SCHEDULE);

        final ConcurrentLinkedQueue<Throwable> threadErrors = new ConcurrentLinkedQueue<Throwable>();

        Exchange origEx = getExchange(true);
        for (int i = 0; i < 5; ++i) {
            origEx.clear().append(i).store();
        }
        _persistit.releaseExchange(origEx);

        final Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                Exchange ex = null;
                try {
                    ex = getExchange(false);
                    ex.removeTree();
                } catch (Throwable t) {
                    threadErrors.add(t);
                }
                if (ex != null) {
                    _persistit.releaseExchange(ex);
                }
            }
        });

        final Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                sequence(TREE_CREATE_REMOVE_B);
                Exchange ex = null;
                try {
                    ex = getExchange(true);
                    int count = 0;
                    while (ex.next(true)) {
                        ++count;
                    }
                    sequence(TREE_CREATE_REMOVE_C);
                    assertEquals("New tree has zero keys in it", 0, count);
                } catch (Throwable t) {
                    threadErrors.add(t);
                }
                if (ex != null) {
                    _persistit.releaseExchange(ex);
                }
            }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        assertEquals("Threads had no exceptions", "[]", threadErrors.toString());

        final int[] actual = rawSequenceHistoryCopy();
        final int[][] expectedSequence = { array(A, B), array(out(B)), array(C), array(out(A), out(C)) };
        if (!historyMeetsPartialOrdering(actual, expectedSequence)) {
            assertEquals("Unexpected sequencing", describePartialOrdering(expectedSequence), describeHistory(actual));
        }

        disableSequencer();
    }
}
