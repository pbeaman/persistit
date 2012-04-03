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

import com.persistit.exception.PersistitException;
import com.persistit.exception.TreeNotFoundException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.persistit.JournalManager.TreeDescriptor;
import static com.persistit.util.SequencerConstants.TREE_CREATE_REMOVE_A;
import static com.persistit.util.SequencerConstants.TREE_CREATE_REMOVE_B;
import static com.persistit.util.SequencerConstants.TREE_CREATE_REMOVE_C;
import static com.persistit.util.SequencerConstants.TREE_CREATE_REMOVE_SCHEDULE;
import static com.persistit.util.ThreadSequencer.*;

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
        assertTrue("Expected tree root <"+treeRoot+"> in garbage list <"+garbage.toString()+">",
                   garbage.contains(treeRoot));
    }
    
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
        assertFalse("Tree <"+TREE_NAME+"> should not be in Volume list <"+treeNames+">", treeNames.contains(TREE_NAME));

        // Check in-memory
        assertFalse("Volume should not know about tree", volume.getStructure().treeMapContainsName(TREE_NAME));
        assertEquals("Journal should not have handle for tree",
                     -1,
                     _persistit.getJournalManager().handleForTree(new TreeDescriptor(volume.getHandle(), TREE_NAME), false));

        txn.commit();
        txn.end();
    }

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
        while(cm.getEnqueuedCount() > 0) {
            Thread.sleep(50);
        }
        assertNull("Tree should not exist after cleanup action", getVolume().getTree(TREE_NAME, false));
    }

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
                } catch(Throwable t) {
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
                } catch(Throwable t) {
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
