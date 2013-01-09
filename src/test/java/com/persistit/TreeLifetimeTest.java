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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.persistit.JournalManager.TreeDescriptor;
import com.persistit.exception.PersistitException;
import com.persistit.exception.TreeNotFoundException;
import com.persistit.unit.UnitTestProperties;

public class TreeLifetimeTest extends PersistitUnitTestCase {
    private static final String TREE_NAME = "tree_one";

    private Volume getVolume() {
        return _persistit.getVolume(UnitTestProperties.VOLUME_NAME);
    }

    private Exchange getExchange(final boolean create) throws PersistitException {
        return _persistit.getExchange(getVolume(), TREE_NAME, create);
    }

    @Test
    public void testRemovedTreeGoesToGarbageChainNoTxn() throws PersistitException {
        Exchange ex = getExchange(true);
        for (int i = 0; i < 5; ++i) {
            ex.clear().append(i).getValue().clear().put(i);
            ex.store();
        }
        _persistit.releaseExchange(ex);
        ex = null;

        ex = getExchange(false);
        final long treeRoot = ex.getTree().getRootPageAddr();
        ex.removeTree();
        _persistit.releaseExchange(ex);
        ex = null;
        _persistit.cleanup();

        final List<Long> garbage = getVolume().getStructure().getGarbageList();
        assertTrue("Expected tree root <" + treeRoot + "> in garbage list <" + garbage.toString() + ">",
                garbage.contains(treeRoot));
    }

    @Test
    public void testRemovedTreeGoesToGarbageChainTxn() throws PersistitException {
        final Transaction txn = _persistit.getTransaction();
        Exchange ex;

        txn.begin();
        ex = getExchange(true);
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
        _persistit.cleanup();
        final List<Long> garbage = getVolume().getStructure().getGarbageList();
        assertTrue("Expected tree root <" + treeRoot + "> in garbage list <" + garbage.toString() + ">",
                garbage.contains(treeRoot));
    }

    @Test
    public void testGetTreeWithoutCreateShouldCreate() throws PersistitException {
        final Transaction txn = _persistit.getTransaction();

        txn.begin();
        try {
            getExchange(false);
            fail("Tree should not have existed!");
        } catch (final TreeNotFoundException e) {
            // expected
        }

        final Volume volume = getVolume();

        // Check on disk
        final List<String> treeNames = Arrays.asList(volume.getTreeNames());
        assertFalse("Tree <" + TREE_NAME + "> should not be in Volume list <" + treeNames + ">",
                treeNames.contains(TREE_NAME));

        // Check in-memory
        assertFalse("Volume should not know about tree", volume.getStructure().treeMapContainsName(TREE_NAME));
        assertEquals("Journal should not have handle for tree", -1,
                _persistit.getJournalManager().handleForTree(new TreeDescriptor(volume.getHandle(), TREE_NAME), false));

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

        final CleanupManager cm = _persistit.getCleanupManager();
        final boolean accepted = cm.offer(new CleanupManager.CleanupPruneAction(treeHandle, rootPage));
        assertTrue("CleanupPruneAction was accepted", accepted);
        cm.kick();
        while (cm.getEnqueuedCount() > 0) {
            Thread.sleep(50);
        }
        assertNull("Tree should not exist after cleanup action", getVolume().getTree(TREE_NAME, false));
    }

}
