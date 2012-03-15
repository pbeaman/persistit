/**
 * Copyright (C) 2012 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import com.persistit.exception.PersistitException;
import com.persistit.exception.TreeNotFoundException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.unit.UnitTestProperties;

import java.util.Arrays;
import java.util.List;

import static com.persistit.JournalManager.TreeDescriptor;

public class TreeLifetimeTest extends PersistitUnitTestCase {
    private static final String TREE_NAME = "tree_one";
    
    public Volume getVolume() {
        return _persistit.getVolume(UnitTestProperties.VOLUME_NAME);
    }

    public Exchange getExchange(boolean create) throws PersistitException {
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

        // Check on disk knowledge
        List<String> treeNames = Arrays.asList(volume.getTreeNames());
        assertFalse("Tree <"+TREE_NAME+"> should not be in Volume list <"+treeNames+">", treeNames.contains(TREE_NAME));

        // Check in-memory maps
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
}
