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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.persistit.JournalManager.TreeDescriptor;

public class Bug1018526Test extends PersistitUnitTestCase {

    @Test
    public void tempVolumesAndTreesDoNotGetHandles() throws Exception {
        final Volume volume = _persistit.createTemporaryVolume();
        final Exchange exchange = _persistit.getExchange(volume, "a_temp_tree", true);
        assertEquals("Handle should be 0", 0, volume.getHandle());
        assertEquals("Handle should be 0", 0, exchange.getTree().getHandle());
    }

    @Test
    public void txnOnTempVolumeDoesNotWriteToJournal() throws Exception {
        final Transaction txn = _persistit.getTransaction();
        final JournalManager jman = _persistit.getJournalManager();
        int failed = 0;
        for (int i = 0; i < 10; i++) {
            final long startingAddress = jman.getCurrentAddress();
            txn.begin();
            try {
                final Volume volume = _persistit.createTemporaryVolume();
                final Exchange exchange = _persistit.getExchange(volume, "a_temp_tree", true);
                exchange.clear().append("abc");
                exchange.getValue().put(RED_FOX);
                exchange.store();
                txn.commit();
                if (jman.getCurrentAddress() != startingAddress) {
                    failed++;
                }
            } finally {
                txn.end();
            }
        }
        /*
         * Don't require 0 because a checkpoint could write to the journal in a
         * race
         */
        assertTrue("Transaction on temporary volume should not have written to journal", failed < 3);
    }

    @Test
    public void temporaryVolumesAndTreesNotReloaded() throws Exception {
        final Set<Integer> permTreeHandleSet = new HashSet<Integer>();
        final Volume permVolume = _persistit.getVolume("persistit");
        _persistit.getJournalManager().unitTestAllowHandlesForTemporaryVolumesAndTrees();
        for (int i = 0; i < 20; i++) {
            final Volume tempVolume = _persistit.createTemporaryVolume();
            for (int j = 0; j < 10; j++) {
                final Tree tempTree = tempVolume.getTree("temp_tree_" + i + "_" + j, true);
                _persistit.getJournalManager().handleForTree(tempTree);
                final Tree permTree = permVolume.getTree("perm_tree_" + i + "_" + j, true);
                _persistit.getJournalManager().handleForTree(permTree);
                if (!permTreeHandleSet.add(permTree.getHandle())) {
                    fail("Duplicate tree handle " + permTree.getHandle() + " for " + permTree);
                }
            }
        }
        final Configuration cfg = _persistit.getConfiguration();
        _persistit.close();
        _persistit = new Persistit(cfg);
        final Map<Integer, TreeDescriptor> map = _persistit.getJournalManager().queryTreeMap();
        for (final Integer handle : permTreeHandleSet) {
            final TreeDescriptor td = map.remove(handle);
            assertNotNull("Permanent Tree should be un the tree map", td);
        }
        // expect 1: the directory tree
        assertEquals("Recovered tree map should contain only permanent trees", 1, map.size());
    }
}
