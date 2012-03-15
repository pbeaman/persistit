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

package com.persistit.unit;

import com.persistit.Exchange;
import com.persistit.TestShim;
import com.persistit.Transaction;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;

import java.util.List;

public class TreeLifetimeTest extends PersistitUnitTestCase {
    private static final String TREE_NAME = "tree_one";
    
    public Exchange getExchange(boolean create) throws PersistitException {
        return _persistit.getExchange(UnitTestProperties.VOLUME_NAME, TREE_NAME, create);
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
        final Volume volume = ex.getVolume();
        ex.removeTree();
        txn.commit();
        txn.end();
        _persistit.releaseExchange(ex);
        ex = null;

        final List<Long> garbage = TestShim.getGarbageList(volume);
        assertTrue("Expected tree root <"+treeRoot+"> in garbage list <"+garbage.toString()+">",
                   garbage.contains(treeRoot));
    }
}
