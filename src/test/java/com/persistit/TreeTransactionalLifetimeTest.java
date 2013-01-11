/**
 * Copyright © 2013 Akiban Technologies, Inc.  All rights reserved.
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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.exception.PersistitException;

public class TreeTransactionalLifetimeTest extends PersistitUnitTestCase {
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
        vstruc().directoryExchange();
        
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
}
