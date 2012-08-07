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

import static org.junit.Assert.assertTrue;

import java.util.WeakHashMap;

import org.junit.Test;


public class AccumulatorMemoryTest extends PersistitUnitTestCase {

    @Test
    public void testReleaseMemory() throws Exception {
        WeakHashMap<Accumulator, Object> weakMap = new WeakHashMap<Accumulator, Object>();
        final Volume volume = _persistit.getVolume("persistit");
        for (int count = 0; count < 5000; count++) {
            Exchange ex = _persistit.getExchange(volume, String.format("tree%6d", count), true);
            Accumulator accumulator = ex.getTree().getAccumulator(Accumulator.Type.SUM, 0);
            weakMap.put(accumulator, null);
            ex.removeTree();
            if ((count % 1000) == 0) {
                System.gc();
            }
            assertTrue(weakMap.size() < 2000);
        }
    }

    @Test
    public void testReleaseMemoryTxn() throws Exception {
        WeakHashMap<Accumulator, Object> weakMap = new WeakHashMap<Accumulator, Object>();
        final Volume volume = _persistit.getVolume("persistit");
        final Transaction txn = _persistit.getTransaction();
        for (int count = 0; count < 5000; count++) {
            txn.begin();
            Exchange ex = _persistit.getExchange(volume, String.format("tree%6d", count), true);
            Accumulator accumulator = ex.getTree().getAccumulator(Accumulator.Type.SUM, 0);
            weakMap.put(accumulator, null);
            ex.removeTree();
            if ((count % 1000) == 0) {
                System.gc();
            }
            assertTrue(weakMap.size() < 2000);
            txn.commit();
            txn.end();
        }
    }

}
