/**
 * Copyright (C) 2011 Akiban Technologies Inc.
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

import java.util.WeakHashMap;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

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
