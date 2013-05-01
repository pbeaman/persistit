/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

import static org.junit.Assert.assertTrue;

import java.util.WeakHashMap;

import org.junit.Test;

public class AccumulatorMemoryTest extends PersistitUnitTestCase {

    @Test
    public void testReleaseMemory() throws Exception {
        final WeakHashMap<Accumulator, Object> weakMap = new WeakHashMap<Accumulator, Object>();
        final Volume volume = _persistit.getVolume("persistit");
        for (int count = 0; count < 5000; count++) {
            final Exchange ex = _persistit.getExchange(volume, String.format("tree%6d", count), true);
            final Accumulator accumulator = ex.getTree().getAccumulator(Accumulator.Type.SUM, 0);
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
        final WeakHashMap<Accumulator, Object> weakMap = new WeakHashMap<Accumulator, Object>();
        final Volume volume = _persistit.getVolume("persistit");
        final Transaction txn = _persistit.getTransaction();
        for (int count = 0; count < 5000; count++) {
            txn.begin();
            final Exchange ex = _persistit.getExchange(volume, String.format("tree%6d", count), true);
            final Accumulator accumulator = ex.getTree().getAccumulator(Accumulator.Type.SUM, 0);
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
