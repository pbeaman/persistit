/**
 * Copyright 2012 Akiban Technologies, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.persistit.BufferPool.BufferHolder;

public class BufferPoolTest extends PersistitUnitTestCase {

    /**
     * Covers allocPage condition in which page in the avaialbleBitMap is
     * unavailable.
     * 
     * @throws Exception
     */
    @Test
    public void testInvalidatedBuffers() throws Exception {
        final Volume vol = _persistit.createTemporaryVolume();
        final Exchange ex = _persistit.getExchange(vol, "BufferPoolTest", true);
        ex.append("k").store();
        // Hold a claim on the page.
        final Buffer buffer1 = vol.getPool().get(vol, 2, false, true);
        buffer1.release();
        // Invalidate the buffers
        vol.getPool().invalidate(vol);
        // reestablish claim on now-invalid buffer
        buffer1.claim(true, 0);
        // Do work that allocates buffers
        final Exchange ex2 = _persistit.getExchange("persistit", "BufferPoolTest", true);
        ex2.getValue().put(RED_FOX);
        for (int i = 0; i < 10000; i++) {
            ex2.to(i).store();
        }
        buffer1.release();
    }

    @Test
    public void testSelectDirtyBuffers() throws Exception {
        final Volume volume = _persistit.getVolume("persistit");
        final BufferPool pool = volume.getPool();
        pool.setFlushTimestamp(-1);
        try {
            final int buffers = pool.getBufferCount();
            final int[] priorities = new int[buffers / 2];
            final BufferHolder[] holders = new BufferHolder[buffers / 2];
            for (int i = 0; i < holders.length; i++) {
                holders[i] = new BufferHolder();
            }
            final long timestamp = _persistit.getTimestampAllocator().getCurrentTimestamp();
            pool.flush(timestamp);

            int count = pool.selectDirtyBuffers(priorities, holders);
            assertEquals("Buffer pool should be clean", 0, count);

            for (int i = 1; i < buffers; i++) {
                final long page = volume.getStorage().allocNewPage();
                final Buffer buffer = pool.get(volume, page, true, false);
                buffer.setDirtyAtTimestamp(timestamp + i);
                buffer.releaseTouched();
            }

            count = pool.selectDirtyBuffers(priorities, holders);
            assertEquals("Selected buffers should fill the arrays", buffers / 2, count);
            long page = -1;
            Arrays.sort(holders);
            for (final BufferHolder holder : holders) {
                assertTrue(holder.getPage() > page);
                page = holder.getPage();
            }
        } finally {
            pool.setFlushTimestamp(1000);
        }
    }

    @Test
    public void testAddSelectedBuffer() throws Exception {
        final Volume volume = _persistit.getVolume("persistit");
        final BufferPool pool = volume.getPool();

        final int total = 100;
        final int[] priorities = new int[total];
        final BufferHolder[] holders = new BufferHolder[total];
        for (int i = 0; i < holders.length; i++) {
            holders[i] = new BufferHolder();
        }

        final Random random = new Random(1);
        final SortedSet<Integer> sorted = new TreeSet<Integer>();
        int count = 0;
        for (int index = 0; index < 10000; index++) {
            final int r = random.nextInt(1000000000);
            if (sorted.contains(r)) {
                index--;
                continue;
            }
            sorted.add(r);
            final Buffer buffer = pool.get(volume, 1, false, false);
            final Buffer copy = new Buffer(buffer);
            copy.setPageAddressAndVolume(index, volume);
            count = pool.addSelectedBufferByPriority(copy, r, priorities, holders, count);
            buffer.release();
        }
        assertEquals("Arrays should be full", total, count);
        final Integer[] sortedArray = sorted.toArray(new Integer[sorted.size()]);

        for (int i = 0; i < count; i++) {
            final int s = sortedArray[sortedArray.length - i - 1];
            final int r = priorities[i];
            assertEquals("Priority order is wrong", s, r);
        }
        long page = -1;
        Arrays.sort(holders);
        for (final BufferHolder holder : holders) {
            assertTrue(holder.getPage() > page);
            page = holder.getPage();
        }

        for (int i = 0; i < count; i++) {
            final BufferHolder holder = holders[i];
            for (int j = i + 1; j < count; j++) {
                assertTrue("Scrambled holders", holder != holders[j]);
            }
        }
    }

    @Test
    public void testWritePriority() throws Exception {
        final long m = 100 * 1000 * 1000;
        final Volume volume = _persistit.getVolume("persistit");
        final BufferPool pool = volume.getPool();
        final Buffer buffer = pool.getBufferCopy(0);
        buffer.claim(true);
        long currentTimestamp = 4 * m;
        final long checkpointTimestamp = 2 * m;
        for (long timestamp = m; timestamp < m * 20; timestamp += m) {
            buffer.setDirtyAtTimestamp(timestamp);
            final int priority = pool.writePriority(buffer, 123456, checkpointTimestamp, currentTimestamp);
            System.out.printf("Timestamp %,15d Checkpoint %,15d Current %,15d Priority %,15d\n", timestamp,
                    checkpointTimestamp, currentTimestamp, priority);
            currentTimestamp += 10000000;
        }
    }

    @Test
    public void testEvictVoume() throws Exception {
        final Volume vol = _persistit.createTemporaryVolume();
        final Exchange ex = _persistit.getExchange(vol, "BufferPoolTest", true);
        _persistit.flush();
        ex.getValue().put(RED_FOX);
        int i;
        for (i = 1;; i++) {
            ex.to(i).store();
            if (vol.getNextAvailablePage() >= 10) {
                break;
            }
        }
        vol.getPool().evict(vol);
        assertTrue("Should be no remaining dirty buffers", vol.getPool().getDirtyPageCount() == 0);
        for (int j = 0; j < i + 100; j++) {
            ex.to(j).fetch();
            assertEquals(j >= 1 && j <= i, ex.getValue().isDefined());
        }
    }

}
