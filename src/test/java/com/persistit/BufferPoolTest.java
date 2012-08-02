/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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
import com.persistit.unit.PersistitUnitTestCase;

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
            int[] priorities = new int[buffers / 2];
            BufferHolder[] holders = new BufferHolder[buffers / 2];
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
            for (BufferHolder holder : holders) {
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

        int total = 100;
        final int[] priorities = new int[total];
        BufferHolder[] holders = new BufferHolder[total];
        for (int i = 0; i < holders.length; i++) {
            holders[i] = new BufferHolder();
        }

        final Random random = new Random(1);
        final SortedSet<Integer> sorted = new TreeSet<Integer>();
        int count = 0;
        for (int index = 0; index < 10000; index++) {
            int r = random.nextInt(1000000000);
            if (sorted.contains(r)) {
                index--;
                continue;
            }
            sorted.add(r);
            Buffer buffer = pool.get(volume, 1, false, false);
            Buffer copy = new Buffer(buffer);
            copy.setPageAddressAndVolume(index, volume);
            count = pool.addSelectedBufferByPriority(copy, r, priorities, holders, count);
            buffer.release();
        }
        assertEquals("Arrays should be full", total, count);
        Integer[] sortedArray = sorted.toArray(new Integer[sorted.size()]);

        for (int i = 0; i < count; i++) {
            int s = sortedArray[sortedArray.length - i - 1];
            int r = priorities[i];
            assertEquals("Priority order is wrong", s, r);
        }
        long page = -1;
        Arrays.sort(holders);
        for (BufferHolder holder : holders) {
            assertTrue(holder.getPage() > page);
            page = holder.getPage();
        }

        for (int i = 0; i < count; i++) {
            BufferHolder holder = holders[i];
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
        Buffer buffer = pool.getBufferCopy(0);
        buffer.claim(true);
        long currentTimestamp = 4 * m;
        long checkpointTimestamp = 2 * m;
        for (long timestamp = m; timestamp < m * 20; timestamp += m) {
            buffer.setDirtyAtTimestamp(timestamp);
            int priority = pool.writePriority(buffer, 123456, checkpointTimestamp, currentTimestamp);
            System.out.printf("Timestamp %,15d Checkpoint %,15d Current %,15d Priority %,15d\n", timestamp,
                    checkpointTimestamp, currentTimestamp, priority);
            currentTimestamp += 10000000;
        }
    }

}
