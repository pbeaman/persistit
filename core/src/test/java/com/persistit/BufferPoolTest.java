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

import com.persistit.BufferPool.BufferHolder;
import com.persistit.unit.PersistitUnitTestCase;

public class BufferPoolTest extends PersistitUnitTestCase {

    /**
     * Covers allocPage condition in which page in the avaialbleBitMap is
     * unavailable.
     * 
     * @throws Exception
     */
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
            for (BufferHolder holder : holders) {
                assertTrue(holder.getPage() > page);
                page = holder.getPage();
            }
        } finally {
            pool.setFlushTimestamp(1000);
        }
    }
}
