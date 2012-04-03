/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

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
            Arrays.sort(holders);
            for (BufferHolder holder : holders) {
                assertTrue(holder.getPage() > page);
                page = holder.getPage();
            }
        } finally {
            pool.setFlushTimestamp(1000);
        }
    }
    
    
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
            for (int j = i+1; j < count; j++) {
                assertTrue("Scrambled holders", holder != holders[j]);
            }
        }
    }
}
