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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.Test;

public class MemoryAllocationTest extends TestCase {

    private final static long MEGA = 1024 * 1024;

    private final static String PNAME = "buffer.memory.";

    @Test
    public void testMemoryAllocationComputation() throws Exception {
        Persistit persistit = new Persistit();
        final long available = persistit.getAvailableHeap();
        for (int bufferSize = 1024; bufferSize <= 16384; bufferSize *= 2) {
            final String pname = PNAME + bufferSize;
            final int bsize = Buffer.bufferSizeWithOverhead(bufferSize);
            assertEquals(MEGA / bsize, persistit.computeBufferCountFromMemoryProperty(pname, "1M", bufferSize));
            assertEquals(10 * MEGA / bsize, persistit.computeBufferCountFromMemoryProperty(pname, "10M,1G", bufferSize));
            assertEquals((available - 64 * MEGA) / bsize, persistit.computeBufferCountFromMemoryProperty(pname,
                    ",100G,64M", bufferSize));
            assertEquals((available / 2) / bsize, persistit.computeBufferCountFromMemoryProperty(pname, ",100G,0,0.5",
                    bufferSize));
            assertEquals((available / 2) / bsize, persistit.computeBufferCountFromMemoryProperty(pname, ",,,0.5",
                    bufferSize));
            assertEquals(10 * MEGA / bsize, persistit.computeBufferCountFromMemoryProperty(pname, "10M,,,0.0",
                    bufferSize));
        }
    }

    @Test
    public void testAllocateAlmostEverything() throws Exception {
        Persistit persistit = new Persistit();
        final long available = persistit.getAvailableHeap();
        Properties properties = new Properties();
        properties.setProperty("buffer.memory.16384", "0,1T,64M,0.8");
        persistit.initialize(properties);
        final MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long used = mu.getUsed();
        System.out.printf("Initially available=%,d Used=%,d", available, used);
    }

}
