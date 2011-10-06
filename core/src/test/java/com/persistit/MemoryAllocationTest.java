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
import java.lang.management.MemoryPoolMXBean;
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
        properties.setProperty("buffer.memory.16384", "0,1T,52M,1.0");
        persistit.initialize(properties);
        final MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long used = mu.getUsed();
        System.out.printf("Initially available=%,d Used=%,d", available, used);
    }

    @Test
    public void testMemoryUtilization() throws Exception {
        byte[] reserve = new byte[65536];
        Persistit db = new Persistit();
        System.out.printf("Available heap=%,d\n", db.getAvailableHeap());
        int computed = db.computeBufferCountFromMemoryProperty("buffer.memory.16384", "0,1T,56M,1.0", 16384);
        System.out.printf("Computed buffer count=%,d\n", computed);

        int count = computed;
        Buffer[] bufferArray = new Buffer[count];
        FastIndex[] fastIndexArray = new FastIndex[count];

        int buffers = 0;
        int fastIndexes = 0;
        boolean failed = true;
        try {
            for (; buffers < count; buffers++) {
                bufferArray[buffers] = new Buffer(16384, buffers, null, null);
                if (buffers % 1000 == 999) {
                    printMemoryUsage(String.format("%8d Buffers: ", buffers + 1));
                }
            }

            for (; fastIndexes < count; fastIndexes++) {
                fastIndexArray[fastIndexes] = new FastIndex(1023);
                if (fastIndexes % 1000 == 999) {
                    printMemoryUsage(String.format("%8d FastIndexes: ", fastIndexes + 1));
                }
            }
            failed = false;
        } finally {
            reserve = null;
            System.err.print(failed ? "OOME: " : "OKAY: ");
            System.err.print(Runtime.getRuntime().freeMemory());
            System.err.print(" bytes free after creating ");
            System.err.print(buffers);
            System.err.print("/");
            System.err.print(count);
            System.err.print(" and ");
            System.err.print(fastIndexes);
            System.err.print("/");
            System.err.print(count);
            System.err.println(" fast indexes ");
            System.err.println(db.getAvailableHeap());
            for (final MemoryPoolMXBean bean : ManagementFactory.getMemoryPoolMXBeans()) {
                System.err.println(bean.getName() + ": " + bean.getUsage() + "  avail="
                        + (bean.getUsage().getMax() - bean.getUsage().getUsed()));
            }
            for (final MemoryPoolMXBean bean : ManagementFactory.getMemoryPoolMXBeans()) {
                System.out.println(bean.getName() + ": " + bean.getUsage() + "  avail="
                        + (bean.getUsage().getMax() - bean.getUsage().getUsed()));
            }

        }
    }

    long previous = 0;

    private void printMemoryUsage(String msg) {
        long free = Runtime.getRuntime().freeMemory();
        if (previous == 0) {
            previous = free;
        }
        System.out.printf("%s  %,12d  %,12d\n", msg, free, previous - free);
        previous = free;
    }
    
    public static void main(final String[] args) throws Exception {
        new MemoryAllocationTest().testMemoryUtilization();
    }
}
