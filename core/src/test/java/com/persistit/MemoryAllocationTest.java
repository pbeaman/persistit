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

import junit.framework.TestCase;

import org.junit.Test;

public class MemoryAllocationTest extends TestCase {

    private final static long MEGA  =1024  * 1024;
    private final static long GIGA = 1024 * MEGA;
    
    private final static String PNAME = "buffer.memory.";

    @Test
    public void testMemoryAllocation() throws Exception {
        final long available = Persistit.availableMemory();
        for (int bufferSize = 1024; bufferSize <= 16384; bufferSize *= 2) {
            final String pname = PNAME + bufferSize;
            final int bsize = Buffer.bufferSizeWithOverhead(bufferSize);
            assertEquals(MEGA / bsize,
                    Persistit.computeBufferCountFromMemoryProperty(pname, "1M",
                            bufferSize));
            assertEquals(GIGA / bsize,
                    Persistit.computeBufferCountFromMemoryProperty(pname, "1G,1G",
                            bufferSize));
            assertEquals((available - 64 * MEGA) / bsize,
                    Persistit.computeBufferCountFromMemoryProperty(pname, "0,100G,64M",
                            bufferSize));
            assertEquals((available / 2) / bsize,
                    Persistit.computeBufferCountFromMemoryProperty(pname, "0,100G,0,0.5",
                            bufferSize));
            assertEquals((available / 2) / bsize,
                    Persistit.computeBufferCountFromMemoryProperty(pname, ",,,0.5",
                            bufferSize));
            assertEquals(GIGA / bsize,
                    Persistit.computeBufferCountFromMemoryProperty(pname, "1G,,,0.0",
                            bufferSize));
        }
    }

}
