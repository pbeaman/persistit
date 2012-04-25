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
//        Persistit persistit = new Persistit();
//        final long available = persistit.getAvailableHeap();
//        for (int bufferSize = 1024; bufferSize <= 16384; bufferSize *= 2) {
//            final String pname = PNAME + bufferSize;
//            final int bsize = Buffer.bufferSizeWithOverhead(bufferSize);
//            assertEquals(MEGA / bsize, persistit.computeBufferCountFromMemoryProperty(pname, "1M", bufferSize));
//            assertEquals(10 * MEGA / bsize, persistit.computeBufferCountFromMemoryProperty(pname, "10M,1G", bufferSize));
//            assertEquals((available - 64 * MEGA) / bsize, persistit.computeBufferCountFromMemoryProperty(pname,
//                    ",100G,64M", bufferSize));
//            assertEquals((available / 2) / bsize, persistit.computeBufferCountFromMemoryProperty(pname, ",100G,0,0.5",
//                    bufferSize));
//            assertEquals((available / 2) / bsize, persistit.computeBufferCountFromMemoryProperty(pname, ",,,0.5",
//                    bufferSize));
//            assertEquals(10 * MEGA / bsize, persistit.computeBufferCountFromMemoryProperty(pname, "10M,,,0.0",
//                    bufferSize));
//        }
    }

    @Test
    public void testAllocateAlmostEverything() throws Exception {
//        Persistit persistit = new Persistit();
//        final long available = persistit.getAvailableHeap();
//        Properties properties = new Properties();
//        properties.setProperty("buffer.memory.16384", "0,1T,64M,0.8");
//        persistit.initializeProperties(properties);
//        persistit.initializeBufferPools();
//        final MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
//        long used = mu.getUsed();
//        System.out.printf("Initially available=%,d Used=%,d", available, used);
//        persistit.close();
    }

}
