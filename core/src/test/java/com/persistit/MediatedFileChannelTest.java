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

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Timer;
import java.util.TimerTask;

import junit.framework.TestCase;

import org.junit.Test;

public class MediatedFileChannelTest extends TestCase {
    // Ten seconds
    final static long TIME = 10L * 1000L * 1000L * 1000L;

    @Test
    public void testFcMethods() throws Exception {
        final File file = File.createTempFile("bug882219", null);
        file.deleteOnExit();
        MediatedFileChannel fc = new MediatedFileChannel(file, "rw");
        
        assertEquals(0, fc.size());
        final ByteBuffer bb = ByteBuffer.allocate(65536);
        for (int i = 0; i < 65536; i++) {
            bb.array()[i] = (byte) (i % 32 + 64);
        }
        assertEquals(65536, fc.write(bb, 0));
        assertEquals(bb.capacity(), fc.size());
        fc.truncate(30000);
        assertEquals(30000, fc.size());
        bb.clear();
        assertEquals(30000, fc.read(bb, 0));
        assertEquals(30000, bb.position());
        fc.force(true);
        fc.close();
        fc.close();
        try {
            assertEquals(30000, fc.read(bb, 0));
            fail("Should have thrown an exception");
        } catch (ClosedChannelException e) {
            // expected
        }
    }

    @Test
    public void testAsynchronousInterrupts() throws Exception {
        final File file = File.createTempFile("bug882219", null);
        file.deleteOnExit();
        MediatedFileChannel fc = new MediatedFileChannel(file, "rw");
        final Thread foregroundThread = Thread.currentThread();

        Timer timer = new Timer("Interrupter");
        timer.schedule(new TimerTask() {
            public void run() {
                foregroundThread.interrupt();
            }
        }, 1000, 1000);
        int errors = 0;
        int interrupts = 0;
        int count = 0;
        try {
            final ByteBuffer bb = ByteBuffer.allocate(65536);
            for (int i = 0; i < 65536; i++) {
                bb.array()[i] = (byte) (i % 32 + 64);
            }

            final long start = System.nanoTime();
            while (errors == 0 && System.nanoTime() - start < TIME) {
                try {
                    assertTrue(fc.isOpen());
                    assertEquals(65536, fc.write(bb, 0));
                    bb.position(0);
                    fc.force(true);
                    count++;
                } catch (InterruptedIOException e) {
                    // ignore -- expected
                    interrupts++;
                    // need to clear the interrupted status
                    Thread.interrupted();
                } catch (IOException e) {
                    e.printStackTrace();
                    errors++;
                }
            }
        } finally {
            timer.cancel();
            Thread.interrupted();
        }
        assertEquals(0, errors);
        System.out.printf("errors=%d interrupts=%d count=%d", errors, interrupts, count);

    }
}
