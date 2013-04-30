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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.Test;

public class MediatedFileChannelTest {
    // Ten seconds
    final static long TIME = 10L * 1000L * 1000L * 1000L;

    @Test
    public void testFcMethods() throws Exception {
        final File file = File.createTempFile("bug882219", null);
        file.deleteOnExit();
        final MediatedFileChannel fc = new MediatedFileChannel(file, "rw");

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
        } catch (final ClosedChannelException e) {
            // expected
        }
    }

    @Test
    public void testAsynchronousInterrupts() throws Exception {
        final File file = File.createTempFile("bug882219", null);
        file.deleteOnExit();
        final MediatedFileChannel fc = new MediatedFileChannel(file, "rw");
        final Thread foregroundThread = Thread.currentThread();

        final Timer timer = new Timer("Interrupter");
        timer.schedule(new TimerTask() {
            @Override
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
                } catch (final InterruptedIOException e) {
                    bb.position(0);
                    // ignore -- expected
                    interrupts++;
                    // need to clear the interrupted status
                    Thread.interrupted();
                } catch (final IOException e) {
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
