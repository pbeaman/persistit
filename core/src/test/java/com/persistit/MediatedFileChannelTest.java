package com.persistit;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Timer;
import java.util.TimerTask;

import junit.framework.TestCase;

import org.junit.Test;

import com.persistit.MediatedFileChannel.IOInterruptedException;

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

        final ByteBuffer bb = ByteBuffer.allocate(65536);
        for (int i = 0; i < 65536; i++) {
            bb.array()[i] = (byte) (i % 32 + 64);
        }

        final long start = System.nanoTime();
        int errors = 0;
        int interrupts = 0;
        int count = 0;
        while (errors == 0 && System.nanoTime() - start < TIME) {
            try {
                assertTrue(fc.isOpen());
                assertEquals(65536, fc.write(bb, 0));
                bb.position(0);
                fc.force(true);
                count++;
            } catch (IOInterruptedException e) {
                // ignore -- expected
                interrupts++;
            } catch (IOException e) {
                e.printStackTrace();
                errors++;
            }
        }
        assertEquals(0, errors);
        System.out.printf("errors=%d interrupts=%d count=%d", errors, interrupts, count);

    }
}
