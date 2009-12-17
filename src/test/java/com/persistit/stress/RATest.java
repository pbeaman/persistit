package com.persistit.stress;

import java.io.RandomAccessFile;

public class RATest implements Runnable {
    public final static String FILENAME = "c:/temp/ratest.dat";
    public final static int DURATION = 60000;

    private static int _threadCount = 5;
    private final int _index;

    public static void main(final String[] args) throws Exception {
        final RandomAccessFile raf = new RandomAccessFile(FILENAME, "rw");
        raf.setLength(1024 * 1024 * _threadCount);
        raf.close();

        for (int i = 0; i < _threadCount; i++) {
            new Thread(new RATest(i)).start();
        }
    }

    private RATest(final int index) {
        _index = index;
    }

    public void run() {
        System.out.println("Starting thread " + Thread.currentThread());
        final long time = System.currentTimeMillis();
        try {
            final RandomAccessFile raf = new RandomAccessFile(FILENAME, "rw");
            final byte[] buffer1 = new byte[1024];
            final byte[] buffer2 = new byte[1024];
            int cycle = 0;
            int errorCount = 0;
            while (System.currentTimeMillis() < time + DURATION) {
                cycle++;

                for (int i = 0; i < 1024; i++) {
                    buffer1[i] = (byte) cycle;
                }

                for (int i = 0; i < 1024; i++) {
                    raf.seek((_index * 1024 + i) * 1024);
                    raf.write(buffer1);
                }

                raf.getFD().sync();

                for (int i = 0; i < 1024; i++) {
                    raf.seek((_index * 1024 + i) * 1024);
                    raf.read(buffer2);
                }

                for (int i = 0; i < 1024; i++) {
                    if (buffer1[i] != buffer2[i]) {
                        errorCount++;
                    }
                }

                System.out.println(Thread.currentThread() + " finished cycle "
                    + cycle + " at " + (System.currentTimeMillis() - time)
                    + " with errorCount=" + errorCount);
            }
        } catch (final Exception e) {
            System.out.println("Thread " + Thread.currentThread() + " " + e);
            e.printStackTrace();
        }
    }
}
