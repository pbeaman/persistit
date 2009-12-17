package com.persistit.stress;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NIOTest {
    public static void main(final String[] args) throws Exception {
        final RandomAccessFile raf1 = new RandomAccessFile("test1.dat", "rw");
        final RandomAccessFile raf2 = new RandomAccessFile("test2.dat", "rw");
        raf1.setLength(512);
        raf2.setLength(16384);
        final FileChannel fc1 = raf1.getChannel();
        final FileChannel fc2 = raf2.getChannel();

        final ByteBuffer bb1 = ByteBuffer.allocate(512);
        final ByteBuffer bb2 = ByteBuffer.allocate(16384);
        fc1.read(bb1); // also fails: fc.write(bb1);

        for (int count = 0; count < 5000; count++) {
            bb2.clear();
            fc2.read(bb2, 0);
            if (count % 500 == 0) {
                System.out.println(count);
            }
        }
    }
}
