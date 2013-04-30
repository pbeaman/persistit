/**
 * Copyright 2012 Akiban Technologies, Inc.
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

import static com.persistit.util.Util.NS_PER_S;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.persistit.util.ArgParser;

/**
 * Benchmark for primitive I/O simulating HARD (durable) commit. This code is
 * intended to explore two elements:
 * 
 * (1) Pre-extending the journal file (so that FileChannel.force(false) usually
 * does not need to write any metadata.
 * 
 * (2) Performing I/O in fixed-length blocks so that to write a some bytes the
 * file system does not first need to read existing data from disk.
 * 
 * Parameters
 * 
 * align - smallest unit of I/O (default = 1) datapath - directory in which fake
 * journal file will be written (default = /tmp/persistit_test_data) buffersize
 * - emulated journal buffer size (default = 64M)
 * 
 * 
 * @author peter
 * 
 */
public class JournalManagerBench {

    private final byte[] NULLS = new byte[65536];

    private final String[] ARG_TEMPLATE = new String[] { "duration|int:10:10:86400|Duration of test in seconds",
            "policy|String:HARD|Commit policy: SOFT, HARD or GROUP",
            "datapath|String:/tmp/persistit_test_data|Datapath property",
            "buffersize|int:64:1:1024|Emulated journal buffer size in MBytes",
            "extension|int:0:0:1024|MBytes by which to extend file when full",
            "prealloc|int:0:0:1024|Preallocated file size in MBytes",
            "align|int:1:1:65536|Blocking factor for I/O size",
            "recsize|int:123:64:65536|Emulated transaction record size" };

    final ByteBuffer buffer;
    final ArgParser ap;

    private File file;
    private FileChannel fc;

    private long writeAddress = 0;
    private long currentAddress = 0;

    long count = 0;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;

    final byte[] bytes = new byte[65536];

    public static void main(final String[] args) throws Exception {
        final JournalManagerBench jmb = new JournalManagerBench(args);
        jmb.runTest();
    }

    private JournalManagerBench(final String[] args) throws Exception {
        ap = new ArgParser("JournalManagerBench", args, ARG_TEMPLATE).strict();
        buffer = ByteBuffer.allocate(ap.getIntValue("buffersize") * 1024 * 1024);
    }

    @SuppressWarnings("resource")
    private void runTest() throws Exception {
        file = new File(ap.getStringValue("datapath"), "JManBench_TestFile");
        fc = new RandomAccessFile(file, "rw").getChannel();
        preallocateFile(ap.getIntValue("prealloc") * 1024 * 1024);
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ('-');
        }
        final int align = ap.getIntValue("align");
        final long extension = ap.getIntValue("extension") * 1024 * 1024;

        final long start = System.nanoTime();
        final long expires = start + ap.getIntValue("duration") * NS_PER_S;
        long now = System.nanoTime();

        while (now < expires) {
            doOneCycle(now - start, align, extension, 100);
            final long then = System.nanoTime();
            count++;
            minTime = Math.min(minTime, then - now);
            maxTime = Math.max(maxTime, then - now);
            now = then;
        }
        final long elapsed = now - start;
        System.out.printf("%,d commits took %,dms at a rate of %,d/second minimum time=%,dns maximumTime=%,dns\n",
                count, elapsed, (count * NS_PER_S) / elapsed, minTime, maxTime);

    }

    private void preallocateFile(final long size) throws Exception {
        if (size > 0 && fc.size() > size) {
            System.out.printf("Truncating file %s from %,d to %,d\n", file, fc.size(), size);
            fc.truncate(size);
        } else if (fc.size() < size) {
            System.out.printf("Preallocating file %s to size %,d ", file, size);
            while (true) {
                long remaining = size - fc.size();
                if (remaining <= 0) {
                    break;
                }
                if (remaining > buffer.capacity()) {
                    remaining = buffer.capacity();
                    final long unaligned = fc.size() % 16384;
                    if (unaligned > 0) {
                        remaining = remaining - (16384 - unaligned);
                    }
                }
                buffer.position(0).limit((int) remaining);
                fc.write(buffer, fc.size());
                System.out.print(".");
            }
            fc.force(true);
            System.out.println("done");
        }
    }

    private void doOneCycle(final long time, final int align, final long extension, final int size) throws Exception {
        // Make a fake transaction record
        final String header = String.format("\nsize=%06d count=%06d time=%012d\n", size, count, time);
        final byte[] b = header.getBytes();
        System.arraycopy(b, 0, bytes, 0, b.length);

        // Add the record, possibly offset to maintaining alignment
        final int toRewrite = (int) (currentAddress - writeAddress);
        buffer.position(toRewrite);
        buffer.put(bytes, 0, size);
        boolean extended = false;

        int position = buffer.position();

        // If extension is needed, add those bytes
        long currentSize;
        if (extension > 0 && writeAddress + buffer.position() > (currentSize = fc.size())) {
            long newSize = currentSize + extension;
            if (newSize - writeAddress > buffer.capacity()) {
                newSize = writeAddress + buffer.capacity();
                assert newSize > currentSize;
            }
            int add = (int) (newSize - writeAddress - buffer.position());
            while (add > 0) {
                buffer.put(NULLS, 0, Math.min(NULLS.length, add));
                add -= NULLS.length;
            }
            extended = true;
        }

        // Write and force the buffer

        buffer.flip();
        fc.write(buffer, writeAddress);
        fc.force(extended);

        // Align the bytes to the beginning of the buffer as needed

        currentAddress = writeAddress + position;
        buffer.limit(position);
        position = (position / align) * align;
        buffer.position(position);
        buffer.compact();
        writeAddress += position;
    }

}
