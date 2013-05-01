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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Test;

import com.persistit.JournalRecord.IV;
import com.persistit.JournalRecord.PA;
import com.persistit.exception.PersistitException;

public class DumpTaskTest extends PersistitUnitTestCase {

    @Test
    public void testDumpBuffer() throws Exception {
        store1();
        final ByteBuffer bb = ByteBuffer.allocate(65536);

        for (long page = 1; page < 3; page++) {
            final Buffer buffer = _persistit.getBufferPool(16384)
                    .getBufferCopy(_persistit.getVolume("persistit"), page);
            buffer.dump(bb, true, true, new HashSet<Volume>());
            bb.flip();
            assertEquals(IV.TYPE, IV.getType(bb));
            assertEquals(buffer.getVolume().getHandle(), IV.getHandle(bb));
            bb.position(IV.getLength(bb));

            assertEquals(PA.TYPE, PA.getType(bb));
            assertEquals(page, PA.getPageAddress(bb));
            Arrays.fill(buffer.getBytes(), (byte) 0);
            final int left = PA.getLeftSize(bb);
            final int recordSize = PA.getLength(bb);
            final int right = recordSize - left - PA.OVERHEAD;
            System.arraycopy(bb.array(), bb.position() + PA.OVERHEAD, buffer.getBytes(), 0, left);
            System.arraycopy(bb.array(), bb.position() + PA.OVERHEAD + left, buffer.getBytes(), buffer.getBufferSize()
                    - right, right);
            // System.out.println(Util.dump(buffer.getBytes(), 16292, 16384));
            // System.out.println(buffer.toStringDetail());
            bb.clear();
        }
    }

    @Test
    public void testDumpCommand() throws Exception {
        store1();
        final CLI cli = new CLI(_persistit, null, null);
        final File file = File.createTempFile("DumpTaskTest", ".zip");
        file.deleteOnExit();
        final Task task = cli.dump(file.getPath(), true, true, false);
        task.setPersistit(_persistit);
        task.run();

        ZipInputStream zis = null;
        DataInputStream stream = null;
        try {
            zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(file)));
            final ZipEntry ze = zis.getNextEntry();
            System.out.println(ze);
            stream = new DataInputStream(new BufferedInputStream(zis));
        } finally {
            if (stream != null) {
                stream.close();
            }
            if (zis != null) {
                zis.close();
            }
            file.delete();
        }
    }

    private void store1() throws PersistitException {
        final Exchange exchange = _persistit.getExchange("persistit", "SimpleTest1", true);
        exchange.removeAll();
        final StringBuilder sb = new StringBuilder();

        for (int i = 1; i < 4000; i++) {
            sb.setLength(0);
            sb.append((char) (i / 20 + 64));
            sb.append((char) (i % 20 + 64));
            exchange.clear().append(sb);
            exchange.getValue().put("Record #" + i);
            exchange.store();
        }
    }
}
