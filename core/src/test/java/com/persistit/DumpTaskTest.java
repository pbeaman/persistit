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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;
import com.persistit.util.Util;

public class DumpTaskTest extends PersistitUnitTestCase {

    @Test
    public void testDumpBuffer() throws Exception {
        store1();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Buffer buffer = _persistit.getBufferPool(16384).getBufferCopy(_persistit.getVolume("persistit"), 1);
        buffer.dump(new DataOutputStream(baos), true, true);
        final DataInputStream stream = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        System.out.println(stream.readUTF());
        int size = stream.readInt();
        Buffer buffer2 = new Buffer(buffer);
        stream.read(buffer2.getBytes(), 0, Buffer.KEY_BLOCK_START);
        buffer2.load();
        int keyBlockEnd = buffer2.getKeyBlockEnd();
        int alloc = buffer2.getAlloc();
        assertEquals(size, keyBlockEnd + buffer.getBufferSize() - alloc);
        stream.read(buffer2.getBytes(), Buffer.KEY_BLOCK_START, buffer2.getKeyBlockEnd() - Buffer.KEY_BLOCK_START);
        stream.read(buffer2.getBytes(), alloc, buffer2.getBufferSize() - alloc);
        System.out.println(Util.dump(buffer2.getBytes(), 16292, 16384));
        System.out.println(buffer2.toStringDetail());
    }

    @Test
    public void testDumpCommand() throws Exception {
        store1();
        final Volume volume = _persistit.getVolume("persistit");
        final Buffer buffer = _persistit.getBufferPool(16384).getBufferCopy(volume, 1);
        CLI cli = new CLI(_persistit, null, null);
        final File file = File.createTempFile("DumpTaskTest", ".tar.gz");
        file.deleteOnExit();
        cli.dump(file.getPath(), true, true, true);
        final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(file)));
        ZipEntry ze = zis.getNextEntry();
        System.out.println(ze);
        DataInputStream stream = new DataInputStream(new BufferedInputStream(zis));

        final int volumeCount = stream.readInt();
        for (int i = 0; i < volumeCount; i++) {
            System.out.println(stream.readUTF());
            int treeCount = stream.readInt();
            for (int j = 0; j < treeCount; j++) {
                System.out.println("  " + stream.readUTF());
            }
        }

        final int bufferPoolCount = stream.readInt();
        for (int i = 0; i < bufferPoolCount; i++) {
            System.out.println(stream.readUTF());
            final int bufferCount = stream.readInt();
            for (int j = 0; j < bufferCount; j++) {
                System.out.println(" " + stream.readUTF());
                final int size = stream.readInt();

                stream.read(buffer.getBytes(), 0, Buffer.KEY_BLOCK_START);
                long page = VolumeHeader.verifySignature(buffer.getBytes()) ? 0 : Util.getLong(buffer.getBytes(),
                        Buffer.PAGE_ADDRESS_OFFSET);
                buffer.setPageAddressAndVolume(page, volume);
                buffer.load();
                int keyBlockEnd = buffer.getKeyBlockEnd();
                int alloc = buffer.getAlloc();
                if (buffer.isDataPage() || buffer.isIndexPage()) {
                    assertEquals(size, keyBlockEnd + buffer.getBufferSize() - alloc);
                    stream.read(buffer.getBytes(), Buffer.KEY_BLOCK_START, buffer.getKeyBlockEnd()
                            - Buffer.KEY_BLOCK_START);
                    stream.read(buffer.getBytes(), alloc, buffer.getBufferSize() - alloc);
                } else {
                    if (size > Buffer.KEY_BLOCK_START) {
                        stream.read(buffer.getBytes(), Buffer.KEY_BLOCK_END_OFFSET, size - Buffer.KEY_BLOCK_START);
                    }
                }

                System.out.println(" " + buffer.toString());

                if (buffer.isDataPage() || buffer.isIndexPage()) {
                    assertEquals(null, buffer.verify(null));
                }
            }
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
