/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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
            Buffer buffer = _persistit.getBufferPool(16384).getBufferCopy(_persistit.getVolume("persistit"), page);
            buffer.dump(bb, true, true, new HashSet<Volume>());
            bb.flip();
            assertEquals(IV.TYPE, IV.getType(bb));
            assertEquals(buffer.getVolume().getHandle(), IV.getHandle(bb));
            bb.position(IV.getLength(bb));

            assertEquals(PA.TYPE, PA.getType(bb));
            assertEquals(page, PA.getPageAddress(bb));
            Arrays.fill(buffer.getBytes(), (byte) 0);
            int left = PA.getLeftSize(bb);
            int recordSize = PA.getLength(bb);
            int right = recordSize - left - PA.OVERHEAD;
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
        final Volume volume = _persistit.getVolume("persistit");
        final Buffer buffer = _persistit.getBufferPool(16384).getBufferCopy(volume, 1);
        CLI cli = new CLI(_persistit, null, null);
        final File file = File.createTempFile("DumpTaskTest", ".zip");
        file.deleteOnExit();
        cli.dump(file.getPath(), true, true, true);

        final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(file)));
        ZipEntry ze = zis.getNextEntry();
        System.out.println(ze);
        DataInputStream stream = new DataInputStream(new BufferedInputStream(zis));

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
