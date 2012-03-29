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
import com.persistit.unit.PersistitUnitTestCase;

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
//            System.out.println(Util.dump(buffer.getBytes(), 16292, 16384));
//            System.out.println(buffer.toStringDetail());
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
