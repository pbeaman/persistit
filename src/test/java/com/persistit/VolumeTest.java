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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Properties;

import com.persistit.unit.UnitTestProperties;
import org.junit.Test;

import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.VolumeFullException;

public class VolumeTest extends PersistitUnitTestCase {

    @Test
    public void testHollowVolume() throws Exception {
        final Volume volume = new Volume("Sassafras", 12345);
        assertEquals("Sassafras", volume.getName());
        assertEquals(12345, volume.getId());
        volume.setId(12345);
        volume.setId(0);
        volume.setId(12346);
        try {
            volume.setId(12345);
            fail("Can't change volume id");
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            assertNull(volume.getSpecification());
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            assertNull(volume.getStructure());
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            assertNull(volume.getStorage());
        } catch (IllegalStateException e) {
            // ok
        }
        try {
            assertNull(volume.getStatistics());
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void testCreateOpenVolume() throws Exception {
        VolumeSpecification vs = validVolumeSpecification("${datapath}/vtest, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create");
        Volume volume1 = new Volume(vs);
        volume1.open(_persistit);
        long id = volume1.getId();
        _persistit.flush();
        _persistit.copyBackPages();
        volume1.close();

        Volume volume2 = new Volume(vs);
        volume2.open(_persistit);
        assertEquals(id, volume2.getId());
        long maxPages = volume2.getSpecification().getMaximumPages();
        assertEquals(1024 * 1024 / 16384, maxPages);
        for (int i = 2; i < 1000; i++) {
            try {
                volume2.getStorage().claimHeadBuffer();
                assertEquals(i, volume2.getStorage().allocNewPage());
                volume2.getStorage().releaseHeadBuffer();
            } catch (VolumeFullException e) {
                assertEquals(maxPages, i);
                break;
            }
        }
        assertEquals(maxPages, volume2.getStorage().getNextAvailablePage());
        assertEquals(maxPages, volume2.getStorage().getExtentedPageCount());
        File file = new File(volume2.getPath());
        assertEquals(maxPages * 16384, file.length());
        volume2.close();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.write(new byte[16]);
        raf.close();

        Volume volume3 = new Volume(vs);
        try {
            volume3.open(_persistit);
            fail("Volume should have been corrupt");
        } catch (CorruptVolumeException e) {
            // okay
        }
    }

    @Test
    public void testDeleteVolume() throws Exception {
        VolumeSpecification vs = validVolumeSpecification("${datapath}/vtest, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create");
        Volume volume1 = new Volume(vs);
        volume1.open(_persistit);
        File file = new File(volume1.getPath());
        assertEquals(32768, file.length());
        assertTrue(_persistit.deleteVolume("vtest"));
        assertTrue(volume1.isClosed());
        assertTrue(!file.exists());
    }

    @Test
    public void testDottedVolumeName() throws Exception {
        VolumeSpecification vs = validVolumeSpecification("${datapath}/.thisStuffWontBeIgnored, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create");
        Volume volume1 = _persistit.loadVolume(vs);
        Exchange ex = _persistit.getExchange(".thisStuffWontBeIgnored", "emptyTreeTest", true);
        assertEquals(ex.getVolume(), volume1);
    }

    @Test
    public void testVolumeSpecification() throws Exception {
        VolumeSpecification vs;

        vs = validVolumeSpecification("/a/b/c,name: crabcake, pageSize: 16384, initialSize: 10m, maximumSize: 100m, extensionSize: 10m, create");
        vs = validVolumeSpecification("/a/b/c,name:crabcake,pageSize:16384,initialSize:10m,maximumSize:100m,extensionSize:10m,create");
        assertEquals("crabcake", vs.getName());
        assertEquals(16384, vs.getPageSize());
        assertEquals(10 * 1024 * 1024 / 16384, vs.getInitialPages());
        assertEquals(10 * 1024 * 1024, vs.getInitialSize());
        assertTrue(vs.isCreate());
        assertFalse(vs.isCreateOnly());
        assertFalse(vs.isReadOnly());

        vs = validVolumeSpecification("/a/b/c");
        assertEquals("c", vs.getName());
        assertEquals(-1, vs.getPageSize());
        assertFalse(vs.isCreate());
        assertFalse(vs.isCreateOnly());
        assertFalse(vs.isReadOnly());

        vs = validVolumeSpecification("/a/b/c.v01");
        assertEquals("c", vs.getName());
        vs = validVolumeSpecification("/a/b/c.d.v01");
        assertEquals("c.d", vs.getName());

        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16383,initialsize:10m,maximumsize:100m,extensionsize:10m,create");
        invalidVolumeSpecification("/a/b/c;name:crabcake,pagesize:16384,initialsize:10m,maximumsize:100m,extensionsize:10m,create");
        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16384,initialsize:10p,maximumsize:100p,extensionsize:10p,create");
        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16384,initialsize:10m,maximumsize:100m,extensionsize:10m,create,readOnly");
    }

    @Test
    public void volumeLoadAndSaveGlobalTimestamp() throws Exception {
        final long MARKER = 123456789L;
        _persistit.getTimestampAllocator().updateTimestamp(MARKER);
        VolumeSpecification vs = validVolumeSpecification("${datapath}/testGlobalTimestamp, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create");

        final Volume vol1 = _persistit.loadVolume(vs);
        vol1.close();

        final Volume vol2 = _persistit.loadVolume(vs);
        final long statTimestamp = vol2.getStatistics().getLastGlobalTimestamp();
        // Greater than is ok (other activity may have occurred)
        if (statTimestamp < MARKER) {
            assertEquals("Saved and loaded timestamp", MARKER, statTimestamp);
        }
    }

    @Test(expected = CorruptVolumeException.class)
    public void volumeFromFutureIsRejected() throws Exception {
        final int RECORDS = 100;

        // Make it more obvious if when we jump backwards
        _persistit.getTimestampAllocator().bumpTimestamp(1000000);

        // Write records to check on later
        Exchange ex = _persistit.getExchange(UnitTestProperties.VOLUME_NAME, "VolumeTest", true);
        Transaction txn = _persistit.getTransaction();
        txn.begin();
        for (int i = 0; i < RECORDS; ++i) {
            ex.clear().append(i).getValue().put(i);
            ex.store();
        }
        txn.commit();
        txn.end();
        _persistit.releaseExchange(ex);

        _persistit.flush();
        _persistit.copyBackPages();

        List<File> journalFiles = _persistit.getJournalManager().unitTestGetAllJournalFiles();
        Properties properties = _persistit.getProperties();
        _persistit.crash();

        /*
         * Worst case (or slipped finger) scenario of missing journal files
         */
        for (File file : journalFiles) {
            boolean success = file.delete();
            assertEquals("Deleted journal file " + file.getName(), true, success);
        }

        _persistit = new Persistit();
        _persistit.initialize(properties);
    }

    @Test
    public void timeoutWhenPageIsInUse() throws Exception {
        final Exchange exchange = _persistit.getExchange(UnitTestProperties.VOLUME_NAME, "VolumeTest", true);
        final Buffer buffer = exchange.getBufferPool().get(exchange.getVolume(), 1, false, true);
        try {
            final long start = System.currentTimeMillis();
            try {
                exchange.getVolume().close(5000);
                fail("Expect an InUseException");
            } catch (InUseException e) {
                final long elapsed = System.currentTimeMillis() - start;
                assertTrue("Expected InUseException to happen at 5000 ms but was " + elapsed, elapsed > 4000
                        && elapsed < 10000);
            }
        } finally {
            buffer.release();
        }
    }

    private VolumeSpecification validVolumeSpecification(final String specification) throws Exception {
        try {
            return _persistit.getConfiguration().volumeSpecification(specification);
        } catch (InvalidVolumeSpecificationException e) {
            fail(specification + " should be valid: " + e);
            return null;
        }
    }

    private void invalidVolumeSpecification(final String specification) throws InvalidVolumeSpecificationException {
        try {
            new VolumeSpecification(specification);
            fail();
        } catch (InvalidVolumeSpecificationException e) {
            // ok
        }
    }

}
