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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.List;

import org.junit.Test;

import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.VolumeFullException;
import com.persistit.unit.UnitTestProperties;

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
        } catch (final IllegalStateException e) {
            // ok
        }
        try {
            assertNull(volume.getStructure());
        } catch (final IllegalStateException e) {
            // ok
        }
        try {
            assertNull(volume.getStorage());
        } catch (final IllegalStateException e) {
            // ok
        }
        try {
            assertNull(volume.getStatistics());
        } catch (final IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void testCreateOpenVolume() throws Exception {
        final VolumeSpecification vs = validVolumeSpecification("${datapath}/vtest, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create");
        final Volume volume1 = new Volume(vs);
        volume1.open(_persistit);
        final long id = volume1.getId();
        _persistit.flush();
        _persistit.copyBackPages();
        volume1.close();

        final Volume volume2 = new Volume(vs);
        volume2.open(_persistit);
        assertEquals(id, volume2.getId());
        final long maxPages = volume2.getSpecification().getMaximumPages();
        assertEquals(1024 * 1024 / 16384, maxPages);
        for (int i = 2; i < 1000; i++) {
            try {
                volume2.getStorage().claimHeadBuffer();
                assertEquals(i, volume2.getStorage().allocNewPage());
                volume2.getStorage().releaseHeadBuffer();
            } catch (final VolumeFullException e) {
                assertEquals(maxPages, i);
                break;
            }
        }
        assertEquals(maxPages, volume2.getStorage().getNextAvailablePage());
        assertEquals(maxPages, volume2.getStorage().getExtentedPageCount());
        final File file = new File(volume2.getPath());
        assertEquals(maxPages * 16384, file.length());
        volume2.close();
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.write(new byte[16]);
        raf.close();

        final Volume volume3 = new Volume(vs);
        try {
            volume3.open(_persistit);
            fail("Volume should have been corrupt");
        } catch (final CorruptVolumeException e) {
            // okay
        }
    }

    @Test
    public void testDeleteVolume() throws Exception {
        final VolumeSpecification vs = validVolumeSpecification("${datapath}/vtest, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create");
        final Volume volume1 = new Volume(vs);
        volume1.open(_persistit);
        final File file = new File(volume1.getPath());
        assertEquals(32768, file.length());
        assertTrue(_persistit.deleteVolume("vtest"));
        assertTrue(volume1.isClosed());
        assertTrue(!file.exists());
    }

    @Test
    public void testDottedVolumeName() throws Exception {
        final VolumeSpecification vs = validVolumeSpecification("${datapath}/.thisStuffWontBeIgnored, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create");
        final Volume volume1 = _persistit.loadVolume(vs);
        final Exchange ex = _persistit.getExchange(".thisStuffWontBeIgnored", "emptyTreeTest", true);
        assertEquals(ex.getVolume(), volume1);
    }

    @Test
    public void testVolumeSpecification() throws Exception {
        VolumeSpecification vs;
        VolumeSpecification vs2;

        vs = validVolumeSpecification("/a/b/c,name: crabcake, pageSize: 16384, initialSize: 10m, maximumSize: 100m, extensionSize: 10m, create");
        vs = validVolumeSpecification("/a/b/c,name:crabcake,pageSize:16384,initialSize:10m,maximumSize:100m,extensionSize:10m,create");
        assertEquals("crabcake", vs.getName());
        assertEquals(16384, vs.getPageSize());
        assertEquals(10 * 1024 * 1024 / 16384, vs.getInitialPages());
        assertEquals(10 * 1024 * 1024, vs.getInitialSize());
        assertTrue(vs.isCreate());
        assertTrue(vs.isAliased());
        assertFalse(vs.isCreateOnly());
        assertFalse(vs.isReadOnly());
        vs2 = validVolumeSpecification(vs.toString());
        assertEquals("Parse of toString should be equal", vs, vs2);

        vs = validVolumeSpecification("/a/b/c");
        assertEquals("c", vs.getName());
        assertEquals(-1, vs.getPageSize());
        assertFalse(vs.isCreate());
        assertFalse(vs.isCreateOnly());
        assertFalse(vs.isReadOnly());
        vs2 = validVolumeSpecification(vs.toString());
        assertEquals("Parse of toString should be equal", vs, vs2);

        vs = validVolumeSpecification("/a/b/c.v01");
        assertEquals("c", vs.getName());
        vs2 = validVolumeSpecification(vs.toString());
        assertEquals("Parse of toString should be equal", vs, vs2);

        vs = validVolumeSpecification("/a/b/c.d.v01");
        assertEquals("c.d", vs.getName());
        vs2 = validVolumeSpecification(vs.toString());
        assertEquals("Parse of toString should be equal", vs, vs2);

        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16383,initialsize:10m,maximumsize:100m,extensionsize:10m,create");
        invalidVolumeSpecification("/a/b/c;name:crabcake,pagesize:16384,initialsize:10m,maximumsize:100m,extensionsize:10m,create");
        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16384,initialsize:10p,maximumsize:100p,extensionsize:10p,create");
        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16384,initialsize:10m,maximumsize:100m,extensionsize:10m,create,readOnly");

        vs = validVolumeSpecification("/a/b/c,pageSize: 16384, initialSize: 10m, maximumSize: 100m, extensionSize: 10m, create");
        assertEquals(10 * 1024 * 1024 / 16384, vs.getInitialPages());
        assertEquals(10 * 1024 * 1024, vs.getInitialSize());
        vs.setInitialPages(42);
        vs.setExtensionPages(58);
        vs.setMaximumPages(Integer.MAX_VALUE);

        vs2 = validVolumeSpecification(vs.toString());
        assertEquals("Parse of toString should be equal", vs, vs2);

        vs.setMaximumSize(Long.MAX_VALUE);
        vs.setInitialSize(Long.MAX_VALUE / 7);
        vs.setExtensionSize(Long.MAX_VALUE / 23);

        assertEquals(Long.MAX_VALUE / 23 / 16384, vs.getExtensionPages());

        vs2 = validVolumeSpecification(vs.toString());
        assertEquals("Parse of toString should be equal", vs, vs2);

    }

    @Test
    public void volumeLoadAndSaveGlobalTimestamp() throws Exception {
        final long MARKER = 123456789L;
        _persistit.getTimestampAllocator().updateTimestamp(MARKER);
        final VolumeSpecification vs = validVolumeSpecification("${datapath}/testGlobalTimestamp, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create");

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
        final Exchange ex = _persistit.getExchange(UnitTestProperties.VOLUME_NAME, "VolumeTest", true);
        final Transaction txn = _persistit.getTransaction();
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

        final List<File> journalFiles = _persistit.getJournalManager().unitTestGetAllJournalFiles();
        _persistit.crash();

        /*
         * Worst case (or slipped finger) scenario of missing journal files
         */
        for (final File file : journalFiles) {
            final boolean success = file.delete();
            assertEquals("Deleted journal file " + file.getName(), true, success);
        }

        _persistit = new Persistit(_config);
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
            } catch (final InUseException e) {
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
        } catch (final InvalidVolumeSpecificationException e) {
            fail(specification + " should be valid: " + e);
            return null;
        }
    }

    private void invalidVolumeSpecification(final String specification) throws InvalidVolumeSpecificationException {
        try {
            new VolumeSpecification(specification);
            fail();
        } catch (final InvalidVolumeSpecificationException e) {
            // ok
        }
    }

}
