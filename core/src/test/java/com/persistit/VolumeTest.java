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

import java.io.File;
import java.io.RandomAccessFile;

import org.junit.Test;

import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.VolumeFullException;
import com.persistit.unit.PersistitUnitTestCase;

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
        VolumeSpecification vs = validVolumeSpecification(_persistit.substituteProperties(
                "${datapath}/vtest, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create", _persistit
                        .getProperties()));
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
        VolumeSpecification vs = validVolumeSpecification(_persistit.substituteProperties(
                "${datapath}/vtest, pageSize:16k, initialSize:1k, maximumSize:1m, extensionSize:1K, create", _persistit
                        .getProperties()));
        Volume volume1 = new Volume(vs);
        volume1.open(_persistit);
        File file = new File(volume1.getPath());
        assertEquals(32768, file.length());
        assertTrue(_persistit.deleteVolume("vtest"));
        assertTrue(volume1.isClosed());
        assertTrue(!file.exists());
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

        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16383,initialsize:10m,maximumsize:100m,extensionsize:10m,create");
        invalidVolumeSpecification("/a/b/c;name:crabcake,pagesize:16384,initialsize:10m,maximumsize:100m,extensionsize:10m,create");
        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16384,initialsize:10p,maximumsize:100p,extensionsize:10p,create");
        invalidVolumeSpecification("/a/b/c,name:crabcake,pagesize:16384,initialsize:10m,maximumsize:100m,extensionsize:10m,create,readOnly");
    }

    private VolumeSpecification validVolumeSpecification(final String specification) throws Exception {
        try {
            return new VolumeSpecification(specification);
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

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

}
