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

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

}
