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

package com.persistit.unit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import org.junit.Test;

import com.persistit.TreeSelector;

public class TreeSelectorTest {

    @Test
    public void allSelector() throws Exception {
        TreeSelector ts = TreeSelector.parseSelector("*", false, '\\');
        assertTrue(ts.isVolumeNameSelected("mydata"));
        assertTrue(ts.isVolumeNameSelected("mystuff"));
        assertTrue(ts.isTreeNameSelected("mydata", "anindex"));
        assertTrue(ts.isTreeNameSelected("mydata", "anxedni"));
    }

    @Test
    public void simpleSelector() throws Exception {
        TreeSelector ts = TreeSelector.parseSelector("*data:*index", false, '\\');
        assertTrue(ts.isVolumeNameSelected("mydata"));
        assertFalse(ts.isVolumeNameSelected("mystuff"));
        assertTrue(ts.isTreeNameSelected("mydata", "anindex"));
        assertFalse(ts.isTreeNameSelected("mydata", "anxedni"));
    }

    @Test
    public void volumeOnlySelector() throws Exception {
        TreeSelector ts = TreeSelector.parseSelector("akiban_data*", false, '\\');
        assertTrue(ts.isVolumeNameSelected("akiban_data.v01"));
        assertFalse(ts.isVolumeNameSelected("mystuff"));
        assertTrue(ts.isTreeNameSelected("akiban_data.v01", "anindex"));
        assertTrue(ts.isTreeNameSelected("akiban_data.v01", "_directory"));
    }

    @Test
    public void toxicCharactersSelector() throws Exception {
        TreeSelector ts = TreeSelector.parseSelector("*:customer$$group(something...*)[a-z]$$$$27", false, '\\');
        assertTrue(ts.isTreeNameSelected("akiban_data.v01", "customer$$group(something...)[a-z]$$$$27"));
        assertTrue(ts.isTreeNameSelected("akiban_data.v01", "customer$$group(something...xyz)[a-z]$$$$27"));
        assertFalse(ts.isTreeNameSelected("akiban_data.v01", "customer$$group(something...)m$$$$27"));
    }

    @Test
    public void simpleSelectorWithKeyFilter() throws Exception {
        TreeSelector ts = TreeSelector.parseSelector("*data:*index{1:2}", false, '\\');
        assertTrue(ts.isVolumeNameSelected("mydata"));
        assertFalse(ts.isVolumeNameSelected("mystuff"));
        assertTrue(ts.isTreeNameSelected("mydata", "anindex"));
        assertFalse(ts.isTreeNameSelected("mydata", "anxedni"));
        assertNotNull(ts.keyFilter("mydata", "anindex"));
        assertNull(ts.keyFilter("mydata", "someothertree"));
    }

    @Test
    public void simpleTreeList() throws Exception {
        TreeSelector ts = TreeSelector.parseSelector("v1:t1,v1:t2,v1:t3,v1:t4", false, '\\');
        assertTrue(ts.isVolumeNameSelected("v1"));
        assertFalse(ts.isVolumeNameSelected("v2"));
        assertTrue(ts.isTreeNameSelected("v1", "t1"));
        assertTrue(ts.isTreeNameSelected("v1", "t2"));
        assertTrue(ts.isTreeNameSelected("v1", "t3"));
        assertTrue(ts.isTreeNameSelected("v1", "t4"));
        assertFalse(ts.isTreeNameSelected("v1", "t5"));
        assertFalse(ts.isTreeNameSelected("v2", "t1"));
        assertEquals("v1:t1,v1:t2,v1:t3,v1:t4", ts.toString());
    }

    @Test
    public void complexCase() throws Exception {
        TreeSelector ts = TreeSelector.parseSelector("v1:t1{1:10,\"a\"},v?:t?{10:20},x*:other*", false, '\\');
        assertTrue(ts.isVolumeNameSelected("v1"));
        assertTrue(ts.isVolumeNameSelected("v9"));
        assertTrue(ts.isVolumeNameSelected("xanything"));
        assertFalse(ts.isVolumeNameSelected("yanything"));
        assertTrue(ts.isTreeNameSelected("v1", "t2"));
        assertNotNull(ts.keyFilter("v1", "t2"));
        try {
            ts.keyFilter("v1", "t1");
            fail();
        } catch (Exception e) {
            // okay
        }
    }

    @Test
    public void emptyCase() throws Exception {
        TreeSelector ts = TreeSelector.parseSelector("", false, '\\');
        assertTrue(ts.isSelectAll());
        ts = TreeSelector.parseSelector("*", false, '\\');
        assertTrue(ts.isSelectAll());
    }

}
