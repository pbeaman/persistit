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
        final TreeSelector ts = TreeSelector.parseSelector("*", false, '\\');
        assertTrue(ts.isVolumeNameSelected("mydata"));
        assertTrue(ts.isVolumeNameSelected("mystuff"));
        assertTrue(ts.isTreeNameSelected("mydata", "anindex"));
        assertTrue(ts.isTreeNameSelected("mydata", "anxedni"));
    }

    @Test
    public void simpleSelector() throws Exception {
        final TreeSelector ts = TreeSelector.parseSelector("*data:*index", false, '\\');
        assertTrue(ts.isVolumeNameSelected("mydata"));
        assertFalse(ts.isVolumeNameSelected("mystuff"));
        assertTrue(ts.isTreeNameSelected("mydata", "anindex"));
        assertFalse(ts.isTreeNameSelected("mydata", "anxedni"));
    }

    @Test
    public void volumeOnlySelector() throws Exception {
        final TreeSelector ts = TreeSelector.parseSelector("akiban_data*", false, '\\');
        assertTrue(ts.isVolumeNameSelected("akiban_data.v01"));
        assertFalse(ts.isVolumeNameSelected("mystuff"));
        assertTrue(ts.isTreeNameSelected("akiban_data.v01", "anindex"));
        assertTrue(ts.isTreeNameSelected("akiban_data.v01", "_directory"));
    }

    @Test
    public void toxicCharactersSelector() throws Exception {
        final TreeSelector ts = TreeSelector.parseSelector("*:customer$$group(something...*)[a-z]$$$$27", false, '\\');
        assertTrue(ts.isTreeNameSelected("akiban_data.v01", "customer$$group(something...)[a-z]$$$$27"));
        assertTrue(ts.isTreeNameSelected("akiban_data.v01", "customer$$group(something...xyz)[a-z]$$$$27"));
        assertFalse(ts.isTreeNameSelected("akiban_data.v01", "customer$$group(something...)m$$$$27"));
    }

    @Test
    public void simpleSelectorWithKeyFilter() throws Exception {
        final TreeSelector ts = TreeSelector.parseSelector("*data:*index{1:2}", false, '\\');
        assertTrue(ts.isVolumeNameSelected("mydata"));
        assertFalse(ts.isVolumeNameSelected("mystuff"));
        assertTrue(ts.isTreeNameSelected("mydata", "anindex"));
        assertFalse(ts.isTreeNameSelected("mydata", "anxedni"));
        assertNotNull(ts.keyFilter("mydata", "anindex"));
        assertNull(ts.keyFilter("mydata", "someothertree"));
    }

    @Test
    public void simpleTreeList() throws Exception {
        final TreeSelector ts = TreeSelector.parseSelector("v1:t1,v1:t2,v1:t3,v1:t4", false, '\\');
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
        final TreeSelector ts = TreeSelector.parseSelector("v1:t1{1:10,\"a\"},v?:t?{10:20},x*:other*", false, '\\');
        assertTrue(ts.isVolumeNameSelected("v1"));
        assertTrue(ts.isVolumeNameSelected("v9"));
        assertTrue(ts.isVolumeNameSelected("xanything"));
        assertFalse(ts.isVolumeNameSelected("yanything"));
        assertTrue(ts.isTreeNameSelected("v1", "t2"));
        assertNotNull(ts.keyFilter("v1", "t2"));
        try {
            ts.keyFilter("v1", "t1");
            fail();
        } catch (final Exception e) {
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
