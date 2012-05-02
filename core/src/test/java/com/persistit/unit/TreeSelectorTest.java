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
