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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;

import org.junit.Test;

import com.persistit.Value;
import com.persistit.Value.Version;
import com.persistit.unit.PersistitUnitTestCase;

public class ValueTest7 extends PersistitUnitTestCase {

    @Test
    public void testUnpackMvvVersionsPrimordial() throws Exception {

        List<Version> versions;
        Version version;
        Value value = new Value(_persistit);

        versions = value.unpackMvvVersions();
        assertEquals(1, versions.size());
        version = versions.get(0);
        assertFalse(version.getValue().isDefined());

        value.put(RED_FOX);
        versions = value.unpackMvvVersions();
        assertEquals(1, versions.size());
        version = versions.get(0);
        assertEquals(0, version.getCommitTimestamp());
        assertEquals(RED_FOX, version.getValue().get());
    }

    @Test
    public void testUnpackMvvVersions() throws Exception {
        Value value = new Value(_persistit);
        value.ensureFit(100000);
        Value v = new Value(_persistit);
        for (int i = 1; i < 5; i++) {
            v.put(RED_FOX + "_" + i);
            int s = TestShim.storeVersion(value.getEncodedBytes(), 0, value.getEncodedSize(), 100000, i, v
                    .getEncodedBytes(), 0, v.getEncodedSize());
            value.setEncodedSize(s);
        }
        List<Version> versions = value.unpackMvvVersions();
        for (int i = 0; i < 5; i++) {
            final Version version = versions.remove(0);
            assertEquals(i, version.getVersionHandle());
            if (i > 0) {
                assertEquals(RED_FOX + "_" + i, version.getValue().get());
            }
        }

    }
}
