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
