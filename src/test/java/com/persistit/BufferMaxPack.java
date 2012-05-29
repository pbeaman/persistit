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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.ValueHelper.RawValueWriter;
import com.persistit.unit.PersistitUnitTestCase;

public class BufferMaxPack extends PersistitUnitTestCase {

    @Test
    public void testJoinBuffer() throws Exception {
        final Buffer b1 = _persistit.getBufferPool(16384).get(_persistit.getVolume("persistit"), 1, true, false);
        b1.init(Buffer.PAGE_TYPE_DATA);
        b1.claim(true);
        final Key key = new Key((Persistit) null);
        final Value value = new Value((Persistit) null);
        RawValueWriter vwriter = new RawValueWriter();
        vwriter.init(value);
        for (int i = 0; i < 4096; i++) {
            key.clear().append(i);
            int at = b1.putValue(key, vwriter);
            if (at < 0) {
                break;
            }
        }

        assertTrue(b1.getFastIndex() != null);
        b1.getFastIndex().invalidate();
        b1.getFastIndex().recompute();
        assertTrue(b1.getFastIndex().isValid());
        assertTrue(b1.getKeyCount() < b1.getBufferSize() / 16);
    }
}
