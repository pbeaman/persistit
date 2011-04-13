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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BufferMaxPack {

    @Test
    public void testJoinBuffer() throws Exception {
        final Persistit db = new Persistit();
        final Buffer b1 = new Buffer(16384, 0, null, db);
        b1.init(Buffer.PAGE_TYPE_DATA);
        b1.claim(true);
        final Key key = new Key((Persistit) null);
        final Value value = new Value((Persistit) null);
        for (int i = 0; i < 4096; i++) {
            key.clear().append(i);
            int at = b1.putValue(key, value);
            if (at < 0) {
                break;
            }
        }
        b1.invalidateFindex();
        b1.recomputeFindex();
        assertTrue(b1.getKeyCount() < b1.getBufferSize() / 16);
    }
}
