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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.persistit.ValueHelper.RawValueWriter;

public class BufferMaxPack extends PersistitUnitTestCase {

    @Test
    public void testJoinBuffer() throws Exception {
        final Buffer b1 = _persistit.getBufferPool(16384).get(_persistit.getVolume("persistit"), 1, true, false);
        b1.init(Buffer.PAGE_TYPE_DATA);
        b1.claim(true);
        final Key key = new Key((Persistit) null);
        final Value value = new Value((Persistit) null);
        final RawValueWriter vwriter = new RawValueWriter();
        vwriter.init(value);
        for (int i = 0; i < 4096; i++) {
            key.clear().append(i);
            final int at = b1.putValue(key, vwriter);
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
