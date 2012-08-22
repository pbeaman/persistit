/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
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
