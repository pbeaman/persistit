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

import com.persistit.Buffer;
import com.persistit.FastIndex;
import com.persistit.unit.PersistitUnitTestCase;

public class FastIndexTest extends PersistitUnitTestCase {

    private Buffer getABuffer() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "FastIndexTest", true);
        return exchange.getBufferPool().get(exchange.getVolume(), 1, false, true);
    }

    public void testIndexSize() throws Exception {
        final Buffer b1 = getABuffer();
        FastIndex fi = b1.getFastIndex();
        /*
         * BUFFER_SIZE - HEADER_SIZE / MAX_KEY_RATIO BUFFER_SIZE = 16384
         * HEADER_SIZE = 32 MAX_KEY_RATIO = 16 16384 - 32 / 16 = 16352 / 16 =
         * 1022 size passed to FastIndex constructor is 1022 + 1 when parameters
         * are as listed above.
         */
        final int expectedSize = (16384 - Buffer.HEADER_SIZE) / Buffer.MAX_KEY_RATIO;
        assertEquals(expectedSize + 1, fi.size());
        b1.release();
    }

    public void testIndexValidity() throws Exception {
        final Buffer b1 = getABuffer();
        b1.init(Buffer.PAGE_TYPE_GARBAGE);
        FastIndex fi = b1.getFastIndex();
        fi.invalidate();
        assertEquals(false, fi.isValid());
        fi.recompute();
        assertEquals(false, fi.isValid());
        b1.init(Buffer.PAGE_TYPE_DATA);
        fi.recompute();
        assertEquals(true, fi.isValid());
        assertEquals(true, fi.verify());
        fi.invalidate();
        assertEquals(false, fi.isValid());
        assertEquals(false, fi.verify());
        b1.release();
    }

    public void testBitShiftFunctions() throws Exception {
        final Buffer b1 = getABuffer();
        FastIndex fi = b1.getFastIndex();
        fi.putDescriminatorByte(0, 55);
        assertEquals(55, fi.getDescriminatorByte(0));
        /* try putting a descriminator byte larger than allowed... */
        fi.putDescriminatorByte(1, 266);
        /* since 266 is 0001 0000 1010 in binary */
        assertEquals(10, fi.getDescriminatorByte(1));
        fi.putRunCount(0, 266);
        assertEquals(266, fi.getRunCount(0));
        fi.putEbc(0, 111);
        assertEquals(111, fi.getEbc(0));
        /* zero out EBC and run count but not descriminator byte */
        fi.putZero(0);
        assertEquals(55, fi.getDescriminatorByte(0));
        assertEquals(0, fi.getRunCount(0));
        assertEquals(0, fi.getEbc(0));
        b1.release();
    }

    @Override
    public void runAllTests() {
    }

}
