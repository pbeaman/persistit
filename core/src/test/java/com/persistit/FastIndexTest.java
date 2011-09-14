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

import java.util.Random;

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
    
    public void testFastIndexRecompute() throws Exception {
        Buffer b1 = getABuffer();
        b1.init(Buffer.PAGE_TYPE_DATA);
        Key key = new Key(_persistit);
        Value value = new Value(_persistit);
        fakeKey(key, "A");
        b1.putValue(key, value);
        fakeKey(key, "ABC");
        b1.putValue(key, value);
        fakeKey(key, "ABK");
        b1.putValue(key, value);
        fakeKey(key, "ABZ");
        b1.putValue(key, value);
        fakeKey(key, "AC");
        b1.putValue(key, value);
        fakeKey(key, "C");
        b1.putValue(key, value);
        fakeKey(key, "B");
        b1.putValue(key, value);
        fakeKey(key, "E");
        b1.putValue(key, value);
        fakeKey(key, "D");
        b1.putValue(key, value);
        fakeKey(key, "DA");
        b1.putValue(key, value);
        fakeKey(key, "ABB");
        b1.putValue(key, value);
        fakeKey(key, "ABA");
        b1.putValue(key, value);
        fakeKey(key, "ABJ");
        b1.putValue(key, value);
        
        FastIndex fi = b1.getFastIndex();
        String inserteds = fi.toString();
        fi.recompute();
        String computeds = fi.toString();
        assertEquals(inserteds, computeds);
        b1.release();
    }
    
    private void fakeKey( final Key key, final String v) {
        key.clear().append(v);
        System.arraycopy(key.getEncodedBytes(), 1, key.getEncodedBytes(), 0, key.getEncodedSize());
        key.setEncodedSize(key.getEncodedSize() - 1);
    }

    public void testRandomInsert() throws Exception {
        Random random = new Random(3);
        Buffer b1 = getABuffer();
        FastIndex f1 = b1.getFastIndex();
        
        b1.init(Buffer.PAGE_TYPE_DATA);
        Key key = new Key(_persistit);
        Value value = new Value(_persistit);
        for (int i = 0; i < 1000; i++) {
            int size = random.nextInt(10) + 2;
            byte[] bytes = new byte[size];
            random.nextBytes(bytes);
            System.arraycopy(bytes, 0, key.getEncodedBytes(), 0, size);
            key.setEncodedSize(size);
            b1.putValue(key, value);

            String s1 = f1.toString();
            f1.recompute();
            String s2 = f1.toString();
            assertEquals(s1, s2);
        }
        b1.release();
    }
    
    
    @Override
    public void runAllTests() {
    }

}
