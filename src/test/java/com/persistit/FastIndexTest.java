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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

import com.persistit.ValueHelper.RawValueWriter;

public class FastIndexTest extends PersistitUnitTestCase {

    private Buffer getABuffer() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "FastIndexTest", true);
        return exchange.getBufferPool().get(exchange.getVolume(), 1, true, true);
    }

    @Test
    public void testIndexSize() throws Exception {
        final Buffer b1 = getABuffer();
        try {
            final FastIndex fi = b1.getFastIndex();
            /*
             * BUFFER_SIZE - HEADER_SIZE / MAX_KEY_RATIO BUFFER_SIZE = 16384
             * HEADER_SIZE = 32 MAX_KEY_RATIO = 16 16384 - 32 / 16 = 16352 / 16
             * = 1022 size passed to FastIndex constructor is 1022 + 1 when
             * parameters are as listed above.
             */
            final int expectedSize = (16384 - Buffer.HEADER_SIZE) / Buffer.MAX_KEY_RATIO;
            assertEquals(expectedSize + 1, fi.size());
        } finally {
            b1.release();
        }
    }

    @Test
    public void testIndexValidity() throws Exception {
        final Buffer b1 = getABuffer();
        try {
            b1.init(Buffer.PAGE_TYPE_GARBAGE);
            final FastIndex fi = b1.getFastIndex();
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
        } finally {
            b1.release();
        }
    }

    @Test
    public void testFastIndexRecompute() throws Exception {
        final Buffer b1 = getABuffer();
        try {
            b1.init(Buffer.PAGE_TYPE_DATA);
            final Key key = new Key(_persistit);
            final Value value = new Value(_persistit);
            final RawValueWriter vwriter = new RawValueWriter();
            final FastIndex fi = b1.getFastIndex();
            fi.recompute();
            vwriter.init(value);
            fakeKey(key, "A");
            b1.putValue(key, vwriter);
            fakeKey(key, "ABC");
            b1.putValue(key, vwriter);
            fakeKey(key, "ABK");
            b1.putValue(key, vwriter);
            fakeKey(key, "ABZ");
            b1.putValue(key, vwriter);
            fakeKey(key, "AC");
            b1.putValue(key, vwriter);
            fakeKey(key, "C");
            b1.putValue(key, vwriter);
            fakeKey(key, "B");
            b1.putValue(key, vwriter);
            fakeKey(key, "E");
            b1.putValue(key, vwriter);
            fakeKey(key, "D");
            b1.putValue(key, vwriter);
            fakeKey(key, "DA");
            b1.putValue(key, vwriter);
            fakeKey(key, "ABB");
            b1.putValue(key, vwriter);
            fakeKey(key, "ABA");
            b1.putValue(key, vwriter);
            fakeKey(key, "ABJ");
            b1.putValue(key, vwriter);

            final String inserteds = fi.toString();
            fi.recompute();
            final String computeds = fi.toString();
            assertEquals(inserteds, computeds);
        } finally {
            b1.release();
        }
    }

    private void fakeKey(final Key key, final String v) {
        key.clear().append(v);
        System.arraycopy(key.getEncodedBytes(), 1, key.getEncodedBytes(), 0, key.getEncodedSize());
        key.setEncodedSize(key.getEncodedSize() - 1);
    }

    @Test
    public void testRandomInsert() throws Exception {
        final Random random = new Random(3);
        final Buffer b1 = getABuffer();
        try {
            final FastIndex f1 = b1.getFastIndex();

            b1.init(Buffer.PAGE_TYPE_DATA);
            final Key key = new Key(_persistit);
            final Value value = new Value(_persistit);
            for (int i = 0; i < 1000; i++) {
                final int size = random.nextInt(10) + 2;
                final byte[] bytes = new byte[size];
                random.nextBytes(bytes);
                System.arraycopy(bytes, 0, key.getEncodedBytes(), 0, size);
                key.setEncodedSize(size);
                final RawValueWriter vwriter = new RawValueWriter();
                vwriter.init(value);
                b1.putValue(key, vwriter);

                final String s1 = f1.toString();
                f1.recompute();
                final String s2 = f1.toString();
                assertEquals(s1, s2);
            }
        } finally {
            b1.release();
        }
    }

}
