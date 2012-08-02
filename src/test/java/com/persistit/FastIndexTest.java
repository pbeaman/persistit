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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

import com.persistit.ValueHelper.RawValueWriter;
import com.persistit.unit.PersistitUnitTestCase;

public class FastIndexTest extends PersistitUnitTestCase {

    private Buffer getABuffer() throws Exception {
        final Exchange exchange = _persistit.getExchange("persistit", "FastIndexTest", true);
        return exchange.getBufferPool().get(exchange.getVolume(), 1, false, true);
    }

    @Test
    public void testIndexSize() throws Exception {
        final Buffer b1 = getABuffer();
        try {
            FastIndex fi = b1.getFastIndex();
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
        } finally {
            b1.release();
        }
    }

    @Test
    public void testFastIndexRecompute() throws Exception {
        Buffer b1 = getABuffer();
        try {
            b1.init(Buffer.PAGE_TYPE_DATA);
            Key key = new Key(_persistit);
            Value value = new Value(_persistit);
            RawValueWriter vwriter = new RawValueWriter();
            FastIndex fi = b1.getFastIndex();
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

            String inserteds = fi.toString();
            fi.recompute();
            String computeds = fi.toString();
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
        Random random = new Random(3);
        Buffer b1 = getABuffer();
        try {
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
                RawValueWriter vwriter = new RawValueWriter();
                vwriter.init(value);
                b1.putValue(key, vwriter);

                String s1 = f1.toString();
                f1.recompute();
                String s2 = f1.toString();
                assertEquals(s1, s2);
            }
        } finally {
            b1.release();
        }
    }

}
