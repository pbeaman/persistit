/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

import static com.persistit.Buffer.EXACT_MASK;
import static com.persistit.Buffer.FIXUP_MASK;
import static com.persistit.Buffer.KEYBLOCK_LENGTH;
import static com.persistit.Buffer.P_MASK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import com.persistit.ValueHelper.RawValueWriter;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RebalanceException;
import com.persistit.policy.JoinPolicy;
import com.persistit.policy.SplitPolicy;
import com.persistit.unit.PersistitUnitTestCase;

public class BufferTest2 extends PersistitUnitTestCase {

    Exchange ex;
    Buffer b1;
    Buffer b2;
    RawValueWriter vw;
    final StringBuilder sb = new StringBuilder();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ex = _persistit.getExchange("persistit", "BufferTest", true);
        b1 = ex.getBufferPool().get(ex.getVolume(), 1, true, false);
        b2 = ex.getBufferPool().get(ex.getVolume(), 2, true, false);
        while (sb.length() < Buffer.MAX_BUFFER_SIZE) {
            sb.append(RED_FOX);
        }
        vw = new RawValueWriter();
        vw.init(ex.getValue());

    }

    @Override
    public void tearDown() throws Exception {
        ex = null;
        b1 = null;
        b2 = null;
        vw = null;
        super.tearDown();
    }

    @Test
    public void testPreviousKey() throws PersistitException {
        setUpPrettyFullBufferWithChangingEbc(RED_FOX.length());
        ex.getKey().clear().append(Key.AFTER);
        for (int p = b1.getKeyBlockEnd(); p > 0; p -= KEYBLOCK_LENGTH) {
            ex.getKey().copyTo(ex.getAuxiliaryKey1());
            final int result = b1.previousKey(ex.getKey(), p);
            if (p > b1.getKeyBlockStart()) {
                assertTrue("Key is out of sequence", ex.getKey().compareTo(ex.getAuxiliaryKey1()) < 0);
                assertEquals("Wrong result from previousKey", result & P_MASK, p - KEYBLOCK_LENGTH);
            }
        }
    }

    @Test
    public void testFindKey() throws PersistitException {
        setUpPrettyFullBufferWithChangingEbc(RED_FOX.length());
        setUpDeepKey(10);
        int foundAt = b1.findKey(ex.getKey());
        assertTrue("Must be an exact match", (foundAt & EXACT_MASK) != 0);
        ex.append("z");
        foundAt = b1.findKey(ex.getKey());
        assertFalse("Must not be an exact match", (foundAt & EXACT_MASK) != 0);
    }

    @Test
    public void testJoinRightBias1() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_INDEX_MIN, 0, false);

        for (int c = b1.getKeyCount() - 1; c > 0; c -= 7) {
            setUpDeepKey(ex, 'a', c, 0);
            final int foundAt = b1.findKey(ex.getKey());
            b1.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        final boolean splitRight = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4,
                ex.getAuxiliaryKey1(), ex.getAuxiliaryKey2(), JoinPolicy.RIGHT_BIAS);

        assertTrue("Should have re-split", splitRight);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    @Test
    public void testJoinRightBias2() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_INDEX_MIN, 0, false);

        for (int c = b1.getKeyCount() + 1; c < b1.getKeyCount() + b2.getKeyCount(); c += 19) {
            setUpDeepKey(ex, 'a', c, 0);
            final int foundAt = b2.findKey(ex.getKey());
            b2.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        final boolean splitRight = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4,
                ex.getAuxiliaryKey1(), ex.getAuxiliaryKey2(), JoinPolicy.RIGHT_BIAS);

        assertTrue("Should have re-split", splitRight);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    @Test
    public void testJoinLeftBias2() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_INDEX_MIN, 0, false);

        for (int c = b1.getKeyCount() - 1; c > 0; c -= 7) {
            setUpDeepKey(ex, 'a', c, 0);
            final int foundAt = b1.findKey(ex.getKey());
            b1.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        final boolean splitLeft = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4,
                ex.getAuxiliaryKey1(), ex.getAuxiliaryKey2(), JoinPolicy.LEFT_BIAS);

        assertTrue("Should have re-split", splitLeft);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    @Test
    public void testJoinEvenBias1() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_INDEX_MIN, 0, false);

        for (int c = b1.getKeyCount() - 1; c > 0; c -= 7) {
            setUpDeepKey(ex, 'a', c, 0);
            final int foundAt = b1.findKey(ex.getKey());
            b1.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        final boolean splitEven = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4,
                ex.getAuxiliaryKey1(), ex.getAuxiliaryKey2(), JoinPolicy.EVEN_BIAS);

        assertTrue("Should have split", splitEven);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    @Test
    public void testJoinEvenBias2() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_DATA, 30, false);

        for (int c = b1.getKeyCount() - 1; c > 0; c -= 7) {
            setUpDeepKey(ex, 'a', c, 0);
            final int foundAt = b1.findKey(ex.getKey());
            b1.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        final boolean splitEven = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 232,
                ex.getAuxiliaryKey1(), ex.getAuxiliaryKey2(), JoinPolicy.EVEN_BIAS);

        assertTrue("Should have split", splitEven);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    @Test
    public void testRebalanceException() throws Exception {

        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_DATA, RED_FOX.length(), true);

        assertTrue(b1.verify(null, null) == null);
        assertTrue(b2.verify(null, null) == null);
        final int end1 = b1.getKeyBlockEnd();
        final int avail1 = b1.getAvailableSize();
        final int end2 = b2.getKeyBlockEnd();
        final int avail2 = b2.getAvailableSize();

        try {
            /*
             * Now attempt to join the two pages while removing the edge key
             */
            b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4, ex.getAuxiliaryKey1(),
                    ex.getAuxiliaryKey2(), JoinPolicy.EVEN_BIAS);
            fail("RebalanceException not thrown");
        } catch (final RebalanceException e) {
            // expected
        }

        // make sure the buffers weren't changed or corrupted

        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
        assertEquals("RebalanceException modified the page", end1, b1.getKeyBlockEnd());
        assertEquals("RebalanceException modified the page", end2, b2.getKeyBlockEnd());
        assertEquals("RebalanceException modified the page", avail1, b1.getAvailableSize());
        assertEquals("RebalanceException modified the page", avail2, b2.getAvailableSize());
    }

    @Test
    public void testSplitAtEqualsInsertAt() throws Exception {
        b1.init(Buffer.PAGE_TYPE_DATA);
        b2.init(Buffer.PAGE_TYPE_DATA);
        ex.clear();
        setUpValue(true, RED_FOX.length());

        // count how many will fit
        int a;
        for (a = 1;; a++) {
            ex.to(String.format("%6dz", a));
            if (b1.putValue(ex.getKey(), vw) == -1) {
                break;
            }
        }

        b1.init(Buffer.PAGE_TYPE_DATA);
        for (int b = 1; b < a - 1; b++) {
            ex.to(String.format("%6dz", b));
            b1.putValue(ex.getKey(), vw);
        }

        ex.getValue().clear();
        ex.to(String.format("%6dz", a - 1));
        assertTrue("Expected empty value to fit at end of buffer", b1.putValue(ex.getKey(), vw) != -1);
        /*
         * Split on key being inserted.
         */
        setUpValue(true, RED_FOX.length());
        ex.to(String.format("%6d", a / 2 + 1));
        final int foundAt1 = b1.findKey(ex.getKey());
        assertTrue("Expect FIXUP_REQUIRED flag", (foundAt1 & FIXUP_MASK) != 0);
        final int splitAt1 = b1.split(b2, ex.getKey(), vw, foundAt1, ex.getAuxiliaryKey1(), Exchange.Sequence.NONE,
                SplitPolicy.EVEN_BIAS);
        assertTrue("Split failed", splitAt1 != -1);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);

        /*
         * Restore buffer setup
         */
        b1.init(Buffer.PAGE_TYPE_DATA);
        b2.init(Buffer.PAGE_TYPE_DATA);
        b1.init(Buffer.PAGE_TYPE_DATA);
        setUpValue(true, RED_FOX.length());
        for (int b = 1; b < a - 1; b++) {
            ex.to(String.format("%6dz", b));
            b1.putValue(ex.getKey(), vw);
        }

        /*
         * Split on key being replaced
         */
        ex.getValue().clear();
        ex.to(String.format("%6dz", a - 1));
        assertTrue("Expected empty value to fit at end of buffer", b1.putValue(ex.getKey(), vw) != -1);
        setUpValue(true, RED_FOX.length());
        ex.to(String.format("%6dz", a / 2));
        final int foundAt2 = b1.findKey(ex.getKey());
        assertTrue("Expect EXACT flag", (foundAt2 & Buffer.EXACT_MASK) != 0);
        setUpValue(true, RED_FOX.length() * 2);
        final int splitAt2 = b1.split(b2, ex.getKey(), vw, foundAt2, ex.getAuxiliaryKey1(), Exchange.Sequence.NONE,
                SplitPolicy.EVEN_BIAS);
        assertTrue("Split failed", splitAt2 != -1);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);

        /*
         * Restore buffer setup
         */
        b1.init(Buffer.PAGE_TYPE_DATA);
        b2.init(Buffer.PAGE_TYPE_DATA);
        b1.init(Buffer.PAGE_TYPE_DATA);
        setUpValue(true, RED_FOX.length());
        for (int b = 1; b < a - 1; b++) {
            ex.to(String.format("%6dz", b));
            b1.putValue(ex.getKey(), vw);
        }

        /*
         * Split on key being replaced at end of buffer
         */
        ex.getValue().clear();
        ex.to(String.format("%6dz", a - 1));
        assertTrue("Expected empty value to fit at end of buffer", b1.putValue(ex.getKey(), vw) != -1);
        setUpValue(true, RED_FOX.length());
        ex.to(String.format("%6dz", a - 2));
        final int foundAt3 = b1.findKey(ex.getKey());
        assertTrue("Expect EXACT flag", (foundAt3 & Buffer.EXACT_MASK) != 0);
        setUpValue(true, RED_FOX.length() * 2);
        final int splitAt3 = b1.split(b2, ex.getKey(), vw, foundAt3, ex.getAuxiliaryKey1(), Exchange.Sequence.NONE,
                SplitPolicy.RIGHT_BIAS);
        assertTrue("Split failed", splitAt3 != -1);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    @Test
    public void testRemoveKeys() throws Exception {
        setUpPrettyFullBufferWithChangingEbc(6);
        setUpDeepKey(104);
        int foundAt1 = b1.findKey(ex.getKey());
        setUpDeepKey(112);
        int foundAt2 = b1.findKey(ex.getKey());
        if ((foundAt1 & Buffer.P_MASK) > (foundAt2 & Buffer.P_MASK)) {
            final int t = foundAt1;
            foundAt1 = foundAt2;
            foundAt2 = t;
        }
        b1.removeKeys(foundAt1, foundAt1, ex.getAuxiliaryKey1());
        assertTrue("Verify failed", b1.verify(null, null) == null);
        b1.removeKeys(foundAt1 & ~EXACT_MASK, foundAt2, ex.getAuxiliaryKey1());
    }

    @Test
    public void splitAndJoinBuffersWithZeroEbcKeys() throws Exception {
        b1.init(Buffer.PAGE_TYPE_DATA);
        b2.init(Buffer.PAGE_TYPE_DATA);
        setUpValue(true, b1.getBufferSize() / 100);

        int a;
        for (a = 1;; a++) {
            setUpShallowKey(a);
            if (b1.putValue(ex.getKey(), vw) == -1) {
                break;
            }
        }
        setUpShallowKey(a / 2);
        ex.getKey().getEncodedBytes()[6] = 0x7F;
        ex.getKey().setEncodedSize(7);
        final int foundAt = b1.findKey(ex.getKey());
        final int splitAt = b1.split(b2, ex.getKey(), vw, foundAt, ex.getAuxiliaryKey1(), Exchange.Sequence.NONE,
                SplitPolicy.EVEN_BIAS);

        assertTrue("Split failed", splitAt != -1);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);

        for (int b = 1; b < a; b++) {
            setUpShallowKey(b);
            if (b <= a / 2) {
                b1.fetch(b1.findKey(ex.getKey()), ex.getValue());
            } else {
                b2.fetch(b2.findKey(ex.getKey()), ex.getValue());
            }
            assertTrue("Value should be defined", ex.getValue().isDefined());
        }

        setUpShallowKey(a / 2);
        ex.getKey().getEncodedBytes()[6] = 0x7F;
        ex.getKey().setEncodedSize(7);
        final int foundAt1 = b1.findKey(ex.getKey());
        int foundAt2 = b2.findKey(ex.getKey());
        assertTrue("Should have found exact match", (foundAt2 & EXACT_MASK) > 0);
        foundAt2 = b2.nextKeyBlock(foundAt2);
        final boolean rebalanced = b1.join(b2, foundAt1, foundAt2, ex.getKey(), ex.getAuxiliaryKey1(),
                JoinPolicy.EVEN_BIAS);
        assertTrue("Should have joined pages", !rebalanced);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    private void setUpPrettyFullBufferWithChangingEbc(final int valueLength) throws PersistitException {
        b1.init(Buffer.PAGE_TYPE_DATA);
        setUpValue(false, valueLength);
        int a;
        for (a = 0;; a++) {
            setUpDeepKey(a);
            if (b1.putValue(ex.getKey(), vw) == -1) {
                a -= 1;
                break;
            }
        }
    }

    private void setUpPrettyFullBuffers(final int type, final int valueLength, final boolean discontinuous)
            throws PersistitException {
        b1.init(type);
        b2.init(type);
        final boolean isData = type == Buffer.PAGE_TYPE_DATA;

        int a, b;

        // load page A with keys of increasing length
        for (a = 10;; a++) {
            setUpDeepKey(ex, 'a', a, isData ? 0 : a + 1000000);
            setUpValue(isData, valueLength);
            if (b1.putValue(ex.getKey(), vw) == -1) {
                a -= 1;
                break;
            }
        }

        // set up the right edge key in page A
        setUpDeepKey(ex, 'a', a, isData ? 0 : -1);
        ex.getValue().clear();
        b1.putValue(ex.getKey(), vw);

        // set up the left edge key in page B
        setUpDeepKey(ex, 'a', a, isData ? 0 : a + 1000000);
        setUpValue(isData, valueLength);
        b2.putValue(ex.getKey(), vw);

        // load additional keys into page B
        for (b = a;; b++) {
            setUpDeepKey(ex, discontinuous && b > a ? 'b' : 'a', b, b + 1000000);
            setUpValue(isData, valueLength);
            if (b2.putValue(ex.getKey(), vw) == -1) {
                break;
            }
        }
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    private void setUpValue(final boolean isData, final int length) {
        if (isData) {
            ex.getValue().put(sb.toString().substring(0, length));
        } else {
            ex.getValue().clear();
        }
    }

    private void setUpDeepKey(final int a) {
        setUpDeepKey(ex, "abcdefg".charAt(a % 7), (a % (2000 / 13)) * 13, 0);
    }

    private void setUpDeepKey(final Exchange ex, final char fill, final int n, final long pointer) {
        ex.getKey().clear().append(keyString(fill, n, n - 4, 4, n)).append(1);
        ex.getValue().setPointerValue(pointer);
    }

    private void setUpShallowKey(final int a) {
        setUpShallowKey(ex, a, 0);
    }

    private void setUpShallowKey(final Exchange ex, final int value, final long pointer) {
        final byte[] bytes = ex.getKey().clear().getEncodedBytes();
        Arrays.fill(bytes, (byte) 0);
        /*
         * Convert int to little-endian form so that byte 0 varies fastest. 7
         * bits per byte, no zeroes.
         */
        bytes[0] = (byte) (((value >>> 0) & 0x7F) + 1);
        bytes[1] = (byte) (((value >>> 7) & 0x7F) + 1);
        bytes[2] = (byte) (((value >>> 14) & 0x7F) + 1);
        bytes[3] = (byte) (((value >>> 21) & 0x7F) + 1);
        bytes[5] = (byte) (((value >>> 28) & 0x7F) + 1);
        ex.getKey().setEncodedSize(6);
        ex.getValue().setPointerValue(pointer);
    }

    String keyString(final char fill, final int length, final int prefix, final int width, final int k) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < prefix && i < length; i++) {
            sb.append(fill);
        }
        sb.append(String.format("%0" + width + "d", k));
        for (int i = length - sb.length(); i < length; i++) {
            sb.append(fill);
        }
        sb.setLength(length);
        return sb.toString();
    }

}
