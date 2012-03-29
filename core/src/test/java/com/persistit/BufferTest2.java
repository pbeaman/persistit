/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import static com.persistit.Buffer.*;
import com.persistit.ValueHelper.RawValueWriter;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
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

    public void testPreviousKey() throws PersistitException {
        setUpPrettyFullBufferWithChangingEbc(RED_FOX.length());
        ex.getKey().clear().append(Key.AFTER);
        for (int p = b1.getKeyBlockEnd(); p > 0; p -= KEYBLOCK_LENGTH) {
            ex.getKey().copyTo(ex.getAuxiliaryKey1());
            int result = b1.previousKey(ex.getKey(), p);
            if (p > b1.getKeyBlockStart()) {
                assertTrue("Key is out of sequence", ex.getKey().compareTo(ex.getAuxiliaryKey1()) < 0);
                assertEquals("Wrong result from previousKey", result & P_MASK, p - KEYBLOCK_LENGTH);
            }
        }
    }
    
    public void testFindKey() throws PersistitException {
        setUpPrettyFullBufferWithChangingEbc(RED_FOX.length());
        setUpDeepKey(10);
        int foundAt = b1.findKey(ex.getKey());
        assertTrue("Must be an exact match", (foundAt & EXACT_MASK) != 0);
        ex.append("z");
        foundAt = b1.findKey(ex.getKey());
        assertFalse("Must not be an exact match", (foundAt & EXACT_MASK) != 0);
    }

    public void testJoinRightBias1() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_INDEX_MIN, 0, false);

        for (int c = b1.getKeyCount() - 1; c > 0; c -= 7) {
            setUpDeepKey(ex, 'a', c, 0);
            int foundAt = b1.findKey(ex.getKey());
            b1.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        boolean splitRight = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4, ex.getAuxiliaryKey1(), ex
                .getAuxiliaryKey2(), JoinPolicy.RIGHT_BIAS);

        assertTrue("Should have re-split", splitRight);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    public void testJoinRightBias2() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_INDEX_MIN, 0, false);

        for (int c = b1.getKeyCount() + 1; c < b1.getKeyCount() + b2.getKeyCount(); c += 19) {
            setUpDeepKey(ex, 'a', c, 0);
            int foundAt = b2.findKey(ex.getKey());
            b2.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        boolean splitRight = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4, ex.getAuxiliaryKey1(), ex
                .getAuxiliaryKey2(), JoinPolicy.RIGHT_BIAS);

        assertTrue("Should have re-split", splitRight);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    public void testJoinLeftBias2() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_INDEX_MIN, 0, false);

        for (int c = b1.getKeyCount() - 1; c > 0; c -= 7) {
            setUpDeepKey(ex, 'a', c, 0);
            int foundAt = b1.findKey(ex.getKey());
            b1.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        boolean splitLeft = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4, ex.getAuxiliaryKey1(), ex
                .getAuxiliaryKey2(), JoinPolicy.LEFT_BIAS);

        assertTrue("Should have re-split", splitLeft);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    public void testJoinEvenBias1() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_INDEX_MIN, 0, false);

        for (int c = b1.getKeyCount() - 1; c > 0; c -= 7) {
            setUpDeepKey(ex, 'a', c, 0);
            int foundAt = b1.findKey(ex.getKey());
            b1.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        boolean splitEven = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4, ex.getAuxiliaryKey1(), ex
                .getAuxiliaryKey2(), JoinPolicy.EVEN_BIAS);

        assertTrue("Should have split", splitEven);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    public void testJoinEvenBias2() throws PersistitException {
        setUpPrettyFullBuffers(Buffer.PAGE_TYPE_DATA, 30, false);

        for (int c = b1.getKeyCount() - 1; c > 0; c -= 7) {
            setUpDeepKey(ex, 'a', c, 0);
            int foundAt = b1.findKey(ex.getKey());
            b1.removeKeys(foundAt, foundAt, ex.getAuxiliaryKey1());
        }

        boolean splitEven = b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 232, ex.getAuxiliaryKey1(), ex
                .getAuxiliaryKey2(), JoinPolicy.EVEN_BIAS);

        assertTrue("Should have split", splitEven);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

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
            b1.join(b2, b1.getKeyBlockEnd() - 4, b2.getKeyBlockStart() + 4, ex.getAuxiliaryKey1(), ex
                    .getAuxiliaryKey2(), JoinPolicy.EVEN_BIAS);
            fail("RebalanceException not thrown");
        } catch (RebalanceException e) {
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
        int foundAt1 = b1.findKey(ex.getKey());
        assertTrue("Expect FIXUP_REQUIRED flag", (foundAt1 & FIXUP_MASK) != 0);
        int splitAt1 = b1.split(b2, ex.getKey(), vw, foundAt1, ex.getAuxiliaryKey1(), Exchange.Sequence.NONE,
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
        int foundAt2 = b1.findKey(ex.getKey());
        assertTrue("Expect EXACT flag", (foundAt2 & Buffer.EXACT_MASK) != 0);
        setUpValue(true, RED_FOX.length() * 2);
        int splitAt2 = b1.split(b2, ex.getKey(), vw, foundAt2, ex.getAuxiliaryKey1(), Exchange.Sequence.NONE,
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
        int foundAt3 = b1.findKey(ex.getKey());
        assertTrue("Expect EXACT flag", (foundAt3 & Buffer.EXACT_MASK) != 0);
        setUpValue(true, RED_FOX.length() * 2);
        int splitAt3 = b1.split(b2, ex.getKey(), vw, foundAt3, ex.getAuxiliaryKey1(), Exchange.Sequence.NONE,
                SplitPolicy.RIGHT_BIAS);
        assertTrue("Split failed", splitAt3 != -1);
        assertTrue("Verify failed", b1.verify(null, null) == null);
        assertTrue("Verify failed", b2.verify(null, null) == null);
    }

    public void testRemoveKeys() throws Exception {
        setUpPrettyFullBufferWithChangingEbc(6);
        setUpDeepKey(104);
        int foundAt1 = b1.findKey(ex.getKey());
        setUpDeepKey(112);
        int foundAt2 = b1.findKey(ex.getKey());
        if ((foundAt1 & Buffer.P_MASK) > (foundAt2 & Buffer.P_MASK)) {
            int t = foundAt1;
            foundAt1 = foundAt2;
            foundAt2 = t;
        }
        b1.removeKeys(foundAt1, foundAt1, ex.getAuxiliaryKey1());
        assertTrue("Verify failed", b1.verify(null, null) == null);
        b1.removeKeys(foundAt1 & ~EXACT_MASK, foundAt2, ex.getAuxiliaryKey1());
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

    private void setUpPrettyFullBuffers(final int type, final int valueLength, boolean discontinuous)
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

    private void setUpDeepKey(Exchange ex, char fill, int n, long pointer) {
        ex.getKey().clear().append(keyString(fill, n, n - 4, 4, n)).append(1);
        ex.getValue().setPointerValue(pointer);
    }

    String keyString(char fill, int length, int prefix, int width, int k) {
        StringBuilder sb = new StringBuilder();
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
