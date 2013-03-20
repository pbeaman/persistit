/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import junit.framework.Assert;

import org.junit.Test;

import com.persistit.Buffer;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.PersistitUnitTestCase;
import com.persistit.TestShim;
import com.persistit.exception.InvalidKeyException;
import com.persistit.exception.KeyTooLongException;
import com.persistit.exception.MissingKeySegmentException;

public class KeyTest1 extends PersistitUnitTestCase {

    private long lv1;
    private long lv2;
    private float fv1;
    private float fv2;
    private double dv1;
    private double dv2;

    private final static long[] TEST_LONGS = { 0, 1, 2, 3, 123, 126, 127, 128, 129, 130, 131, 132, 250, 251, 252, 253,
            254, 255, 256, 257, 258, 259, 260, 4094, 4095, 4096, 4097, 4098, 16383, 16384, 16385, 0x1FFFFEL, 0x1FFFFFL,
            0x200000L, 0x3FFFFFFFFFEL, 0x3FFFFFFFFFFL, 0x2000000000000L, 0x1FFFFFFFFFFFFL, 0x1FFFFFFFFFFFEL,
            Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 1, Integer.MAX_VALUE + 0, Integer.MAX_VALUE + 1,
            Integer.MAX_VALUE + 2, Integer.MAX_VALUE + 3, Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 2,
            Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE - 2, };

    private final static float[] TEST_FLOATS = { 0.0F, 1.0F, 2.0F, 12345.0F, 0.0003F, 1.2345E-10F, 1.12345E-20F, 0.0F,
            -1.0F, -2.0F, -12345.0F, -0.0003F, -1.2345E-10F, -1.12345E-20F, Float.MAX_VALUE, Float.MIN_VALUE,
            Float.MAX_VALUE / 2.0F, Float.MAX_VALUE / 3.0F, Float.MAX_VALUE / 4.0F, Float.MIN_VALUE / 2.0F,
            Float.MIN_VALUE / 3.0F, Float.MIN_VALUE / 4.0F, Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY };

    private final static double[] TEST_DOUBLES = { 0.0, 1.0, 2.0, 12345.0, 0.0003, 1.2345E-10, 1.12345E-20, 0.0, -1.0,
            -2.0, -12345.0, -0.0003, -1.2345E-10, -1.12345E-20, Double.MAX_VALUE, Double.MIN_VALUE,
            Double.MAX_VALUE / 2.0, Double.MAX_VALUE / 3.0, Double.MAX_VALUE / 4.0, Double.MIN_VALUE / 2.0,
            Double.MIN_VALUE / 3.0, Double.MIN_VALUE / 4.0, Double.NaN, Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY };

    private final static Object[] TEST_OBJECTS = new Object[] { null, true, false, (byte) 1, (char) 2, (short) 3, 4,
            (long) 5, 6.0f, 7.0d, "This is a String", new Date(), new BigInteger("1"), new BigDecimal("2.2") };

    @Test
    public void test1() {
        System.out.print("test1 ");
        for (int index = 0; index < TEST_LONGS.length; index++) {
            for (int m = -1; m <= 1; m += 2) {
                lv1 = TEST_LONGS[index] * m;
                final Key key1 = new Key(_persistit);

                key1.clear();

                key1.append(lv1);
                key1.indexTo(0);
                lv2 = key1.decodeLong();

                assertEquals(lv1, lv2);

                lv1 = -lv1;

                key1.to(lv1);
                key1.reset();
                lv2 = key1.decodeLong();

                assertEquals(lv1, lv2);

                Assert.assertNull(t1a(lv1));
                Assert.assertNull(t1a(-lv1));
            }
        }
        System.out.println("- done");
    }

    private String t1a(final long lv) {
        final Key key1 = new Key(_persistit);

        key1.clear().append((byte) lv);
        if (key1.indexTo(0).decodeByte() != (byte) lv) {
            return "byte " + (byte) lv;
        }
        if (!(new Byte((byte) lv)).equals(key1.indexTo(0).decode())) {
            return "Byte " + (byte) lv;
        }

        key1.clear().append((short) lv);
        if (key1.indexTo(0).decodeShort() != (short) lv) {
            return "short " + (short) lv;
        }
        if (!(new Short((short) lv)).equals(key1.indexTo(0).decode())) {
            return "Short " + (short) lv;
        }

        key1.clear().append((char) lv);
        if (key1.indexTo(0).decodeChar() != (char) lv) {
            return "char " + (char) lv;
        }
        if (!(new Character((char) lv)).equals(key1.indexTo(0).decode())) {
            return "Character " + (char) lv;
        }

        key1.clear().append((int) lv);
        if (key1.indexTo(0).decodeInt() != (int) lv) {
            return "int " + (int) lv;
        }
        if (!(new Integer((int) lv)).equals(key1.indexTo(0).decode())) {
            return "Integer " + (int) lv;
        }

        key1.clear().append(lv);
        if (key1.indexTo(0).decodeLong() != lv) {
            return "long " + lv;
        }
        if (!(new Long(lv)).equals(key1.indexTo(0).decode())) {
            return "Long " + lv;
        }

        return null;

    }

    @Test
    public void test2() {
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);

        System.out.print("test2 ");
        for (int i1 = 0; i1 < TEST_LONGS.length; i1++) {
            for (int i2 = 0; i2 < TEST_LONGS.length; i2++) {
                for (int s = 0; s < 4; s++) {
                    lv1 = TEST_LONGS[i1];
                    lv2 = TEST_LONGS[i2];
                    if ((s & 1) != 0) {
                        lv1 = -lv1;
                    }
                    if ((s & 2) != 0) {
                        lv2 = -lv2;
                    }
                    key1.clear();
                    key1.append(lv1);
                    key2.clear();
                    key2.append(lv2);
                    final int compare = key1.compareTo(key2);

                    final boolean result = ((compare == 0) && (lv1 == lv2)) || ((compare > 0) && (lv1 > lv2))
                            || ((compare < 0) && (lv1 < lv2));

                    assertTrue(result);
                }
            }
        }
        System.out.println("- done");
    }

    @Test
    public void test3() {
        final Key key1 = new Key(_persistit);

        System.out.print("test3 ");
        for (int index = 0; index < TEST_FLOATS.length; index++) {
            fv1 = TEST_FLOATS[index];
            key1.clear();

            key1.append(fv1);
            key1.indexTo(0);
            fv2 = key1.decodeFloat();

            assertTrue(floatEquals(fv1, fv2));

            fv1 = -fv1;

            key1.to(fv1);
            key1.indexTo(0);
            fv2 = key1.decodeFloat();

            assertTrue(floatEquals(fv1, fv2));
        }
        System.out.println("- done");
    }

    @Test
    public void test4() {
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);

        System.out.print("test4 ");
        for (int i1 = 0; i1 < TEST_FLOATS.length; i1++) {
            for (int i2 = 0; i2 < TEST_FLOATS.length; i2++) {
                for (int s = 0; s < 4; s++) {
                    fv1 = TEST_FLOATS[i1];
                    fv2 = TEST_FLOATS[i2];
                    if ((s & 1) != 0) {
                        fv1 = -fv1;
                    }
                    if ((s & 2) != 0) {
                        fv2 = -fv2;
                    }
                    key1.clear();
                    key1.append(fv1);
                    key2.clear();
                    key2.append(fv2);
                    final int compare = key1.compareTo(key2);

                    // Can't use static Float.compare on JDK 1.3.1
                    final Float f1 = new Float(fv1);
                    final Float f2 = new Float(fv2);

                    final boolean result = ((compare == 0) && (f1.compareTo(f2) == 0))
                            || ((compare > 0) && (f1.compareTo(f2) > 0)) || ((compare < 0) && (f1.compareTo(f2) < 0));

                    assertTrue(result);
                }
            }
        }
        System.out.println("- done");
    }

    private static boolean floatEquals(final float f1, final float f2) {
        if (Float.isNaN(f1)) {
            return Float.isNaN(f2);
        }
        if (Float.isInfinite(f1)) {
            return Float.isInfinite(f2);
        }
        return f1 == f2;
    }

    @Test
    public void test5() {
        final Key key1 = new Key(_persistit);

        System.out.print("test5 ");
        for (int index = 0; index < TEST_DOUBLES.length; index++) {
            dv1 = TEST_DOUBLES[index];
            key1.clear();

            key1.append(dv1);
            key1.reset();
            dv2 = key1.decodeDouble();

            assertTrue(doubleEquals(dv1, dv2));

            dv1 = -dv1;

            key1.to(dv1);
            key1.reset();
            dv2 = key1.decodeDouble();

            assertTrue(doubleEquals(dv1, dv2));
        }
        System.out.println("- done");
    }

    @Test
    public void test6() {
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);

        System.out.print("test6 ");
        for (int i1 = 0; i1 < TEST_DOUBLES.length; i1++) {
            for (int i2 = 0; i2 < TEST_DOUBLES.length; i2++) {
                for (int s = 0; s < 4; s++) {
                    dv1 = TEST_DOUBLES[i1];
                    dv2 = TEST_DOUBLES[i2];
                    if ((s & 1) != 0) {
                        dv1 = -dv1;
                    }
                    if ((s & 2) != 0) {
                        dv2 = -dv2;
                    }
                    key1.clear();
                    key1.append(dv1);
                    key2.clear();
                    key2.append(dv2);
                    final int compare = key1.compareTo(key2);

                    // Can't use static Double.compare on JDK1.3.1
                    final Double d1 = new Double(dv1);
                    final Double d2 = new Double(dv2);

                    final boolean result = ((compare == 0) && (d1.compareTo(d2) == 0))
                            || ((compare > 0) && (d1.compareTo(d2) > 0)) || ((compare < 0) && (d1.compareTo(d2) < 0));

                    assertTrue(result);
                }
            }
        }
        System.out.println("- done");
    }

    /**
     * Verify key type ordering
     * 
     */

    @Test
    public void test7() {
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);

        System.out.print("test7 ");

        key1.to(Key.BEFORE);
        key2.to(null);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to(null);
        key2.to(false);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to(false);
        key2.to(true);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to(true);
        key2.to((byte) 0);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to((byte) 0);
        key2.to((byte) -1);
        assertTrue(key2.compareTo(key1) < 0);

        key1.to((byte) 1);
        key2.to((byte) -1);
        assertTrue(key2.compareTo(key1) < 0);

        key1.to((byte) 1);
        key2.clear().append(new Byte((byte) 1));
        assertTrue(key2.compareTo(key1) == 0);

        key1.to(new Byte((byte) 1));
        key2.to((short) 1);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to((short) 1);
        key2.to((char) 1);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to((char) 1);
        key2.to(1);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to(1);
        key2.to((long) 1);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to((long) 1);
        key2.to((float) 1.0);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to((float) 1.0);
        key2.to(1.0);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to(1.0);
        key2.to(new byte[10]);
        assertTrue(key2.compareTo(key1) > 0);

        key1.to(new byte[10]);
        key2.to(new byte[] { -1, -1 });
        assertTrue(key2.compareTo(key1) > 0);

        key1.to(new byte[] { -1, -1 });
        key2.to("x");
        assertTrue(key2.compareTo(key1) > 0);

        key1.to("x");
        key2.to("y");
        assertTrue(key2.compareTo(key1) > 0);

        key1.to("y");
        key2.to("\u0199y");
        assertTrue(key2.compareTo(key1) > 0);

        key1.to("\u0199y");
        key2.to("\u0200y");
        assertTrue(key2.compareTo(key1) > 0);

        System.out.println("- done");
    }

    // Note: this test has been run with INCREMENT = 1 (i.e., every int value),
    // that's too slow (5 minutes) for unit tests in normal builds.
    //
    private static final int INCREMENT = 1237;

    @Test
    public void test8() {
        final Key key1 = new Key(_persistit);

        System.out.print("test8 ");
        final int start = Integer.MIN_VALUE;
        final int end = Integer.MAX_VALUE - INCREMENT;
        for (int u = start; u < end; u += INCREMENT) {
            if ((u % 100000000) == 0) {
                System.out.print(" " + u);
            }
            key1.clear().append(u);
            final int v = key1.reset().decodeInt();
            if (u != v) {
                assertEquals(u, v);
            }
        }

        System.out.println("- done");
    }

    @Test
    public void test9() {
        final Key key1 = new Key(_persistit);

        System.out.print("test9 ");

        final int start = 0;
        final int end = 10000000;
        for (int u = start; u < end; u++) {
            if ((u % 10000000) == 0) {
                System.out.print(" " + u);
            }
            int k = (u * 123) % end;
            if (k < 0) {
                k += end;
            }
            if (k < 0) {
                System.out.println("at u=" + u + " k=" + k);
                break;
            }
            key1.to(k);
            final int v = key1.reset().decodeInt();
            if (k != -v) {
                assertEquals(k, v);
            }
        }

        System.out.println("- done");
    }

    @Test
    public void test10() {
        final Key key1 = new Key(_persistit);

        System.out.print("test10 ");

        for (int index = 0; index < TEST_LONGS.length; index++) {
            lv1 = TEST_LONGS[index];
            key1.clear();

            BigInteger biv1 = BigInteger.valueOf(lv1);

            key1.append(biv1);
            key1.indexTo(0);

            BigInteger biv2 = key1.decodeBigInteger();

            assertEquals(biv1, biv2);

            lv1 = -lv1;
            biv1 = BigInteger.valueOf(lv1);

            key1.to(biv1);
            key1.indexTo(0);
            biv2 = key1.decodeBigInteger();

            assertEquals(biv1, biv2);

            Assert.assertNull(t1a(lv1));
            Assert.assertNull(t1a(-lv1));
        }

        System.out.println("- done");
    }

    @Test
    public void test11() {
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);

        System.out.print("test11 ");
        for (int i1 = 0; i1 < TEST_LONGS.length; i1++) {
            for (int i2 = 0; i2 < TEST_LONGS.length; i2++) {
                for (int s = 0; s < 4; s++) {
                    lv1 = TEST_LONGS[i1];
                    lv2 = TEST_LONGS[i2];
                    if ((s & 1) != 0) {
                        lv1 = -lv1;
                    }
                    if ((s & 2) != 0) {
                        lv2 = -lv2;
                    }

                    final BigInteger biv1 = BigInteger.valueOf(lv1);
                    final BigInteger biv2 = BigInteger.valueOf(lv2);
                    key1.clear();
                    key1.append(biv1);
                    key2.clear();
                    key2.append(biv2);
                    final int compare = key1.compareTo(key2);

                    final boolean result = ((compare == 0) && (lv1 == lv2)) || ((compare > 0) && (lv1 > lv2))
                            || ((compare < 0) && (lv1 < lv2));

                    assertTrue(result);
                }
            }
        }
        System.out.println("- done");
    }

    @Test
    public void test12() {
        System.out.print("test12 ");
        final Key key = new Key(_persistit);
        final StringBuilder sb = new StringBuilder();

        sb.setLength(0);
        sb.append("1");
        for (int j = 0; j < 200; j++) {
            sb.append("0");
            final BigInteger bi1 = new BigInteger(sb.toString());
            key.clear().append(bi1);
            final BigInteger bi2 = (BigInteger) key.reset().decode();
            assertTrue(bi2.compareTo(bi1) == 0);
        }

        sb.setLength(0);
        sb.append("-1");
        for (int j = 0; j < 200; j++) {
            sb.append("0");
            final BigInteger bi1 = new BigInteger(sb.toString());
            key.clear().append(bi1);
            final BigInteger bi2 = (BigInteger) key.reset().decode();
            assertTrue(bi2.compareTo(bi1) == 0);
        }
        System.out.println("- done");
    }

    @Test
    public void test13() {
        System.out.print("test13 ");
        final StringBuilder sb = new StringBuilder();
        final Key key = new Key(_persistit);
        sb.setLength(0);
        sb.append("0.");
        for (int j = 0; j < 200; j++) {
            sb.append("9");
            final BigDecimal bi1 = new BigDecimal(sb.toString());
            key.clear().append(bi1);
            final BigDecimal bi2 = (BigDecimal) key.reset().decode();
            assertTrue(bi2.compareTo(bi1) == 0);
        }

        for (int a = 0; a < 50; a++) {
            for (int b = 0; b < 50; b++) {
                sb.setLength(0);
                sb.append("5");
                for (int i = 0; i < b; i++) {
                    sb.append("0");
                }
                sb.append(".");
                for (int i = 0; i < a; i++) {
                    sb.append("0");
                }
                sb.append("5");
                final BigDecimal bi1 = new BigDecimal(sb.toString());
                key.clear().append(bi1);
                final BigDecimal bi2 = (BigDecimal) key.reset().decode();
                assertTrue(bi2.compareTo(bi1) == 0);
            }
        }

        for (int a = 0; a < 50; a++) {
            for (int b = 0; b < 50; b++) {
                sb.setLength(0);
                sb.append("-5");
                for (int i = 0; i < b; i++) {
                    sb.append("0");
                }
                sb.append(".");
                for (int i = 0; i < a; i++) {
                    sb.append("0");
                }
                sb.append("5");
                final BigDecimal bi1 = new BigDecimal(sb.toString());
                key.clear().append(bi1);
                final BigDecimal bi2 = (BigDecimal) key.reset().decode();
                assertTrue(bi2.compareTo(bi1) == 0);
            }
        }

        for (int a = 0; a < 50; a++) {
            for (int b = 0; b < 50; b++) {
                sb.setLength(0);
                sb.append("-5");
                for (int i = 0; i < b; i++) {
                    sb.append("0");
                }
                sb.append(".");
                for (int i = 0; i < a; i++) {
                    sb.append("0");
                }
                sb.append("0");
                final BigDecimal bi1 = new BigDecimal(sb.toString());
                key.clear().append(bi1);
                final BigDecimal bi2 = (BigDecimal) key.reset().decode();
                assertTrue(bi2.compareTo(bi1) == 0);
            }
        }

        System.out.println("- done");
    }

    @Test
    public void test14() {
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);
        key1.clear().append("a").append("b").append(1).append(2);
        key2.clear().append("a").append("b").append(2).append(1);
        assertEquals(7, key1.firstUniqueByteIndex(key2));
        assertEquals(2, key1.firstUniqueSegmentDepth(key2));
    }

    /*
     * Test the equals methods in Key and KeyState
     */

    @Test
    public void testKeyEquality() {
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);
        key1.clear().to(5);
        key2.clear().to(5);
        assertEquals(key1, key2);
        KeyState ks = new KeyState(key2);
        assertEquals(key1, ks);
        assertEquals(key2, ks);
        key1.clear().to("PADRAIG");
        key2.clear().to("PADRAIG");
        assertEquals(key1, key2);
        ks = new KeyState(key2);
        assertEquals(ks, key2);
        assertEquals(ks, key1);
    }

    @Test
    public void testIndexTo() {
        final Key key1 = new Key(_persistit);
        key1.clear().append("a").append("b").append("c").append("d");
        assertEquals("a", key1.indexTo(0).decodeString());
        assertEquals("b", key1.indexTo(1).decodeString());
        assertEquals("c", key1.indexTo(2).decodeString());
        assertEquals("d", key1.indexTo(3).decodeString());
        assertEquals(key1.getEncodedSize(), key1.indexTo(4).getIndex());
        assertEquals(key1.getEncodedSize(), key1.indexTo(5).getIndex());
        assertEquals("d", key1.indexTo(-1).decodeString());
        assertEquals("c", key1.indexTo(-2).decodeString());
        assertEquals("b", key1.indexTo(-3).decodeString());
        assertEquals("a", key1.indexTo(-4).decodeString());
        assertEquals("a", key1.indexTo(-5).decodeString());
        assertEquals("a", key1.indexTo(-6).decodeString());
    }

    @Test
    public void testIsNull() {
        final Key key1 = new Key(_persistit);
        key1.clear().append(null);
        assertTrue("seg0 is null: " + key1, key1.indexTo(0).isNull());
        key1.clear().append(1);
        assertFalse("seg0 is not null: " + key1, key1.indexTo(0).isNull());
        key1.clear().append(5L).append(null);
        assertTrue("seg1 is null: " + key1, key1.indexTo(1).isNull());
        key1.clear().append("abc");
        assertFalse("seg0 is not null:" + key1, key1.indexTo(0).isNull());
        key1.clear().append(BigInteger.valueOf(42)).append(null);
        assertTrue("seg1 is null: " + key1, key1.indexTo(1).isNull());

        try {
            key1.clear().reset().isNull();
            Assert.fail("Expected MissingKeySegmentException!");
        } catch (final MissingKeySegmentException e) {
            // expected
        }
    }

    @Test
    public void testSkipNull() {
        final Key key1 = new Key(_persistit);
        key1.clear().append(null).append(1).append(2).append(null).append(null).append(5);
        key1.reset();
        assertTrue("seg0 is null: " + key1, key1.isNull(true));
        assertFalse("seg1 is not null: " + key1, key1.isNull(true));
        assertEquals("expect seg1 value", 1, key1.decode());
        assertFalse("seg2 is not null: " + key1, key1.isNull(true));
        assertEquals("expect seg2 value", 2, key1.decode());
        assertTrue("seg3 is null: " + key1, key1.isNull(true));
        assertTrue("seg4 is null: " + key1, key1.isNull(true));
        assertFalse("seg5 is not null:" + key1, key1.isNull(true));
        assertFalse("seg5 is not null:" + key1, key1.isNull(true));
        assertFalse("seg5 is not null:" + key1, key1.isNull(true));
        assertEquals("expect seg5 value", 5, key1.decodeInt());

        try {
            key1.isNull(true);
            Assert.fail("Expected MissingKeySegmentException!");
        } catch (final MissingKeySegmentException e) {
            // expected
        }
    }

    @Test
    public void testFirstUniqueSegmentDepth() {
        final Key key1 = new Key(_persistit);
        final Key key2 = new Key(_persistit);
        key1.append("a").append("b").append("c");
        key2.append("a").append("b").append("d");
        assertEquals("Incorrect depth", 2, key1.firstUniqueSegmentDepth(key2));
        assertEquals("Incorrect depth", 2, key2.firstUniqueSegmentDepth(key1));
        key1.cut();
        assertEquals("Incorrect depth", 2, key1.firstUniqueSegmentDepth(key2));
        assertEquals("Incorrect depth", 2, key2.firstUniqueSegmentDepth(key1));
        key2.cut();
        assertEquals("Incorrect depth", 2, key1.firstUniqueSegmentDepth(key2));
        assertEquals("Incorrect depth", 2, key2.firstUniqueSegmentDepth(key1));
        key2.cut();
        assertEquals("Incorrect depth", 1, key1.firstUniqueSegmentDepth(key2));
        assertEquals("Incorrect depth", 1, key2.firstUniqueSegmentDepth(key1));
    }

    @Test
    public void testValidForAppendLeftEdge() {
        final Key key1 = newKey();
        Key.LEFT_GUARD_KEY.copyTo(key1);
        try {
            TestShim.testValidForAppend(key1);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testValidForAppendRightEdge() {
        final Key key1 = newKey();
        Key.RIGHT_GUARD_KEY.copyTo(key1);
        try {
            TestShim.testValidForAppend(key1);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testValidForAppendBefore() {
        final Key key1 = newKey();
        key1.clear().append(Key.BEFORE);
        try {
            TestShim.testValidForAppend(key1);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testValidForAppendAfter() {
        final Key key1 = newKey();
        key1.append(Key.AFTER);
        try {
            TestShim.testValidForAppend(key1);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testValidForAppendInvalidFinalSegment() {
        final Key key1 = newKey();
        appendInvalidSegment(key1);
        try {
            TestShim.testValidForAppend(key1);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testValidForStoreAndFetchEmptyKey() {
        final Key key1 = newKey();
        try {
            TestShim.testValidForStoreAndFetch(key1, Buffer.MIN_BUFFER_SIZE);
            fail("Expected InvalidKeyException");
        } catch (final InvalidKeyException e) {
            // Expected
        }
    }

    @Test
    public void testValidForStoreAndFetchBeforeKey() {
        final Key key1 = newKey();
        key1.append(Key.BEFORE);
        try {
            TestShim.testValidForStoreAndFetch(key1, Buffer.MIN_BUFFER_SIZE);
            fail("Expected InvalidKeyException");
        } catch (final InvalidKeyException e) {
            // Expected
        }
    }

    @Test
    public void testValidForStoreAndFetchInvalidFinalSegment() {
        final Key key1 = newKey();
        appendInvalidSegment(key1);
        try {
            TestShim.testValidForStoreAndFetch(key1, Buffer.MAX_BUFFER_SIZE);
            fail("Expected InvalidKeyException");
        } catch (final InvalidKeyException e) {
            // expected
        }
    }

    @Test
    public void testValidForStoreAndFetchAfterKey() {
        final Key key1 = newKey();
        key1.append(Key.AFTER);
        try {
            TestShim.testValidForStoreAndFetch(key1, Buffer.MIN_BUFFER_SIZE);
            fail("Expected InvalidKeyException");
        } catch (final InvalidKeyException e) {
            // Expected
        }
    }

    @Test
    public void testValidForStoreAndFetchKeyTooLarge() {
        final Key key1 = new Key(_persistit);
        final int BMIN = Buffer.MIN_BUFFER_SIZE;
        final int BMAX = Buffer.MAX_BUFFER_SIZE;
        final int KMAX = key1.getMaximumSize();
        for (int bsize = BMIN; bsize <= BMAX && bsize < KMAX; bsize *= 2) {
            key1.clear();
            key1.setEncodedSize(bsize + 1);
            try {
                TestShim.testValidForStoreAndFetch(key1, bsize);
                fail("Expected IllegalArgumentException for buffer buffer size " + bsize);
            } catch (final InvalidKeyException e) {
                // Expected
            }
        }
    }

    @Test
    public void testValidForTraverseEmptyKey() {
        final Key key1 = newKey();
        try {
            TestShim.testValidForTraverse(key1);
            fail("Expected InvalidKeyException");
        } catch (final InvalidKeyException e) {
            // Expected
        }
    }

    @Test
    public void testNudgeDeeperFullKey() {
        final Key key1 = newKey();
        fillKey(key1);

        final long gen1 = key1.getGeneration();
        final int fullSize = key1.getEncodedSize();
        TestShim.nudgeDeeper(key1);

        final long gen2 = key1.getGeneration();
        final int nudge1Size = key1.getEncodedSize();
        assertEquals("can nudge when key is full", fullSize + 1, nudge1Size);
        assertTrue("Generation changed after successful nudge", gen2 > gen1);

        TestShim.nudgeDeeper(key1);

        final long gen3 = key1.getGeneration();
        final int nudge2Size = key1.getEncodedSize();
        assertEquals("cannot nudge full key twice", fullSize + 1, nudge2Size);
        assertEquals("Generation did not change after unsuccessful nudge", gen2, gen3);
    }

    @Test
    public void testNudgeLeftRightNoChangeSpecialKeys() {
        final Key key1 = newKey();
        final Key key2 = newKey();

        Key.LEFT_GUARD_KEY.copyTo(key1);
        TestShim.nudgeLeft(key1);
        assertEquals("no nudgeLeft LEFT_GUARD", 0, Key.LEFT_GUARD_KEY.compareTo(key1));
        TestShim.nudgeRight(key1);
        assertEquals("no nudgeRight LEFT_GUARD", 0, Key.LEFT_GUARD_KEY.compareTo(key1));

        Key.RIGHT_GUARD_KEY.copyTo(key1);
        TestShim.nudgeLeft(key1);
        assertEquals("no nudgeLeft RIGHT_GUARD", 0, Key.RIGHT_GUARD_KEY.compareTo(key1));
        TestShim.nudgeRight(key1);
        assertEquals("no nudgeRight RIGHT_GUARD", 0, Key.RIGHT_GUARD_KEY.compareTo(key1));

        key1.clear().append(Key.BEFORE);
        key1.copyTo(key2);
        TestShim.nudgeLeft(key1);
        assertEquals("no nudgeLeft BEFORE", 0, key2.compareTo(key1));
        TestShim.nudgeRight(key1);
        assertEquals("no nudgeRight BEFORE", 0, key2.compareTo(key1));

        key1.clear().append(Key.AFTER);
        key1.copyTo(key2);
        TestShim.nudgeLeft(key1);
        assertEquals("no nudgeLeft AFTER", 0, key2.compareTo(key1));
        TestShim.nudgeRight(key1);
        assertEquals("no nudgeRight AFTER", 0, key2.compareTo(key1));
    }

    @Test
    public void testNudgeLeftRightFullKey() {
        final Key key1 = newKey();
        fillKey(key1);

        final Key key2 = newKey();
        key1.copyTo(key2);

        TestShim.nudgeLeft(key1);
        assertTrue("nudgeLeft on full key compares less", key1.compareTo(key2) < 0);

        key2.copyTo(key1);
        TestShim.nudgeRight(key1);
        assertTrue("nudgeRight on full key compares greater", key1.compareTo(key2) > 0);
    }

    @Test
    public void testNonAsciiString() {
        final String[] strings = { "\u0000\u0001\u0000\u0001", // small, <= 0x01
                "asdf", // ascii
                "\u03A3\u03A4\u03A6\u03A8", // medium, <= 0x7FF (greek capital
                                            // sigma, tau, phi, psi)
                "\u2654\u2655\u2656\u2657", // large, (white chess king, queen,
                                            // rook, bishop)
        };
        final Key key1 = newKey();
        for (final String s : strings) {
            assertEquals("character count", 4, s.length());
            key1.clear().append(s);
            final String decoded = key1.decodeString();
            assertEquals("append and decode", s, decoded);
        }
    }

    @Test
    public void testCompareKeySegment() throws Exception {
        final Key key1 = newKey();
        final Key key2 = newKey();
        key1.append(1).append(2).append(3).append("abc");
        key2.append(3).append(2).append(1).append("abcd");
        key1.indexTo(1);
        key2.indexTo(1);
        assertTrue("Should be == 0", 0 == key1.compareKeySegment(key2));
        assertTrue("Should be == 0", 0 == key2.compareKeySegment(key1));
        key1.indexTo(0);
        assertTrue("Should be < 0", 0 > key1.compareKeySegment(key2));
        assertTrue("Should be > 0", 0 < key2.compareKeySegment(key1));
        key1.indexTo(2);
        assertTrue("Should be > 0", 0 < key1.compareKeySegment(key2));
        assertTrue("Should be < 0", 0 > key2.compareKeySegment(key1));
        key1.indexTo(3);
        assertTrue("Should be > 0", 0 < key1.compareKeySegment(key2));
        assertTrue("Should be < 0", 0 > key2.compareKeySegment(key1));
        key2.indexTo(3);
        assertTrue("Should be < 0", 0 > key1.compareKeySegment(key2));
        assertTrue("Should be > 0", 0 < key2.compareKeySegment(key1));
    }

    @Test
    public void testAppendKeySegment() throws Exception {
        final Key key1 = newKey();
        final Key key2 = newKey();
        key1.append(1).append(2).append(3).append("abc");
        key1.indexTo(3);
        key2.appendKeySegment(key1);
        key1.indexTo(2);
        key2.appendKeySegment(key1);
        key1.indexTo(1);
        key2.appendKeySegment(key1);
        key1.indexTo(0);
        key2.appendKeySegment(key1);
        assertEquals("Key value incorrect", "{\"abc\",3,2,1}", key2.toString());
        key1.indexTo(3);
        key1.appendKeySegment(key1);
        assertEquals("Key value incorrect", "{1,2,3,\"abc\",\"abc\"}", key1.toString());
    }

    @Test
    public void testKeyTooLong() throws Exception {
        for (int maxSize = 1; maxSize < 99; maxSize++) {
            mainLoop: for (final Object value : TEST_OBJECTS) {
                final Key key = new Key(_persistit, maxSize);
                // Every appended value consumes at least 2 bytes
                for (int i = 0; i < 50; i++) {
                    try {
                        key.append(value);
                    } catch (final KeyTooLongException e) {
                        continue mainLoop;
                    }
                }
                fail("Should have thrown a KeyTooLongException for maxSize=" + maxSize + " and value " + value);
            }
        }
    }

    private static boolean doubleEquals(final double f1, final double f2) {
        if (Double.isNaN(f1)) {
            return Double.isNaN(f2);
        }
        if (Double.isInfinite(f1)) {
            return Double.isInfinite(f2);
        }
        return f1 == f2;
    }

    private Key newKey() {
        return new Key(_persistit);
    }

    private static Key appendInvalidSegment(final Key key) {
        key.append("asdf");
        assertTrue("encoded size > 1", key.getEncodedSize() > 1);
        key.setEncodedSize(key.getEncodedSize() - 1);
        return key;
    }

    private static void fillKey(final Key key) {
        final byte[] array = new byte[key.getMaximumSize() - 2];
        for (int i = 0; i < array.length; ++i) {
            array[i] = 10;
        }
        key.clear().append(array);
        assertEquals("encoded size is max size", key.getEncodedSize(), key.getMaximumSize());
    }
}
