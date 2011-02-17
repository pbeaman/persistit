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

package com.persistit.unit;

import java.math.BigDecimal;

import java.math.BigInteger;

import junit.framework.Assert;

import org.junit.Test;

import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Util;

public class KeyTest1 extends PersistitUnitTestCase {

    private final Key _key1 = new Key(_persistit);
    private final Key _key2 = new Key(_persistit);
    private long lv1;
    private long lv2;
    private float fv1;
    private float fv2;
    private double dv1;
    private double dv2;

    long[] TEST_LONGS = { 0, 1, 2, 3, 123, 126, 127, 128, 129, 130, 131, 132,
            250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 4094, 4095,
            4096, 4097, 4098, 16383, 16384, 16385, Integer.MAX_VALUE - 2,
            Integer.MAX_VALUE - 1, Integer.MAX_VALUE + 0,
            Integer.MAX_VALUE + 1, Integer.MAX_VALUE + 2,
            Integer.MAX_VALUE + 3, Long.MIN_VALUE, Long.MIN_VALUE + 1,
            Long.MIN_VALUE + 2, Long.MAX_VALUE, Long.MAX_VALUE - 1,
            Long.MAX_VALUE - 2, };

    float[] TEST_FLOATS = { 0.0F, 1.0F, 2.0F, 12345.0F, 0.0003F, 1.2345E-10F,
            1.12345E-20F, 0.0F, -1.0F, -2.0F, -12345.0F, -0.0003F,
            -1.2345E-10F, -1.12345E-20F, Float.MAX_VALUE, Float.MIN_VALUE,
            Float.MAX_VALUE / 2.0F, Float.MAX_VALUE / 3.0F,
            Float.MAX_VALUE / 4.0F, Float.MIN_VALUE / 2.0F,
            Float.MIN_VALUE / 3.0F, Float.MIN_VALUE / 4.0F, Float.NaN,
            Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY };

    double[] TEST_DOUBLES = { 0.0, 1.0, 2.0, 12345.0, 0.0003, 1.2345E-10,
            1.12345E-20, 0.0, -1.0, -2.0, -12345.0, -0.0003, -1.2345E-10,
            -1.12345E-20, Double.MAX_VALUE, Double.MIN_VALUE,
            Double.MAX_VALUE / 2.0, Double.MAX_VALUE / 3.0,
            Double.MAX_VALUE / 4.0, Double.MIN_VALUE / 2.0,
            Double.MIN_VALUE / 3.0, Double.MIN_VALUE / 4.0, Double.NaN,
            Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY };

    @Test
    public void test1() {
        System.out.print("test1 ");
        for (int index = 0; index < TEST_LONGS.length; index++) {
            lv1 = TEST_LONGS[index];
            _key1.clear();

            _key1.append(lv1);
            _key1.indexTo(0);
            lv2 = _key1.decodeLong();

            debugAssert(lv1 == lv2);

            lv1 = -lv1;

            _key1.to(lv1);
            _key1.reset();
            lv2 = _key1.decodeLong();

            debugAssert(lv1 == lv2);

            Assert.assertNull(t1a(lv1));
            Assert.assertNull(t1a(-lv1));
        }
        System.out.println("- done");
    }

    private String t1a(final long lv) {

        _key1.clear().append((byte) lv);
        if (_key1.indexTo(0).decodeByte() != (byte) lv) {
            return "byte " + (byte) lv;
        }
        if (!(new Byte((byte) lv)).equals(_key1.indexTo(0).decode())) {
            return "Byte " + (byte) lv;
        }

        _key1.clear().append((short) lv);
        if (_key1.indexTo(0).decodeShort() != (short) lv) {
            return "short " + (short) lv;
        }
        if (!(new Short((short) lv)).equals(_key1.indexTo(0).decode())) {
            return "Short " + (short) lv;
        }

        _key1.clear().append((char) lv);
        if (_key1.indexTo(0).decodeChar() != (char) lv) {
            return "char " + (char) lv;
        }
        if (!(new Character((char) lv)).equals(_key1.indexTo(0).decode())) {
            return "Character " + (char) lv;
        }

        _key1.clear().append((int) lv);
        if (_key1.indexTo(0).decodeInt() != (int) lv) {
            return "int " + (int) lv;
        }
        if (!(new Integer((int) lv)).equals(_key1.indexTo(0).decode())) {
            return "Integer " + (int) lv;
        }

        _key1.clear().append(lv);
        if (_key1.indexTo(0).decodeLong() != lv) {
            return "long " + lv;
        }
        if (!(new Long((long) lv)).equals(_key1.indexTo(0).decode())) {
            return "Long " + (long) lv;
        }

        return null;

    }

    @Test
    public void test2() {
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
                    _key1.clear();
                    _key1.append(lv1);
                    _key2.clear();
                    _key2.append(lv2);
                    final int compare = _key1.compareTo(_key2);

                    final boolean result = ((compare == 0) && (lv1 == lv2))
                            || ((compare > 0) && (lv1 > lv2))
                            || ((compare < 0) && (lv1 < lv2));

                    debugAssert(result);
                }
            }
        }
        System.out.println("- done");
    }

    @Test
    public void test3() {
        System.out.print("test3 ");
        for (int index = 0; index < TEST_FLOATS.length; index++) {
            fv1 = TEST_FLOATS[index];
            _key1.clear();

            _key1.append(fv1);
            _key1.indexTo(0);
            fv2 = _key1.decodeFloat();

            debugAssertFloat(floatEquals(fv1, fv2));

            fv1 = -fv1;

            _key1.to(fv1);
            _key1.indexTo(0);
            fv2 = _key1.decodeFloat();

            debugAssertFloat(floatEquals(fv1, fv2));
        }
        System.out.println("- done");
    }

    @Test
    public void test4() {
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
                    _key1.clear();
                    _key1.append(fv1);
                    _key2.clear();
                    _key2.append(fv2);
                    final int compare = _key1.compareTo(_key2);

                    // Can't use static Float.compare on JDK 1.3.1
                    final Float f1 = new Float(fv1);
                    final Float f2 = new Float(fv2);

                    final boolean result = ((compare == 0) && (f1.compareTo(f2) == 0))
                            || ((compare > 0) && (f1.compareTo(f2) > 0))
                            || ((compare < 0) && (f1.compareTo(f2) < 0));

                    debugAssertFloat(result);
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
        System.out.print("test5 ");
        for (int index = 0; index < TEST_DOUBLES.length; index++) {
            dv1 = TEST_DOUBLES[index];
            _key1.clear();

            _key1.append(dv1);
            _key1.reset();
            dv2 = _key1.decodeDouble();

            debugAssertDouble(doubleEquals(dv1, dv2));

            dv1 = -dv1;

            _key1.to(dv1);
            _key1.reset();
            dv2 = _key1.decodeDouble();

            debugAssertDouble(doubleEquals(dv1, dv2));
        }
        System.out.println("- done");
    }

    @Test
    public void test6() {
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
                    _key1.clear();
                    _key1.append(dv1);
                    _key2.clear();
                    _key2.append(dv2);
                    final int compare = _key1.compareTo(_key2);

                    // Can't use static Double.compare on JDK1.3.1
                    final Double d1 = new Double(dv1);
                    final Double d2 = new Double(dv2);

                    final boolean result = ((compare == 0) && (d1.compareTo(d2) == 0))
                            || ((compare > 0) && (d1.compareTo(d2) > 0))
                            || ((compare < 0) && (d1.compareTo(d2) < 0));

                    debugAssertDouble(result);
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
        System.out.print("test7 ");

        _key1.to(Key.BEFORE);
        _key2.to(null);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to(null);
        _key2.to(false);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to(false);
        _key2.to(true);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to(true);
        _key2.to((byte) 0);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to((byte) 0);
        _key2.to((byte) -1);
        assertTrue(_key2.compareTo(_key1) < 0);

        _key1.to((byte) 1);
        _key2.to((byte) -1);
        assertTrue(_key2.compareTo(_key1) < 0);

        _key1.to((byte) 1);
        _key2.clear().append(new Byte((byte) 1));
        assertTrue(_key2.compareTo(_key1) == 0);

        _key1.to(new Byte((byte) 1));
        _key2.to((short) 1);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to((short) 1);
        _key2.to((char) 1);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to((char) 1);
        _key2.to((int) 1);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to((int) 1);
        _key2.to((long) 1);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to((long) 1);
        _key2.to((float) 1.0);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to((float) 1.0);
        _key2.to((double) 1.0);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to((double) 1.0);
        _key2.to(new byte[10]);
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to(new byte[10]);
        _key2.to(new byte[] { -1, -1 });
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to(new byte[] { -1, -1 });
        _key2.to("x");
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to("x");
        _key2.to("y");
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to("y");
        _key2.to("\u0199y");
        assertTrue(_key2.compareTo(_key1) > 0);

        _key1.to("\u0199y");
        _key2.to("\u0200y");
        assertTrue(_key2.compareTo(_key1) > 0);

        System.out.println("- done");
    }

    // Note: this test has been run with INCREMENT = 1 (i.e., every int value),
    // that's too slow (5 minutes) for unit tests in normal builds.
    //
    private static final int INCREMENT = 1237;

    @Test
    public void test8() {
        System.out.print("test8 ");
        final int start = Integer.MIN_VALUE;
        final int end = Integer.MAX_VALUE - INCREMENT;
        for (int u = start; u < end; u += INCREMENT) {
            if ((u % 100000000) == 0) {
                System.out.print(" " + u);
            }
            _key1.clear().append(u);
            final int v = _key1.reset().decodeInt();
            if (u != v) {
                assertEquals(u, v);
            }
        }

        System.out.println("- done");
    }

    @Test
    public void test9() {
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
            _key1.to(k);
            int v = _key1.reset().decodeInt();
            if (k != -v) {
                assertEquals(k, v);
            }
        }

        System.out.println("- done");
    }

    @Test
    public void test10() {
        System.out.print("test10 ");

        for (int index = 0; index < TEST_LONGS.length; index++) {
            lv1 = TEST_LONGS[index];
            _key1.clear();

            BigInteger biv1 = BigInteger.valueOf(lv1);

            _key1.append(biv1);
            _key1.indexTo(0);

            BigInteger biv2 = _key1.decodeBigInteger();

            assertEquals(biv1, biv2);

            lv1 = -lv1;
            biv1 = BigInteger.valueOf(lv1);

            _key1.to(biv1);
            _key1.indexTo(0);
            biv2 = _key1.decodeBigInteger();

            assertEquals(biv1, biv2);

            Assert.assertNull(t1a(lv1));
            Assert.assertNull(t1a(-lv1));
        }

        System.out.println("- done");
    }

    @Test
    public void test11() {
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
                    _key1.clear();
                    _key1.append(biv1);
                    _key2.clear();
                    _key2.append(biv2);
                    final int compare = _key1.compareTo(_key2);

                    final boolean result = ((compare == 0) && (lv1 == lv2))
                            || ((compare > 0) && (lv1 > lv2))
                            || ((compare < 0) && (lv1 < lv2));

                    debugAssert(result);
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
            debugAssert(bi2.compareTo(bi1) == 0);
        }

        sb.setLength(0);
        sb.append("-1");
        for (int j = 0; j < 200; j++) {
            sb.append("0");
            final BigInteger bi1 = new BigInteger(sb.toString());
            key.clear().append(bi1);
            final BigInteger bi2 = (BigInteger) key.reset().decode();
            debugAssert(bi2.compareTo(bi1) == 0);
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
            debugAssert(bi2.compareTo(bi1) == 0);
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
                debugAssert(bi2.compareTo(bi1) == 0);
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
                debugAssert(bi2.compareTo(bi1) == 0);
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
                debugAssert(bi2.compareTo(bi1) == 0);
            }
        }

        System.out.println("- done");
    }
    
    /*
     * Test the equals methods in Key and KeyState
     */
    public void testKeyEquality() {
        _key1.clear().to(5);
        _key2.clear().to(5);
        assertEquals(_key1, _key2);
        KeyState ks = new KeyState(_key2);
        assertEquals(_key1, ks);
        assertEquals(_key2, ks);
        _key1.clear().to("PADRAIG");
        _key2.clear().to("PADRAIG");
        assertEquals(_key1, _key2);
        ks = new KeyState(_key2);
        assertEquals(ks, _key2);
        assertEquals(ks, _key1);
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

    // public static void main(final String[] args) throws Exception {
    // new KeyTest1().runTest();
    // }

    public void runAllTests() throws Exception {
        test1();
        test2();
        test3();
        test4();
        test5();
        test6();
        test7();
        test8();
        test9();
        test10();
        test11();
        test12();
        test13();
    }

    public void debugAssert(boolean condition) {
        if (!condition) {
            System.out.println();
            System.out.println(" lv1=" + lv1 + " lv2=" + lv2);
            System.out.println(" key1="
                    + Util.hexDump(_key1.getEncodedBytes(), 0,
                            _key1.getEncodedSize()));
            System.out.println(" key2="
                    + Util.hexDump(_key2.getEncodedBytes(), 0,
                            _key2.getEncodedSize()));
            System.out.println("Assertion failure breakpoint");
        }
        Assert.assertTrue(condition);
    }

    public void debugAssertFloat(boolean condition) {
        if (!condition) {
            System.out.println();
            System.out.println(" fv1=" + fv1 + " (" + floatBits(fv1) + ")"
                    + " fv2=" + fv2 + " (" + floatBits(fv2) + ")");
            System.out.println(" key1="
                    + Util.hexDump(_key1.getEncodedBytes(), 0,
                            _key1.getEncodedSize()));
            System.out.println(" key2="
                    + Util.hexDump(_key2.getEncodedBytes(), 0,
                            _key2.getEncodedSize()));
            System.out.println("Assertion failure breakpoint");
        }
        Assert.assertTrue(condition);
    }

    public void debugAssertDouble(boolean condition) {
        if (!condition) {
            System.out.println();
            System.out.println(" dv1=" + dv1 + " (" + doubleBits(dv1) + ")"
                    + " dv2=" + dv2 + " (" + doubleBits(dv2) + ")");
            System.out.println(" key1="
                    + Util.hexDump(_key1.getEncodedBytes(), 0,
                            _key1.getEncodedSize()));
            System.out.println(" key2="
                    + Util.hexDump(_key2.getEncodedBytes(), 0,
                            _key2.getEncodedSize()));
            System.out.println("Assertion failure breakpoint");
        }
        Assert.assertTrue(condition);
    }

    private String floatBits(final float v) {
        final int bits = Float.floatToIntBits(v);
        final StringBuilder sb = new StringBuilder();
        Util.hex(sb, bits, 8);
        return sb.toString();
    }

    private String doubleBits(final double v) {
        final long bits = Double.doubleToLongBits(v);
        final StringBuilder sb = new StringBuilder();
        Util.hex(sb, bits, 16);
        return sb.toString();
    }

    private void debug(boolean condition) {
        if (!condition) {
            return;
        }
        return; // <-- breakpoint here
    }
}
