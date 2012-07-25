/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.PersistitUnitTestCase;
import com.persistit.Value;
import com.persistit.encoding.CollectionValueCoder;
import com.persistit.exception.PersistitException;

public class ValueTest2 extends PersistitUnitTestCase {

    Exchange _exchange;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _exchange = _persistit.getExchange("persistit", getClass().getSimpleName(), true);
    }

    @Override
    public void tearDown() throws Exception {
        _persistit.releaseExchange(_exchange);
        _exchange = null;
        super.tearDown();
    }

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");

        final TreeMap tmap = new TreeMap();
        for (int i = 0; i < 1000; i++) {
            tmap.put("Key " + i, "Value " + i);
        }
        _exchange.getValue().put(tmap);
        _exchange.clear().append("test1").store();

        _exchange.getValue().clear();

        _exchange.fetch();

        final Object tmap2 = _exchange.getValue().get();
        assertEquals(tmap2, tmap);
        System.out.println("- done");
    }

    /**
     * 
     * Every primitive type and its wrapper. Tests get() getTTT() where TTT is
     * Boolean, Byte, Short, Character, etc. getType() isDefined() isNull()
     * trim() trim(int n) clear() ensureFit() equals() put(ttt) where ttt is
     * null, boolean, byte, ..., double put(TTT) where TTT is Boolean, Byte, ...
     * Double put(ttt) where ttt is boolean, byte, ..., double
     * 
     */

    @Test
    public void test2() throws PersistitException {
        System.out.print("test2 ");
        final Value value = _exchange.getValue();

        value.clear();
        value.trim();
        assertEquals(false, value.isDefined());
        assertEquals(0, value.getEncodedSize());
        assertEquals(0, value.getEncodedBytes().length);

        value.put(null);
        value.trim();
        assertEquals(Void.TYPE, value.getType());
        assertEquals(true, value.isNull());
        assertEquals(true, value.isDefined());
        assertEquals(value.get(), null);

        value.put(true);
        value.trim();
        assertEquals(value.getType(), Boolean.TYPE);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getBoolean(), true);
        assertEquals(value.get(), Boolean.TRUE);

        value.put(false);
        assertEquals(value.getType(), Boolean.TYPE);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getBoolean(), false);
        assertEquals(value.get(), Boolean.FALSE);

        value.put(Boolean.TRUE);
        value.trim();
        assertEquals(value.getType(), Boolean.class);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getBoolean(), true);
        assertEquals(value.get(), Boolean.TRUE);

        value.put(Boolean.FALSE);
        assertEquals(value.getType(), Boolean.class);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getBoolean(), false);
        assertEquals(value.get(), Boolean.FALSE);

        value.put((byte) 123);
        assertEquals(value.getType(), Byte.TYPE);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getByte(), (byte) 123);
        assertEquals(value.get(), new Byte((byte) 123));

        value.put(new Byte((byte) 123));
        assertEquals(value.getType(), Byte.class);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getByte(), (byte) 123);
        assertEquals(value.get(), new Byte((byte) 123));

        value.put((short) 234);
        assertEquals(value.getType(), Short.TYPE);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getShort(), (short) 234);
        assertEquals(value.get(), new Short((short) 234));

        value.put(new Short((short) 234));
        assertEquals(value.getType(), Short.class);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getShort(), (short) 234);
        assertEquals(value.get(), new Short((short) 234));

        value.put((char) 345);
        assertEquals(value.getType(), Character.TYPE);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getChar(), (char) 345);
        assertEquals(value.get(), new Character((char) 345));

        value.put(new Character((char) 345));
        assertEquals(value.getType(), Character.class);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getChar(), (char) 345);
        assertEquals(value.get(), new Character((char) 345));

        value.put(456);
        assertEquals(value.getType(), Integer.TYPE);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getInt(), 456);
        assertEquals(value.get(), new Integer(456));

        value.put(new Integer(456));
        assertEquals(value.getType(), Integer.class);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        value.trim();
        assertEquals(value.getInt(), 456);
        assertEquals(value.get(), new Integer(456));

        value.put((float) 567);
        assertEquals(value.getType(), Float.TYPE);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getFloat(), 567, 0.0001f);
        assertEquals(value.get(), new Float(567));

        value.put(new Float(567));
        assertEquals(value.getType(), Float.class);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getFloat(), 567, 0.0001f);
        assertEquals(value.get(), new Float(567));

        value.put((double) 678);
        assertEquals(value.getType(), Double.TYPE);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        assertEquals(value.getDouble(), 678, 0.0001f);
        assertEquals(value.get(), new Double(678));

        value.put(new Double(678));
        assertEquals(value.getType(), Double.class);
        assertEquals(value.isNull(), false);
        assertEquals(value.isDefined(), true);
        value.trim(100);
        assertEquals(value.getDouble(), 678, 0.0001f);
        assertEquals(value.get(), new Double(678));

        final Value value2 = new Value(value);
        assertTrue(value.equals(value2));
        value2.put("not a Double");
        assertTrue(!value.equals(value2));
        value.copyTo(value2);
        assertTrue(value.equals(value2));

        assertTrue(value2.isDefined());
        value2.clear();
        assertTrue(!value2.isDefined());
        assertEquals(0, value2.getEncodedSize());
        _exchange.clear().append("test2").store();
        System.out.println("- done");
    }

    @Test
    public void test3() throws PersistitException {
        System.out.print("test3 ");
        testNullArray();
        testByteArray();
        testShortArray();
        testCharArray();
        testIntArray();
        testLongArray();
        testFloatArray();
        testDoubleArray();
        _exchange.clear().append("test3").store();
        System.out.println("- done");
    }

    @Test
    public void testNullArray() {
        final Value value = _exchange.getValue();
        final Object[] o1 = new Object[10]; // leave null
        value.putObjectArray(o1);
        assertEquals(Object[].class, value.getType());
        assertTrue(equals(o1, value.get()));
        assertTrue(equals(o1, value.getObjectArray()));
        value.clear();
        value.put(o1);
        assertEquals(Object[].class, value.getType());
        assertTrue(equals(o1, value.get()));
        assertTrue(equals(o1, value.getObjectArray()));
        o1[5] = "Not null";
        assertTrue(!equals(o1, value.getObjectArray()));
        value.put(o1);
        assertTrue(equals(o1, value.getObjectArray()));
        assertEquals(10, value.getArrayLength());
        System.out.print(".");
    }

    /**
     * Tests array methods on byte[] and Byte[]
     * 
     */
    @Test
    public void testByteArray() {
        final Value value = _exchange.getValue();

        final byte[] a1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        value.putByteArray(a1);
        assertEquals(byte[].class, value.getType());
        assertTrue(equals(a1, value.get()));
        assertTrue(equals(a1, value.getByteArray()));
        value.put(a1);
        assertTrue(equals(a1, value.getByteArray()));
        final byte[] a2 = new byte[a1.length];
        value.getByteArray(a2, 0, 0, a2.length);
        assertTrue(equals(a1, a2));
        value.putByteArray(a1, 2, 7);
        a2[5] = -1;
        value.getByteArray(a2, 0, 2, 7);
        assertTrue(equals(a1, a2));

        final Byte[] b1 = new Byte[10];
        for (int i = 0; i < 10; i++) {
            b1[i] = new Byte((byte) i);
        }
        value.putObjectArray(b1);
        assertEquals(Byte[].class, value.getType());
        assertTrue(equals(b1, value.get()));
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getArray()));
        assertEquals(10, value.getArrayLength());
        System.out.print(".");
    }

    /**
     * Tests array methods on short[] and Short[]
     * 
     */
    @Test
    public void testShortArray() {
        final Value value = _exchange.getValue();

        final short[] a1 = new short[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        value.putShortArray(a1);
        assertEquals(short[].class, value.getType());
        assertTrue(equals(a1, value.get()));
        assertTrue(equals(a1, value.getShortArray()));
        value.put(a1);
        assertTrue(equals(a1, value.getShortArray()));
        final short[] a2 = new short[a1.length];
        value.getShortArray(a2, 0, 0, a2.length);
        assertTrue(equals(a1, a2));
        value.putShortArray(a1, 2, 7);
        a2[5] = -1;
        value.getShortArray(a2, 0, 2, 7);
        assertTrue(equals(a1, a2));

        final Short[] b1 = new Short[10];
        for (int i = 0; i < 10; i++) {
            b1[i] = new Short((short) i);
        }
        value.putObjectArray(b1);
        assertEquals(Short[].class, value.getType());
        assertTrue(equals(b1, value.get()));
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getArray()));
        assertEquals(10, value.getArrayLength());
        System.out.print(".");
    }

    /**
     * Tests array methods on char[] and Character[]
     * 
     */
    @Test
    public void testCharArray() {
        final Value value = _exchange.getValue();

        final char[] a1 = new char[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        value.putCharArray(a1);
        assertEquals(char[].class, value.getType());
        assertTrue(equals(a1, value.get()));
        assertTrue(equals(a1, value.getCharArray()));
        value.put(a1);
        assertTrue(equals(a1, value.getCharArray()));
        final char[] a2 = new char[a1.length];
        value.getCharArray(a2, 0, 0, a2.length);
        assertTrue(equals(a1, a2));
        value.putCharArray(a1, 2, 7);
        a2[5] = 2345;
        value.getCharArray(a2, 0, 2, 7);
        assertTrue(equals(a1, a2));

        final Character[] b1 = new Character[10];
        for (int i = 0; i < 10; i++) {
            b1[i] = new Character((char) i);
        }
        value.putObjectArray(b1);
        assertEquals(Character[].class, value.getType());
        assertTrue(equals(b1, value.get()));
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getArray()));
        assertEquals(10, value.getArrayLength());
        System.out.print(".");
    }

    /**
     * Tests array methods on int[] and Integer[]
     * 
     */
    @Test
    public void testIntArray() {
        final Value value = _exchange.getValue();

        final int[] a1 = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        value.putIntArray(a1);
        assertEquals(int[].class, value.getType());
        assertTrue(equals(a1, value.get()));
        assertTrue(equals(a1, value.getIntArray()));
        value.put(a1);
        assertTrue(equals(a1, value.getIntArray()));
        final int[] a2 = new int[a1.length];
        value.getIntArray(a2, 0, 0, a2.length);
        assertTrue(equals(a1, a2));
        value.putIntArray(a1, 2, 7);
        a2[5] = 2345;
        value.getIntArray(a2, 0, 2, 7);
        assertTrue(equals(a1, a2));

        final Integer[] b1 = new Integer[10];
        for (int i = 0; i < 10; i++) {
            b1[i] = new Integer(i);
        }
        value.putObjectArray(b1);
        assertEquals(Integer[].class, value.getType());
        assertTrue(equals(b1, value.get()));
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getArray()));
        assertEquals(10, value.getArrayLength());
        System.out.print(".");
    }

    /**
     * Tests array methods on long[] and Long[]
     * 
     */
    @Test
    public void testLongArray() {
        final Value value = _exchange.getValue();

        final long[] a1 = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        value.putLongArray(a1);
        assertEquals(long[].class, value.getType());
        assertTrue(equals(a1, value.get()));
        assertTrue(equals(a1, value.getLongArray()));
        value.put(a1);
        assertTrue(equals(a1, value.getLongArray()));
        final long[] a2 = new long[a1.length];
        value.getLongArray(a2, 0, 0, a2.length);
        assertTrue(equals(a1, a2));
        value.putLongArray(a1, 2, 7);
        a2[5] = 2345;
        value.getLongArray(a2, 0, 2, 7);
        assertTrue(equals(a1, a2));

        final Long[] b1 = new Long[10];
        for (int i = 0; i < 10; i++) {
            b1[i] = new Long(i);
        }
        value.putObjectArray(b1);
        assertEquals(Long[].class, value.getType());
        assertTrue(equals(b1, value.get()));
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getArray()));
        assertEquals(10, value.getArrayLength());
        System.out.print(".");
    }

    /**
     * Tests array methods on float[] and Float[]
     * 
     */
    @Test
    public void testFloatArray() {
        final Value value = _exchange.getValue();

        final float[] a1 = new float[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        value.putFloatArray(a1);
        assertEquals(float[].class, value.getType());
        assertTrue(equals(a1, value.get()));
        assertTrue(equals(a1, value.getFloatArray()));
        value.put(a1);
        assertTrue(equals(a1, value.getFloatArray()));
        final float[] a2 = new float[a1.length];
        value.getFloatArray(a2, 0, 0, a2.length);
        assertTrue(equals(a1, a2));
        value.putFloatArray(a1, 2, 7);
        a2[5] = -1;
        value.getFloatArray(a2, 0, 2, 7);
        assertTrue(equals(a1, a2));

        final Float[] b1 = new Float[10];
        for (int i = 0; i < 10; i++) {
            b1[i] = new Float(i);
        }
        value.putObjectArray(b1);
        assertEquals(Float[].class, value.getType());
        assertTrue(equals(b1, value.get()));
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getArray()));
        assertEquals(10, value.getArrayLength());
        System.out.print(".");
    }

    /**
     * Tests array methods on double[] and Double[]
     * 
     */
    @Test
    public void testDoubleArray() {
        final Value value = _exchange.getValue();

        final double[] a1 = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        value.putDoubleArray(a1);
        assertEquals(double[].class, value.getType());
        assertTrue(equals(a1, value.get()));
        assertTrue(equals(a1, value.getDoubleArray()));
        value.put(a1);
        assertTrue(equals(a1, value.getDoubleArray()));
        final double[] a2 = new double[a1.length];
        value.getDoubleArray(a2, 0, 0, a2.length);
        assertTrue(equals(a1, a2));
        value.putDoubleArray(a1, 2, 7);
        a2[5] = -1;
        value.getDoubleArray(a2, 0, 2, 7);
        assertTrue(equals(a1, a2));

        final Double[] b1 = new Double[10];
        for (int i = 0; i < 10; i++) {
            b1[i] = new Double(i);
        }
        value.putObjectArray(b1);
        assertEquals(Double[].class, value.getType());
        assertTrue(equals(b1, value.get()));
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getObjectArray()));
        value.put(b1);
        assertTrue(equals(b1, value.getArray()));
        assertEquals(10, value.getArrayLength());
        System.out.print(".");
    }

    /**
     * Test multi-dimenional arrays
     * 
     */
    @Test
    public void test4() throws PersistitException {
        System.out.print("test4 ");
        final Value value = _exchange.getValue();

        final double[] a1 = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        final double[][] aa1 = new double[][] { a1, a1, a1 };
        final double[][][] aaa1 = new double[][][] { aa1, aa1, aa1 };

        value.put(a1);
        assertEquals(double[].class, value.getType());
        assertEquals(10, value.getArrayLength());

        value.put(aa1);
        assertEquals(double[][].class, value.getType());
        assertEquals(3, value.getArrayLength());

        value.put(aaa1);
        assertEquals(double[][][].class, value.getType());
        assertEquals(3, value.getArrayLength());

        assertTrue(equals(aaa1, value.get()));
        assertTrue(equals(aaa1, value.getArray()));
        assertTrue(equals(aaa1, value.getObjectArray()));

        assertEquals(3, value.getArrayLength());

        final String[] s1 = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" };
        final String[][] ss1 = new String[][] { s1, s1, s1 };
        final String[][][] sss1 = new String[][][] { ss1, ss1, ss1 };

        value.put(s1);
        assertEquals(String[].class, value.getType());
        assertEquals(10, value.getArrayLength());

        value.put(ss1);
        assertEquals(String[][].class, value.getType());
        assertEquals(3, value.getArrayLength());

        value.put(sss1);
        assertEquals(String[][][].class, value.getType());
        assertEquals(3, value.getArrayLength());

        assertTrue(equals(sss1, value.get()));
        assertTrue(equals(sss1, value.getArray()));
        assertTrue(equals(sss1, value.getObjectArray()));

        assertEquals(3, value.getArrayLength());
        _exchange.clear().append("test4").store();

        System.out.println("- done");
    }

    @Test
    public void test5() throws PersistitException {
        System.out.print("test5 ");
        final Value value = _exchange.getValue();
        System.gc();
        final String[] strings = new String[20];
        for (int iter = 0; iter < 10000; iter++) {
            if ((iter % 2000) == 0) {
                final long inUseMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                System.out.println("ValueTest2.test5: memory in use=" + inUseMem);
            }
            for (int i = 0; i < 20; i++) {
                if (i >= 10) {
                    strings[i] = strings[i - 10];
                } else {
                    strings[i] = new String("this is a brand new string");
                }
            }
            value.put(strings);
            final String[] t = (String[]) value.get();
        }
        _exchange.clear().append("test5").store();

        System.out.println("- done");
    }

    public static class CustomSet extends ArrayList {
        private final static long serialVersionUID = 1L;
    }

    @Test
    public void test6() throws PersistitException {
        System.out.print("test6 ");
        final String a1 = "a1";
        final String a2 = "a2";
        final String a3 = "a3";
        _persistit.getCoderManager().registerValueCoder(CustomSet.class, new CollectionValueCoder());
        final Value value = _exchange.getValue();
        final Map map1 = new TreeMap();
        final CustomSet cs1 = new CustomSet();
        cs1.add(a1);
        cs1.add(a2);
        cs1.add(a3);
        map1.put(a1, cs1);
        map1.put(a2, cs1);
        map1.put(a3, cs1);
        value.put(map1);
        final Map map2 = (Map) value.get(map1);

        assertEquals(3, map2.size());
        final Iterator iter = map2.keySet().iterator();
        final String z1 = (String) iter.next();
        final String z2 = (String) iter.next();
        final String z3 = (String) iter.next();
        assertEquals(a1 + a2 + a3, z1 + z2 + z3);

        final Object o = map2.get(z1);
        assertTrue(o instanceof CustomSet);
        final CustomSet cs2 = (CustomSet) o;
        assertEquals(3, cs2.size());
        for (final Iterator iter2 = cs2.iterator(); iter2.hasNext();) {
            final Object p = iter2.next();
            assertTrue(p.equals(a1) || p.equals(a2) || p.equals(a3));
        }
        assertEquals(map2.get(z2), cs2);
        assertEquals(map2.get(z3), cs2);

        _persistit.getCoderManager().unregisterValueCoder(CustomSet.class);
        _exchange.clear().append("test6").store();

        System.out.println("- done");
    }

    @Test
    public void test7() throws PersistitException {
        System.out.print("test7 ");
        final String a1 = "a1";
        final String a2 = "a2";
        final String a3 = "a3";
        final Value value = _exchange.getValue();
        final Map map1 = new TreeMap();
        final CustomSet cs1 = new CustomSet();
        cs1.add(a1);
        cs1.add(a2);
        cs1.add(a3);
        cs1.add(map1);
        cs1.add(cs1);
        map1.put(a1, cs1);
        map1.put(a2, cs1);
        map1.put(a3, cs1);
        value.put(map1);
        final Map map2 = (Map) value.get();

        assertEquals(3, map2.size());
        final Iterator iter = map2.keySet().iterator();
        final String z1 = (String) iter.next();
        final String z2 = (String) iter.next();
        final String z3 = (String) iter.next();
        assertEquals(a1 + a2 + a3, z1 + z2 + z3);

        final Object o = map2.get(z1);
        assertTrue(o instanceof CustomSet);
        final CustomSet cs2 = (CustomSet) o;
        assertEquals(5, cs2.size());
        for (final Iterator iter2 = cs2.iterator(); iter2.hasNext();) {
            final Object p = iter2.next();
            if (p instanceof String) {
                assertTrue(p.equals(a1) || p.equals(a2) || p.equals(a3));
            } else if (p instanceof Map) {
                assertTrue(p == map2);
            } else {
                assertTrue(p == cs2);
            }
        }
        assertEquals(map2.get(z2), cs2);
        assertEquals(map2.get(z3), cs2);
        _exchange.clear().append("test7").store();
        System.out.println("- done");
    }

    @Test
    public void test8() throws PersistitException {
        System.out.print("test8 ");
        final String a1 = "a1";
        final String a2 = "a2";
        final String a3 = "a3";
        final Value value = _exchange.getValue();
        final Map map1 = new TreeMap();
        final CustomSet cs1 = new CustomSet();
        cs1.add(a1);
        cs1.add(a2);
        cs1.add(a3);
        cs1.add(map1);
        cs1.add(cs1);
        map1.put(a1, cs1);
        map1.put(a2, cs1);
        map1.put(a3, cs1);
        value.put(map1);
        final Map map2 = (Map) value.get();

        assertEquals(3, map2.size());
        final Iterator iter = map2.keySet().iterator();
        final String z1 = (String) iter.next();
        final String z2 = (String) iter.next();
        final String z3 = (String) iter.next();
        assertEquals(a1 + a2 + a3, z1 + z2 + z3);

        final Object o = map2.get(z1);
        assertTrue(o instanceof CustomSet);
        final CustomSet cs2 = (CustomSet) o;
        assertEquals(5, cs2.size());
        for (final Iterator iter2 = cs2.iterator(); iter2.hasNext();) {
            final Object p = iter2.next();
            if (p instanceof String) {
                assertTrue(p.equals(a1) || p.equals(a2) || p.equals(a3));
            } else if (p instanceof Map) {
                assertTrue(p == map2);
            } else {
                assertTrue(p == cs2);
            }
        }
        assertEquals(map2.get(z2), cs2);
        assertEquals(map2.get(z3), cs2);
        _exchange.clear().append("test8").store();
        System.out.println("- done");
    }

    @Test
    public void test9() throws PersistitException {
        System.out.print("test9 ");
        final String[] strings = { "a", "b", "c" };
        final HashMap map = new HashMap();
        final Object[] example = new Object[] { new Byte((byte) 1), new Integer(2), new Long(3L), new Float(4.0F),
                new Double(5.0), new Date(), "a", strings, "a", strings, null, map, };

        example[11] = example;
        map.put("aKey", "aValue");
        map.put("bKey", "bValue");
        map.put("example", example);

        final Value value = _exchange.getValue();
        value.put(example);
        _exchange.clear().append("test9").store();
        System.out.println("Displayable form: " + value);
        System.out.println("- done");
    }

    @Test
    public void test10() throws PersistitException {
        System.out.print("test10 ");
        final Person p1 = new Person("Jones", "Mary", new Date(), 65000);
        final Person p2 = new Person("Smith", "John", new Date(), 80000);
        p1.friends = new Person[] { p2 };
        p2.friends = new Person[] { p1 };
        final Value value = _exchange.getValue();
        value.put(p1);
        _exchange.clear().append("test10").store();
        System.out.println("Displayable form: " + value);

        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new ValueTest2().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        // try {
        // // Enable the security manager
        // System.out.println("Attempting to install SecurityManager");
        // final SecurityManager sm = new SecurityManager();
        // System.setSecurityManager(sm);
        // } catch (final SecurityException se) {
        // se.printStackTrace();
        // }

        _exchange = _persistit.getExchange("persistit", "ValueTest2", true);

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
    }

    public boolean equals(final Object a, final Object b) {
        if ((a == null) || (b == null)) {
            return a == b;
        }
        if (a.getClass().isArray()) {
            if (!b.getClass().isArray()) {
                return false;
            }
            if (a.getClass().getComponentType() != b.getClass().getComponentType()) {
                return false;
            }
            if (Array.getLength(a) != Array.getLength(b)) {
                return false;
            }
            for (int index = Array.getLength(a); --index >= 0;) {
                if (!equals(Array.get(a, index), Array.get(b, index))) {
                    return false;
                }
            }
            return true;
        } else if (a.getClass().isPrimitive()) {
            return a == b;
        } else {
            return a.equals(b);
        }
    }

    private void debug(boolean condition) {
        if (!condition) {
            return;
        }
        return; // <-- breakpoint here
    }

    static class Person implements Serializable {
        private final static long serialVersionUID = 1L;

        String lastName;
        String firstName;
        Date dob;
        long salary;
        Person[] friends;

        private Person() {
        }

        Person(final String lastName, final String firstName, final Date dob, final long salary) {
            this.lastName = lastName;
            this.firstName = firstName;
            this.dob = dob;
            this.salary = salary;
        }
    }

}
