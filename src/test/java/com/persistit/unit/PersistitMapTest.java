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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.KeyFilter;
import com.persistit.PersistitMap;
import com.persistit.exception.PersistitException;
import com.persistit.util.Util;

public class PersistitMapTest extends PersistitUnitTestCase {

    private final Object[] TEST_VALUES = new Object[] { "Value 0", "Value 1", new Integer(2), new TreeMap(),
            new S("Serializable4"), new S("Serializable5"), BigDecimal.valueOf(6), new Boolean(true),
            new Byte((byte) 8), new Short((short) 9), new Character((char) 10), new Integer(11), new Long(12),
            new Float(13.0f), new Double(14.0d), new Date(), // 15
            new ArrayList(), // 16
    };

    private final Object[] TEST_KEYS = new Object[] { "key0", "key1", BigDecimal.valueOf(2), new Boolean(true),
            new Byte((byte) 4), new Short((short) 5), new Character((char) 6), new Integer(7), new Long(8),
            new Float(9.0f), new Double(10.0d), new Date(), new Integer(12), new byte[] { 1, 2, 3, 4, 5, 0, 1, 2 }, "", // 14
            new Integer(15), new Integer(16), };

    private static class S implements Serializable {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        private final String _stuff;

        S(final String s) {
            _stuff = s;
        }

        public boolean equals(final Object o) {
            return ((S) o)._stuff.equals(_stuff);
        }
    }

    @Test
    public void test1() throws PersistitException {
        System.out.print("test1 ");

        final TreeMap tmap = new TreeMap();
        final PersistitMap pmap = new PersistitMap(_persistit.getExchange("persistit", "PersistitMapTest", true));

        final TreeMap zmap = new TreeMap();
        zmap.put("this", "that");

        pmap.clear();
        assertTrue(pmap.size() == 0);

        for (int i = 0; i < TEST_KEYS.length; i++) {
            assertEquals(null, pmap.get(TEST_KEYS[i]));
        }

        for (int i = 0; i < TEST_KEYS.length; i++) {
            pmap.put(TEST_KEYS[i], TEST_VALUES[i]);
        }

        assertEquals(TEST_KEYS.length, pmap.size());

        for (int i = 0; i < TEST_KEYS.length; i++) {
            final Object value = pmap.get(TEST_KEYS[i]);
            debug(!TEST_VALUES[i].equals(value));
            assertEquals(TEST_VALUES[i], value);
        }

        int count = 0;
        for (final Iterator iter = pmap.entrySet().iterator(); iter.hasNext();) {
            count++;
            final Map.Entry entry = (Map.Entry) iter.next();
            if ((count % 2) == 0) {
                iter.remove();
            } else {
                entry.setValue(null);
            }
        }

        count = 0;
        for (final Iterator iter = pmap.entrySet().iterator(); iter.hasNext();) {
            count++;
            final Map.Entry entry = (Map.Entry) iter.next();
            assertEquals(null, entry.getValue());
        }
        assertEquals(TEST_KEYS.length / 2 + 1, count);

        System.out.println("- done");
    }

    @Test
    public void test2() throws PersistitException {
        System.out.print("test2 ");

        final TreeMap tmap = new TreeMap();
        final PersistitMap pmap = new PersistitMap(_persistit.getExchange("persistit", "PersistitMapTest", true));
        final TreeMap zmap = new TreeMap();
        zmap.put("this", "that");

        tmap.clear();
        pmap.clear();
        assertTrue(tmap.size() == pmap.size());

        tmap.put("k1", "value");
        tmap.put("k2", null);
        tmap.put("k3", new Integer(3));
        tmap.put("k4", "13");
        tmap.put("Big Int 1", BigInteger.valueOf(1));
        tmap.put("Big Int 2", BigInteger.valueOf(2));
        tmap.put("k2a_map", zmap);

        pmap.putAll(tmap);
        assertTrue(tmap.size() == pmap.size());
        assertTrue(tmap.equals(pmap));
        assertTrue(pmap.equals(tmap));
        assertTrue(pmap.get("not present") == null);
        assertTrue(pmap.get("k3") instanceof Integer);
        assertTrue(pmap.get("k2") == null);
        assertTrue(pmap.containsKey("k2"));
        assertTrue(!pmap.containsKey("not present"));

        final SortedMap pmap1 = pmap.subMap("k2", "k4");
        final SortedMap tmap1 = tmap.subMap("k2", "k4");
        assertTrue(tmap1.size() == pmap1.size());
        assertTrue(tmap1.equals(pmap1));
        assertTrue(pmap1.equals(tmap1));
        assertTrue(pmap1.get("not present") == null);
        assertTrue(pmap1.get("k3") instanceof Integer);
        assertTrue(pmap1.get("k2") == null);
        assertTrue(pmap1.containsKey("k2"));
        assertTrue(!pmap1.containsKey("not present"));
        assertTrue(!pmap1.containsKey("k1"));
        assertTrue(!pmap1.containsKey("k1"));

        System.out.println("- done");
    }

    @Test
    public void test3() throws PersistitException {
        System.out.print("test3 ");

        final TreeMap tmap = new TreeMap();
        final PersistitMap pmap = new PersistitMap(_persistit.getExchange("persistit", "PersistitMapTest", true));

        tmap.clear();
        pmap.clear();

        for (int i = 0; i < 10; i++) {
            tmap.put("" + i, new Integer(0));
            pmap.put("" + i, new Integer(0));
        }

        for (int i = 0; i < 10; i++) {
            for (final Iterator iter = tmap.entrySet().iterator(); iter.hasNext();) {
                final Map.Entry entry = (Map.Entry) iter.next();
                final int v = ((Integer) entry.getValue()).intValue();
                entry.setValue(new Integer(v + 1));
            }

            for (final Iterator iter = pmap.entrySet().iterator(); iter.hasNext();) {
                final Map.Entry entry = (Map.Entry) iter.next();
                final int v = ((Integer) entry.getValue()).intValue();
                entry.setValue(new Integer(v + 1));
            }

        }

        assertTrue(pmap.equals(tmap));
        assertTrue(tmap.equals(pmap));

        System.out.println("- done");
    }

    @Test
    public void test4() throws PersistitException {
        System.out.print("test4 ");

        final TreeMap tmap = new TreeMap();
        final PersistitMap pmap = new PersistitMap(_persistit.getExchange("persistit", "PersistitMapTest", true));

        tmap.clear();
        pmap.clear();

        final Date date1 = new Date();
        final Date date2 = new Date(date1.getTime() + 100);
        final Date date3 = new Date(date1.getTime() + 200);

        tmap.put("d1a", date1);
        tmap.put("d1b", date1);
        tmap.put("d1c", date1);
        tmap.put("d2a", date2);
        tmap.put("d2b", date2);
        tmap.put("d2c", date2);
        tmap.put("d3a", date3);
        tmap.put("d3b", date3);
        tmap.put("d3c", date3);

        pmap.putAll(tmap);
        final TreeMap tmap2 = new TreeMap(pmap);

        pmap.put("serialized_tmap", tmap);
        final TreeMap tmap3 = (TreeMap) pmap.get("serialized_tmap");

        assertTrue(tmap.equals(tmap2));

        final Date d1a = (Date) tmap.get("d1a");
        final Date d1b = (Date) tmap.get("d1b");

        assertTrue(d1a == date1);
        assertTrue(d1b == date1);

        final Date p1a = (Date) pmap.get("d1a");
        final Date p1b = (Date) pmap.get("d1b");

        assertTrue(p1a.equals(p1b));
        assertTrue(p1a != date1);
        assertTrue(p1b != date1);
        assertTrue(p1a != p1b);

        final Date s1a = (Date) tmap3.get("d1a");
        final Date s1b = (Date) tmap3.get("d1b");

        assertTrue(s1a.equals(s1b));
        assertTrue(s1a != date1);
        assertTrue(s1b != date1);
        assertTrue(s1a == s1b);

        System.out.println("- done");
    }

    @Test
    public void test5() throws PersistitException {
        System.out.print("test5 ");

        final TreeMap tmap = new TreeMap();
        final PersistitMap pmap = new PersistitMap(_persistit.getExchange("persistit", "PersistitMapTest", true));

        pmap.clear();
        assertTrue(pmap.size() == 0);
        TreeMap tmap0 = null;
        TreeMap tmap1 = null;

        for (int index = 0; index < 1000; index++) {
            if (index % 100 == 0) {
                System.out.print(" i" + index);
            }
            try {
                tmap.put(new Integer(index), "value=" + index);
                tmap1 = (TreeMap) pmap.put("myKey", tmap);
                // if (index > 662)
                assertEquals(tmap1, tmap0);
                // if (index > 660)
                tmap0 = new TreeMap(tmap);
            } catch (final RuntimeException e) {
                System.out.println("inserting index=" + index);
                e.printStackTrace();
                throw e;
            }
        }

        for (int index = 0; index < 1000; index++) {
            if (index % 100 == 0) {
                System.out.print(" r" + index);
            }
            try {
                tmap.remove(new Integer(index));
                // if (index == 823) Debug.debug0(true);
                tmap1 = (TreeMap) pmap.put("myKey", tmap);
                assertEquals(tmap1, tmap0);
                tmap0 = new TreeMap(tmap);
            } catch (final RuntimeException e) {
                System.out.println("inserting index=" + index);
                e.printStackTrace();
                throw e;
            }
        }

        System.out.println("- done");
    }

    @Test
    public void test6() throws PersistitException {
        System.out.print("test6 ");

        final Exchange ex = _persistit.getExchange("persistit", "PersistitMapTest", true).append("a").append("b");

        final PersistitMap pmap = new PersistitMap(ex);

        pmap.clear();
        assertTrue(pmap.size() == 0);

        for (int index = 0; index < 1000; index++) {
            // com.persistit.Debug.debug1(index == 625);
            pmap.put(new Integer(index), "Value=" + index);
            ex.getValue().put(index);
            ex.append(index);
            ex.append("c").store();
            ex.to("d").store();
            ex.cut(2);
        }

        final PersistitMap.ExchangeIterator iter = (PersistitMap.ExchangeIterator) pmap.keySet().iterator();

        iter.setFilterTerm(KeyFilter.orTerm(new KeyFilter.Term[] {
                KeyFilter.rangeTerm(new Integer(100), new Integer(300)),
                KeyFilter.rangeTerm(new Integer(800), new Integer(900), false, false),
                KeyFilter.simpleTerm(new Integer(999)) }));

        final String s = iter.getKeyFilter().toString();
        final String t = "{\"a\",\"b\",>{100:300,(800:900),999}<}";
        assertEquals(t, s);

        int count = 0;
        while (iter.hasNext()) {
            final Object object = iter.next();
            final Integer key = (Integer) object;
            final int k = key.intValue();
            assertTrue(((k >= 100) && (k <= 300)) || ((k > 800) && (k < 900)) || (k == 999));
            count++;
        }
        assertEquals(201 + 99 + 1, count);

        System.out.println("- done");
    }

    @Test
    public void test7() throws PersistitException {
        System.out.print("test7 ");

        final Exchange ex = _persistit.getExchange("persistit", "PersistitMapTest", true);

        final PersistitMap pmap = new PersistitMap(ex);

        pmap.clear();
        assertEquals(0, pmap.size());
        assertTrue(pmap.isEmpty());
        pmap.put("a", "first");
        assertEquals(1, pmap.size());
        // long cc = ex.getChangeCount();
        final String was = (String) pmap.put("a", "second");
        assertEquals("first", was);
        // assertEquals(cc, ex.getChangeCount());
        pmap.remove("b");
        assertEquals(1, pmap.size());
        // assertEquals(cc, ex.getChangeCount());
        pmap.put("b", "something");
        boolean cme = false;
        Iterator iterator = pmap.entrySet().iterator();
        final Object key = ((Map.Entry) iterator.next()).getKey();
        assertEquals("a", key);
        assertEquals(2, pmap.size());
        pmap.put("b", "somethingElse");

        iterator = pmap.entrySet().iterator();
        try {
            ((Map.Entry) iterator.next()).getKey();
        } catch (final ConcurrentModificationException e) {
            cme = true;
        }
        // assertTrue(!cme);
        pmap.remove("c");
        try {
            ((Map.Entry) iterator.next()).getKey();
        } catch (final ConcurrentModificationException e) {
            cme = true;
        }
        // assertTrue(!cme);
        pmap.put("c", "aardvaark");
        try {
            ((Map.Entry) iterator.next()).getKey();
        } catch (final ConcurrentModificationException e) {
            cme = true;
        }
        // assertTrue(cme);
        assertEquals(pmap.size(), 3);
        assertTrue(!pmap.isEmpty());
        ex.getValue().put("not through map");
        ex.clear().append("d").store();
        assertTrue(!pmap.isEmpty());
        assertEquals(pmap.size(), 4);

        assertTrue(pmap.containsKey("d"));
        assertTrue(pmap.containsKey("b"));
        assertTrue(!pmap.containsKey("e"));
        assertTrue(pmap.containsValue("not through map"));
        assertTrue(pmap.containsValue("somethingElse"));
        assertTrue(!pmap.containsValue("anotherThing"));

        iterator = pmap.entrySet().iterator();
        iterator.next();
        iterator.remove();
        assertEquals(pmap.size(), 3);
        assertTrue(!pmap.isEmpty());
        iterator.next();
        iterator.remove();
        assertEquals(pmap.size(), 2);
        assertTrue(!pmap.isEmpty());
        iterator.next();
        iterator.remove();
        assertEquals(pmap.size(), 1);
        assertTrue(!pmap.isEmpty());
        iterator.next();
        iterator.remove();
        assertEquals(pmap.size(), 0);
        assertTrue(pmap.isEmpty());
        assertTrue(!iterator.hasNext());
        ex.getValue().put("not through map");
        ex.clear().append("d").store();
        assertTrue(!pmap.isEmpty());
        assertEquals(1, pmap.size());
        ex.remove();
        assertTrue(pmap.isEmpty());
        assertEquals(0, pmap.size());

        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new PersistitMapTest().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        test1();
        test2();
        test3();
        test4();
        test5();
        test6();
        test7();
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
