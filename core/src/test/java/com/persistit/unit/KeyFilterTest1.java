/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Aug 15, 2004
 */
package com.persistit.unit;

import java.util.TreeMap;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.KeyState;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

/**
 * @version 1.0
 */
public class KeyFilterTest1 extends PersistitUnitTestCase {

    @Test
    public void test1() {
        System.out.print("test1 ");
        final Key key = new Key(_persistit);
        key.append("atlantic");
        key.append((float) 1.3);
        KeyFilter kf = new KeyFilter(key);
        String s = kf.toString();
        assertEquals("{\"atlantic\",(float)1.3}", s);

        kf = kf.append(KeyFilter.rangeTerm("x", "z"));

        key.append("y");
        assertTrue(kf.selected(key));
        key.to("w");
        assertTrue(!kf.selected(key));
        key.to("x");
        assertTrue(kf.selected(key));
        key.to("xx");
        assertTrue(kf.selected(key));
        key.to("yzzz");
        assertTrue(kf.selected(key));
        key.to("z");
        assertTrue(kf.selected(key));
        key.to("z0");
        assertTrue(!kf.selected(key));

        kf = kf.append(
                KeyFilter.orTerm(new KeyFilter.Term[] {
                        KeyFilter.rangeTerm(new Integer(100), new Integer(150)),
                        KeyFilter.rangeTerm(new Integer(200), new Integer(250)),
                        KeyFilter.rangeTerm(new Integer(300), new Integer(350),
                                true, false, null), })).limit(2, 5);

        s = kf.toString();
        String t = "{\"atlantic\",>(float)1.3,\"x\":\"z\",{100:150,200:250,[300:350)},*<}";
        assertEquals(t, s);

        key.to("x");

        key.append(125);
        assertTrue(kf.selected(key));
        key.to(175);
        assertTrue(!kf.selected(key));

        key.to(200);
        assertTrue(kf.selected(key));
        key.append("tom");
        assertTrue(kf.selected(key));
        key.append("dick");
        assertTrue(!kf.selected(key));
        key.append("harry");
        assertTrue(!kf.selected(key));
        final KeyFilter kf2 = kf.limit(2, 7);
        assertTrue(kf2.selected(key));

        s = kf2.toString();
        t = "{\"atlantic\",>(float)1.3,\"x\":\"z\",{100:150,200:250,[300:350)},*,*,*<}";
        assertEquals(t, s);

        key.cut(3);

        key.to(249);
        assertTrue(kf.selected(key));
        key.to(250);
        assertTrue(kf.selected(key));
        key.to(251);
        assertTrue(!kf.selected(key));
        key.to(299);
        assertTrue(!kf.selected(key));
        key.to(300);
        assertTrue(kf.selected(key));
        key.to(350);
        assertTrue(!kf.selected(key));

        System.out.println("- done");
    }

    @Test
    public void test2() {
        System.out.print("test2 ");
        final Key key = new Key(_persistit);
        key.append("atlantic");
        key.append((float) 1.3);
        KeyFilter kf = new KeyFilter(key);
        final String s = kf.toString();
        assertEquals("{\"atlantic\",(float)1.3}", s);
        kf = kf.append(KeyFilter.rangeTerm("x", "z", true, false));
        key.append("a");
        assertTrue(kf.next(key, Key.GT));
        assertEquals("{\"atlantic\",(float)1.3,\"x\"}-", key.toString());
        key.to("zz");
        assertTrue(kf.next(key, Key.LT));
        assertEquals("{\"atlantic\",(float)1.3,\"z\"}", key.toString());
        System.out.println("- done");
    }

    @Test
    public void test3() throws PersistitException {
        System.out.print("test3 ");
        final Exchange ex = _persistit.getExchange("persistit", "KeyFilter1",
                true);
        final Key key = ex.getKey();
        ex.removeAll();
        for (int i = 0; i < 100; i++) {
            ex.getValue().put("Value " + i);
            ex.to(i).store();
        }
        final KeyFilter.Term orTerm = KeyFilter.orTerm(new KeyFilter.Term[] {
                KeyFilter.rangeTerm(new Integer(10), new Integer(20), true,
                        false, null),
                KeyFilter.rangeTerm(new Integer(50), new Integer(60), true,
                        false, null),
                KeyFilter.rangeTerm(new Integer(80), new Integer(90), false,
                        true, null), });
        final KeyFilter kf = new KeyFilter().append(orTerm);

        ex.to(Key.BEFORE);
        boolean[] traversed = new boolean[100];

        while (ex.traverse(Key.GT, true)) {
            if (kf.selected(ex.getKey())) {
                final int k = key.reset().decodeInt();
                traversed[k] = true;
            } else {
                if (!kf.next(key, Key.GT)) {
                    break;
                }
            }
        }
        for (int k = 0; k < 100; k++) {
            final boolean expected = ((k >= 10) && (k < 20))
                    || ((k >= 50) && (k < 60)) || ((k > 80) && (k <= 90));
            assertEquals(expected, traversed[k]);
        }

        ex.to(Key.AFTER);
        traversed = new boolean[100];
        while (ex.traverse(Key.LT, true)) {
            if (kf.selected(ex.getKey())) {
                final int k = key.reset().decodeInt();
                traversed[k] = true;
            } else {
                if (!kf.next(key, Key.LT)) {
                    break;
                }
            }
        }
        for (int k = 0; k < 100; k++) {
            final boolean expected = ((k >= 10) && (k < 20))
                    || ((k >= 50) && (k < 60)) || ((k > 80) && (k <= 90));
            assertEquals(expected, traversed[k]);
        }
        System.out.println("- done");
    }

    @Test
    public void test4() throws PersistitException {
        System.out.print("test4 ");
        final TreeMap treeMap = new TreeMap();
        final Exchange ex = _persistit.getExchange("persistit", "KeyFilter1",
                true);
        ex.removeAll();

        final Key key = ex.getKey();
        key.clear().append("atlantic");
        key.append((float) 1.3);
        final KeyFilter kf = new KeyFilter(key)
                .append(new KeyFilter.Term[] {
                        KeyFilter.rangeTerm("x", "z"),
                        KeyFilter.orTerm(new KeyFilter.Term[] {
                                KeyFilter.rangeTerm(new Integer(100),
                                        new Integer(150), true, false, null),
                                KeyFilter.rangeTerm(new Integer(200),
                                        new Integer(250), true, false, null),
                                KeyFilter.rangeTerm(new Integer(300),
                                        new Integer(350), true, false, null), }) })
                .limit(2, 5);

        final String s = kf.toString();
        final String t = "{\"atlantic\",<(float)1.3,[\"x\":\"z\"),{[100:150),[200:250),[300:350)},*<}";

        key.append("x");
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.append(125);
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.to(175);
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.to(200);
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.append("tom");
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.append("dick");
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.append("harry");
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.cut(3);

        key.to(249);
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.to(250);
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.to(299);
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.to(300);
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        key.to(350);
        if (kf.selected(key)) {
            treeMap.put(new KeyState(key), key.toString());
        }
        ex.getValue().put(key.toString());
        ex.store();

        TreeMap treeMapCopy = new TreeMap(treeMap);
        key.clear().append(Key.BEFORE);
        while (ex.traverse(Key.GT, true)) {
            final KeyState ks = new KeyState(key);
            if (kf.selected(key)) {
                assertEquals(key.toString(), treeMapCopy.get(ks));
                treeMapCopy.remove(ks);
            } else {
                assertEquals(null, treeMap.get(ks));
                if (!kf.next(key, Key.GT)) {
                    break;
                }

            }
        }
        assertEquals(0, treeMapCopy.size());

        treeMapCopy = new TreeMap(treeMap);
        key.clear().append(Key.AFTER);
        while (ex.traverse(key.LT, true)) {
            final KeyState ks = new KeyState(key);
            if (kf.selected(key)) {
                assertEquals(key.toString(), treeMapCopy.get(ks));
                treeMapCopy.remove(ks);
            } else {
                assertEquals(null, treeMap.get(ks));
                if (!kf.next(key, key.LT)) {
                    break;
                }
            }
        }
        assertEquals(0, treeMapCopy.size());
        System.out.println("- done");
    }

    @Test
    public void test5() throws PersistitException {
        System.out.print("test5 ");
        KeyFilter filter;
        filter = new KeyFilter("{:1}");
        filter = new KeyFilter("{ :1 }");
        filter = new KeyFilter("{1:}");
        filter = new KeyFilter("{ 1: }");
        filter = new KeyFilter("{\"id\", (long) 100:  }");
        filter = new KeyFilter("{\"id\", : (long) 200 }");
        System.out.println("- done");
    }

    @Test
    public void test6() throws PersistitException {
        System.out.print("test6 ");
        final Exchange ex = _persistit.getExchange("persistit", "KeyFilter1",
                true);
        ex.removeAll();
        final KeyFilter filter = new KeyFilter(
                "{2,*,4,(long)1004:(long)1007,3,>*<}");
        ex.append(2).append(1L).append(4).append(1006L).append(3)
                .append(1005007L);
        ex.getValue().put("test6");
        ex.store();
        ex.clear().append(Key.BEFORE);
        assertTrue(ex.traverse(Key.GTEQ, filter, 0));
        System.out.println("- done");
    }

    @Test
    public void test7() throws PersistitException {
        final KeyFilter filter = new KeyFilter("{\"byName\",\"foo\",>*<}");
        final Key key = new Key((Persistit) null);
        boolean result;

        key.clear().append(Key.BEFORE);
        result = filter.next(key, Key.GT);
        assertTrue(result);
        key.clear().append(Key.BEFORE);
        result = filter.next(key, Key.LT);
        assertFalse(result);

        key.clear().append(Key.AFTER);
        result = filter.next(key, Key.LT);
        assertTrue(result);
        key.clear().append(Key.AFTER);
        result = filter.next(key, Key.GT);
        assertFalse(result);

    }

    @Test
    public void test8() throws PersistitException {
        final KeyFilter filter = new KeyFilter("{1,*<}");
        final Key key = new Key((Persistit) null);
        key.append(Key.AFTER);
        assertFalse(filter.selected(key));
        assertTrue(filter.next(key, Key.LT));
        assertEquals("{1}+", key.toString());
    }

    @Test
    public void test9() throws PersistitException {
        final KeyFilter filter = new KeyFilter("{1:2}");
        final Key key = new Key((Persistit) null);
        final Key key2 = new Key((Persistit) null);
        key.append(Key.AFTER);
        assertFalse(filter.selected(key));
        assertTrue(filter.next(key, Key.LT));
//        key2.append(2).nudgeDeeper();
//        assertTrue(filter.selected(key));
//        assertTrue(key.compareTo(key2) == 0);
        // assertFalse(filter.traverse(key, false));
    }
    
    @Test
    public void test10() throws PersistitException {
//        final KeyFilter filter = new KeyFilter("{[3:8]<}");
//        final Key key = new Key((Persistit) null);
//        assertTrue(filter.traverse(key, true));
//        assertEquals("{3}-", key.toString());
//        assertTrue(filter.traverse(key, true));
//        assertTrue(filter.selected(key));
//        assertEquals("{3}+", key.toString());
//        assertTrue(filter.traverse(key, true));
//        assertTrue(filter.selected(key));
//        key.clear().append(4);
//        assertTrue(filter.traverse(key, true));
//        assertEquals("{4}+", key.toString());
//        assertTrue(filter.selected(key));
//        key.clear().append(8);
//        assertFalse(filter.traverse(key, true));
    }

    public static void main(final String[] args) throws Exception {
        new KeyFilterTest1().initAndRunTest();
    }

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
    }

}
