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

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import com.persistit.Exchange;
import com.persistit.PersistitUnitTestCase;
import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.ValueCoder;
import com.persistit.encoding.ValueDisplayer;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;

public class ValueCoderTest1 extends PersistitUnitTestCase {

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
        final Value value = _exchange.getValue();
        final TreeMap map1 = new TreeMap();
        for (int i = 0; i < 10; i++) {
            map1.put(new Integer(i), Integer.toString(i));
        }
        value.put(map1);
        final TreeMap map2 = (TreeMap) value.get();
        assertEquals(map1, map2);
        final String toString = value.toString();
        assertEquals("(java.util.TreeMap)[" + "(Integer)0->\"0\",(Integer)1->\"1\",(Integer)2->\"2\","
                + "(Integer)3->\"3\",(Integer)4->\"4\",(Integer)5->\"5\","
                + "(Integer)6->\"6\",(Integer)7->\"7\",(Integer)8->\"8\"," + "(Integer)9->\"9\"]", toString);
        System.out.println("- done");
    }

    @Test
    public void test2() {
        System.out.print("test2 ");
        final Value value = _exchange.getValue();

        final HashMap map1 = new HashMap();
        for (int i = 0; i < 10; i++) {
            map1.put(new Integer(i), Integer.toString(i));
        }
        value.put(map1);
        final HashMap map2 = (HashMap) value.get();
        assertEquals(map1, map2);

        System.out.println("- done");
    }

    @Test
    public void test3() {
        System.out.print("test3 ");
        final Value value = _exchange.getValue();

        final HashMap map1 = new HashMap();
        for (int i = 0; i < 10; i++) {
            map1.put(new Integer(i), Integer.toString(i));
        }
        value.put(map1);
        final TreeMap map2 = new TreeMap();
        value.get(map2);

        final HashMap map3 = new HashMap(map2);
        assertEquals(map1, map3);

        System.out.println("- done");
    }

    @Test
    public void test4() {
        System.out.print("test4 ");
        final Value value = _exchange.getValue();

        final Set set1 = new TreeSet();
        for (int i = 0; i < 10; i++) {
            set1.add(new Integer(i));
        }
        value.put(set1);
        final Set set2 = (Set) value.get();
        assertEquals(set1, set2);
        final String toString = value.toString();
        assertEquals("(java.util.TreeSet)[(Integer)0,(Integer)1,(Integer)2,(Integer)3,(Integer)4,"
                + "(Integer)5,(Integer)6,(Integer)7,(Integer)8,(Integer)9]", toString);
        System.out.println("- done");
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

    private static class MapValueRenderer implements ValueRenderer, ValueDisplayer {
        Class _clazz;

        MapValueRenderer(final Class clazz) {
            _clazz = clazz;
        }

        @Override
        public void put(final Value value, final Object object, final CoderContext context) throws ConversionException {
            final Map map = (Map) object;

            value.put(map.size());

            for (final Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
                final Map.Entry entry = (Map.Entry) iter.next();
                value.put(entry.getKey());
                value.put(entry.getValue());
            }
        }

        @Override
        public Object get(final Value value, final Class clazz, final CoderContext context) throws ConversionException {
            Map map = null;
            try {
                map = (Map) _clazz.newInstance();
                final int size = value.getInt();
                for (int index = 0; index < size; index++) {
                    final Object k = value.get();
                    final Object v = value.get();
                    map.put(k, v);
                }
            } catch (final Exception e) {
                throw new ConversionException(e);
            }
            return map;
        }

        @Override
        public void render(final Value value, final Object target, final Class clazz, final CoderContext context)
                throws ConversionException {
            final Map map = (Map) target;
            final int size = value.getInt();
            for (int index = 0; index < size; index++) {
                final Object k = value.get();
                final Object v = value.get();
                map.put(k, v);
            }
        }

        @Override
        public void display(final Value value, final StringBuilder sb, final Class clazz, final CoderContext context) {
            final int size = value.getInt();
            sb.append("[");
            for (int index = 0; index < size; index++) {
                if (index > 0) {
                    sb.append(",");
                }
                value.decodeDisplayable(true, sb, context);
                sb.append("->");
                value.decodeDisplayable(true, sb, context);
            }
            sb.append("]");
        }
    }

    static class TreeSetValueCoder implements ValueCoder {
        public TreeSetValueCoder() {
        }

        @Override
        public Object get(final Value value, final Class clazz, final CoderContext context) throws ConversionException {
            Set set = null;
            try {
                set = (Set) clazz.newInstance();
            } catch (final InstantiationException ex) {
                throw new ConversionException(ex);
            } catch (final IllegalAccessException ex) {
                throw new ConversionException(ex);
            }

            // value.setStreamMode(true);
            final int size = value.getInt();
            for (int i = 0; i < size; i++) {
                set.add(value.get());
            }
            return set;
        }

        @Override
        public void put(final Value value, final Object object, final CoderContext context) {
            final Set set = (Set) object;
            // value.setStreamMode(true);
            value.put(set.size());
            for (final Iterator iterObject = set.iterator(); iterObject.hasNext();) {
                final Object curObject = iterObject.next();
                value.put(curObject);
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        new ValueCoderTest1().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        try {
            _persistit.getCoderManager().registerValueCoder(TreeMap.class, new MapValueRenderer(TreeMap.class));

            _persistit.getCoderManager().registerValueCoder(HashMap.class, new MapValueRenderer(HashMap.class));

            _persistit.getCoderManager().registerValueCoder(TreeSet.class, new TreeSetValueCoder());

            _persistit.getCoderManager().registerValueCoder(Set.class, new TreeSetValueCoder());

            _exchange = _persistit.getExchange("persistit", "ValueCoderTest1", true);

            test1();
            test2();
            test3();
            test4();

        } finally {
            _persistit.getCoderManager().unregisterValueCoder(TreeMap.class);

            _persistit.getCoderManager().unregisterValueCoder(HashMap.class);
        }
    }

}
