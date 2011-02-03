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

import java.util.Date;
import java.util.Properties;

import junit.framework.AssertionFailedError;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

public class TransactionTest3 extends PersistitUnitTestCase {

    public void test1() throws PersistitException {
        System.out.print("test1 ");

        final Exchange ex = _persistit.getExchange("persistit",
                "TransactionTest3", true);
        ex.clear().append("test1").remove(Key.GTEQ);

        final Transaction txn = ex.getTransaction();
        final int[] expectedKeys = new int[] { 0, 1, 2, 4, 5, 7, 8 };

        txn.begin();
        try {
            for (int i = 0; i < 10; i++) {
                ex.getValue().put("String value #" + i + " for test1");
                ex.clear().append("test1").append(i).store();
            }
            for (int i = 3; i < 10; i += 3) {
                final boolean removed = ex.clear().append("test1").append(i)
                        .remove(Key.GTEQ);
                assertTrue(removed);
            }

            for (int i = -1; i < 12; i++) {
                ex.clear().append("test1").append(i).fetch();
                if ((i < 0) || (i >= 10) || ((i != 0) && ((i % 3) == 0))) {
                    assertTrue(!ex.getValue().isDefined());
                } else {
                    assertTrue(ex.getValue().isDefined());
                    assertEquals(ex.getValue().get(), "String value #" + i
                            + " for test1");
                }
            }

            ex.clear().append("test1");
            for (int index = 0; ex.traverse(Key.GT, true); index++) {
                assertTrue(index < expectedKeys.length);
                final int key = ex.getKey().reset().indexTo(1).decodeInt();
                assertEquals(expectedKeys[index], key);
            }

            txn.commit();
        } finally {
            txn.end();
        }
        for (int i = -1; i < 12; i++) {
            ex.clear().append("test1").append(i).fetch();
            if ((i < 0) || (i >= 10) || ((i != 0) && ((i % 3) == 0))) {
                assertTrue(!ex.getValue().isDefined());
            } else {
                assertTrue(ex.getValue().isDefined());
                assertEquals(ex.getValue().get(), "String value #" + i
                        + " for test1");
            }
        }

        ex.clear();
        for (int index = 0; ex.traverse(Key.GT, true); index++) {
            assertTrue(index < expectedKeys.length);
            final int key = ex.getKey().reset().indexTo(1).decodeInt();
            assertEquals(expectedKeys[index], key);
        }

        System.out.println("- done");
    }

    public void test2() throws PersistitException {
        System.out.print("test2 ");
        final Exchange ex = _persistit.getExchange("persistit",
                "TransactionTest3", true);
        ex.clear().append("test2").remove(Key.GTEQ);
        final String[] expectedKeys = { "{\"test2\",\"a\",0}",
                "{\"test2\",\"a\",1}", "{\"test2\",\"a\",2}",
                "{\"test2\",\"a\",3}", "{\"test2\",\"a\",4}",
                "{\"test2\",\"a\",5}", "{\"test2\",\"a\",7}",
                "{\"test2\",\"a\",8}", "{\"test2\",\"b\",0}",
                "{\"test2\",\"b\",1}", "{\"test2\",\"b\",2}",
                "{\"test2\",\"b\",4}", "{\"test2\",\"b\",5}",
                "{\"test2\",\"b\",6}", "{\"test2\",\"b\",7}",
                "{\"test2\",\"b\",8}", };

        for (int i = 0; i < 10; i++) {
            ex.getValue().put("String value #" + i + " for test2");
            ex.clear().append("test2").append("a").append(i).store();
            ex.clear().append("test2").append("b").append(i).store();
        }

        final Transaction txn = ex.getTransaction();
        txn.begin();
        try {
            for (int i = 3; i < 10; i += 3) {
                ex.clear().append("test2").append("a").append(i)
                        .remove(Key.GTEQ);
                ex.clear().append("test2").append("b").append(i)
                        .remove(Key.GTEQ);
            }

            ex.getValue().put("String value #" + 3 + " for test2 - pending");
            ex.clear().append("test2").append("a").append(3).store();
            ex.getValue().put("String value #" + 6 + " for test2 - pending");
            ex.clear().append("test2").append("b").append(6).store();

            ex.clear().append("test2");
            for (int index = 0; ex.traverse(Key.GT, true)
                    && (index < expectedKeys.length); index++) {
                assertEquals(expectedKeys[index], ex.getKey().toString());
            }

            final boolean removed1 = ex.clear().append("test2").append("a")
                    .remove(Key.GTEQ);
            assertTrue(removed1);

            boolean removed2 = ex.clear().append("test2").append("c")
                    .remove(Key.GTEQ);
            assertTrue(!removed2);

            ex.clear().append("test2");
            for (int index = 8; ex.traverse(Key.GT, true)
                    && (index < expectedKeys.length); index++) {
                assertEquals(expectedKeys[index], ex.getKey().toString());
            }

            txn.commit();
        } catch (final AssertionFailedError e) {
            System.out.println("Assertion failed: " + e);
            e.printStackTrace();
        } finally {
            txn.end();
        }

        ex.clear().append("test2");
        for (int index = 8; ex.traverse(Key.GT, true)
                && (index < expectedKeys.length); index++) {
            assertEquals(expectedKeys[index], ex.getKey().toString());
        }

        System.out.println("- done");
    }

    public void test3() throws PersistitException {
        System.out.print("test3 ");
        final Exchange ex = _persistit.getExchange("persistit",
                "TransactionTest3", true);
        ex.append("test3").remove(Key.GT);
        final StringBuilder sb = new StringBuilder(20000);
        for (int i = 0; i < 20000; i++) {
            sb.append("1");
        }
        ex.getValue().putString(sb);
        ex.clear().append("test3").append("a").store();
        ex.clear().append("test3").append("b").store();
        ex.clear().append("test3").append("c").store();
        sb.setLength(0);

        for (int i = 0; i < 20000; i++) {
            sb.append("2");
        }

        ex.getTransaction().begin();
        try {
            ex.getValue().put(Boolean.FALSE);
            assertTrue(ex.clear().append("test3").append("a").hasNext());
            assertEquals(Boolean.FALSE, ex.getValue().get());
            assertTrue(ex.clear().append("test3").append("b").hasNext());
            assertEquals(Boolean.FALSE, ex.getValue().get());
            assertTrue(!ex.clear().append("test3").append("c").hasNext());
            assertEquals(Boolean.FALSE, ex.getValue().get());

            ex.getValue().putString(sb);
            ex.clear().append("test3").append("d").store();
            ex.getValue().put(Boolean.TRUE);

            assertTrue(ex.clear().append("test3").append("c").hasNext());
            assertEquals(Boolean.TRUE, ex.getValue().get());

            ex.clear().append("test3").append("b").remove(Key.GT);
            ex.fetch(201);
            final String s = ex.getValue().getString();
            assertTrue(s.length() >= 200);
            assertTrue(s.startsWith("111111111111"));

            ex.getValue().putString(sb);
            ex.clear().append("test3").append("b").remove(Key.EQ);

            ex.clear().append("test3").append("a");
            ex.getValue().put(new Date());

            assertTrue(ex.traverse(Key.GT, false, 0));
            assertEquals("{\"test3\",\"c\"}", ex.getKey().toString());
            assertTrue(ex.getValue().get() instanceof Date);

            assertTrue(ex.traverse(Key.GT, false, 0));
            assertEquals("{\"test3\",\"d\"}", ex.getKey().toString());
            assertTrue(ex.getValue().get() instanceof Date);

            ex.clear().append("test3").append("bz");
            assertTrue(ex.traverse(Key.GT, false, 0));
            assertEquals("{\"test3\",\"c\"}", ex.getKey().toString());
            assertTrue(ex.getValue().get() instanceof Date);

            ex.clear().append("test3").append("b");
            ex.fetch();
            assertTrue(!ex.getValue().isDefined());

            ex.getValue().putString(sb);
            ex.clear().append("test3").append("aa").store();
            ex.clear().append("test3").append("b").store();
            ex.clear().append("test3").append("bb").store();

            ex.cut().append(Key.BEFORE);
            assertTrue(ex.next());
            assertEquals("{\"test3\",\"a\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("1111111111"));

            assertTrue(ex.next());
            assertEquals("{\"test3\",\"aa\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("2222222222"));

            assertTrue(ex.next());
            assertEquals("{\"test3\",\"b\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("2222222222"));

            assertTrue(ex.next());
            assertEquals("{\"test3\",\"bb\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("2222222222"));

            assertTrue(ex.next());
            assertEquals("{\"test3\",\"c\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("11111111111"));

            assertTrue(ex.next());
            assertEquals("{\"test3\",\"d\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("2222222222"));

            assertTrue(!ex.next());
            assertTrue(!ex.getValue().isDefined());

            ex.cut().append(Key.AFTER);

            assertTrue(ex.previous());
            assertEquals("{\"test3\",\"d\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("2222222222"));

            assertTrue(ex.previous());
            assertEquals("{\"test3\",\"c\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("11111111111"));

            assertTrue(ex.previous());
            assertEquals("{\"test3\",\"bb\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("2222222222"));

            assertTrue(ex.previous());
            assertEquals("{\"test3\",\"b\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("2222222222"));

            assertTrue(ex.previous());
            assertEquals("{\"test3\",\"aa\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("2222222222"));

            assertTrue(ex.previous());
            assertEquals("{\"test3\",\"a\"}", ex.getKey().toString());
            assertTrue(ex.getValue().getString().startsWith("1111111111"));

            assertTrue(!ex.previous());
            assertTrue(!ex.getValue().isDefined());

            ex.getTransaction().commit();

        } finally {
            ex.getTransaction().end();
        }
        System.out.println("- done");
    }

    public void test4() throws PersistitException {
        System.out.print("test4 ");
        final Exchange ex = _persistit.getExchange("persistit",
                "TransactionTest3", true);
        ex.clear().append("test4").remove(Key.GTEQ);

        final String[] expectedKeys = { "{\"test4\",\"a\",0}",
                "{\"test4\",\"a\",1}", "{\"test4\",\"a\",2}",
                "{\"test4\",\"a\",3}", "{\"test4\",\"a\",4}",
                "{\"test4\",\"a\",5}", "{\"test4\",\"a\",6}",
                "{\"test4\",\"a\",7}", "{\"test4\",\"a\",8}",
                "{\"test4\",\"a\",9}", "{\"test4\",\"b\",0}",
                "{\"test4\",\"b\",1}", "{\"test4\",\"b\",2}",
                "{\"test4\",\"b\",3}", "{\"test4\",\"b\",4}",
                "{\"test4\",\"c\",5}", "{\"test4\",\"c\",6}",
                "{\"test4\",\"c\",7}", "{\"test4\",\"c\",8}",
                "{\"test4\",\"c\",9}", "{\"test4\",\"d\",0}",
                "{\"test4\",\"d\",1}", "{\"test4\",\"d\",7}",
                "{\"test4\",\"d\",8}", "{\"test4\",\"d\",9}",
                "{\"test4\",\"e\"}", };

        for (int i = 0; i < 10; i++) {
            ex.getValue().put("String value #" + i + " for test4");
            ex.clear().append("test4").append("b").append(i).store();
            ex.clear().append("test4").append("d").append(i).store();
        }

        final Transaction txn = ex.getTransaction();
        txn.begin();
        try {
            for (int i = 0; i < 10; i++) {
                ex.getValue().put("String value #" + i + " for test4");
                ex.clear().append("test4").append("a").append(i).store();
                ex.clear().append("test4").append("c").append(i).store();
                ex.clear().append("test4").append("e").append(i).store();
            }
            final Key key1 = ex.getKey();
            Key key2;

            ex.clear().append("test4").append("c").append(5);
            key2 = new Key(ex.getKey());
            ex.clear().append("test4").append("b").append(5);
            ex.removeKeyRange(key1, key2);

            ex.clear().append("test4").append("d").append(5);
            key2 = new Key(ex.getKey());
            ex.clear().append("test4").append("d").append(2);
            ex.removeKeyRange(key1, key2);

            ex.clear().append("test4").append("d").append(7);
            key2 = new Key(ex.getKey());
            ex.clear().append("test4").append("d").append(4);
            ex.removeKeyRange(key1, key2);

            ex.clear().append("test4").append("e").append(2);
            key2 = new Key(ex.getKey());
            ex.clear().append("test4").append("e").append(1);
            ex.removeKeyRange(key1, key2);

            ex.clear().append("test4").append("e").append(6);
            key2 = new Key(ex.getKey());
            ex.clear().append("test4").append("e").append(4);
            ex.removeKeyRange(key1, key2);

            ex.clear().append("test4").append("e").append(5);
            key2 = new Key(ex.getKey());
            ex.clear().append("test4").append("e").append(3);
            ex.removeKeyRange(key1, key2);

            ex.clear().append("test4").append("e").append(7);
            key2 = new Key(ex.getKey());
            ex.clear().append("test4").append("e").append(5);
            ex.removeKeyRange(key1, key2);

            ex.getValue().put("test");
            ex.clear().append("test4").append("e").store();
            ex.remove(Key.GT);

            ex.clear().append("test4").append(Key.BEFORE);
            for (int index = 0; ex.traverse(Key.GT, true)
                    && ex.getKey().reset().decode().equals("test4"); index++) {
                assertEquals(expectedKeys[index], ex.getKey().toString());
            }

            ex.clear().append("test4").append(Key.AFTER);
            for (int index = expectedKeys.length - 1; ex.traverse(Key.LT, true)
                    && ex.getKey().reset().decode().equals("test4"); index--) {
                assertEquals(expectedKeys[index], ex.getKey().toString());
            }

            txn.commit();

        } catch (final AssertionFailedError e) {
            System.out.println("Assertion failed: " + e);
            e.printStackTrace();
        } finally {
            txn.end();
        }

        ex.clear().append("test4").append(Key.BEFORE);
        for (int index = 0; ex.traverse(Key.GT, true)
                && ex.getKey().reset().decode().equals("test4"); index++) {
            assertEquals(expectedKeys[index], ex.getKey().toString());
        }

        ex.clear().append("test4").append(Key.AFTER);
        for (int index = expectedKeys.length - 1; ex.traverse(Key.LT, true)
                && ex.getKey().reset().decode().equals("test4"); index--) {
            assertEquals(expectedKeys[index], ex.getKey().toString());
        }

        System.out.println("- done");
    }

    public void test5() throws PersistitException {
        System.out.print("test5 ");
        final Exchange ex = _persistit.getExchange("persistit",
                "TransactionTest3", true);
        final String[] values = new String[100];
        ex.clear().append("test5").remove(Key.GTEQ);
        for (int i = 0; i < 100; i += 2) {
            ex.getValue().put("a." + i);
            ex.clear().append("test5").append(i).store();
            values[i] = ex.getValue().getString();
        }
        ex.getTransaction().begin();
        try {
            for (int i = 0; i < 100; i += 5) {
                ex.clear().append("test5").append(i).remove();
                values[i] = null;
            }

            for (int i = 0; i < 100; i += 8) {
                ex.getValue().put("b." + i);
                ex.clear().append("test5").append(i).store();
                values[i] = ex.getValue().getString();
            }

            for (int i = 0; i < 100; i += 16) {
                ex.clear().append("test5").append(i).remove();
                values[i] = null;
            }

            for (int i = 0; i < 100; i += 32) {
                ex.getValue().put("c." + i);
                ex.clear().append("test5").append(i).store();
                values[i] = ex.getValue().getString();
            }

            for (int i = -1; i < 101; i++) {
                ex.clear().append("test5").append(i).fetch();
                if ((i < 0) || (i >= 100) || (values[i] == null)) {
                    assertTrue(!ex.getValue().isDefined());
                } else {
                    assertTrue(ex.getValue().isDefined());
                    assertEquals(values[i], ex.getValue().get());
                }
            }

            ex.clear().append("test5").append(Key.BEFORE);
            for (int index = 0; ex.traverse(Key.GT, true)
                    && ex.getKey().reset().decode().equals("test5"); index++) {
                for (; (index < values.length) && (values[index] == null); index++) {
                }
                final int k = ex.getKey().indexTo(-1).decodeInt();
                assertEquals(index, k);
                assertEquals(values[index], ex.getValue().getString());
            }

            ex.getTransaction().commit();

        } finally {
            ex.getTransaction().end();
        }

        ex.clear().append("test5").append(Key.BEFORE);
        for (int index = 0; ex.traverse(Key.GT, true)
                && ex.getKey().reset().decode().equals("test5"); index++) {
            for (; (index < values.length) && (values[index] == null); index++) {
            }
            final int k = ex.getKey().indexTo(-1).decodeInt();
            assertEquals(index, k);
            assertTrue(ex.getValue().getString().equals(values[index]));
        }

        System.out.println("- done");
    }

    @Override
    public Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getBiggerProperties(cleanup);
    }

    public static void main(final String[] args) throws Exception {
        new TransactionTest3().initAndRunTest();
    }

    public void runAllTests() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit",
                "TransactionTest3", true);
        ex.removeAll();
        test1();
        test2();
        test3();
        test4();
        test5();
    }
}
